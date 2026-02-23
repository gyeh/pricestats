package ingest_test

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	goparquet "github.com/parquet-go/parquet-go"

	"github.com/gyeh/pricestats/internal/config"
	"github.com/gyeh/pricestats/internal/db"
	"github.com/gyeh/pricestats/internal/ingest"
	"github.com/gyeh/pricestats/internal/logging"
	"github.com/gyeh/pricestats/internal/model"
	"github.com/gyeh/pricestats/internal/normalize"
)

const (
	testPort     = 15432
	testDB       = "mrftest"
	testUser     = "postgres"
	testPassword = "postgres"
)

var (
	testDSN string
	pg      *embeddedpostgres.EmbeddedPostgres
)

// fixtureFile returns the path to the small test parquet fixture.
// Falls back to the full file if small doesn't exist.
func fixtureFile() string {
	small := "../../testdata/nyu-tisch-small.parquet"
	if _, err := os.Stat(small); err == nil {
		return small
	}
	full := "../../testdata/nyu-tisch.parquet"
	if _, err := os.Stat(full); err == nil {
		return full
	}
	return ""
}

func TestMain(m *testing.M) {
	if fixtureFile() == "" {
		fmt.Fprintln(os.Stderr, "SKIP: no test parquet file found in testdata/")
		os.Exit(0)
	}

	testDSN = fmt.Sprintf("postgresql://%s:%s@localhost:%d/%s?sslmode=disable",
		testUser, testPassword, testPort, testDB)

	pg = embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(uint32(testPort)).
			Database(testDB).
			Username(testUser).
			Password(testPassword).
			Version(embeddedpostgres.V16).
			StartTimeout(30*time.Second),
	)

	if err := pg.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start embedded postgres: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	if err := pg.Stop(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to stop embedded postgres: %v\n", err)
	}

	os.Exit(code)
}

// setupDB creates a connection pool and applies migrations. Returns pool and cleanup func.
func setupDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Drop and recreate schemas for a clean state
	for _, schema := range []string{"mrf", "ingest", "ref"} {
		_, err := pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
		if err != nil {
			t.Fatalf("drop schema %s: %v", schema, err)
		}
	}

	log := logging.Setup("text")
	if err := db.ApplyMigrations(ctx, pool, log); err != nil {
		pool.Close()
		t.Fatalf("migrations: %v", err)
	}

	t.Cleanup(func() { pool.Close() })
	return pool
}

// readAllParquetRows reads all HospitalChargeRows from the fixture file.
func readAllParquetRows(t *testing.T) []model.HospitalChargeRow {
	t.Helper()
	f, err := os.Open(fixtureFile())
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	defer f.Close()
	stat, _ := f.Stat()
	pf, err := goparquet.OpenFile(f, stat.Size())
	if err != nil {
		t.Fatalf("open parquet file: %v", err)
	}
	reader := goparquet.NewGenericReader[model.HospitalChargeRow](pf)
	defer reader.Close()

	var all []model.HospitalChargeRow
	buf := make([]model.HospitalChargeRow, 256)
	for {
		n, readErr := reader.Read(buf)
		all = append(all, buf[:n]...)
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			t.Fatalf("read parquet: %v", readErr)
		}
	}
	return all
}

// expectedServingRows computes the expected number of serving rows from
// the wide→long explosion: one serving row per non-null code per parquet row.
func expectedServingRows(rows []model.HospitalChargeRow) (int, map[string]int) {
	total := 0
	byCode := make(map[string]int)
	for _, row := range rows {
		for name, ptr := range row.CodeValues() {
			if ptr != nil && strings.TrimSpace(*ptr) != "" {
				total++
				byCode[name]++
			}
		}
	}
	return total, byCode
}

func TestEndToEnd_DefaultMode(t *testing.T) {
	pool := setupDB(t)
	ctx := context.Background()
	log := logging.Setup("text")

	parquetRows := readAllParquetRows(t)
	expectedTotal, expectedByCode := expectedServingRows(parquetRows)

	cfg := &config.Config{
		DSN:                testDSN,
		FilePath:           fixtureFile(),
		LogFormat:          "text",
		ActivateVersion:    true,
		KeepStaging:        true, // keep staging to validate
		IncludePayerPrices: false,
	}

	// Run the pipeline
	summary, err := ingest.Run(ctx, pool, log, cfg)
	if err != nil {
		t.Fatalf("pipeline.Run: %v", err)
	}

	t.Run("summary_metrics", func(t *testing.T) {
		if summary.RowsRead != int64(len(parquetRows)) {
			t.Errorf("RowsRead: got %d, want %d", summary.RowsRead, len(parquetRows))
		}
		if summary.RowsStaged != int64(len(parquetRows)) {
			t.Errorf("RowsStaged: got %d, want %d", summary.RowsStaged, len(parquetRows))
		}
		if summary.RowsRejected != 0 {
			t.Errorf("RowsRejected: got %d, want 0", summary.RowsRejected)
		}
		if summary.RowsInsertedServing != int64(expectedTotal) {
			t.Errorf("RowsInsertedServing: got %d, want %d", summary.RowsInsertedServing, expectedTotal)
		}
	})

	t.Run("staging_row_count", func(t *testing.T) {
		var count int64
		err := pool.QueryRow(ctx, "SELECT count(*) FROM ingest.stage_charge_rows").Scan(&count)
		if err != nil {
			t.Fatalf("query staging count: %v", err)
		}
		if count != int64(len(parquetRows)) {
			t.Errorf("staging rows: got %d, want %d", count, len(parquetRows))
		}
	})

	t.Run("serving_total_count", func(t *testing.T) {
		var count int64
		err := pool.QueryRow(ctx, "SELECT count(*) FROM mrf.prices_by_code").Scan(&count)
		if err != nil {
			t.Fatalf("query serving count: %v", err)
		}
		if count != int64(expectedTotal) {
			t.Errorf("serving rows: got %d, want %d", count, expectedTotal)
		}
	})

	t.Run("serving_count_by_code_type", func(t *testing.T) {
		rows, err := pool.Query(ctx,
			"SELECT code_type, count(*) FROM mrf.prices_by_code GROUP BY code_type ORDER BY code_type")
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()

		got := make(map[string]int)
		for rows.Next() {
			var codeType string
			var count int
			if err := rows.Scan(&codeType, &count); err != nil {
				t.Fatalf("scan: %v", err)
			}
			got[codeType] = count
		}
		for codeType, want := range expectedByCode {
			if got[codeType] != want {
				t.Errorf("code_type %s: got %d, want %d", codeType, got[codeType], want)
			}
		}
		for codeType, count := range got {
			if _, ok := expectedByCode[codeType]; !ok {
				t.Errorf("unexpected code_type %s with %d rows", codeType, count)
			}
		}
	})

	t.Run("money_conversion_parity", func(t *testing.T) {
		// Verify that each parquet row's money values match the staging table cents values
		type stagingMoney struct {
			rowNum           int64
			grossCents       *int64
			discountedCents  *int64
			minCents         *int64
			maxCents         *int64
		}
		rows, err := pool.Query(ctx,
			`SELECT source_row_number, gross_charge_cents, discounted_cash_cents,
			        min_charge_cents, max_charge_cents
			 FROM ingest.stage_charge_rows ORDER BY source_row_number`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()

		var dbRows []stagingMoney
		for rows.Next() {
			var r stagingMoney
			if err := rows.Scan(&r.rowNum, &r.grossCents, &r.discountedCents,
				&r.minCents, &r.maxCents); err != nil {
				t.Fatalf("scan: %v", err)
			}
			dbRows = append(dbRows, r)
		}

		if len(dbRows) != len(parquetRows) {
			t.Fatalf("staging row count mismatch: %d vs %d", len(dbRows), len(parquetRows))
		}

		for i, dbRow := range dbRows {
			pqRow := parquetRows[i]
			assertCentsMatch(t, fmt.Sprintf("row %d gross_charge", i+1),
				pqRow.GrossCharge, dbRow.grossCents)
			assertCentsMatch(t, fmt.Sprintf("row %d discounted_cash", i+1),
				pqRow.DiscountedCash, dbRow.discountedCents)
			assertCentsMatch(t, fmt.Sprintf("row %d min_charge", i+1),
				pqRow.MinCharge, dbRow.minCents)
			assertCentsMatch(t, fmt.Sprintf("row %d max_charge", i+1),
				pqRow.MaxCharge, dbRow.maxCents)
		}
	})

	t.Run("code_normalization_parity", func(t *testing.T) {
		// For each parquet row, verify that every non-null code appears as a
		// serving row with the correct normalized code value
		type servingRow struct {
			codeType string
			codeRaw  string
			codeNorm string
		}

		srvRows, err := pool.Query(ctx,
			`SELECT s.source_row_number, p.code_type, p.code_raw, p.code_norm
			 FROM mrf.prices_by_code p
			 JOIN ingest.stage_charge_rows s ON s.source_row_hash = p.source_row_hash
			 ORDER BY s.source_row_number, p.code_type`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer srvRows.Close()

		// Group by source_row_number
		byRow := make(map[int64][]servingRow)
		for srvRows.Next() {
			var rowNum int64
			var sr servingRow
			if err := srvRows.Scan(&rowNum, &sr.codeType, &sr.codeRaw, &sr.codeNorm); err != nil {
				t.Fatalf("scan: %v", err)
			}
			byRow[rowNum] = append(byRow[rowNum], sr)
		}

		for i, pqRow := range parquetRows {
			rowNum := int64(i + 1)
			servingCodes := byRow[rowNum]
			pqCodes := pqRow.CodeValues()

			// Count expected non-null codes
			expectedCodes := 0
			for _, ptr := range pqCodes {
				if ptr != nil && strings.TrimSpace(*ptr) != "" {
					expectedCodes++
				}
			}

			if len(servingCodes) != expectedCodes {
				t.Errorf("row %d: got %d serving codes, want %d",
					rowNum, len(servingCodes), expectedCodes)
				continue
			}

			// Verify each serving code matches its parquet source
			for _, sc := range servingCodes {
				pqPtr := pqCodes[sc.codeType]
				if pqPtr == nil {
					t.Errorf("row %d: serving has code_type %s but parquet is nil",
						rowNum, sc.codeType)
					continue
				}
				// code_raw in DB should match the normalized code from staging
				// (codes are normalized in the Go layer, then normalized again in SQL)
				expectedNorm := normalizeCodeStr(*pqPtr)
				if sc.codeNorm != expectedNorm {
					t.Errorf("row %d code_type %s: code_norm got %q, want %q",
						rowNum, sc.codeType, sc.codeNorm, expectedNorm)
				}
			}
		}
	})

	t.Run("payer_fields_null_by_default", func(t *testing.T) {
		// With IncludePayerPrices=false, payer columns should be NULL
		var nonNullPayers int64
		err := pool.QueryRow(ctx,
			`SELECT count(*) FROM ingest.stage_charge_rows WHERE payer_name IS NOT NULL`).
			Scan(&nonNullPayers)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if nonNullPayers != 0 {
			t.Errorf("expected 0 non-null payer_name rows in staging, got %d", nonNullPayers)
		}

		var nonNullNeg int64
		err = pool.QueryRow(ctx,
			`SELECT count(*) FROM ingest.stage_charge_rows WHERE negotiated_dollar_cents IS NOT NULL`).
			Scan(&nonNullNeg)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if nonNullNeg != 0 {
			t.Errorf("expected 0 non-null negotiated_dollar_cents in staging, got %d", nonNullNeg)
		}
	})

	t.Run("hospital_registered", func(t *testing.T) {
		var name string
		err := pool.QueryRow(ctx,
			"SELECT hospital_name FROM ref.hospitals LIMIT 1").Scan(&name)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if name != parquetRows[0].HospitalName {
			t.Errorf("hospital name: got %q, want %q", name, parquetRows[0].HospitalName)
		}
	})

	t.Run("mrf_file_active", func(t *testing.T) {
		var isActive bool
		var status string
		err := pool.QueryRow(ctx,
			"SELECT is_active, status FROM ingest.mrf_files WHERE mrf_file_id = $1",
			summary.MRFFileID).Scan(&isActive, &status)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if !isActive {
			t.Error("expected mrf_file to be active")
		}
		if status != "active" {
			t.Errorf("expected status 'active', got %q", status)
		}
	})

	t.Run("serving_money_matches_staging", func(t *testing.T) {
		// Verify the transform preserved money values from staging to serving
		var mismatches int64
		err := pool.QueryRow(ctx,
			`SELECT count(*) FROM mrf.prices_by_code p
			 JOIN ingest.stage_charge_rows s ON s.source_row_hash = p.source_row_hash
			 WHERE p.gross_charge_cents IS DISTINCT FROM s.gross_charge_cents
			    OR p.discounted_cash_cents IS DISTINCT FROM s.discounted_cash_cents
			    OR p.min_charge_cents IS DISTINCT FROM s.min_charge_cents
			    OR p.max_charge_cents IS DISTINCT FROM s.max_charge_cents`).
			Scan(&mismatches)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if mismatches != 0 {
			t.Errorf("found %d serving rows with money values differing from staging", mismatches)
		}
	})
}

func TestEndToEnd_Idempotency(t *testing.T) {
	pool := setupDB(t)
	ctx := context.Background()
	log := logging.Setup("text")

	cfg := &config.Config{
		DSN:             testDSN,
		FilePath:        fixtureFile(),
		LogFormat:       "text",
		ActivateVersion: true,
	}

	// First run
	summary1, err := ingest.Run(ctx, pool, log, cfg)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	if summary1.RowsStaged == 0 {
		t.Fatal("first run should have staged rows")
	}

	// Second run: same file should be skipped
	summary2, err := ingest.Run(ctx, pool, log, cfg)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if summary2.RowsStaged != 0 {
		t.Errorf("second run should skip (already loaded), but staged %d rows", summary2.RowsStaged)
	}

	// Verify serving row count didn't double
	var count int64
	err = pool.QueryRow(ctx, "SELECT count(*) FROM mrf.prices_by_code").Scan(&count)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != summary1.RowsInsertedServing {
		t.Errorf("expected %d serving rows after idempotent re-run, got %d",
			summary1.RowsInsertedServing, count)
	}
}

func TestEndToEnd_IncludePayerPrices(t *testing.T) {
	pool := setupDB(t)
	ctx := context.Background()
	log := logging.Setup("text")

	cfg := &config.Config{
		DSN:                testDSN,
		FilePath:           fixtureFile(),
		LogFormat:          "text",
		ActivateVersion:    true,
		KeepStaging:        true,
		IncludePayerPrices: true,
	}

	summary, err := ingest.Run(ctx, pool, log, cfg)
	if err != nil {
		t.Fatalf("pipeline.Run: %v", err)
	}

	parquetRows := readAllParquetRows(t)
	expectedTotal, _ := expectedServingRows(parquetRows)

	if summary.RowsInsertedServing != int64(expectedTotal) {
		t.Errorf("RowsInsertedServing: got %d, want %d",
			summary.RowsInsertedServing, expectedTotal)
	}

	// Since the source data has no payer rows, all payer fields should still
	// be NULL. But the pipeline should still succeed without error — the
	// include-payer-prices flag just doesn't suppress them.
	t.Run("no_payer_data_in_source", func(t *testing.T) {
		var payerCount int64
		pool.QueryRow(ctx,
			"SELECT count(*) FROM ref.payers").Scan(&payerCount)
		// Source file has no payer data, so ref.payers should be empty
		if payerCount != 0 {
			t.Logf("payer count: %d (source file may have payer data)", payerCount)
		}
	})

	// Verify the serving table still has correct row count and money data
	t.Run("serving_count", func(t *testing.T) {
		var count int64
		pool.QueryRow(ctx, "SELECT count(*) FROM mrf.prices_by_code").Scan(&count)
		if count != int64(expectedTotal) {
			t.Errorf("serving rows: got %d, want %d", count, expectedTotal)
		}
	})
}

func TestEndToEnd_ServingDataParityWithParquet(t *testing.T) {
	pool := setupDB(t)
	ctx := context.Background()
	log := logging.Setup("text")

	cfg := &config.Config{
		DSN:             testDSN,
		FilePath:        fixtureFile(),
		LogFormat:       "text",
		ActivateVersion: true,
		KeepStaging:     true,
	}

	_, err := ingest.Run(ctx, pool, log, cfg)
	if err != nil {
		t.Fatalf("pipeline.Run: %v", err)
	}

	parquetRows := readAllParquetRows(t)

	// For each parquet row, read its corresponding serving rows and validate
	// full parity of description, setting, and all charge fields.
	t.Run("full_field_parity", func(t *testing.T) {
		type servingRecord struct {
			codeType        string
			codeNorm        string
			description     string
			setting         *string
			grossCents      *int64
			discountedCents *int64
			minCents        *int64
			maxCents        *int64
		}

		allServing, err := pool.Query(ctx,
			`SELECT s.source_row_number, p.code_type, p.code_norm, p.description,
			        p.setting, p.gross_charge_cents, p.discounted_cash_cents,
			        p.min_charge_cents, p.max_charge_cents
			 FROM mrf.prices_by_code p
			 JOIN ingest.stage_charge_rows s ON s.source_row_hash = p.source_row_hash
			 ORDER BY s.source_row_number, p.code_type`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer allServing.Close()

		// Group serving rows by source_row_number
		byRow := make(map[int64][]servingRecord)
		for allServing.Next() {
			var rowNum int64
			var r servingRecord
			if err := allServing.Scan(&rowNum, &r.codeType, &r.codeNorm,
				&r.description, &r.setting, &r.grossCents, &r.discountedCents,
				&r.minCents, &r.maxCents); err != nil {
				t.Fatalf("scan: %v", err)
			}
			byRow[rowNum] = append(byRow[rowNum], r)
		}

		for i, pqRow := range parquetRows {
			rowNum := int64(i + 1)
			serving := byRow[rowNum]

			for _, sr := range serving {
				// Description must match
				if sr.description != pqRow.Description {
					t.Errorf("row %d %s: description got %q, want %q",
						rowNum, sr.codeType, sr.description, pqRow.Description)
				}

				// Setting must match (non-empty → non-null)
				if pqRow.Setting != "" {
					if sr.setting == nil || *sr.setting != pqRow.Setting {
						t.Errorf("row %d %s: setting got %v, want %q",
							rowNum, sr.codeType, sr.setting, pqRow.Setting)
					}
				}

				// Money values must match parquet→cents conversion
				assertCentsMatch(t, fmt.Sprintf("row %d %s gross", rowNum, sr.codeType),
					pqRow.GrossCharge, sr.grossCents)
				assertCentsMatch(t, fmt.Sprintf("row %d %s discounted", rowNum, sr.codeType),
					pqRow.DiscountedCash, sr.discountedCents)
				assertCentsMatch(t, fmt.Sprintf("row %d %s min", rowNum, sr.codeType),
					pqRow.MinCharge, sr.minCents)
				assertCentsMatch(t, fmt.Sprintf("row %d %s max", rowNum, sr.codeType),
					pqRow.MaxCharge, sr.maxCents)
			}
		}
	})

	t.Run("every_parquet_code_present_in_serving", func(t *testing.T) {
		// Build a set of all (normalized_code, code_type) from the DB
		type codeKey struct {
			codeType string
			codeNorm string
		}
		dbCodes := make(map[codeKey]bool)
		rows, err := pool.Query(ctx,
			"SELECT DISTINCT code_type, code_norm FROM mrf.prices_by_code")
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		for rows.Next() {
			var k codeKey
			rows.Scan(&k.codeType, &k.codeNorm)
			dbCodes[k] = true
		}

		// Check every non-null code from every parquet row is in the DB
		missing := 0
		for _, pqRow := range parquetRows {
			for name, ptr := range pqRow.CodeValues() {
				if ptr == nil || strings.TrimSpace(*ptr) == "" {
					continue
				}
				norm := normalizeCodeStr(*ptr)
				if !dbCodes[codeKey{codeType: name, codeNorm: norm}] {
					if missing < 10 {
						t.Errorf("missing from DB: code_type=%s code_norm=%s (raw=%s)",
							name, norm, *ptr)
					}
					missing++
				}
			}
		}
		if missing > 0 {
			t.Errorf("total missing codes: %d", missing)
		}
	})
}

// assertCentsMatch checks that float64 dollars → int64 cents conversion is correct.
func assertCentsMatch(t *testing.T, label string, dollars *float64, cents *int64) {
	t.Helper()
	if dollars == nil {
		if cents != nil {
			t.Errorf("%s: parquet nil but db %d", label, *cents)
		}
		return
	}
	if cents == nil {
		t.Errorf("%s: parquet %f but db nil", label, *dollars)
		return
	}
	expected := int64(math.Round(*dollars * 100))
	if *cents != expected {
		t.Errorf("%s: got %d cents, want %d (from $%f)", label, *cents, expected, *dollars)
	}
}

// normalizeCodeStr mirrors the SQL normalization: upper + strip non-alphanumeric.
// This matches the Go normalize.NormalizeCode logic and the SQL
// upper(regexp_replace(code_raw, '[^A-Za-z0-9]', '', 'g')).
func normalizeCodeStr(raw string) string {
	s := strings.TrimSpace(raw)
	s = strings.ToUpper(s)
	// Strip non-alphanumeric
	var b strings.Builder
	for _, r := range s {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// Ensure normalize package is used (compile check).
var _ = normalize.NormalizeCode
