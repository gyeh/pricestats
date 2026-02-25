package ingest_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/gyeh/pricestats/internal/db"
	"github.com/gyeh/pricestats/internal/model"
	"github.com/gyeh/pricestats/internal/sqlcgen"
)

// ---------- helpers ----------

// insertHospital is a test helper that inserts a hospital and returns its ID.
func insertHospital(t *testing.T, q *sqlcgen.Queries, name string) int64 {
	t.Helper()
	ctx := context.Background()
	id, err := q.ResolveHospital(ctx, sqlcgen.ResolveHospitalParams{
		HospitalName: name,
	})
	if err != nil {
		t.Fatalf("insert hospital %q: %v", name, err)
	}
	return id
}

// insertMRFFile is a test helper that registers an MRF file and returns its ID.
func insertMRFFile(t *testing.T, q *sqlcgen.Queries, hospitalID int64, sha string) int64 {
	t.Helper()
	ctx := context.Background()
	affirmation := true
	fileSize := int64(1000)
	result, err := q.RegisterMRFFile(ctx, sqlcgen.RegisterMRFFileParams{
		HospitalID:       hospitalID,
		SourceFileName:   "test.parquet",
		SourceFileSha256: sha,
		Affirmation:      &affirmation,
		FileSizeBytes:    &fileSize,
	})
	if err != nil {
		t.Fatalf("insert mrf_file sha=%s: %v", sha, err)
	}
	return result.MrfFileID
}

// insertStagingRow inserts a single staging row via COPY for test setup.
func insertStagingRow(t *testing.T, pool *pgxpool.Pool, row *model.StagingRow) {
	t.Helper()
	ctx := context.Background()
	ch := make(chan *model.StagingRow, 1)
	ch <- row
	close(ch)
	src := db.NewChannelSource(ch)
	_, err := pool.CopyFrom(ctx,
		pgx.Identifier{"ingest", "stage_charge_rows"},
		model.StagingColumns(),
		src,
	)
	if err != nil {
		t.Fatalf("insert staging row: %v", err)
	}
}

// makeStagingRow builds a minimal StagingRow with required fields populated.
func makeStagingRow(batchID uuid.UUID, mrfFileID int64, rowNum int64, opts ...func(*model.StagingRow)) *model.StagingRow {
	hash := []byte(fmt.Sprintf("hash-%d", rowNum))
	r := &model.StagingRow{
		IngestBatchID:   batchID,
		MRFFileID:       mrfFileID,
		SourceRowNumber: rowNum,
		SourceRowHash:   hash,
		HospitalName:    "Test Hospital",
		Affirmation:     true,
		Description:     fmt.Sprintf("Test charge %d", rowNum),
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

func strPtr(s string) *string   { return &s }
func int64Ptr(v int64) *int64   { return &v }
func int32Ptr(v int32) *int32   { return &v }
func boolPtr(v bool) *bool      { return &v }

// ---------- Migration tests ----------

func TestMigrations_Idempotent(t *testing.T) {
	pool := setupDB(t) // applies migrations once via setupDB
	ctx := context.Background()
	log := setupLog()

	// Apply again — should succeed because everything uses IF NOT EXISTS
	if err := db.ApplyMigrations(ctx, pool, log); err != nil {
		t.Fatalf("second migration run should be idempotent: %v", err)
	}

	// Verify key objects exist
	for _, tbl := range []string{
		"ref.hospitals", "ref.payers", "ref.plans",
		"ingest.mrf_files", "ingest.stage_charge_rows",
		"mrf.prices_by_code",
	} {
		var exists bool
		err := pool.QueryRow(ctx, fmt.Sprintf(
			"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema || '.' || table_name = '%s')", tbl)).
			Scan(&exists)
		if err != nil {
			t.Fatalf("check table %s: %v", tbl, err)
		}
		if !exists {
			t.Errorf("table %s should exist after migrations", tbl)
		}
	}
}

func TestMigrations_PartitionsExist(t *testing.T) {
	pool := setupDB(t)
	ctx := context.Background()

	var count int
	err := pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_inherits
		 JOIN pg_class parent ON parent.oid = inhparent
		 JOIN pg_namespace pns ON pns.oid = parent.relnamespace
		 WHERE pns.nspname = 'mrf' AND parent.relname = 'prices_by_code'`).Scan(&count)
	if err != nil {
		t.Fatalf("query partitions: %v", err)
	}
	if count != 5 {
		t.Errorf("expected 5 partitions, got %d", count)
	}
}

// ---------- resolve_hospital.sql ----------

func TestResolveHospital(t *testing.T) {
	pool := setupDB(t)
	q := sqlcgen.New(pool)
	ctx := context.Background()

	t.Run("insert_new", func(t *testing.T) {
		id, err := q.ResolveHospital(ctx, sqlcgen.ResolveHospitalParams{
			HospitalName:     "General Hospital",
			HospitalLocation: strPtr("New York"),
			HospitalAddress:  strPtr("123 Main St"),
			LicenseNumber:    strPtr("LIC-001"),
			LicenseState:     strPtr("NY"),
		})
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		if id <= 0 {
			t.Errorf("expected positive hospital_id, got %d", id)
		}

		// Verify data persisted
		var name, loc, addr string
		err = pool.QueryRow(ctx,
			"SELECT hospital_name, hospital_location, hospital_address FROM ref.hospitals WHERE hospital_id = $1", id).
			Scan(&name, &loc, &addr)
		if err != nil {
			t.Fatalf("verify: %v", err)
		}
		if name != "General Hospital" || loc != "New York" || addr != "123 Main St" {
			t.Errorf("data mismatch: got name=%q loc=%q addr=%q", name, loc, addr)
		}
	})

	t.Run("duplicate_name_inserts_again", func(t *testing.T) {
		// No unique constraint on hospital_name, so same name creates a new row.
		// Dedup is handled in Go layer via SELECT lookup before insert.
		id1 := insertHospital(t, q, "Dupe Hospital")
		id2 := insertHospital(t, q, "Dupe Hospital")
		if id1 == id2 {
			t.Errorf("expected different IDs for duplicate names, got same: %d", id1)
		}
	})

	t.Run("go_layer_dedup_by_name_lookup", func(t *testing.T) {
		// The actual dedup is a SELECT by name before the INSERT (as in preflight.go)
		insertHospital(t, q, "Lookup Hospital")

		id, err := q.LookupHospitalByName(ctx, "Lookup Hospital")
		if err != nil {
			t.Fatalf("lookup: %v", err)
		}
		if id <= 0 {
			t.Errorf("expected positive id from lookup, got %d", id)
		}
	})

	t.Run("null_optional_fields", func(t *testing.T) {
		id, err := q.ResolveHospital(ctx, sqlcgen.ResolveHospitalParams{
			HospitalName: "Minimal Hospital",
		})
		if err != nil {
			t.Fatalf("insert with nulls: %v", err)
		}
		if id <= 0 {
			t.Errorf("expected positive id, got %d", id)
		}
	})
}

// ---------- register_mrf_file.sql ----------

func TestRegisterMRFFile(t *testing.T) {
	pool := setupDB(t)
	q := sqlcgen.New(pool)
	ctx := context.Background()

	hospitalID := insertHospital(t, q, "Test Hospital")

	t.Run("insert_new", func(t *testing.T) {
		result, err := q.RegisterMRFFile(ctx, sqlcgen.RegisterMRFFileParams{
			HospitalID:       hospitalID,
			SourceFileName:   "file1.parquet",
			SourceFileSha256: "sha256-aaa",
			Version:          strPtr("v1.0"),
			Affirmation:      boolPtr(true),
			FileSizeBytes:    int64Ptr(5000),
		})
		if err != nil {
			t.Fatalf("register: %v", err)
		}
		if result.MrfFileID <= 0 {
			t.Errorf("expected positive mrf_file_id, got %d", result.MrfFileID)
		}
		if result.Status != "pending" {
			t.Errorf("expected status 'pending', got %q", result.Status)
		}
	})

	t.Run("duplicate_sha_returns_no_rows", func(t *testing.T) {
		// First insert
		insertMRFFile(t, q, hospitalID, "sha256-bbb")

		// Second insert with same hospital + sha → conflict, no RETURNING
		_, err := q.RegisterMRFFile(ctx, sqlcgen.RegisterMRFFileParams{
			HospitalID:       hospitalID,
			SourceFileName:   "file.parquet",
			SourceFileSha256: "sha256-bbb",
			Affirmation:      boolPtr(true),
			FileSizeBytes:    int64Ptr(1000),
		})
		if err != pgx.ErrNoRows {
			t.Fatalf("expected ErrNoRows on duplicate sha, got err=%v", err)
		}
	})

	t.Run("different_hospital_same_sha_ok", func(t *testing.T) {
		otherHospital := insertHospital(t, q, "Other Hospital")
		id := insertMRFFile(t, q, otherHospital, "sha256-ccc")
		if id <= 0 {
			t.Errorf("expected positive id, got %d", id)
		}
	})
}

// ---------- update_mrf_status.sql ----------

func TestUpdateMRFStatus(t *testing.T) {
	pool := setupDB(t)
	q := sqlcgen.New(pool)
	ctx := context.Background()

	hospitalID := insertHospital(t, q, "Status Hospital")
	fileID := insertMRFFile(t, q, hospitalID, "sha-status")

	transitions := []string{"staging", "staged", "transforming", "transformed", "active"}

	for _, newStatus := range transitions {
		t.Run("to_"+newStatus, func(t *testing.T) {
			err := q.UpdateMRFStatus(ctx, sqlcgen.UpdateMRFStatusParams{MrfFileID: fileID, Status: newStatus})
			if err != nil {
				t.Fatalf("update to %s: %v", newStatus, err)
			}

			var got string
			pool.QueryRow(ctx, "SELECT status FROM ingest.mrf_files WHERE mrf_file_id = $1", fileID).Scan(&got)
			if got != newStatus {
				t.Errorf("expected status %q, got %q", newStatus, got)
			}
		})
	}

	t.Run("to_failed", func(t *testing.T) {
		err := q.UpdateMRFStatus(ctx, sqlcgen.UpdateMRFStatusParams{MrfFileID: fileID, Status: "failed"})
		if err != nil {
			t.Fatalf("update to failed: %v", err)
		}
		var got string
		pool.QueryRow(ctx, "SELECT status FROM ingest.mrf_files WHERE mrf_file_id = $1", fileID).Scan(&got)
		if got != "failed" {
			t.Errorf("expected 'failed', got %q", got)
		}
	})
}

// ---------- COPY to staging (ChannelSource) ----------

func TestCopyToStaging(t *testing.T) {
	pool := setupDB(t)
	q := sqlcgen.New(pool)
	ctx := context.Background()

	hospitalID := insertHospital(t, q, "Copy Hospital")
	fileID := insertMRFFile(t, q, hospitalID, "sha-copy")
	batchID := uuid.New()

	t.Run("single_row", func(t *testing.T) {
		row := makeStagingRow(batchID, fileID, 1,
			func(r *model.StagingRow) {
				r.CPTCode = strPtr("99213")
				r.GrossChargeCents = int64Ptr(15000)
			},
		)
		insertStagingRow(t, pool, row)

		var count int64
		pool.QueryRow(ctx, "SELECT count(*) FROM ingest.stage_charge_rows WHERE ingest_batch_id = $1", batchID).Scan(&count)
		if count != 1 {
			t.Errorf("expected 1 row, got %d", count)
		}

		// Verify data
		var desc string
		var cpt *string
		var gross *int64
		pool.QueryRow(ctx,
			"SELECT description, cpt_code, gross_charge_cents FROM ingest.stage_charge_rows WHERE ingest_batch_id = $1",
			batchID).Scan(&desc, &cpt, &gross)
		if desc != "Test charge 1" {
			t.Errorf("description: got %q", desc)
		}
		if cpt == nil || *cpt != "99213" {
			t.Errorf("cpt_code: got %v", cpt)
		}
		if gross == nil || *gross != 15000 {
			t.Errorf("gross_charge_cents: got %v", gross)
		}
	})

	t.Run("multiple_rows", func(t *testing.T) {
		batch2 := uuid.New()
		ch := make(chan *model.StagingRow, 10)
		for i := int64(1); i <= 5; i++ {
			ch <- makeStagingRow(batch2, fileID, i,
				func(r *model.StagingRow) { r.HCPCSCode = strPtr(fmt.Sprintf("J%04d", i)) },
			)
		}
		close(ch)
		src := db.NewChannelSource(ch)
		n, err := pool.CopyFrom(ctx,
			pgx.Identifier{"ingest", "stage_charge_rows"},
			model.StagingColumns(),
			src,
		)
		if err != nil {
			t.Fatalf("copy: %v", err)
		}
		if n != 5 {
			t.Errorf("expected 5 rows copied, got %d", n)
		}
	})

	t.Run("null_optional_fields", func(t *testing.T) {
		batch3 := uuid.New()
		row := makeStagingRow(batch3, fileID, 1) // no codes, no money
		insertStagingRow(t, pool, row)

		var cpt, hcpcs *string
		var gross *int64
		pool.QueryRow(ctx,
			"SELECT cpt_code, hcpcs_code, gross_charge_cents FROM ingest.stage_charge_rows WHERE ingest_batch_id = $1",
			batch3).Scan(&cpt, &hcpcs, &gross)
		if cpt != nil || hcpcs != nil || gross != nil {
			t.Errorf("expected all nulls, got cpt=%v hcpcs=%v gross=%v", cpt, hcpcs, gross)
		}
	})

	t.Run("duplicate_batch_rowhash_rejected", func(t *testing.T) {
		batch4 := uuid.New()
		row1 := makeStagingRow(batch4, fileID, 1)
		row2 := makeStagingRow(batch4, fileID, 2)
		row2.SourceRowHash = row1.SourceRowHash // same hash

		ch := make(chan *model.StagingRow, 2)
		ch <- row1
		ch <- row2
		close(ch)
		src := db.NewChannelSource(ch)
		_, err := pool.CopyFrom(ctx,
			pgx.Identifier{"ingest", "stage_charge_rows"},
			model.StagingColumns(),
			src,
		)
		if err == nil {
			t.Fatal("expected error on duplicate (batch_id, source_row_hash)")
		}
	})
}

// ---------- upsert_payers.sql ----------

func TestUpsertPayers(t *testing.T) {
	pool := setupDB(t)
	q := sqlcgen.New(pool)
	ctx := context.Background()

	hospitalID := insertHospital(t, q, "Payer Hospital")
	fileID := insertMRFFile(t, q, hospitalID, "sha-payer")
	batchID := uuid.New()

	t.Run("inserts_distinct_payers", func(t *testing.T) {
		// Insert staging rows with payer data
		for i, payer := range []string{"Aetna", "BCBS", "Aetna"} { // Aetna appears twice
			row := makeStagingRow(batchID, fileID, int64(i+1), func(r *model.StagingRow) {
				r.PayerName = strPtr(payer)
				r.PayerNameNorm = strPtr(payer) // simplified for test
				r.CPTCode = strPtr("99213")
			})
			insertStagingRow(t, pool, row)
		}

		_, err := q.UpsertPayers(ctx, batchID)
		if err != nil {
			t.Fatalf("upsert payers: %v", err)
		}

		var count int64
		pool.QueryRow(ctx, "SELECT count(*) FROM ref.payers").Scan(&count)
		if count != 2 { // Aetna + BCBS
			t.Errorf("expected 2 distinct payers, got %d", count)
		}
	})

	t.Run("idempotent", func(t *testing.T) {
		// Run again — ON CONFLICT DO NOTHING
		_, err := q.UpsertPayers(ctx, batchID)
		if err != nil {
			t.Fatalf("second upsert: %v", err)
		}

		var count int64
		pool.QueryRow(ctx, "SELECT count(*) FROM ref.payers").Scan(&count)
		if count != 2 {
			t.Errorf("expected 2 payers after idempotent run, got %d", count)
		}
	})

	t.Run("skips_null_payer", func(t *testing.T) {
		batch2 := uuid.New()
		row := makeStagingRow(batch2, fileID, 1) // no payer fields set
		insertStagingRow(t, pool, row)

		tag, err := q.UpsertPayers(ctx, batch2)
		if err != nil {
			t.Fatalf("upsert: %v", err)
		}
		if tag.RowsAffected() != 0 {
			t.Errorf("expected 0 rows affected for null payer batch, got %d", tag.RowsAffected())
		}
	})
}

// ---------- upsert_plans.sql ----------

func TestUpsertPlans(t *testing.T) {
	pool := setupDB(t)
	q := sqlcgen.New(pool)
	ctx := context.Background()

	hospitalID := insertHospital(t, q, "Plan Hospital")
	fileID := insertMRFFile(t, q, hospitalID, "sha-plan")
	batchID := uuid.New()

	// Insert staging rows with payer + plan data
	plans := []struct{ payer, plan string }{
		{"Aetna", "Gold PPO"},
		{"Aetna", "Silver HMO"},
		{"BCBS", "Basic Plan"},
		{"Aetna", "Gold PPO"}, // duplicate
	}
	for i, p := range plans {
		row := makeStagingRow(batchID, fileID, int64(i+1), func(r *model.StagingRow) {
			r.PayerName = strPtr(p.payer)
			r.PayerNameNorm = strPtr(p.payer)
			r.PlanName = strPtr(p.plan)
			r.PlanNameNorm = strPtr(p.plan)
			r.CPTCode = strPtr("99213")
		})
		insertStagingRow(t, pool, row)
	}

	// Must upsert payers first (plans references payers)
	_, err := q.UpsertPayers(ctx, batchID)
	if err != nil {
		t.Fatalf("upsert payers: %v", err)
	}

	t.Run("inserts_distinct_plans", func(t *testing.T) {
		_, err := q.UpsertPlans(ctx, batchID)
		if err != nil {
			t.Fatalf("upsert plans: %v", err)
		}

		var count int64
		pool.QueryRow(ctx, "SELECT count(*) FROM ref.plans").Scan(&count)
		if count != 3 { // Gold PPO, Silver HMO, Basic Plan
			t.Errorf("expected 3 distinct plans, got %d", count)
		}
	})

	t.Run("plan_has_correct_payer", func(t *testing.T) {
		var payerName string
		err := pool.QueryRow(ctx,
			`SELECT p.payer_name FROM ref.payers p
			 JOIN ref.plans pl ON pl.payer_id = p.payer_id
			 WHERE pl.plan_name = 'Basic Plan'`).Scan(&payerName)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if payerName != "BCBS" {
			t.Errorf("expected payer 'BCBS' for 'Basic Plan', got %q", payerName)
		}
	})

	t.Run("idempotent", func(t *testing.T) {
		_, err := q.UpsertPlans(ctx, batchID)
		if err != nil {
			t.Fatalf("second upsert: %v", err)
		}
		var count int64
		pool.QueryRow(ctx, "SELECT count(*) FROM ref.plans").Scan(&count)
		if count != 3 {
			t.Errorf("expected 3 plans after idempotent run, got %d", count)
		}
	})

	t.Run("skips_null_plan", func(t *testing.T) {
		batch2 := uuid.New()
		row := makeStagingRow(batch2, fileID, 1, func(r *model.StagingRow) {
			r.PayerName = strPtr("Cigna")
			r.PayerNameNorm = strPtr("Cigna")
			// PlanName/PlanNameNorm left nil
		})
		insertStagingRow(t, pool, row)
		_, _ = q.UpsertPayers(ctx, batch2)

		tag, err := q.UpsertPlans(ctx, batch2)
		if err != nil {
			t.Fatalf("upsert: %v", err)
		}
		if tag.RowsAffected() != 0 {
			t.Errorf("expected 0 rows for null plan, got %d", tag.RowsAffected())
		}
	})
}

// ---------- transform_wide_to_long.sql ----------

func TestTransformWideToLong(t *testing.T) {
	pool := setupDB(t)
	q := sqlcgen.New(pool)
	ctx := context.Background()

	hospitalID := insertHospital(t, q, "Transform Hospital")
	fileID := insertMRFFile(t, q, hospitalID, "sha-transform")
	batchID := uuid.New()

	t.Run("single_code_produces_one_row", func(t *testing.T) {
		row := makeStagingRow(batchID, fileID, 1, func(r *model.StagingRow) {
			r.CPTCode = strPtr("99213")
			r.GrossChargeCents = int64Ptr(15000)
			r.Description = "Office visit"
		})
		insertStagingRow(t, pool, row)

		tag, err := q.TransformWideToLong(ctx, sqlcgen.TransformWideToLongParams{IngestBatchID: batchID})
		if err != nil {
			t.Fatalf("transform: %v", err)
		}
		if tag.RowsAffected() != 1 {
			t.Errorf("expected 1 serving row, got %d", tag.RowsAffected())
		}

		var codeType, codeRaw, codeNorm, desc string
		var gross *int64
		pool.QueryRow(ctx,
			"SELECT code_type, code_raw, code_norm, description, gross_charge_cents FROM mrf.prices_by_code WHERE mrf_file_id = $1",
			fileID).Scan(&codeType, &codeRaw, &codeNorm, &desc, &gross)
		if codeType != "CPT" {
			t.Errorf("code_type: got %q", codeType)
		}
		if codeRaw != "99213" {
			t.Errorf("code_raw: got %q", codeRaw)
		}
		if codeNorm != "99213" {
			t.Errorf("code_norm: got %q", codeNorm)
		}
		if desc != "Office visit" {
			t.Errorf("description: got %q", desc)
		}
		if gross == nil || *gross != 15000 {
			t.Errorf("gross_charge_cents: got %v", gross)
		}
	})

	// Clean up for next subtest
	pool.Exec(ctx, "DELETE FROM mrf.prices_by_code WHERE mrf_file_id = $1", fileID)
	pool.Exec(ctx, "DELETE FROM ingest.stage_charge_rows WHERE ingest_batch_id = $1", batchID)

	t.Run("multiple_codes_explode", func(t *testing.T) {
		batch2 := uuid.New()
		row := makeStagingRow(batch2, fileID, 1, func(r *model.StagingRow) {
			r.CPTCode = strPtr("99213")
			r.HCPCSCode = strPtr("J0120")
			r.NDCCode = strPtr("0250")
			r.Description = "Multi code"
		})
		insertStagingRow(t, pool, row)

		tag, err := q.TransformWideToLong(ctx, sqlcgen.TransformWideToLongParams{IngestBatchID: batch2})
		if err != nil {
			t.Fatalf("transform: %v", err)
		}
		if tag.RowsAffected() != 3 {
			t.Errorf("expected 3 serving rows (CPT+HCPCS+NDC), got %d", tag.RowsAffected())
		}

		// Verify each code type exists
		for _, ct := range []string{"CPT", "HCPCS", "NDC"} {
			var exists bool
			pool.QueryRow(ctx,
				"SELECT EXISTS (SELECT 1 FROM mrf.prices_by_code WHERE mrf_file_id = $1 AND code_type = $2)",
				fileID, ct).Scan(&exists)
			if !exists {
				t.Errorf("expected code_type %s in serving", ct)
			}
		}

		pool.Exec(ctx, "DELETE FROM mrf.prices_by_code WHERE mrf_file_id = $1", fileID)
		pool.Exec(ctx, "DELETE FROM ingest.stage_charge_rows WHERE ingest_batch_id = $1", batch2)
	})

	t.Run("null_and_empty_codes_filtered", func(t *testing.T) {
		batch3 := uuid.New()
		row := makeStagingRow(batch3, fileID, 1, func(r *model.StagingRow) {
			r.CPTCode = strPtr("99213") // non-null
			r.HCPCSCode = strPtr("")    // empty string — should be filtered
			// all others nil — should be filtered
		})
		insertStagingRow(t, pool, row)

		tag, err := q.TransformWideToLong(ctx, sqlcgen.TransformWideToLongParams{IngestBatchID: batch3})
		if err != nil {
			t.Fatalf("transform: %v", err)
		}
		if tag.RowsAffected() != 1 {
			t.Errorf("expected 1 row (only CPT), got %d", tag.RowsAffected())
		}

		pool.Exec(ctx, "DELETE FROM mrf.prices_by_code WHERE mrf_file_id = $1", fileID)
		pool.Exec(ctx, "DELETE FROM ingest.stage_charge_rows WHERE ingest_batch_id = $1", batch3)
	})

	t.Run("code_normalization", func(t *testing.T) {
		batch4 := uuid.New()
		row := makeStagingRow(batch4, fileID, 1, func(r *model.StagingRow) {
			r.CPTCode = strPtr("99.213-A") // has punctuation
		})
		insertStagingRow(t, pool, row)

		q.TransformWideToLong(ctx, sqlcgen.TransformWideToLongParams{IngestBatchID: batch4})

		var codeRaw, codeNorm string
		pool.QueryRow(ctx,
			"SELECT code_raw, code_norm FROM mrf.prices_by_code WHERE mrf_file_id = $1 AND code_type = 'CPT'",
			fileID).Scan(&codeRaw, &codeNorm)
		if codeRaw != "99.213-A" {
			t.Errorf("code_raw should preserve original: got %q", codeRaw)
		}
		if codeNorm != "99213A" {
			t.Errorf("code_norm should be upper+stripped: got %q, want %q", codeNorm, "99213A")
		}

		pool.Exec(ctx, "DELETE FROM mrf.prices_by_code WHERE mrf_file_id = $1", fileID)
		pool.Exec(ctx, "DELETE FROM ingest.stage_charge_rows WHERE ingest_batch_id = $1", batch4)
	})

	t.Run("all_5_code_types", func(t *testing.T) {
		batch5 := uuid.New()
		row := makeStagingRow(batch5, fileID, 1, func(r *model.StagingRow) {
			r.CPTCode = strPtr("C1")
			r.HCPCSCode = strPtr("H1")
			r.MSDRGCode = strPtr("M1")
			r.NDCCode = strPtr("N1")
			r.CDTCode = strPtr("CT1")
		})
		insertStagingRow(t, pool, row)

		tag, err := q.TransformWideToLong(ctx, sqlcgen.TransformWideToLongParams{IngestBatchID: batch5})
		if err != nil {
			t.Fatalf("transform: %v", err)
		}
		if tag.RowsAffected() != 5 {
			t.Errorf("expected 5 serving rows, got %d", tag.RowsAffected())
		}

		// Verify each goes to correct partition
		allTypes := []string{"CPT", "HCPCS", "MS-DRG", "NDC", "CDT"}
		for _, ct := range allTypes {
			var exists bool
			pool.QueryRow(ctx,
				"SELECT EXISTS (SELECT 1 FROM mrf.prices_by_code WHERE mrf_file_id = $1 AND code_type = $2)",
				fileID, ct).Scan(&exists)
			if !exists {
				t.Errorf("missing code_type %s", ct)
			}
		}

		pool.Exec(ctx, "DELETE FROM mrf.prices_by_code WHERE mrf_file_id = $1", fileID)
		pool.Exec(ctx, "DELETE FROM ingest.stage_charge_rows WHERE ingest_batch_id = $1", batch5)
	})

	t.Run("money_values_preserved", func(t *testing.T) {
		batch6 := uuid.New()
		row := makeStagingRow(batch6, fileID, 1, func(r *model.StagingRow) {
			r.CPTCode = strPtr("99213")
			r.GrossChargeCents = int64Ptr(15099)
			r.DiscountedCashCents = int64Ptr(10050)
			r.NegotiatedDollarCents = int64Ptr(8000)
			r.NegotiatedPercentageBPS = int32Ptr(5000)
			r.EstimatedAmountCents = int64Ptr(12000)
			r.MinChargeCents = int64Ptr(5000)
			r.MaxChargeCents = int64Ptr(20000)
		})
		insertStagingRow(t, pool, row)

		q.TransformWideToLong(ctx, sqlcgen.TransformWideToLongParams{IngestBatchID: batch6})

		var gross, disc, neg, est, min, max *int64
		var negPct *int32
		pool.QueryRow(ctx,
			`SELECT gross_charge_cents, discounted_cash_cents, negotiated_dollar_cents,
			        negotiated_percentage_bps, estimated_amount_cents, min_charge_cents, max_charge_cents
			 FROM mrf.prices_by_code WHERE mrf_file_id = $1 AND code_type = 'CPT'`, fileID).
			Scan(&gross, &disc, &neg, &negPct, &est, &min, &max)

		checks := []struct {
			name string
			got  *int64
			want int64
		}{
			{"gross", gross, 15099},
			{"discounted", disc, 10050},
			{"negotiated", neg, 8000},
			{"estimated", est, 12000},
			{"min", min, 5000},
			{"max", max, 20000},
		}
		for _, c := range checks {
			if c.got == nil || *c.got != c.want {
				t.Errorf("%s: got %v, want %d", c.name, c.got, c.want)
			}
		}
		if negPct == nil || *negPct != 5000 {
			t.Errorf("negotiated_percentage_bps: got %v, want 5000", negPct)
		}

		pool.Exec(ctx, "DELETE FROM mrf.prices_by_code WHERE mrf_file_id = $1", fileID)
		pool.Exec(ctx, "DELETE FROM ingest.stage_charge_rows WHERE ingest_batch_id = $1", batch6)
	})

	t.Run("payer_plan_joins", func(t *testing.T) {
		batch7 := uuid.New()

		// Set up payer and plan
		row := makeStagingRow(batch7, fileID, 1, func(r *model.StagingRow) {
			r.CPTCode = strPtr("99213")
			r.PayerName = strPtr("Aetna")
			r.PayerNameNorm = strPtr("aetna")
			r.PlanName = strPtr("Gold PPO")
			r.PlanNameNorm = strPtr("gold ppo")
		})
		insertStagingRow(t, pool, row)

		// Create payers and plans first
		q.UpsertPayers(ctx, batch7)
		q.UpsertPlans(ctx, batch7)

		q.TransformWideToLong(ctx, sqlcgen.TransformWideToLongParams{IngestBatchID: batch7})

		var payerID, planID *int64
		var payerRaw, planRaw *string
		pool.QueryRow(ctx,
			"SELECT payer_id, plan_id, payer_name_raw, plan_name_raw FROM mrf.prices_by_code WHERE mrf_file_id = $1 AND code_type = 'CPT'",
			fileID).Scan(&payerID, &planID, &payerRaw, &planRaw)

		if payerID == nil || *payerID <= 0 {
			t.Errorf("expected payer_id to be populated, got %v", payerID)
		}
		if planID == nil || *planID <= 0 {
			t.Errorf("expected plan_id to be populated, got %v", planID)
		}
		if payerRaw == nil || *payerRaw != "Aetna" {
			t.Errorf("payer_name_raw: got %v", payerRaw)
		}
		if planRaw == nil || *planRaw != "Gold PPO" {
			t.Errorf("plan_name_raw: got %v", planRaw)
		}

		pool.Exec(ctx, "DELETE FROM mrf.prices_by_code WHERE mrf_file_id = $1", fileID)
		pool.Exec(ctx, "DELETE FROM ingest.stage_charge_rows WHERE ingest_batch_id = $1", batch7)
	})

	t.Run("no_codes_produces_zero_rows", func(t *testing.T) {
		batch8 := uuid.New()
		row := makeStagingRow(batch8, fileID, 1) // no codes set
		insertStagingRow(t, pool, row)

		tag, err := q.TransformWideToLong(ctx, sqlcgen.TransformWideToLongParams{IngestBatchID: batch8})
		if err != nil {
			t.Fatalf("transform: %v", err)
		}
		if tag.RowsAffected() != 0 {
			t.Errorf("expected 0 rows for no-code row, got %d", tag.RowsAffected())
		}

		pool.Exec(ctx, "DELETE FROM ingest.stage_charge_rows WHERE ingest_batch_id = $1", batch8)
	})
}

// ---------- transform code_types filter ----------

func TestTransformWideToLong_CodeTypeFilter(t *testing.T) {
	pool := setupDB(t)
	q := sqlcgen.New(pool)
	ctx := context.Background()

	hospitalID := insertHospital(t, q, "Filter Hospital")
	fileID := insertMRFFile(t, q, hospitalID, "sha-filter")
	batchID := uuid.New()

	// Stage a row with all 5 codes set
	row := makeStagingRow(batchID, fileID, 1, func(r *model.StagingRow) {
		r.CPTCode = strPtr("C1")
		r.HCPCSCode = strPtr("H1")
		r.MSDRGCode = strPtr("M1")
		r.NDCCode = strPtr("N1")
		r.CDTCode = strPtr("CT1")
	})
	insertStagingRow(t, pool, row)

	t.Run("filter_to_subset", func(t *testing.T) {
		tag, err := q.TransformWideToLong(ctx, sqlcgen.TransformWideToLongParams{
			IngestBatchID: batchID,
			CodeTypes:     []string{"CPT", "HCPCS"},
		})
		if err != nil {
			t.Fatalf("transform: %v", err)
		}
		if tag.RowsAffected() != 2 {
			t.Errorf("expected 2 serving rows (CPT+HCPCS), got %d", tag.RowsAffected())
		}

		pool.Exec(ctx, "DELETE FROM mrf.prices_by_code WHERE mrf_file_id = $1", fileID)
	})

	t.Run("nil_code_types_includes_all", func(t *testing.T) {
		tag, err := q.TransformWideToLong(ctx, sqlcgen.TransformWideToLongParams{
			IngestBatchID: batchID,
			CodeTypes:     nil,
		})
		if err != nil {
			t.Fatalf("transform: %v", err)
		}
		if tag.RowsAffected() != 5 {
			t.Errorf("expected 5 serving rows (all codes), got %d", tag.RowsAffected())
		}

		pool.Exec(ctx, "DELETE FROM mrf.prices_by_code WHERE mrf_file_id = $1", fileID)
	})
}

// ---------- deactivate_older_versions.sql ----------

func TestDeactivateOlderVersions(t *testing.T) {
	pool := setupDB(t)
	q := sqlcgen.New(pool)
	ctx := context.Background()

	hospitalID := insertHospital(t, q, "Version Hospital")
	file1 := insertMRFFile(t, q, hospitalID, "sha-v1")
	file2 := insertMRFFile(t, q, hospitalID, "sha-v2")

	// Activate file1
	q.ActivateVersion(ctx, file1)

	t.Run("deactivates_older_for_same_hospital", func(t *testing.T) {
		tag, err := q.DeactivateOlderVersions(ctx, sqlcgen.DeactivateOlderVersionsParams{
			HospitalID: hospitalID,
			MrfFileID:  file2,
		})
		if err != nil {
			t.Fatalf("deactivate: %v", err)
		}
		if tag.RowsAffected() != 1 {
			t.Errorf("expected 1 deactivated, got %d", tag.RowsAffected())
		}

		var isActive bool
		pool.QueryRow(ctx, "SELECT is_active FROM ingest.mrf_files WHERE mrf_file_id = $1", file1).Scan(&isActive)
		if isActive {
			t.Error("file1 should be deactivated")
		}
	})

	t.Run("preserves_current_version", func(t *testing.T) {
		// Activate file2 to have something active
		q.ActivateVersion(ctx, file2)

		// Deactivating with file2 as current should not touch file2
		q.DeactivateOlderVersions(ctx, sqlcgen.DeactivateOlderVersionsParams{
			HospitalID: hospitalID,
			MrfFileID:  file2,
		})

		var isActive bool
		pool.QueryRow(ctx, "SELECT is_active FROM ingest.mrf_files WHERE mrf_file_id = $1", file2).Scan(&isActive)
		if !isActive {
			t.Error("file2 (current) should remain active")
		}
	})

	t.Run("ignores_other_hospitals", func(t *testing.T) {
		otherHospital := insertHospital(t, q, "Other Version Hospital")
		otherFile := insertMRFFile(t, q, otherHospital, "sha-other")
		q.ActivateVersion(ctx, otherFile)

		// Deactivating for hospitalID should not touch otherFile
		q.DeactivateOlderVersions(ctx, sqlcgen.DeactivateOlderVersionsParams{
			HospitalID: hospitalID,
			MrfFileID:  file2,
		})

		var isActive bool
		pool.QueryRow(ctx, "SELECT is_active FROM ingest.mrf_files WHERE mrf_file_id = $1", otherFile).Scan(&isActive)
		if !isActive {
			t.Error("other hospital's file should remain active")
		}
	})
}

// ---------- activate_version.sql ----------

func TestActivateVersion(t *testing.T) {
	pool := setupDB(t)
	q := sqlcgen.New(pool)
	ctx := context.Background()

	hospitalID := insertHospital(t, q, "Activate Hospital")
	fileID := insertMRFFile(t, q, hospitalID, "sha-activate")

	// Initially: is_active=false, status='pending'
	var isActive bool
	var status string
	pool.QueryRow(ctx, "SELECT is_active, status FROM ingest.mrf_files WHERE mrf_file_id = $1", fileID).
		Scan(&isActive, &status)
	if isActive {
		t.Fatal("file should start inactive")
	}
	if status != "pending" {
		t.Fatalf("file should start as pending, got %q", status)
	}

	// Activate
	err := q.ActivateVersion(ctx, fileID)
	if err != nil {
		t.Fatalf("activate: %v", err)
	}

	pool.QueryRow(ctx, "SELECT is_active, status FROM ingest.mrf_files WHERE mrf_file_id = $1", fileID).
		Scan(&isActive, &status)
	if !isActive {
		t.Error("expected is_active=true")
	}
	if status != "active" {
		t.Errorf("expected status 'active', got %q", status)
	}
}

// ---------- delete_staging_batch.sql ----------

func TestDeleteStagingBatch(t *testing.T) {
	pool := setupDB(t)
	q := sqlcgen.New(pool)
	ctx := context.Background()

	hospitalID := insertHospital(t, q, "Delete Hospital")
	fileID := insertMRFFile(t, q, hospitalID, "sha-delete")

	batch1 := uuid.New()
	batch2 := uuid.New()

	// Insert rows in two batches
	for i := int64(1); i <= 3; i++ {
		insertStagingRow(t, pool, makeStagingRow(batch1, fileID, i))
	}
	for i := int64(1); i <= 2; i++ {
		insertStagingRow(t, pool, makeStagingRow(batch2, fileID, i))
	}

	t.Run("deletes_only_matching_batch", func(t *testing.T) {
		tag, err := q.DeleteStagingBatch(ctx, batch1)
		if err != nil {
			t.Fatalf("delete: %v", err)
		}
		if tag.RowsAffected() != 3 {
			t.Errorf("expected 3 rows deleted, got %d", tag.RowsAffected())
		}
	})

	t.Run("preserves_other_batches", func(t *testing.T) {
		var count int64
		pool.QueryRow(ctx, "SELECT count(*) FROM ingest.stage_charge_rows WHERE ingest_batch_id = $1", batch2).Scan(&count)
		if count != 2 {
			t.Errorf("expected 2 rows in batch2, got %d", count)
		}
	})

	t.Run("delete_nonexistent_batch_ok", func(t *testing.T) {
		tag, err := q.DeleteStagingBatch(ctx, uuid.New())
		if err != nil {
			t.Fatalf("delete nonexistent: %v", err)
		}
		if tag.RowsAffected() != 0 {
			t.Errorf("expected 0 rows affected, got %d", tag.RowsAffected())
		}
	})
}

// ---------- delete_serving_by_file.sql ----------

func TestDeleteServingByFile(t *testing.T) {
	pool := setupDB(t)
	q := sqlcgen.New(pool)
	ctx := context.Background()

	hospitalID := insertHospital(t, q, "Serving Delete Hospital")
	file1 := insertMRFFile(t, q, hospitalID, "sha-serv1")
	file2 := insertMRFFile(t, q, hospitalID, "sha-serv2")

	// Stage and transform rows for both files
	batch1 := uuid.New()
	batch2 := uuid.New()
	for i := int64(1); i <= 3; i++ {
		insertStagingRow(t, pool, makeStagingRow(batch1, file1, i, func(r *model.StagingRow) {
			r.CPTCode = strPtr(fmt.Sprintf("9921%d", i))
		}))
	}
	for i := int64(1); i <= 2; i++ {
		insertStagingRow(t, pool, makeStagingRow(batch2, file2, i, func(r *model.StagingRow) {
			r.HCPCSCode = strPtr(fmt.Sprintf("J010%d", i))
		}))
	}
	if _, err := q.TransformWideToLong(ctx, sqlcgen.TransformWideToLongParams{IngestBatchID: batch1}); err != nil {
		t.Fatalf("transform batch1: %v", err)
	}
	if _, err := q.TransformWideToLong(ctx, sqlcgen.TransformWideToLongParams{IngestBatchID: batch2}); err != nil {
		t.Fatalf("transform batch2: %v", err)
	}

	t.Run("deletes_only_matching_file", func(t *testing.T) {
		err := q.DeleteServingByFile(ctx, file1)
		if err != nil {
			t.Fatalf("delete serving: %v", err)
		}

		var count int64
		pool.QueryRow(ctx, "SELECT count(*) FROM mrf.prices_by_code WHERE mrf_file_id = $1", file1).Scan(&count)
		if count != 0 {
			t.Errorf("expected 0 rows for file1 after delete, got %d", count)
		}
	})

	t.Run("preserves_other_files", func(t *testing.T) {
		var count int64
		pool.QueryRow(ctx, "SELECT count(*) FROM mrf.prices_by_code WHERE mrf_file_id = $1", file2).Scan(&count)
		if count != 2 {
			t.Errorf("expected 2 rows for file2, got %d", count)
		}
	})

	t.Run("noop_on_nonexistent_file", func(t *testing.T) {
		err := q.DeleteServingByFile(ctx, 999999)
		if err != nil {
			t.Fatalf("delete nonexistent: %v", err)
		}
	})

	// Clean up for next subtest
	pool.Exec(ctx, "DELETE FROM mrf.prices_by_code WHERE mrf_file_id = $1", file2)
	pool.Exec(ctx, "DELETE FROM ingest.stage_charge_rows WHERE ingest_batch_id = $1", batch1)
	pool.Exec(ctx, "DELETE FROM ingest.stage_charge_rows WHERE ingest_batch_id = $1", batch2)

	t.Run("force_reimport_no_duplicates", func(t *testing.T) {
		fileID := insertMRFFile(t, q, hospitalID, "sha-reimport")

		// First import: stage + transform
		batchA := uuid.New()
		for i := int64(1); i <= 2; i++ {
			insertStagingRow(t, pool, makeStagingRow(batchA, fileID, i, func(r *model.StagingRow) {
				r.CPTCode = strPtr("99213")
				r.GrossChargeCents = int64Ptr(10000)
				r.Description = fmt.Sprintf("original charge %d", i)
			}))
		}
		tag, err := q.TransformWideToLong(ctx, sqlcgen.TransformWideToLongParams{IngestBatchID: batchA})
		if err != nil {
			t.Fatalf("first transform: %v", err)
		}
		if tag.RowsAffected() != 2 {
			t.Fatalf("expected 2 rows on first import, got %d", tag.RowsAffected())
		}

		// Clean staging (as pipeline does)
		q.DeleteStagingBatch(ctx, batchA)

		// Re-import: delete old serving rows, stage new data, transform
		if err := q.DeleteServingByFile(ctx, fileID); err != nil {
			t.Fatalf("delete old serving: %v", err)
		}

		batchB := uuid.New()
		for i := int64(1); i <= 3; i++ {
			insertStagingRow(t, pool, makeStagingRow(batchB, fileID, i, func(r *model.StagingRow) {
				r.CPTCode = strPtr("99214")
				r.GrossChargeCents = int64Ptr(20000)
				r.Description = fmt.Sprintf("updated charge %d", i)
			}))
		}
		tag, err = q.TransformWideToLong(ctx, sqlcgen.TransformWideToLongParams{IngestBatchID: batchB})
		if err != nil {
			t.Fatalf("second transform: %v", err)
		}
		if tag.RowsAffected() != 3 {
			t.Fatalf("expected 3 rows on re-import, got %d", tag.RowsAffected())
		}

		// Verify: exactly 3 rows, all with new data (no stale duplicates)
		var total int64
		pool.QueryRow(ctx, "SELECT count(*) FROM mrf.prices_by_code WHERE mrf_file_id = $1", fileID).Scan(&total)
		if total != 3 {
			t.Errorf("expected exactly 3 serving rows after re-import, got %d (duplicates present)", total)
		}

		var gross *int64
		pool.QueryRow(ctx,
			"SELECT DISTINCT gross_charge_cents FROM mrf.prices_by_code WHERE mrf_file_id = $1", fileID).Scan(&gross)
		if gross == nil || *gross != 20000 {
			t.Errorf("expected all rows to have updated gross=20000, got %v", gross)
		}
	})
}

// ---------- delete_staging_by_file.sql ----------

func TestDeleteStagingByFile(t *testing.T) {
	pool := setupDB(t)
	q := sqlcgen.New(pool)
	ctx := context.Background()

	hospitalID := insertHospital(t, q, "Staging Delete Hospital")
	file1 := insertMRFFile(t, q, hospitalID, "sha-stgdel1")
	file2 := insertMRFFile(t, q, hospitalID, "sha-stgdel2")

	batch1 := uuid.New()
	batch2 := uuid.New()
	for i := int64(1); i <= 3; i++ {
		insertStagingRow(t, pool, makeStagingRow(batch1, file1, i, func(r *model.StagingRow) {
			r.CPTCode = strPtr("99213")
		}))
	}
	for i := int64(1); i <= 2; i++ {
		insertStagingRow(t, pool, makeStagingRow(batch2, file2, i, func(r *model.StagingRow) {
			r.CPTCode = strPtr("99214")
		}))
	}

	t.Run("deletes_only_matching_file", func(t *testing.T) {
		err := q.DeleteStagingByFile(ctx, file1)
		if err != nil {
			t.Fatalf("delete staging by file: %v", err)
		}

		var count int64
		pool.QueryRow(ctx, "SELECT count(*) FROM ingest.stage_charge_rows WHERE mrf_file_id = $1", file1).Scan(&count)
		if count != 0 {
			t.Errorf("expected 0 staging rows for file1 after delete, got %d", count)
		}
	})

	t.Run("preserves_other_files", func(t *testing.T) {
		var count int64
		pool.QueryRow(ctx, "SELECT count(*) FROM ingest.stage_charge_rows WHERE mrf_file_id = $1", file2).Scan(&count)
		if count != 2 {
			t.Errorf("expected 2 staging rows for file2, got %d", count)
		}
	})

	t.Run("noop_on_nonexistent_file", func(t *testing.T) {
		err := q.DeleteStagingByFile(ctx, 999999)
		if err != nil {
			t.Fatalf("delete nonexistent: %v", err)
		}
	})

	t.Run("cleans_orphaned_staging_before_reimport", func(t *testing.T) {
		fileID := insertMRFFile(t, q, hospitalID, "sha-stgorphan")

		// First import: stage rows
		batchA := uuid.New()
		for i := int64(1); i <= 3; i++ {
			insertStagingRow(t, pool, makeStagingRow(batchA, fileID, i, func(r *model.StagingRow) {
				r.CPTCode = strPtr("99213")
			}))
		}

		// Simulate a failed import — staging rows left behind (no cleanup)

		// Re-import: delete orphaned staging, then stage new data
		if err := q.DeleteStagingByFile(ctx, fileID); err != nil {
			t.Fatalf("delete orphaned staging: %v", err)
		}

		var countBefore int64
		pool.QueryRow(ctx, "SELECT count(*) FROM ingest.stage_charge_rows WHERE mrf_file_id = $1", fileID).Scan(&countBefore)
		if countBefore != 0 {
			t.Fatalf("expected 0 rows after cleanup, got %d", countBefore)
		}

		// Stage new batch
		batchB := uuid.New()
		for i := int64(1); i <= 2; i++ {
			insertStagingRow(t, pool, makeStagingRow(batchB, fileID, i, func(r *model.StagingRow) {
				r.CPTCode = strPtr("99214")
			}))
		}

		var countAfter int64
		pool.QueryRow(ctx, "SELECT count(*) FROM ingest.stage_charge_rows WHERE mrf_file_id = $1", fileID).Scan(&countAfter)
		if countAfter != 2 {
			t.Errorf("expected exactly 2 staging rows after re-stage, got %d", countAfter)
		}
	})
}

// ---------- analyze (prices + staging) ----------

func TestAnalyze(t *testing.T) {
	pool := setupDB(t)
	q := sqlcgen.New(pool)
	ctx := context.Background()

	if err := q.AnalyzePrices(ctx); err != nil {
		t.Fatalf("ANALYZE prices should succeed on empty tables: %v", err)
	}
	if err := q.AnalyzeStaging(ctx); err != nil {
		t.Fatalf("ANALYZE staging should succeed on empty tables: %v", err)
	}
}

// ---------- WithTx ----------

func TestWithTx(t *testing.T) {
	pool := setupDB(t)
	ctx := context.Background()

	t.Run("commits_on_success", func(t *testing.T) {
		err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
			qtx := sqlcgen.New(tx)
			_, err := qtx.ResolveHospital(ctx, sqlcgen.ResolveHospitalParams{
				HospitalName: "TxCommit Hospital",
			})
			return err
		})
		if err != nil {
			t.Fatalf("WithTx: %v", err)
		}

		var count int64
		pool.QueryRow(ctx, "SELECT count(*) FROM ref.hospitals WHERE hospital_name = 'TxCommit Hospital'").Scan(&count)
		if count != 1 {
			t.Errorf("expected 1 hospital after commit, got %d", count)
		}
	})

	t.Run("rolls_back_on_error", func(t *testing.T) {
		err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
			qtx := sqlcgen.New(tx)
			qtx.ResolveHospital(ctx, sqlcgen.ResolveHospitalParams{
				HospitalName: "TxRollback Hospital",
			})
			return fmt.Errorf("intentional error")
		})
		if err == nil {
			t.Fatal("expected error")
		}

		var count int64
		pool.QueryRow(ctx, "SELECT count(*) FROM ref.hospitals WHERE hospital_name = 'TxRollback Hospital'").Scan(&count)
		if count != 0 {
			t.Errorf("expected 0 hospitals after rollback, got %d", count)
		}
	})

	t.Run("rolls_back_on_panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic to propagate")
			}
		}()

		db.WithTx(ctx, pool, func(tx pgx.Tx) error {
			qtx := sqlcgen.New(tx)
			qtx.ResolveHospital(ctx, sqlcgen.ResolveHospitalParams{
				HospitalName: "TxPanic Hospital",
			})
			panic("intentional panic")
		})
	})

	// Verify the panic-case didn't commit
	t.Run("panic_data_not_persisted", func(t *testing.T) {
		var count int64
		pool.QueryRow(ctx, "SELECT count(*) FROM ref.hospitals WHERE hospital_name = 'TxPanic Hospital'").Scan(&count)
		if count != 0 {
			t.Errorf("expected 0 hospitals after panic rollback, got %d", count)
		}
	})
}

// ---------- ChannelSource unit behavior ----------

func TestChannelSource(t *testing.T) {
	t.Run("empty_channel", func(t *testing.T) {
		ch := make(chan *model.StagingRow)
		close(ch)
		src := db.NewChannelSource(ch)
		if src.Next() {
			t.Error("Next() should return false on closed empty channel")
		}
		if src.Err() != nil {
			t.Errorf("Err() should be nil, got %v", src.Err())
		}
	})

	t.Run("values_returns_copy_values", func(t *testing.T) {
		ch := make(chan *model.StagingRow, 1)
		row := makeStagingRow(uuid.New(), 1, 1)
		ch <- row
		close(ch)

		src := db.NewChannelSource(ch)
		if !src.Next() {
			t.Fatal("Next() should return true")
		}
		vals, err := src.Values()
		if err != nil {
			t.Fatalf("Values(): %v", err)
		}
		if len(vals) != len(model.StagingColumns()) {
			t.Errorf("expected %d values, got %d", len(model.StagingColumns()), len(vals))
		}
		if src.Next() {
			t.Error("Next() should return false after last row")
		}
	})
}

// ---------- helper to reduce noise ----------

func setupLog() zerolog.Logger {
	return zerolog.Nop()
}
