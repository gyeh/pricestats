package ingest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/gyeh/pricestats/internal/model"
	"github.com/gyeh/pricestats/internal/normalize"
	"github.com/gyeh/pricestats/internal/parquetread"
	embedsql "github.com/gyeh/pricestats/internal/sql"
)

// PreflightResult holds all context resolved during the preflight phase.
type PreflightResult struct {
	FilePath      string
	FileSHA256    string
	FileSize      int64
	HospitalID    int64
	MRFFileID     int64
	IngestBatchID uuid.UUID
	NumRows       int64
	AlreadyLoaded bool
	FirstRow      *model.HospitalChargeRow
}

// Preflight opens the file, computes SHA-256, validates the schema,
// resolves the hospital, and registers the MRF file.
func Preflight(ctx context.Context, pool *pgxpool.Pool, log zerolog.Logger, filePath string, force bool) (*PreflightResult, error) {
	start := time.Now()

	// Compute file hash
	sha, err := normalize.FileHash(filePath)
	if err != nil {
		return nil, fmt.Errorf("preflight hash: %w", err)
	}

	stat, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("preflight stat: %w", err)
	}

	// Open and validate Parquet schema
	reader, err := parquetread.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("preflight open: %w", err)
	}
	defer reader.Close()

	if err := parquetread.ValidateSchema(reader.Schema()); err != nil {
		return nil, fmt.Errorf("preflight validate: %w", err)
	}

	numRows := reader.NumRows()

	// Read first row for hospital metadata
	rows := make([]model.HospitalChargeRow, 1)
	n, err := reader.Read(rows)
	if err != nil && n == 0 {
		return nil, fmt.Errorf("preflight read first row: %w", err)
	}
	firstRow := &rows[0]

	log.Info().
		Str("file", filepath.Base(filePath)).
		Str("sha256", sha).
		Int64("rows", numRows).
		Str("hospital", firstRow.HospitalName).
		Dur("duration", time.Since(start)).
		Msg("preflight complete")

	// Resolve hospital
	hospitalID, err := resolveHospital(ctx, pool, firstRow)
	if err != nil {
		return nil, fmt.Errorf("preflight resolve hospital: %w", err)
	}

	// Register MRF file
	mRFFileID, alreadyLoaded, err := registerMRFFile(ctx, pool, hospitalID, filePath, sha, stat.Size(), firstRow, force)
	if err != nil {
		return nil, fmt.Errorf("preflight register file: %w", err)
	}

	return &PreflightResult{
		FilePath:      filePath,
		FileSHA256:    sha,
		FileSize:      stat.Size(),
		HospitalID:    hospitalID,
		MRFFileID:     mRFFileID,
		IngestBatchID: uuid.New(),
		NumRows:       numRows,
		AlreadyLoaded: alreadyLoaded,
		FirstRow:      firstRow,
	}, nil
}

func resolveHospital(ctx context.Context, pool *pgxpool.Pool, row *model.HospitalChargeRow) (int64, error) {
	// Try to find existing hospital by name first
	var hospitalID int64
	err := pool.QueryRow(ctx,
		"SELECT hospital_id FROM ref.hospitals WHERE hospital_name = $1 LIMIT 1",
		row.HospitalName,
	).Scan(&hospitalID)
	if err == nil {
		return hospitalID, nil
	}

	// Insert new hospital
	err = pool.QueryRow(ctx, embedsql.ResolveHospital,
		row.HospitalName,
		nilIfEmpty(row.HospitalLocation),
		nilIfEmpty(row.HospitalAddress),
		row.LicenseNumber,
		row.LicenseState,
	).Scan(&hospitalID)
	if err != nil {
		// Might have been created concurrently; try lookup again
		err2 := pool.QueryRow(ctx,
			"SELECT hospital_id FROM ref.hospitals WHERE hospital_name = $1 LIMIT 1",
			row.HospitalName,
		).Scan(&hospitalID)
		if err2 != nil {
			return 0, fmt.Errorf("resolve hospital: insert=%w, lookup=%w", err, err2)
		}
	}
	return hospitalID, nil
}

func registerMRFFile(ctx context.Context, pool *pgxpool.Pool, hospitalID int64, filePath, sha string, fileSize int64, row *model.HospitalChargeRow, force bool) (int64, bool, error) {
	lastUpdated := normalize.ParseDate(row.LastUpdatedOn)

	var mRFFileID int64
	var status string
	err := pool.QueryRow(ctx, embedsql.RegisterMRFFile,
		hospitalID,
		filepath.Base(filePath),
		sha,
		nilIfEmpty(row.Version),
		lastUpdated,
		row.Affirmation,
		fileSize,
	).Scan(&mRFFileID, &status)

	if err == pgx.ErrNoRows {
		// Already exists (ON CONFLICT DO NOTHING returned no rows)
		err2 := pool.QueryRow(ctx,
			"SELECT mrf_file_id, status FROM ingest.mrf_files WHERE hospital_id = $1 AND source_file_sha256 = $2",
			hospitalID, sha,
		).Scan(&mRFFileID, &status)
		if err2 != nil {
			return 0, false, fmt.Errorf("lookup existing mrf_file: %w", err2)
		}

		if !force && (status == "active" || status == "transformed") {
			return mRFFileID, true, nil
		}

		// Reset status for re-import
		_, err3 := pool.Exec(ctx, embedsql.UpdateMRFStatus, mRFFileID, "pending")
		if err3 != nil {
			return 0, false, fmt.Errorf("reset mrf status: %w", err3)
		}
		return mRFFileID, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("register mrf file: %w", err)
	}

	return mRFFileID, false, nil
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
