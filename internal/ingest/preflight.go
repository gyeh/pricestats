package ingest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	"github.com/gyeh/pricestats/internal/model"
	"github.com/gyeh/pricestats/internal/normalize"
	"github.com/gyeh/pricestats/internal/parquetread"
	"github.com/gyeh/pricestats/internal/sqlcgen"
)

// PreflightResult holds all context resolved during the preflight phase.
type PreflightResult struct {
	// FilePath is the original path passed to Preflight, stored as-is.
	FilePath string
	// FileSHA256 is the hex-encoded SHA-256 digest of the file, computed by normalize.FileHash.
	FileSHA256 string
	// FileSize is the file size in bytes from os.Stat.
	FileSize int64
	// HospitalID is the DB primary key for the hospital, resolved (or created) by
	// matching the hospital name from the first row of the Parquet file.
	HospitalID int64
	// MRFFileID is the DB primary key for this MRF file record, returned by
	// RegisterMRFFile (inserted or looked up via hospital_id + sha256).
	MRFFileID int64
	// IngestBatchID is a freshly generated UUIDv4 that uniquely identifies this
	// ingest run, used to tag staged rows for later transform/cleanup.
	IngestBatchID uuid.UUID
	// NumRows is the total row count reported by the Parquet file metadata.
	NumRows int64
	// AlreadyLoaded is true when the file's sha256 already exists in the DB with
	// status "active" or "transformed" and force mode is off, signaling the
	// pipeline can skip this file.
	AlreadyLoaded bool
	// FirstRow is the first row read from the Parquet file, used to extract
	// hospital metadata (name, location, address, license) for resolution.
	FirstRow *model.HospitalChargeRow
}

// Preflight opens the file, computes SHA-256, validates the schema,
// resolves the hospital, and registers the MRF file.
func Preflight(ctx context.Context, q *sqlcgen.Queries, log zerolog.Logger, filePath string, force bool) (*PreflightResult, error) {
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
	hospitalID, err := resolveHospital(ctx, q, firstRow)
	if err != nil {
		return nil, fmt.Errorf("preflight resolve hospital: %w", err)
	}

	// Register MRF file
	mRFFileID, alreadyLoaded, err := registerMRFFile(ctx, q, hospitalID, filePath, sha, stat.Size(), firstRow, force)
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

func resolveHospital(ctx context.Context, q *sqlcgen.Queries, row *model.HospitalChargeRow) (int64, error) {
	// Try to find existing hospital by name first
	hospitalID, err := q.LookupHospitalByName(ctx, row.HospitalName)
	if err == nil {
		return hospitalID, nil
	}

	// Insert new hospital
	hospitalID, err = q.ResolveHospital(ctx, sqlcgen.ResolveHospitalParams{
		HospitalName:     row.HospitalName,
		HospitalLocation: nilIfEmpty(row.HospitalLocation),
		HospitalAddress:  nilIfEmpty(row.HospitalAddress),
		LicenseNumber:    row.LicenseNumber,
		LicenseState:     row.LicenseState,
	})
	if err != nil {
		// Might have been created concurrently; try lookup again
		hospitalID, err2 := q.LookupHospitalByName(ctx, row.HospitalName)
		if err2 != nil {
			return 0, fmt.Errorf("resolve hospital: insert=%w, lookup=%w", err, err2)
		}
		return hospitalID, nil
	}
	return hospitalID, nil
}

func registerMRFFile(ctx context.Context, q *sqlcgen.Queries, hospitalID int64, filePath, sha string, fileSize int64, row *model.HospitalChargeRow, force bool) (int64, bool, error) {
	lastUpdated := normalize.ParseDate(row.LastUpdatedOn)
	affirmation := row.Affirmation

	result, err := q.RegisterMRFFile(ctx, sqlcgen.RegisterMRFFileParams{
		HospitalID:       hospitalID,
		SourceFileName:   filepath.Base(filePath),
		SourceFileSha256: sha,
		Version:          nilIfEmpty(row.Version),
		LastUpdatedOn:    lastUpdated,
		Affirmation:      &affirmation,
		FileSizeBytes:    &fileSize,
	})

	if err == pgx.ErrNoRows {
		// Already exists (ON CONFLICT DO NOTHING returned no rows)
		lookupResult, err2 := q.LookupMRFFile(ctx, sqlcgen.LookupMRFFileParams{
			HospitalID:       hospitalID,
			SourceFileSha256: sha,
		})
		if err2 != nil {
			return 0, false, fmt.Errorf("lookup existing mrf_file: %w", err2)
		}

		if !force && (lookupResult.Status == "active" || lookupResult.Status == "transformed") {
			return lookupResult.MrfFileID, true, nil
		}

		// Reset status for re-import
		if err3 := q.UpdateMRFStatus(ctx, sqlcgen.UpdateMRFStatusParams{MrfFileID: lookupResult.MrfFileID, Status: "pending"}); err3 != nil {
			return 0, false, fmt.Errorf("reset mrf status: %w", err3)
		}
		return lookupResult.MrfFileID, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("register mrf file: %w", err)
	}

	return result.MrfFileID, false, nil
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
