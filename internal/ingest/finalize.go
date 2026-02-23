package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	embedsql "github.com/gyeh/pricestats/internal/sql"
)

// Finalize activates the version, deactivates older versions, and runs ANALYZE.
func Finalize(ctx context.Context, pool *pgxpool.Pool, log zerolog.Logger, hospitalID, mRFFileID int64, activate bool) (time.Duration, error) {
	start := time.Now()

	if activate {
		// Deactivate older versions for this hospital
		tag, err := pool.Exec(ctx, embedsql.DeactivateOlderVersions, hospitalID, mRFFileID)
		if err != nil {
			return 0, fmt.Errorf("deactivate older versions: %w", err)
		}
		log.Info().Int64("deactivated", tag.RowsAffected()).Msg("older versions deactivated")

		// Activate this version
		_, err = pool.Exec(ctx, embedsql.ActivateVersion, mRFFileID)
		if err != nil {
			return 0, fmt.Errorf("activate version: %w", err)
		}
		log.Info().Int64("mrf_file_id", mRFFileID).Msg("version activated")
	} else {
		// Just mark as transformed
		if err := UpdateStatus(ctx, pool, mRFFileID, "transformed"); err != nil {
			return 0, fmt.Errorf("update status to transformed: %w", err)
		}
	}

	// ANALYZE
	_, err := pool.Exec(ctx, embedsql.AnalyzePartitions)
	if err != nil {
		return 0, fmt.Errorf("analyze partitions: %w", err)
	}
	log.Info().Msg("ANALYZE complete")

	return time.Since(start), nil
}
