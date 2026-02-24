package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"

	"github.com/gyeh/pricestats/internal/sqlcgen"
)

// Finalize activates the version, deactivates older versions, and runs ANALYZE.
func Finalize(ctx context.Context, q *sqlcgen.Queries, log zerolog.Logger, hospitalID, mRFFileID int64, activate bool) (time.Duration, error) {
	start := time.Now()

	if activate {
		// Deactivate older versions for this hospital
		tag, err := q.DeactivateOlderVersions(ctx, sqlcgen.DeactivateOlderVersionsParams{
			HospitalID: hospitalID,
			MrfFileID:  mRFFileID,
		})
		if err != nil {
			return 0, fmt.Errorf("deactivate older versions: %w", err)
		}
		log.Info().Int64("deactivated", tag.RowsAffected()).Msg("older versions deactivated")

		// Activate this version
		if err := q.ActivateVersion(ctx, mRFFileID); err != nil {
			return 0, fmt.Errorf("activate version: %w", err)
		}
		log.Info().Int64("mrf_file_id", mRFFileID).Msg("version activated")
	} else {
		// Just mark as transformed
		if err := q.UpdateMRFStatus(ctx, sqlcgen.UpdateMRFStatusParams{MrfFileID: mRFFileID, Status: "transformed"}); err != nil {
			return 0, fmt.Errorf("update status to transformed: %w", err)
		}
	}

	// ANALYZE
	if err := q.AnalyzePrices(ctx); err != nil {
		return 0, fmt.Errorf("analyze prices: %w", err)
	}
	if err := q.AnalyzeStaging(ctx); err != nil {
		return 0, fmt.Errorf("analyze staging: %w", err)
	}
	log.Info().Msg("ANALYZE complete")

	return time.Since(start), nil
}
