package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/gyeh/pricestats/internal/config"
	"github.com/gyeh/pricestats/internal/model"
	"github.com/gyeh/pricestats/internal/sqlcgen"
)

// PipelineError wraps an error with the phase where it occurred.
type PipelineError struct {
	Phase string
	Err   error
}

func (e *PipelineError) Error() string {
	return fmt.Sprintf("%s: %s", e.Phase, e.Err)
}

func (e *PipelineError) Unwrap() error {
	return e.Err
}

// Run executes the full ingest pipeline: preflight → stage → dimensions →
// transform → finalize → cleanup.
func Run(ctx context.Context, pool *pgxpool.Pool, log zerolog.Logger, cfg *config.Config) (*model.IngestSummary, error) {
	totalStart := time.Now()
	q := sqlcgen.New(pool)

	// Phase 1: Preflight
	log.Info().Str("file", cfg.FilePath).Msg("starting preflight")
	pf, err := Preflight(ctx, q, log, cfg.FilePath, cfg.Force)
	if err != nil {
		return nil, &PipelineError{Phase: "preflight", Err: err}
	}

	if pf.AlreadyLoaded {
		log.Info().
			Int64("mrf_file_id", pf.MRFFileID).
			Str("sha256", pf.FileSHA256).
			Msg("file already imported, skipping (use --force to re-import)")
		return &model.IngestSummary{
			FilePath:      pf.FilePath,
			FileSHA256:    pf.FileSHA256,
			MRFFileID:     pf.MRFFileID,
			IngestBatchID: pf.IngestBatchID.String(),
			DurationTotal: time.Since(totalStart),
		}, nil
	}

	// Phase 2: Stage
	log.Info().Msg("starting staging")
	if err := q.UpdateMRFStatus(ctx, sqlcgen.UpdateMRFStatusParams{Status: "staging", MrfFileID: pf.MRFFileID}); err != nil {
		return nil, &PipelineError{Phase: "stage", Err: err}
	}

	// Delete orphaned staging rows from prior failed imports of this file
	if err := q.DeleteStagingByFile(ctx, pf.MRFFileID); err != nil {
		_ = q.UpdateMRFStatus(ctx, sqlcgen.UpdateMRFStatusParams{Status: "failed", MrfFileID: pf.MRFFileID})
		return nil, &PipelineError{Phase: "stage", Err: fmt.Errorf("delete old staging rows: %w", err)}
	}

	stageResult, err := Stage(ctx, pool, log, pf, cfg.IncludePayerPrices)
	if err != nil {
		_ = q.UpdateMRFStatus(ctx, sqlcgen.UpdateMRFStatusParams{Status: "failed", MrfFileID: pf.MRFFileID})
		return nil, &PipelineError{Phase: "stage", Err: err}
	}

	if err := q.UpdateMRFStatus(ctx, sqlcgen.UpdateMRFStatusParams{Status: "staged", MrfFileID: pf.MRFFileID}); err != nil {
		return nil, &PipelineError{Phase: "stage", Err: err}
	}

	// Phase 3: Dimension upserts (only when payer/plan data is included)
	if cfg.IncludePayerPrices {
		log.Info().Msg("upserting dimensions")
		if err := UpsertDimensions(ctx, q, log, pf.IngestBatchID); err != nil {
			_ = q.UpdateMRFStatus(ctx, sqlcgen.UpdateMRFStatusParams{Status: "failed", MrfFileID: pf.MRFFileID})
			return nil, &PipelineError{Phase: "dimensions", Err: err}
		}
	} else {
		log.Info().Msg("skipping dimension upserts (payer/plan data excluded)")
	}

	// Phase 4: Transform
	log.Info().Msg("starting transform")
	if err := q.UpdateMRFStatus(ctx, sqlcgen.UpdateMRFStatusParams{Status: "transforming", MrfFileID: pf.MRFFileID}); err != nil {
		return nil, &PipelineError{Phase: "transform", Err: err}
	}

	// Delete old serving rows before inserting new ones (no-op on first import)
	if err := q.DeleteServingByFile(ctx, pf.MRFFileID); err != nil {
		_ = q.UpdateMRFStatus(ctx, sqlcgen.UpdateMRFStatusParams{Status: "failed", MrfFileID: pf.MRFFileID})
		return nil, &PipelineError{Phase: "transform", Err: fmt.Errorf("delete old serving rows: %w", err)}
	}

	transformResult, err := Transform(ctx, q, log, pf.IngestBatchID, cfg.CodeTypes)
	if err != nil {
		_ = q.UpdateMRFStatus(ctx, sqlcgen.UpdateMRFStatusParams{Status: "failed", MrfFileID: pf.MRFFileID})
		return nil, &PipelineError{Phase: "transform", Err: err}
	}

	if err := q.UpdateMRFStatus(ctx, sqlcgen.UpdateMRFStatusParams{Status: "transformed", MrfFileID: pf.MRFFileID}); err != nil {
		return nil, &PipelineError{Phase: "transform", Err: err}
	}

	// Phase 5: Finalize
	log.Info().Msg("finalizing")
	finalizeDur, err := Finalize(ctx, q, log, pf.HospitalID, pf.MRFFileID, cfg.ActivateVersion)
	if err != nil {
		_ = q.UpdateMRFStatus(ctx, sqlcgen.UpdateMRFStatusParams{Status: "failed", MrfFileID: pf.MRFFileID})
		return nil, &PipelineError{Phase: "finalize", Err: err}
	}

	// Phase 6: Cleanup staging
	if !cfg.KeepStaging {
		log.Info().Msg("cleaning up staging")
		if err := Cleanup(ctx, q, log, pf.IngestBatchID); err != nil {
			log.Warn().Err(err).Msg("staging cleanup failed (non-fatal)")
		}
	}

	summary := &model.IngestSummary{
		FilePath:            pf.FilePath,
		FileSHA256:          pf.FileSHA256,
		MRFFileID:           pf.MRFFileID,
		IngestBatchID:       pf.IngestBatchID.String(),
		RowsRead:            stageResult.RowsRead,
		RowsStaged:          stageResult.RowsStaged,
		RowsRejected:        stageResult.RowsRejected,
		RowsInsertedServing: transformResult.RowsInserted,
		DurationRead:        stageResult.Duration,
		DurationCopy:        stageResult.Duration,
		DurationTransform:   transformResult.Duration,
		DurationFinalize:    finalizeDur,
		DurationTotal:       time.Since(totalStart),
	}

	log.Info().
		Int64("rows_read", summary.RowsRead).
		Int64("rows_staged", summary.RowsStaged).
		Int64("rows_serving", summary.RowsInsertedServing).
		Int64("rows_rejected", summary.RowsRejected).
		Str("total_duration", summary.DurationTotal.String()).
		Msg("ingest pipeline complete")

	return summary, nil
}
