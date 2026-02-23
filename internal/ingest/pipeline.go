package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/gyeh/pricestats/internal/config"
	"github.com/gyeh/pricestats/internal/model"
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

	// Phase 1: Preflight
	log.Info().Str("file", cfg.FilePath).Msg("starting preflight")
	pf, err := Preflight(ctx, pool, log, cfg.FilePath, cfg.Force)
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
	if err := UpdateStatus(ctx, pool, pf.MRFFileID, "staging"); err != nil {
		return nil, &PipelineError{Phase: "stage", Err: err}
	}

	stageResult, err := Stage(ctx, pool, log, pf, cfg.IncludePayerPrices)
	if err != nil {
		_ = UpdateStatus(ctx, pool, pf.MRFFileID, "failed")
		return nil, &PipelineError{Phase: "stage", Err: err}
	}

	if err := UpdateStatus(ctx, pool, pf.MRFFileID, "staged"); err != nil {
		return nil, &PipelineError{Phase: "stage", Err: err}
	}

	// Phase 3: Dimension upserts (only when payer/plan data is included)
	if cfg.IncludePayerPrices {
		log.Info().Msg("upserting dimensions")
		if err := UpsertDimensions(ctx, pool, log, pf.IngestBatchID); err != nil {
			_ = UpdateStatus(ctx, pool, pf.MRFFileID, "failed")
			return nil, &PipelineError{Phase: "dimensions", Err: err}
		}
	} else {
		log.Info().Msg("skipping dimension upserts (payer/plan data excluded)")
	}

	// Phase 4: Transform
	log.Info().Msg("starting transform")
	if err := UpdateStatus(ctx, pool, pf.MRFFileID, "transforming"); err != nil {
		return nil, &PipelineError{Phase: "transform", Err: err}
	}

	transformResult, err := Transform(ctx, pool, log, pf.IngestBatchID)
	if err != nil {
		_ = UpdateStatus(ctx, pool, pf.MRFFileID, "failed")
		return nil, &PipelineError{Phase: "transform", Err: err}
	}

	if err := UpdateStatus(ctx, pool, pf.MRFFileID, "transformed"); err != nil {
		return nil, &PipelineError{Phase: "transform", Err: err}
	}

	// Phase 5: Finalize
	log.Info().Msg("finalizing")
	finalizeDur, err := Finalize(ctx, pool, log, pf.HospitalID, pf.MRFFileID, cfg.ActivateVersion)
	if err != nil {
		_ = UpdateStatus(ctx, pool, pf.MRFFileID, "failed")
		return nil, &PipelineError{Phase: "finalize", Err: err}
	}

	// Phase 6: Cleanup staging
	if !cfg.KeepStaging {
		log.Info().Msg("cleaning up staging")
		if err := Cleanup(ctx, pool, log, pf.IngestBatchID); err != nil {
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
