package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/gyeh/pricestats/internal/sqlcgen"
)

// TransformResult holds metrics from the wide→long transformation.
type TransformResult struct {
	RowsInserted int64
	Duration     time.Duration
}

// Transform executes the wide→long INSERT...SELECT from staging into the
// serving table (mrf.prices_by_code).
func Transform(ctx context.Context, q *sqlcgen.Queries, log zerolog.Logger, batchID uuid.UUID) (*TransformResult, error) {
	start := time.Now()

	tag, err := q.TransformWideToLong(ctx, batchID)
	if err != nil {
		return nil, fmt.Errorf("transform wide to long: %w", err)
	}

	dur := time.Since(start)
	rows := tag.RowsAffected()

	log.Info().
		Int64("rows_inserted", rows).
		Str("duration", dur.String()).
		Float64("rows_per_sec", float64(rows)/dur.Seconds()).
		Msg("transform complete")

	return &TransformResult{
		RowsInserted: rows,
		Duration:     dur,
	}, nil
}
