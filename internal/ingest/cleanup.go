package ingest

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/gyeh/pricestats/internal/sqlcgen"
)

// Cleanup deletes staging rows for the given batch.
func Cleanup(ctx context.Context, q *sqlcgen.Queries, log zerolog.Logger, batchID uuid.UUID) error {
	start := time.Now()

	tag, err := q.DeleteStagingBatch(ctx, batchID)
	if err != nil {
		return err
	}

	log.Info().
		Int64("rows_deleted", tag.RowsAffected()).
		Dur("duration", time.Since(start)).
		Msg("staging cleanup complete")

	return nil
}
