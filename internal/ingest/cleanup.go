package ingest

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	embedsql "github.com/gyeh/pricestats/internal/sql"
)

// Cleanup deletes staging rows for the given batch.
func Cleanup(ctx context.Context, pool *pgxpool.Pool, log zerolog.Logger, batchID uuid.UUID) error {
	start := time.Now()

	tag, err := pool.Exec(ctx, embedsql.DeleteStagingBatch, batchID)
	if err != nil {
		return err
	}

	log.Info().
		Int64("rows_deleted", tag.RowsAffected()).
		Dur("duration", time.Since(start)).
		Msg("staging cleanup complete")

	return nil
}
