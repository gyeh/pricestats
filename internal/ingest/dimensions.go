package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	embedsql "github.com/gyeh/pricestats/internal/sql"
)

// UpsertDimensions upserts payers and plans from the staging batch into ref tables.
func UpsertDimensions(ctx context.Context, pool *pgxpool.Pool, log zerolog.Logger, batchID uuid.UUID) error {
	start := time.Now()

	// Upsert payers
	tag, err := pool.Exec(ctx, embedsql.UpsertPayers, batchID)
	if err != nil {
		return fmt.Errorf("upsert payers: %w", err)
	}
	log.Info().Int64("payers_upserted", tag.RowsAffected()).Msg("payers upserted")

	// Upsert plans
	tag, err = pool.Exec(ctx, embedsql.UpsertPlans, batchID)
	if err != nil {
		return fmt.Errorf("upsert plans: %w", err)
	}
	log.Info().
		Int64("plans_upserted", tag.RowsAffected()).
		Dur("duration", time.Since(start)).
		Msg("plans upserted")

	return nil
}
