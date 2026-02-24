package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/gyeh/pricestats/internal/sqlcgen"
)

// UpsertDimensions upserts payers and plans from the staging batch into ref tables.
func UpsertDimensions(ctx context.Context, q *sqlcgen.Queries, log zerolog.Logger, batchID uuid.UUID) error {
	start := time.Now()

	// Upsert payers
	tag, err := q.UpsertPayers(ctx, batchID)
	if err != nil {
		return fmt.Errorf("upsert payers: %w", err)
	}
	log.Info().Int64("payers_upserted", tag.RowsAffected()).Msg("payers upserted")

	// Upsert plans
	tag, err = q.UpsertPlans(ctx, batchID)
	if err != nil {
		return fmt.Errorf("upsert plans: %w", err)
	}
	log.Info().
		Int64("plans_upserted", tag.RowsAffected()).
		Dur("duration", time.Since(start)).
		Msg("plans upserted")

	return nil
}
