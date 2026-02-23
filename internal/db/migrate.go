package db

import (
	"context"
	"fmt"
	"io/fs"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	embedsql "github.com/gyeh/pricestats/internal/sql"
)

// ApplyMigrations runs all embedded SQL migrations in filename order.
// All DDL uses IF NOT EXISTS so migrations are idempotent.
func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool, log zerolog.Logger) error {
	entries, err := fs.ReadDir(embedsql.Migrations, "migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}

	// Sort by filename to ensure correct ordering.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		data, err := fs.ReadFile(embedsql.Migrations, "migrations/"+name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}

		log.Info().Str("migration", name).Msg("applying migration")
		if _, err := pool.Exec(ctx, string(data)); err != nil {
			return fmt.Errorf("execute migration %s: %w", name, err)
		}
	}

	log.Info().Int("count", len(entries)).Msg("all migrations applied")
	return nil
}
