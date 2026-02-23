package main

import (
	"context"
	"os"

	"github.com/spf13/cobra"

	"github.com/gyeh/pricestats/internal/db"
	"github.com/gyeh/pricestats/internal/exitcode"
	"github.com/gyeh/pricestats/internal/logging"
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Apply database schema migrations",
	RunE:  runMigrate,
}

func init() {
	rootCmd.AddCommand(migrateCmd)
}

func runMigrate(cmd *cobra.Command, args []string) error {
	log := logging.Setup(cfg.LogFormat)
	ctx := context.Background()

	if cfg.DSN == "" {
		log.Error().Msg("--dsn or SUPABASE_DB_URL is required")
		os.Exit(exitcode.UsageError)
	}

	pool, err := db.NewPool(ctx, cfg.DSN)
	if err != nil {
		log.Error().Err(err).Msg("database connection failed")
		os.Exit(exitcode.DBConnError)
	}
	defer pool.Close()

	if err := db.ApplyMigrations(ctx, pool, log); err != nil {
		log.Error().Err(err).Msg("migration failed")
		os.Exit(exitcode.TransformError)
	}

	log.Info().Msg("all migrations applied successfully")
	return nil
}
