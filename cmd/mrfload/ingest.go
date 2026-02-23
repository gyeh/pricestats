package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/gyeh/pricestats/internal/db"
	"github.com/gyeh/pricestats/internal/exitcode"
	"github.com/gyeh/pricestats/internal/ingest"
	"github.com/gyeh/pricestats/internal/logging"
)

var ingestCmd = &cobra.Command{
	Use:   "ingest",
	Short: "Ingest a Parquet file into the database",
	RunE:  runIngest,
}

func init() {
	f := ingestCmd.Flags()
	f.StringVar(&cfg.FilePath, "file", "", "Path to Parquet file (required)")
	f.BoolVar(&cfg.ActivateVersion, "activate-version", false, "Mark this file version as active")
	f.BoolVar(&cfg.Force, "force", false, "Re-import even if file SHA already exists")
	f.BoolVar(&cfg.KeepStaging, "keep-staging", false, "Keep staging rows after transform")
	f.BoolVar(&cfg.IncludePayerPrices, "include-payer-prices", false, "Include payer/plan names and negotiated price fields (excluded by default)")
	_ = ingestCmd.MarkFlagRequired("file")
	rootCmd.AddCommand(ingestCmd)
}

func runIngest(cmd *cobra.Command, args []string) error {
	log := logging.Setup(cfg.LogFormat)
	ctx := context.Background()

	if err := cfg.ValidateWithDSN(); err != nil {
		log.Error().Err(err).Msg("config validation failed")
		os.Exit(exitcode.UsageError)
	}

	pool, err := db.NewPool(ctx, cfg.DSN)
	if err != nil {
		log.Error().Err(err).Msg("database connection failed")
		os.Exit(exitcode.DBConnError)
	}
	defer pool.Close()

	summary, err := ingest.Run(ctx, pool, log, &cfg)
	if err != nil {
		if pe, ok := err.(*ingest.PipelineError); ok {
			log.Error().Err(pe.Err).Str("phase", pe.Phase).Msg("ingest failed")
			switch pe.Phase {
			case "preflight":
				os.Exit(exitcode.ValidationError)
			case "stage":
				os.Exit(exitcode.CopyError)
			default:
				os.Exit(exitcode.TransformError)
			}
		}
		log.Error().Err(err).Msg("ingest failed")
		os.Exit(exitcode.TransformError)
	}

	fmt.Printf("Ingest complete: %d rows staged, %d rows in serving table (%.1fs)\n",
		summary.RowsStaged, summary.RowsInsertedServing, summary.DurationTotal.Seconds())
	return nil
}
