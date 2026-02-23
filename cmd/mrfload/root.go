package main

import (
	"os"

	"github.com/spf13/cobra"

	"github.com/gyeh/pricestats/internal/config"
)

var cfg config.Config

var rootCmd = &cobra.Command{
	Use:   "mrfload",
	Short: "Hospital MRF Parquet → Postgres bulk loader",
	Long:  "Reads hospital MRF Parquet files and bulk-loads them into Supabase/Postgres via the COPY protocol.",
}

func init() {
	pf := rootCmd.PersistentFlags()
	pf.StringVar(&cfg.DSN, "dsn", os.Getenv("SUPABASE_DB_URL"), "Postgres connection string (or set SUPABASE_DB_URL)")
	pf.StringVar(&cfg.LogFormat, "log-format", "text", "Log format: text or json")
}
