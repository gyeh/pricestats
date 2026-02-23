package main

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/gyeh/pricestats/internal/exitcode"
	"github.com/gyeh/pricestats/internal/logging"
	"github.com/gyeh/pricestats/internal/model"
	"github.com/gyeh/pricestats/internal/normalize"
	"github.com/gyeh/pricestats/internal/parquetread"
)

var planCmd = &cobra.Command{
	Use:   "plan",
	Short: "Dry-run validation and stats (no writes)",
	RunE:  runPlan,
}

func init() {
	planCmd.Flags().StringVar(&cfg.FilePath, "file", "", "Path to Parquet file (required)")
	_ = planCmd.MarkFlagRequired("file")
	rootCmd.AddCommand(planCmd)
}

func runPlan(cmd *cobra.Command, args []string) error {
	log := logging.Setup(cfg.LogFormat)

	if err := cfg.Validate(); err != nil {
		log.Error().Err(err).Msg("config validation failed")
		os.Exit(exitcode.UsageError)
	}

	// Compute file hash
	sha, err := normalize.FileHash(cfg.FilePath)
	if err != nil {
		log.Error().Err(err).Msg("failed to hash file")
		os.Exit(exitcode.ValidationError)
	}

	stat, err := os.Stat(cfg.FilePath)
	if err != nil {
		log.Error().Err(err).Msg("failed to stat file")
		os.Exit(exitcode.ValidationError)
	}

	// Open and validate
	reader, err := parquetread.Open(cfg.FilePath)
	if err != nil {
		log.Error().Err(err).Msg("failed to open parquet file")
		os.Exit(exitcode.ValidationError)
	}
	defer reader.Close()

	if err := parquetread.ValidateSchema(reader.Schema()); err != nil {
		log.Error().Err(err).Msg("schema validation failed")
		os.Exit(exitcode.ValidationError)
	}

	numRows := reader.NumRows()

	// Sample rows to estimate code explosion
	sampleSize := int64(1000)
	if sampleSize > numRows {
		sampleSize = numRows
	}

	codeCounts := make(map[string]int64)
	buf := make([]model.HospitalChargeRow, 256)
	var sampled int64
	var hospitalName string

	for sampled < sampleSize {
		n, readErr := reader.Read(buf)
		for i := 0; i < n && sampled < sampleSize; i++ {
			sampled++
			if hospitalName == "" {
				hospitalName = buf[i].HospitalName
			}
			for name, ptr := range buf[i].CodeValues() {
				if ptr != nil && *ptr != "" {
					codeCounts[name]++
				}
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			log.Error().Err(readErr).Msg("failed to read sample rows")
			os.Exit(exitcode.ValidationError)
		}
	}

	// Print report
	fmt.Println("=== mrfload plan ===")
	fmt.Printf("File:       %s\n", cfg.FilePath)
	fmt.Printf("SHA-256:    %s\n", sha)
	fmt.Printf("Size:       %d bytes\n", stat.Size())
	fmt.Printf("Total rows: %d\n", numRows)
	fmt.Printf("Hospital:   %s\n", hospitalName)
	fmt.Printf("Sampled:    %d rows\n", sampled)
	fmt.Println()
	fmt.Println("Code distribution (sampled):")

	totalExploded := int64(0)
	for _, ct := range model.AllCodeTypes {
		count := codeCounts[ct.Name]
		if count > 0 {
			projected := count * numRows / sampled
			totalExploded += projected
			fmt.Printf("  %-10s %6d sampled → ~%d projected serving rows\n", ct.Name, count, projected)
		}
	}
	fmt.Printf("\nEstimated total serving rows: ~%d\n", totalExploded)
	fmt.Println("Schema validation: OK")

	return nil
}
