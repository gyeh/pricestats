package config

import (
	"fmt"
	"os"
)

// Config holds all runtime configuration for a mrfload run.
type Config struct {
	DSN             string
	FilePath        string
	HospitalName    string
	LogFormat       string // "text" or "json"
	ActivateVersion    bool
	Force              bool
	KeepStaging        bool
	DryRun             bool
	IncludePayerPrices bool // opt-in: include payer/plan names and negotiated price fields
}

// Validate checks required fields and returns an error if the config is invalid.
func (c *Config) Validate() error {
	if c.FilePath == "" {
		return fmt.Errorf("--file is required")
	}
	if _, err := os.Stat(c.FilePath); err != nil {
		return fmt.Errorf("file not accessible: %w", err)
	}
	return nil
}

// ValidateWithDSN checks both file and DSN fields.
func (c *Config) ValidateWithDSN() error {
	if err := c.Validate(); err != nil {
		return err
	}
	if c.DSN == "" {
		return fmt.Errorf("--dsn or SUPABASE_DB_URL is required")
	}
	return nil
}
