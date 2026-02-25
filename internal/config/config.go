package config

import (
	"fmt"
	"os"

	"github.com/gyeh/pricestats/internal/model"

	"gopkg.in/yaml.v3"
)

// Config holds all runtime configuration for a mrfload run.
type Config struct {
	DSN                string
	FilePath           string
	HospitalName       string
	LogFormat          string // "text" or "json"
	ActivateVersion    bool
	Force              bool
	KeepStaging        bool
	DryRun             bool
	IncludePayerPrices bool     // opt-in: include payer/plan names and negotiated price fields
	CodeTypes          []string `yaml:"code_types"` // subset of AllCodeTypes to process
}

// yamlConfig is the on-disk YAML structure.
type yamlConfig struct {
	CodeTypes []string `yaml:"code_types"`
}

// LoadFromFile reads a YAML config file and merges its values into Config.
func (c *Config) LoadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config file: %w", err)
	}
	var yc yamlConfig
	if err := yaml.Unmarshal(data, &yc); err != nil {
		return fmt.Errorf("parse config file: %w", err)
	}
	c.CodeTypes = yc.CodeTypes
	return c.validateCodeTypes()
}

// validateCodeTypes checks that every entry in CodeTypes is a known code type name.
// If CodeTypes is empty, it defaults to all AllCodeTypes names.
func (c *Config) validateCodeTypes() error {
	if len(c.CodeTypes) == 0 {
		c.CodeTypes = make([]string, len(model.AllCodeTypes))
		for i, ct := range model.AllCodeTypes {
			c.CodeTypes[i] = ct.Name
		}
		return nil
	}
	for _, name := range c.CodeTypes {
		if _, ok := model.CodeTypeByName(name); !ok {
			return fmt.Errorf("unknown code type %q in config", name)
		}
	}
	return nil
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
		return fmt.Errorf("--dsn or DATABASE_URL is required")
	}
	return nil
}
