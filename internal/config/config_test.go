package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFromFile_Valid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte("code_types:\n  - CPT\n  - NDC\n"), 0644)

	var c Config
	if err := c.LoadFromFile(path); err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}
	if len(c.CodeTypes) != 2 {
		t.Fatalf("expected 2 code types, got %d", len(c.CodeTypes))
	}
	if c.CodeTypes[0] != "CPT" || c.CodeTypes[1] != "NDC" {
		t.Errorf("unexpected code types: %v", c.CodeTypes)
	}
}

func TestLoadFromFile_UnknownCodeType(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte("code_types:\n  - CPT\n  - BOGUS\n"), 0644)

	var c Config
	err := c.LoadFromFile(path)
	if err == nil {
		t.Fatal("expected error for unknown code type")
	}
}

func TestLoadFromFile_EmptyDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte("code_types: []\n"), 0644)

	var c Config
	if err := c.LoadFromFile(path); err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}
	if len(c.CodeTypes) != 5 {
		t.Errorf("expected 5 default code types, got %d: %v", len(c.CodeTypes), c.CodeTypes)
	}
}

func TestLoadFromFile_MissingFile(t *testing.T) {
	var c Config
	err := c.LoadFromFile("/nonexistent/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}
