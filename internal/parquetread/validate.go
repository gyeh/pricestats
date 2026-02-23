package parquetread

import (
	"fmt"
	"strings"

	"github.com/parquet-go/parquet-go"

	"github.com/gyeh/pricestats/internal/model"
)

// ValidateSchema checks that the Parquet schema contains all required columns
// and at least one code column.
func ValidateSchema(schema *parquet.Schema) error {
	columns := make(map[string]bool)
	for _, field := range schema.Fields() {
		columns[strings.ToLower(field.Name())] = true
	}

	// Required columns
	required := []string{"description", "hospital_name"}
	for _, col := range required {
		if !columns[col] {
			return fmt.Errorf("missing required column: %s", col)
		}
	}

	// At least one code column must be present
	codeCols := model.CodeTypeColumns()
	hasCode := false
	for _, col := range codeCols {
		if columns[col] {
			hasCode = true
			break
		}
	}
	if !hasCode {
		return fmt.Errorf("no code columns found; need at least one of: %s",
			strings.Join(codeCols, ", "))
	}

	return nil
}
