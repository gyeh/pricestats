package normalize

import (
	"strings"
	"time"
)

// Common date formats found in hospital MRF files.
var dateFormats = []string{
	"2006-01-02",
	"01/02/2006",
	"1/2/2006",
	"01-02-2006",
	"2006/01/02",
	"January 2, 2006",
	"Jan 2, 2006",
	"2006-01-02T15:04:05Z",
	"2006-01-02T15:04:05",
}

// ParseDate attempts to parse a date string in multiple common formats.
// Returns nil if the input is empty or unparseable.
func ParseDate(s string) *time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	for _, fmt := range dateFormats {
		if t, err := time.Parse(fmt, s); err == nil {
			return &t
		}
	}
	return nil
}
