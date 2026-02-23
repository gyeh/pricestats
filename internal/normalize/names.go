package normalize

import (
	"regexp"
	"strings"
)

var multiSpace = regexp.MustCompile(`\s+`)

// NormalizeName lowercases, collapses whitespace, and trims the input.
// Returns nil if the input is nil or the result is empty.
func NormalizeName(v *string) *string {
	if v == nil {
		return nil
	}
	s := strings.TrimSpace(*v)
	if s == "" {
		return nil
	}
	s = strings.ToLower(s)
	s = multiSpace.ReplaceAllString(s, " ")
	return &s
}
