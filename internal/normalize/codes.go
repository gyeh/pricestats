package normalize

import (
	"regexp"
	"strings"
)

var nonAlphanumeric = regexp.MustCompile(`[^A-Za-z0-9]`)

// NormalizeCode trims whitespace, uppercases, and strips non-alphanumeric characters.
// Returns nil if the input is nil or the result is empty.
func NormalizeCode(v *string) *string {
	if v == nil {
		return nil
	}
	s := strings.TrimSpace(*v)
	if s == "" {
		return nil
	}
	s = strings.ToUpper(s)
	s = nonAlphanumeric.ReplaceAllString(s, "")
	if s == "" {
		return nil
	}
	return &s
}
