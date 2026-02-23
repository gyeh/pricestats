package logging

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

// Setup initializes a zerolog.Logger based on the requested format.
// format can be "text" (human-friendly console) or "json" (structured).
func Setup(format string) zerolog.Logger {
	if format == "text" {
		return zerolog.New(zerolog.ConsoleWriter{
			Out:        os.Stderr,
			TimeFormat: time.RFC3339,
		}).With().Timestamp().Logger()
	}
	return zerolog.New(os.Stderr).With().Timestamp().Logger()
}
