// Package logging builds the process logger.
package logging

import (
	"log/slog"
	"os"

	"github.com/anwexhaa/murmur/internal/platform/config"
)

// New returns a logger tagged with the service name, so a single log stream
// from four binaries is still readable. Production defaults to JSON because
// that is what a log pipeline can index; development stays on text because
// that is what a human can read.
func New(cfg config.Config) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.LogLevel}

	var handler slog.Handler
	if cfg.LogFormat == "json" || cfg.IsProduction() {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	return slog.New(handler).With(
		slog.String("service", cfg.Service),
		slog.String("env", cfg.Env),
	)
}
