// Command social-svc owns the write path: users, the follow graph, posts, and
// the transactional outbox that publishes post.created to the bus.
//
// From Phase 1 it serves gRPC. For now it stands up the admin surface and
// proves it can reach both Postgres and NATS.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/anwexhaa/murmur/internal/platform/bus"
	"github.com/anwexhaa/murmur/internal/platform/config"
	"github.com/anwexhaa/murmur/internal/platform/db"
	"github.com/anwexhaa/murmur/internal/platform/health"
	"github.com/anwexhaa/murmur/internal/platform/httpx"
	"github.com/anwexhaa/murmur/internal/platform/lifecycle"
	"github.com/anwexhaa/murmur/internal/platform/logging"
)

func main() {
	if err := run(); err != nil {
		slog.Error("social-svc exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("social-svc", ":8081")
	if err != nil {
		return err
	}
	log := logging.New(cfg)
	ctx := context.Background()

	pool, closePool, err := db.Open(ctx, cfg.Postgres, log)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer closePool()

	events, closeBus, err := bus.Open(ctx, cfg.NATS, log)
	if err != nil {
		return fmt.Errorf("nats: %w", err)
	}
	defer closeBus()

	checks := health.New(2 * time.Second)
	checks.Register("postgres", pool.Ping)
	checks.Register("nats", events.Healthy)

	mux := http.NewServeMux()
	mux.Handle("GET /healthz", health.LiveHandler())
	mux.Handle("GET /readyz", checks.ReadyHandler())

	return lifecycle.Run(ctx, log, cfg.ShutdownTimeout,
		httpx.Server("http", cfg.HTTPAddr, mux, log),
	)
}
