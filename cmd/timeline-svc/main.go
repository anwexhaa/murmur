// Command timeline-svc owns the read path: it ranges the per-user timeline
// sorted set, merges in posts from authors too large to fan out to, and pages
// the result.
//
// From Phase 3 it serves gRPC. For now it stands up the admin surface and
// proves it can reach Redis.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/anwexhaa/murmur/internal/platform/config"
	"github.com/anwexhaa/murmur/internal/platform/health"
	"github.com/anwexhaa/murmur/internal/platform/httpx"
	"github.com/anwexhaa/murmur/internal/platform/kv"
	"github.com/anwexhaa/murmur/internal/platform/lifecycle"
	"github.com/anwexhaa/murmur/internal/platform/logging"
)

func main() {
	if err := run(); err != nil {
		slog.Error("timeline-svc exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("timeline-svc", ":8082")
	if err != nil {
		return err
	}
	log := logging.New(cfg)
	ctx := context.Background()

	redis, closeRedis, err := kv.Open(ctx, cfg.Redis, log)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	defer closeRedis()

	checks := health.New(2 * time.Second)
	checks.Register("redis", func(ctx context.Context) error { return redis.Ping(ctx).Err() })

	mux := http.NewServeMux()
	mux.Handle("GET /healthz", health.LiveHandler())
	mux.Handle("GET /readyz", checks.ReadyHandler())

	return lifecycle.Run(ctx, log, cfg.ShutdownTimeout,
		httpx.Server("http", cfg.HTTPAddr, mux, log),
	)
}
