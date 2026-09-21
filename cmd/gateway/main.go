// Command gateway is Murmur's public edge.
//
// From Phase 2 it serves the GraphQL API and, from Phase 6, subscriptions over
// WebSocket. For now it stands up the admin surface and proves it can connect
// to Redis and shut down cleanly.
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
		slog.Error("gateway exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("gateway", ":8080")
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
