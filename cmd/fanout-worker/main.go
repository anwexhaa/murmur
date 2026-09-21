// Command fanout-worker consumes post.created from JetStream and writes post
// IDs into follower timelines — or, for authors above the measured threshold,
// declines to fan out at all and leaves the work to read time.
//
// From Phase 3 it runs the consumer. For now it stands up the admin surface
// and proves it can reach both Redis and NATS.
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
	"github.com/anwexhaa/murmur/internal/platform/health"
	"github.com/anwexhaa/murmur/internal/platform/httpx"
	"github.com/anwexhaa/murmur/internal/platform/kv"
	"github.com/anwexhaa/murmur/internal/platform/lifecycle"
	"github.com/anwexhaa/murmur/internal/platform/logging"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fanout-worker exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("fanout-worker", ":8083")
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

	events, closeBus, err := bus.Open(ctx, cfg.NATS, log)
	if err != nil {
		return fmt.Errorf("nats: %w", err)
	}
	defer closeBus()

	checks := health.New(2 * time.Second)
	checks.Register("redis", func(ctx context.Context) error { return redis.Ping(ctx).Err() })
	checks.Register("nats", events.Healthy)

	mux := http.NewServeMux()
	mux.Handle("GET /healthz", health.LiveHandler())
	mux.Handle("GET /readyz", checks.ReadyHandler())

	return lifecycle.Run(ctx, log, cfg.ShutdownTimeout,
		httpx.Server("http", cfg.HTTPAddr, mux, log),
	)
}
