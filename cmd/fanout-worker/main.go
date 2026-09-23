// Command fanout-worker consumes post.created and writes post IDs into
// follower timelines.
//
// It holds no inbound API beyond its health probes. Everything it does is
// driven by the bus, which is what lets it be scaled, restarted and killed
// without anything upstream noticing.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
	"github.com/anwexhaa/murmur/internal/fanout"
	"github.com/anwexhaa/murmur/internal/platform/bus"
	"github.com/anwexhaa/murmur/internal/platform/config"
	"github.com/anwexhaa/murmur/internal/platform/health"
	"github.com/anwexhaa/murmur/internal/platform/httpx"
	"github.com/anwexhaa/murmur/internal/platform/kv"
	"github.com/anwexhaa/murmur/internal/platform/lifecycle"
	"github.com/anwexhaa/murmur/internal/platform/logging"
	"github.com/anwexhaa/murmur/internal/platform/metrics"
	"github.com/anwexhaa/murmur/internal/platform/otelx"
	"github.com/anwexhaa/murmur/internal/timeline"
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

	shutdownTracing, err := otelx.Setup(ctx, otelx.Config{
		ServiceName: cfg.Service,
		Environment: cfg.Env,
		Endpoint:    cfg.OTLPEndpoint,
		SampleRatio: cfg.TraceSampleRatio,
	}, log)
	if err != nil {
		log.Warn("tracing setup failed, continuing without it", "error", err)
	}
	defer func() {
		if err := shutdownTracing(context.Background()); err != nil {
			log.Warn("flushing traces", "error", err)
		}
	}()

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

	// Every service that touches the bus ensures the stream, so there is no
	// ordering requirement between them and no provisioning step to forget.
	if err := bus.EnsureStreams(ctx, events.JS); err != nil {
		return err
	}
	consumer, err := bus.EnsureFanoutConsumer(ctx, events.JS, cfg.FanoutMaxDeliver, cfg.FanoutAckWait)
	if err != nil {
		return err
	}

	social, err := grpc.NewClient(cfg.SocialAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("social client: %w", err)
	}
	defer func() { _ = social.Close() }()

	registry := metrics.New()
	worker := fanout.New(
		consumer,
		events.JS,
		events.NC,
		socialv1.NewSocialServiceClient(social),
		timeline.NewStore(redis, cfg.TimelineCap),
		fanout.Options{
			Concurrency:   cfg.FanoutConcurrency,
			FollowerPage:  cfg.FanoutFollowerPage,
			MaxDeliver:    cfg.FanoutMaxDeliver,
			AckWait:       cfg.FanoutAckWait,
			Threshold:     cfg.FanoutThreshold,
			RouteCacheTTL: cfg.RouteCacheTTL,
			Log:           log,
			Metrics:       fanout.NewMetrics(registry),
		})

	checks := health.New(2 * time.Second)
	checks.Register("redis", func(ctx context.Context) error { return redis.Ping(ctx).Err() })
	checks.Register("nats", events.Healthy)

	mux := http.NewServeMux()
	mux.Handle("GET /healthz", health.LiveHandler())
	mux.Handle("GET /readyz", checks.ReadyHandler())
	mux.Handle("GET /metrics", registry.Handler())

	// The consumer is listed first so it is stopped last: the admin surface
	// outlives it and can still answer probes while in-flight fanouts finish.
	return lifecycle.Run(ctx, log, cfg.ShutdownTimeout,
		worker.Component(),
		httpx.Server("http", cfg.HTTPAddr, mux, log),
	)
}
