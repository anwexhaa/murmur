// Command timeline-svc owns the read path.
//
// It reads a timeline that was materialised at write time and hydrates it
// through the social service. One Redis range per request, regardless of how
// many accounts the viewer follows.
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
	timelinev1 "github.com/anwexhaa/murmur/api/gen/murmur/timeline/v1"
	"github.com/anwexhaa/murmur/internal/platform/config"
	"github.com/anwexhaa/murmur/internal/platform/grpcx"
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

	social, err := grpc.NewClient(cfg.SocialAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("social client: %w", err)
	}
	defer func() { _ = social.Close() }()

	registry := metrics.New()
	service := timeline.NewService(
		timeline.NewStore(redis, cfg.TimelineCap),
		socialv1.NewSocialServiceClient(social),
		log,
	)

	checks := health.New(2 * time.Second)
	checks.Register("redis", func(ctx context.Context) error { return redis.Ping(ctx).Err() })

	mux := http.NewServeMux()
	mux.Handle("GET /healthz", health.LiveHandler())
	mux.Handle("GET /readyz", checks.ReadyHandler())
	mux.Handle("GET /metrics", registry.Handler())

	return lifecycle.Run(ctx, log, cfg.ShutdownTimeout,
		grpcx.Server(grpcx.Options{
			Name: "grpc",
			Addr: cfg.GRPCAddr,
			Log:  log,
			Register: func(srv *grpc.Server) {
				timelinev1.RegisterTimelineServiceServer(srv, service)
			},
			Reflection: !cfg.IsProduction(),
		}),
		httpx.Server("http", cfg.HTTPAddr, mux, log),
	)
}
