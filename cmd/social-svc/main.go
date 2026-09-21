// Command social-svc owns the write path: users, the follow graph, posts, and
// the transactional outbox that publishes post.created to the bus.
//
// It serves gRPC on GRPC_ADDR and an admin surface on HTTP_ADDR. The outbox
// relay arrives in phase 3.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"google.golang.org/grpc"

	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
	"github.com/anwexhaa/murmur/internal/domain"
	"github.com/anwexhaa/murmur/internal/platform/bus"
	"github.com/anwexhaa/murmur/internal/platform/config"
	"github.com/anwexhaa/murmur/internal/platform/db"
	"github.com/anwexhaa/murmur/internal/platform/grpcx"
	"github.com/anwexhaa/murmur/internal/platform/health"
	"github.com/anwexhaa/murmur/internal/platform/httpx"
	"github.com/anwexhaa/murmur/internal/platform/lifecycle"
	"github.com/anwexhaa/murmur/internal/platform/logging"
	"github.com/anwexhaa/murmur/internal/platform/otelx"
	"github.com/anwexhaa/murmur/internal/social"
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

	service := social.NewService(social.NewStore(pool), domain.NewIDGenerator(), time.Now)

	checks := health.New(2 * time.Second)
	checks.Register("postgres", pool.Ping)
	checks.Register("nats", events.Healthy)

	mux := http.NewServeMux()
	mux.Handle("GET /healthz", health.LiveHandler())
	mux.Handle("GET /readyz", checks.ReadyHandler())

	// The gRPC server is registered before the HTTP one so that, on shutdown,
	// components stop in reverse and the admin surface outlives the RPC
	// server — readiness can still answer while in-flight calls drain.
	return lifecycle.Run(ctx, log, cfg.ShutdownTimeout,
		grpcx.Server(grpcx.Options{
			Name: "grpc",
			Addr: cfg.GRPCAddr,
			Log:  log,
			Register: func(srv *grpc.Server) {
				socialv1.RegisterSocialServiceServer(srv, service)
			},
			// Reflection lets grpcurl drive the service without a local copy
			// of the schema. Off in production, where it is free schema
			// disclosure to anyone who can reach the port.
			Reflection: !cfg.IsProduction(),
		}),
		httpx.Server("http", cfg.HTTPAddr, mux, log),
	)
}
