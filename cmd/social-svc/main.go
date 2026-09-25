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

	authv1 "github.com/anwexhaa/murmur/api/gen/murmur/auth/v1"
	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
	"github.com/anwexhaa/murmur/internal/auth"
	"github.com/anwexhaa/murmur/internal/domain"
	"github.com/anwexhaa/murmur/internal/platform/authx"
	"github.com/anwexhaa/murmur/internal/platform/bus"
	"github.com/anwexhaa/murmur/internal/platform/config"
	"github.com/anwexhaa/murmur/internal/platform/db"
	"github.com/anwexhaa/murmur/internal/platform/grpcx"
	"github.com/anwexhaa/murmur/internal/platform/health"
	"github.com/anwexhaa/murmur/internal/platform/httpx"
	"github.com/anwexhaa/murmur/internal/platform/lifecycle"
	"github.com/anwexhaa/murmur/internal/platform/logging"
	"github.com/anwexhaa/murmur/internal/platform/metrics"
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

	if err := bus.EnsureStreams(ctx, events.JS); err != nil {
		return err
	}

	// social-svc verifies and never signs. It is the one process that holds
	// the user table, the credentials and the follow graph, and it holds no
	// key that could mint an identity -- so a compromise here cannot be turned
	// into impersonating a user to anything else.
	verifier, err := authx.LoadVerifier(cfg, auth.AudienceInternal, log)
	if err != nil {
		return err
	}

	store := social.NewStore(pool)
	service := social.NewService(store, domain.NewIDGenerator(), time.Now)

	authService, err := social.NewAuthService(store, auth.NewHasher(auth.HashParams{
		Memory:      uint32(cfg.Argon2Memory),
		Time:        uint32(cfg.Argon2Time),
		Parallelism: uint8(cfg.Argon2Threads),
	}), domain.NewIDGenerator(), log, time.Now)
	if err != nil {
		return fmt.Errorf("auth service: %w", err)
	}

	registry := metrics.New()
	relay := social.NewRelay(store, events.JS, social.RelayOptions{
		Batch:    cfg.OutboxBatch,
		Interval: cfg.OutboxInterval,
		Log:      log,
		Metrics:  social.NewRelayMetrics(registry),
	})

	// Postgres is on readiness here, and the asymmetry with the gateway is
	// the point rather than an inconsistency. This service is the source of
	// truth: without its pool it can answer nothing, and a replica whose pool
	// has broken while its siblings' pools are healthy is exactly the case
	// readiness exists for. NATS is not checked, for the same reason the
	// gateway does not check it -- the outbox relay falling behind is a
	// backlog to alert on, not a reason to stop serving reads.
	checks := health.New(2 * time.Second)
	checks.Register("postgres", pool.Ping)

	mux := http.NewServeMux()
	mux.Handle("GET /healthz", health.LiveHandler())
	mux.Handle("GET /readyz", checks.ReadyHandler())
	mux.Handle("GET /metrics", registry.Handler())

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
				authv1.RegisterAuthServiceServer(srv, authService)
			},
			Verifier: verifier,
			// Reflection lets grpcurl drive the service without a local copy
			// of the schema. Off in production, where it is free schema
			// disclosure to anyone who can reach the port.
			Reflection: !cfg.IsProduction(),
		}),
		relay.Component(),
		httpx.Server("http", cfg.HTTPAddr, mux, log),
	)
}
