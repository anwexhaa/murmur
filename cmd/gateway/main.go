// Command gateway is Murmur's public edge.
//
// It serves the GraphQL API over HTTP, holds the gRPC connections to the
// backend services, and counts every downstream call it makes so the cost of
// the current design is a number rather than an opinion.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/99designs/gqlgen/graphql/playground"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/anwexhaa/murmur/internal/gateway"
	"github.com/anwexhaa/murmur/internal/gateway/gqlgen"
	"github.com/anwexhaa/murmur/internal/platform/bus"
	"github.com/anwexhaa/murmur/internal/platform/config"
	"github.com/anwexhaa/murmur/internal/platform/health"
	"github.com/anwexhaa/murmur/internal/platform/httpx"
	"github.com/anwexhaa/murmur/internal/platform/kv"
	"github.com/anwexhaa/murmur/internal/platform/lifecycle"
	"github.com/anwexhaa/murmur/internal/platform/logging"
	"github.com/anwexhaa/murmur/internal/platform/metrics"
	"github.com/anwexhaa/murmur/internal/platform/otelx"
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

	shutdownTracing, err := otelx.Setup(ctx, otelx.Config{
		ServiceName: cfg.Service,
		Environment: cfg.Env,
		Endpoint:    cfg.OTLPEndpoint,
		SampleRatio: cfg.TraceSampleRatio,
	}, log)
	if err != nil {
		// Tracing is observability, not a dependency. Log it and serve.
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

	clients, closeClients, err := gateway.Dial(ctx, cfg.SocialAddr, cfg.TimelineAddr, log)
	if err != nil {
		return fmt.Errorf("social client: %w", err)
	}
	defer closeClients()

	events, closeBus, err := bus.Open(ctx, cfg.NATS, log)
	if err != nil {
		return fmt.Errorf("nats: %w", err)
	}
	defer closeBus()

	registry := metrics.New()
	gatewayMetrics := gateway.NewMetrics(registry)

	hub := gateway.NewHub(events.NC, gateway.HubOptions{
		BufferSize: cfg.SubscriptionBuffer,
		MaxDrops:   int64(cfg.SubscriptionMaxDrops),
		Log:        log,
		Metrics:    gateway.NewHubMetrics(registry),
	})

	// One hydration per post rather than one per socket. The capacity run in
	// docs/phase6-subscriptions.md is what this is here for: without it, the
	// cost of delivering a post scales with the number of connected clients
	// instead of with the number of posts.
	hydrator, err := gateway.NewHydrator(clients.Social, gateway.HydratorOptions{
		Entries: cfg.PostCacheEntries,
		TTL:     cfg.PostCacheLocalTTL,
		Metrics: gateway.NewHydratorMetrics(registry),
	})
	if err != nil {
		return fmt.Errorf("hydrator: %w", err)
	}

	resolver := &gateway.Resolver{
		Clients:        clients,
		Log:            log,
		Hub:            hub,
		Hydrator:       hydrator,
		TimelineFanout: cfg.TimelineFanout,
	}

	srv := handler.New(gqlgen.NewExecutableSchema(gqlgen.Config{Resolvers: resolver}))
	// The WebSocket transport must be registered before POST and GET: gqlgen
	// picks the first transport whose Supports returns true, and the HTTP ones
	// would claim an upgrade request first.
	srv.AddTransport(transport.Websocket{
		KeepAlivePingInterval: cfg.SubscriptionPingInterval,
		// Same header as every other request. Phase 7 replaces it with a token,
		// and a WebSocket carries no Authorization header on the upgrade in
		// most browsers, so the init payload is where that will have to live.
		InitFunc: func(ctx context.Context, initPayload transport.InitPayload) (context.Context, *transport.InitPayload, error) {
			if viewer, ok := initPayload[gateway.ViewerHeader].(string); ok && viewer != "" {
				ctx = gateway.WithViewer(ctx, viewer)
			}
			return ctx, nil, nil
		},
	})
	srv.AddTransport(transport.POST{})
	srv.AddTransport(transport.GET{})
	srv.AddTransport(transport.Options{})
	// Automatic persisted queries are off until phase 7, where the query
	// allowlist that makes them a security control also arrives.
	srv.Use(extension.Introspection{})

	// The operation name lives in the request body, which the HTTP middleware
	// must not read. This hands it back once gqlgen has parsed the query, so
	// metrics are labelled by operation instead of by a path every GraphQL
	// request shares.
	srv.AroundOperations(func(ctx context.Context, next graphql.OperationHandler) graphql.ResponseHandler {
		if oc := graphql.GetOperationContext(ctx); oc != nil {
			gateway.RecordOperationName(ctx, oc.OperationName)
		}
		return next(ctx)
	})

	mux := http.NewServeMux()
	mux.Handle("GET /healthz", health.LiveHandler())
	mux.Handle("GET /metrics", registry.Handler())

	checks := health.New(2 * time.Second)
	checks.Register("redis", func(ctx context.Context) error { return redis.Ping(ctx).Err() })
	checks.Register("nats", events.Healthy)
	mux.Handle("GET /readyz", checks.ReadyHandler())

	// otelhttp opens the root span; Instrument then adds the request ID,
	// deadline, viewer and call counter inside it, so every downstream span
	// hangs off one trace and the log line carries that trace's ID.
	query := otelhttp.NewHandler(
		gateway.Instrument(srv, clients, gatewayMetrics, log, cfg.RequestTimeout),
		"graphql",
	)
	// Methods are listed explicitly rather than registering a bare "/query".
	// Go's ServeMux rejects a method-less pattern alongside "GET /{$}" as
	// ambiguous, and being explicit is better anyway: anything but these three
	// verbs gets a 405 from the mux instead of reaching the executor.
	mux.Handle("POST /query", query)
	mux.Handle("GET /query", query)
	mux.Handle("OPTIONS /query", query)

	if !cfg.IsProduction() {
		// "{$}" matches the root path exactly, not everything under it, so the
		// playground cannot shadow a route added later.
		mux.Handle("GET /{$}", playground.Handler("Murmur", "/query"))
		log.Info("graphql playground enabled", "url", "http://localhost"+cfg.HTTPAddr+"/")
	}

	return lifecycle.Run(ctx, log, cfg.ShutdownTimeout,
		httpx.Server("http", cfg.HTTPAddr, mux, log),
	)
}
