// Command gateway is Murmur's public edge.
//
// It serves the GraphQL API over HTTP, holds the gRPC connections to the
// backend services, and counts every downstream call it makes so the cost of
// the current design is a number rather than an opinion.
package main

import (
	"context"
	"crypto/ed25519"
	"errors"
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

	"github.com/anwexhaa/murmur/internal/auth"
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
	"github.com/anwexhaa/murmur/internal/platform/ratelimit"
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

	// The keys come first: everything downstream either signs with them or
	// verifies against them, and a gateway that cannot do either has no
	// business accepting a request.
	signer, verifier, err := authKeys(cfg, log)
	if err != nil {
		return err
	}

	clients, closeClients, err := gateway.Dial(ctx, cfg.SocialAddr, cfg.TimelineAddr, signer, log)
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

	var limiter *ratelimit.Limiter
	if cfg.RateLimit {
		limiter = ratelimit.New(redis, ratelimit.NewMetrics(registry), nil)
		if err := limiter.Prepare(ctx); err != nil {
			return fmt.Errorf("rate limiter: %w", err)
		}
	} else {
		log.Warn("rate limiting is disabled; every limit is unenforced")
	}

	resolver := &gateway.Resolver{
		Clients:        clients,
		Log:            log,
		Hub:            hub,
		Hydrator:       hydrator,
		Sessions:       gateway.NewSessions(clients.Auth, clients.Social, signer, cfg.AccessTokenTTL, nil),
		Limiter:        limiter,
		TimelineFanout: cfg.TimelineFanout,
	}

	srv := handler.New(gqlgen.NewExecutableSchema(gqlgen.Config{
		Resolvers:  resolver,
		Complexity: gateway.Complexity(),
	}))
	// The WebSocket transport must be registered before POST and GET: gqlgen
	// picks the first transport whose Supports returns true, and the HTTP ones
	// would claim an upgrade request first.
	srv.AddTransport(transport.Websocket{
		KeepAlivePingInterval: cfg.SubscriptionPingInterval,
		// The same bearer token as every other request, carried in the init
		// payload rather than a header: a browser cannot set headers on a
		// WebSocket upgrade, so this is the only place a credential can
		// travel. It is verified with the same verifier and the same audience
		// as an HTTP request -- the transport differs, the trust does not.
		InitFunc: func(ctx context.Context, initPayload transport.InitPayload) (context.Context, *transport.InitPayload, error) {
			header, _ := initPayload[gateway.AuthorizationHeader].(string)
			ctx = gateway.Authenticate(ctx, verifier, header)

			// A subscription is long-lived and personal; an anonymous one
			// would subscribe to nobody's timeline and sit there consuming a
			// connection slot. Refusing at the handshake is the cheapest
			// point to refuse.
			if gateway.Viewer(ctx) == "" {
				return ctx, nil, errUnauthenticatedSocket
			}
			return ctx, nil, nil
		},
	})
	srv.AddTransport(transport.POST{})
	srv.AddTransport(transport.GET{})
	srv.AddTransport(transport.Options{})

	// A complexity budget, not a depth cap.
	//
	// Depth alone is the wrong measure: `{ timeline(first: 100) { edges { node
	// { author { followerCount } } } } }` is four levels deep and a hundred
	// authors wide, and a depth limit of ten waves it through. Complexity
	// multiplies a list's cost by the page size it asked for, so the thing
	// that scales is the thing that is counted.
	srv.Use(extension.FixedComplexityLimit(cfg.QueryComplexityLimit))

	// Introspection is free schema disclosure: it hands an attacker every
	// type, field and argument without them having to guess. On in
	// development, where the playground needs it and there is nothing to
	// protect; off in production unless somebody turns it on deliberately.
	if cfg.Introspection {
		srv.Use(extension.Introspection{})
	}

	// Automatic persisted queries stay off. They are a bandwidth optimisation
	// that is only a security control when paired with an allowlist of
	// approved query hashes, and without the allowlist they are a way to have
	// the server cache arbitrary queries on an attacker's behalf.

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
		gateway.Instrument(srv, clients, verifier, gatewayMetrics, log, cfg.RequestTimeout),
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

// errUnauthenticatedSocket refuses a subscription handshake with no viewer.
var errUnauthenticatedSocket = errors.New("not signed in: send an Authorization: Bearer <access token> field in connection_init")

// authKeys loads the deployment's signing keypair.
//
// The gateway needs both halves: it signs access tokens and the assertions it
// sends to the backends, and it verifies the access tokens that come back on
// the next request. A configured verifying key is honoured when it is present
// so a deployment can rotate by trusting the new public key before issuing
// with the new private one.
//
// With no key at all it generates one and says so, loudly. That is a working
// development stack and a broken production one: a per-process key means a
// token issued by replica A is rejected by replica B, which is an
// authentication system that survives exactly until it is scaled.
func authKeys(cfg config.Config, log *slog.Logger) (*auth.Signer, *auth.Verifier, error) {
	var (
		private ed25519.PrivateKey
		err     error
	)

	if cfg.AuthSigningKey != "" {
		private, err = auth.DecodeSeed(cfg.AuthSigningKey)
		if err != nil {
			return nil, nil, fmt.Errorf("AUTH_SIGNING_KEY: %w", err)
		}
	} else {
		if cfg.IsProduction() {
			return nil, nil, errors.New("AUTH_SIGNING_KEY is required in production")
		}
		private, err = auth.GenerateKey()
		if err != nil {
			return nil, nil, err
		}
		log.Warn("no AUTH_SIGNING_KEY set; generated an ephemeral one",
			"consequence", "tokens will not survive a restart and will not be accepted by another replica",
			"public_key", auth.EncodePublicKey(private.Public().(ed25519.PublicKey)))
	}

	signer, err := auth.NewSigner(private, auth.ServiceGateway)
	if err != nil {
		return nil, nil, err
	}

	public := signer.PublicKey()
	if cfg.AuthVerifyingKey != "" {
		if public, err = auth.DecodePublicKey(cfg.AuthVerifyingKey); err != nil {
			return nil, nil, fmt.Errorf("AUTH_VERIFYING_KEY: %w", err)
		}
	}

	verifier, err := auth.NewVerifier(public, auth.AudienceClient)
	if err != nil {
		return nil, nil, err
	}
	return signer, verifier, nil
}
