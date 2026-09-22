// Package config loads service configuration from the environment.
//
// Every value has a default that works against the local docker-compose stack,
// so a fresh clone runs without a .env file. Parse failures are collected
// rather than returned one at a time: a misconfigured deployment should report
// every bad value in a single log line instead of one per restart.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"time"
)

// Config is the full configuration for any Murmur binary. Services read only
// the sections they use — the gateway never opens Postgres, the fanout worker
// never serves GraphQL — but they share one loader so an operator has one set
// of variable names to learn.
type Config struct {
	Service         string
	Env             string
	LogLevel        slog.Level
	LogFormat       string
	HTTPAddr        string
	GRPCAddr        string
	ShutdownTimeout time.Duration
	Postgres        Postgres
	Redis           Redis
	NATS            NATS

	// Gateway-only settings. They live on the shared Config because one set
	// of variable names across four binaries is easier to operate than four
	// overlapping sets, and a service that ignores a value costs nothing.
	SocialAddr       string
	TimelineAddr     string
	RequestTimeout   time.Duration
	TimelineFanout   int
	OTLPEndpoint     string
	TraceSampleRatio float64

	// Fanout and timeline settings.
	TimelineCap        int
	FanoutConcurrency  int
	FanoutFollowerPage int
	FanoutMaxDeliver   int
	FanoutAckWait      time.Duration
	OutboxBatch        int
	OutboxInterval     time.Duration

	// FanoutThreshold is the follower count at or above which a post stops
	// being pushed to follower timelines and is merged in at read time
	// instead. Zero pushes everything. The default is the crossover the
	// phase 4 benchmark measured — see docs/adr-001-fanout-threshold.md.
	FanoutThreshold int64
	RouteCacheTTL   time.Duration
	HeavySetRefresh time.Duration

	// Post cache. The local TTL is the documented bound on how long a deleted
	// post can still be served by a replica that missed the invalidation
	// event; see the phase 5 notes.
	PostCacheEntries  int
	PostCacheLocalTTL time.Duration
	PostCacheRedisTTL time.Duration
}

// Postgres holds the source-of-truth database settings.
type Postgres struct {
	DSN            string
	MaxConns       int32
	MinConns       int32
	ConnectTimeout time.Duration
}

// Redis holds the timeline and cache store settings.
type Redis struct {
	Addr        string
	Password    string
	DB          int
	PoolSize    int
	DialTimeout time.Duration
}

// NATS holds the event bus settings.
type NATS struct {
	URL            string
	ConnectTimeout time.Duration
	MaxReconnects  int
}

// IsProduction reports whether the process is running in the production
// environment, which switches logging to JSON and disables developer aids.
func (c Config) IsProduction() bool { return c.Env == "production" }

// Load reads configuration for the named service. defaultHTTPAddr is the
// admin/HTTP listen address used when HTTP_ADDR is unset, which differs per
// binary so all four can run on one machine. Services that serve gRPC take
// their port from GRPC_ADDR, defaulting to the admin port plus 1000.
func Load(service, defaultHTTPAddr string) (Config, error) {
	var p parser

	cfg := Config{
		Service:         service,
		Env:             p.str("MURMUR_ENV", "development"),
		LogLevel:        p.level("LOG_LEVEL", slog.LevelInfo),
		LogFormat:       p.str("LOG_FORMAT", "text"),
		HTTPAddr:        p.str("HTTP_ADDR", defaultHTTPAddr),
		GRPCAddr:        p.str("GRPC_ADDR", defaultGRPCAddr(defaultHTTPAddr)),
		ShutdownTimeout: p.dur("SHUTDOWN_TIMEOUT", 20*time.Second),

		SocialAddr:   p.str("SOCIAL_ADDR", "localhost:9081"),
		TimelineAddr: p.str("TIMELINE_ADDR", "localhost:9082"),
		// A ceiling on the whole GraphQL query, inherited by every downstream
		// call, so a request the client has already abandoned stops costing
		// the backends anything.
		RequestTimeout: p.dur("REQUEST_TIMEOUT", 15*time.Second),
		TimelineFanout: p.num("TIMELINE_FANOUT", 25),
		// Empty disables tracing. Set to jaeger:4317 against the compose stack.
		OTLPEndpoint:     p.str("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		TraceSampleRatio: p.ratio("OTEL_TRACES_SAMPLER_ARG", 1.0),

		TimelineCap:        p.num("TIMELINE_CAP", 800),
		FanoutConcurrency:  p.num("FANOUT_CONCURRENCY", 8),
		FanoutFollowerPage: p.num("FANOUT_FOLLOWER_PAGE", 1000),
		// Five attempts before an event is parked. Enough to ride out a
		// restart of a dependency, few enough that a genuinely poisoned event
		// stops wasting capacity quickly.
		FanoutMaxDeliver: p.num("FANOUT_MAX_DELIVER", 5),
		// Longer than the slowest plausible fanout. Too short and JetStream
		// redelivers a message that is still being processed, doubling the
		// work; harmless, because the writes are idempotent, but wasteful.
		FanoutAckWait:  p.dur("FANOUT_ACK_WAIT", 60*time.Second),
		OutboxBatch:    p.num("OUTBOX_BATCH", 100),
		OutboxInterval: p.dur("OUTBOX_INTERVAL", 250*time.Millisecond),

		FanoutThreshold: int64(p.num("FANOUT_THRESHOLD", 10_000)),
		RouteCacheTTL:   p.dur("ROUTE_CACHE_TTL", 30*time.Second),
		HeavySetRefresh: p.dur("HEAVY_SET_REFRESH", 15*time.Second),

		PostCacheEntries:  p.num("POST_CACHE_ENTRIES", 10_000),
		PostCacheLocalTTL: p.dur("POST_CACHE_LOCAL_TTL", 5*time.Second),
		PostCacheRedisTTL: p.dur("POST_CACHE_REDIS_TTL", 10*time.Minute),

		Postgres: Postgres{
			DSN:            p.str("POSTGRES_DSN", "postgres://murmur:murmur@localhost:5432/murmur?sslmode=disable"),
			MaxConns:       int32(p.num("POSTGRES_MAX_CONNS", 16)),
			MinConns:       int32(p.num("POSTGRES_MIN_CONNS", 2)),
			ConnectTimeout: p.dur("POSTGRES_CONNECT_TIMEOUT", 5*time.Second),
		},

		Redis: Redis{
			Addr:        p.str("REDIS_ADDR", "localhost:6379"),
			Password:    p.str("REDIS_PASSWORD", ""),
			DB:          p.num("REDIS_DB", 0),
			PoolSize:    p.num("REDIS_POOL_SIZE", 32),
			DialTimeout: p.dur("REDIS_DIAL_TIMEOUT", 5*time.Second),
		},

		NATS: NATS{
			URL:            p.str("NATS_URL", "nats://localhost:4222"),
			ConnectTimeout: p.dur("NATS_CONNECT_TIMEOUT", 5*time.Second),
			// -1 means reconnect forever. A backend service should keep trying
			// rather than give up and require an operator to restart it.
			MaxReconnects: p.num("NATS_MAX_RECONNECTS", -1),
		},
	}

	return cfg, p.err()
}

// defaultGRPCAddr derives the gRPC port from the admin port by adding 1000, so
// social-svc on :8081 serves gRPC on :9081. One convention beats four
// hard-coded ports, and it keeps the two surfaces obviously paired in logs and
// in kubectl output.
func defaultGRPCAddr(httpAddr string) string {
	host, port, err := net.SplitHostPort(httpAddr)
	if err != nil {
		return ""
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		return ""
	}
	return net.JoinHostPort(host, strconv.Itoa(n+1000))
}

// parser reads environment variables, accumulating failures so Load can report
// all of them at once.
type parser struct {
	errs []error
}

func (p *parser) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func (p *parser) num(key string, def int) int {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: %q is not an integer", key, raw))
		return def
	}
	return n
}

func (p *parser) dur(key string, def time.Duration) time.Duration {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: %q is not a duration (try 20s, 500ms)", key, raw))
		return def
	}
	return d
}

// ratio reads a value that must land between 0 and 1. Out of range is an
// error rather than a clamp: a sample ratio of 100 almost certainly means
// somebody meant 100 percent, and silently reading it as 1 would hide that.
func (p *parser) ratio(key string, def float64) float64 {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: %q is not a number", key, raw))
		return def
	}
	if f < 0 || f > 1 {
		p.errs = append(p.errs, fmt.Errorf("%s: %v is outside 0..1", key, f))
		return def
	}
	return f
}

func (p *parser) level(key string, def slog.Level) slog.Level {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def
	}
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(raw)); err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: %q is not a level (debug, info, warn, error)", key, raw))
		return def
	}
	return lvl
}

func (p *parser) err() error {
	if len(p.errs) == 0 {
		return nil
	}
	return fmt.Errorf("invalid configuration: %w", errors.Join(p.errs...))
}
