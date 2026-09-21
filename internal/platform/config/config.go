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
