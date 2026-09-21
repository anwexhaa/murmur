// Package db opens the Postgres connection pool.
//
// Postgres is the only source of truth in Murmur. Everything in Redis is a
// derived view that can be rebuilt from here, which is what makes it safe to
// flush the entire cache and watch the system heal.
package db

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anwexhaa/murmur/internal/platform/config"
)

// Open connects the pool and verifies it before returning, so a service with a
// bad DSN fails at startup rather than on its first request. The returned
// function closes the pool.
func Open(ctx context.Context, cfg config.Postgres, log *slog.Logger) (*pgxpool.Pool, func(), error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns

	// Every query becomes a span. This is the deepest level of the trace and
	// the one that answers the question the other levels only raise: when the
	// gateway makes sixty calls and each makes one query, the waterfall shows
	// sixty identical spans side by side, which is what an N+1 looks like.
	// Span names are the trimmed statement (otelpgx's default), so a trace
	// shows "SELECT" repeated sixty times rather than sixty copies of the same
	// long query text. The full SQL stays on the span as an attribute.
	poolCfg.ConnConfig.Tracer = otelpgx.NewTracer()

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("create postgres pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("ping postgres: %w", err)
	}

	log.Info("postgres connected",
		"host", poolCfg.ConnConfig.Host,
		"database", poolCfg.ConnConfig.Database,
		"max_conns", cfg.MaxConns,
	)

	return pool, func() {
		pool.Close()
		log.Info("postgres pool closed")
	}, nil
}
