// Package kv opens the Redis client.
//
// Redis holds the per-user timeline sorted sets and the post cache. Nothing
// here is authoritative — see package db — so eviction is a performance event,
// never a data loss event.
package kv

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"

	"github.com/anwexhaa/murmur/internal/platform/config"
)

// Open connects to Redis and pings it before returning. The returned function
// closes the client and its pool.
func Open(ctx context.Context, cfg config.Redis, log *slog.Logger) (*redis.Client, func(), error) {
	client := redis.NewClient(&redis.Options{
		Addr:        cfg.Addr,
		Password:    cfg.Password,
		DB:          cfg.DB,
		PoolSize:    cfg.PoolSize,
		DialTimeout: cfg.DialTimeout,
	})

	pingCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, nil, fmt.Errorf("ping redis at %s: %w", cfg.Addr, err)
	}

	log.Info("redis connected", "addr", cfg.Addr, "db", cfg.DB, "pool_size", cfg.PoolSize)

	return client, func() {
		if err := client.Close(); err != nil {
			log.Warn("closing redis", "error", err)
			return
		}
		log.Info("redis client closed")
	}, nil
}
