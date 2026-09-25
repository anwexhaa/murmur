// Package kv opens the Redis client.
//
// Redis holds the per-user timeline sorted sets and the post cache. Nothing
// here is authoritative — see package db — so eviction is a performance event,
// never a data loss event, and from phase 8 an outage is a latency event
// rather than an availability one.
package kv

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/anwexhaa/murmur/internal/platform/config"
)

// Open connects to Redis. The returned function closes the client and its
// pool.
//
// A failed ping is a warning, not an error, and that is a deliberate reversal
// of what this function used to do.
//
// The reasoning is the same one that keeps Redis off the gateway's readiness
// probe. Nothing in Redis is authoritative; the services that read it now
// degrade to Postgres, and the one that writes it reports itself unready and
// waits. But the old behaviour — ping, fail, exit — meant a pod could not
// *start* while Redis was down, however well it would have coped once
// running. During a Redis incident that is the worst possible property: no
// rollout, no rescheduling, no scaling, and every pod that restarts for any
// unrelated reason stays down until the incident ends. A phase 8 chaos run
// turned three healthy gateways into a CrashLoopBackOff that way.
//
// go-redis dials lazily and reconnects on its own, so a client built against
// an unreachable server becomes useful the moment the server returns, with no
// restart and nothing to orchestrate.
func Open(ctx context.Context, cfg config.Redis, log *slog.Logger) (*redis.Client, func(), error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
		PoolSize: cfg.PoolSize,

		DialTimeout: cfg.DialTimeout,
		// Read and write timeouts matter more than the dial timeout once a
		// caller has a budget of its own. The gateway gives each downstream
		// gRPC call three seconds; a Redis operation that can block for five
		// spends that budget discovering something the caller could have been
		// told in a fraction of it. These are deliberately short: the answer
		// to "is Redis there" should arrive long before anybody upstream
		// gives up waiting for it.
		ReadTimeout:  cfg.OpTimeout,
		WriteTimeout: cfg.OpTimeout,

		// No retries. go-redis retries three times by default with backoff,
		// which is right for a transient blip on a server that is there and
		// exactly wrong for a server that is not: it turns one 500ms failure
		// into several seconds of rediscovering the same fact, and spends the
		// caller's whole budget doing it. A phase 8 chaos run showed the
		// effect directly -- one timeline replica fell back to Postgres and
		// served the read, while its identical sibling was still retrying
		// when the gateway's three-second deadline expired.
		//
		// The retry for this system is the fallback, and the fallback cannot
		// run until the first attempt gives up.
		MaxRetries: -1,
	})

	pingCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		log.Warn("redis is not reachable; starting anyway and retrying in the background",
			"addr", cfg.Addr,
			"error", err,
			"consequence", "reads degrade to the source of truth and rate limits are unenforced until it returns")
	} else {
		log.Info("redis connected", "addr", cfg.Addr, "db", cfg.DB, "pool_size", cfg.PoolSize)
	}

	return client, func() {
		if err := client.Close(); err != nil {
			log.Warn("closing redis", "error", err)
			return
		}
		log.Info("redis client closed")
	}, nil
}

// Reachable reports whether Redis is answering, for readiness probes on the
// services that genuinely cannot work without it.
func Reachable(client *redis.Client, timeout time.Duration) func(context.Context) error {
	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return client.Ping(ctx).Err()
	}
}
