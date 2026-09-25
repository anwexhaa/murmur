// Package ratelimit implements a token bucket in Redis.
//
// Redis rather than process memory, because the gateway runs as several
// replicas and a per-process limiter divides the real limit by however many
// pods happen to be running. That is not a limit anybody can reason about: it
// changes when the autoscaler acts, and the effective allowance is highest
// exactly when the system is busiest.
//
// A token bucket rather than a fixed window, because a fixed window lets a
// caller spend its whole allowance in the last millisecond of one window and
// again in the first of the next -- twice the intended rate, at the worst
// possible instant. The bucket refills continuously, so the burst it permits
// is the capacity and nothing larger.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// Rule is one bucket's shape.
type Rule struct {
	// Capacity is the burst: how many requests can arrive at once with a full
	// bucket.
	Capacity float64
	// Refill is tokens added per second, which is the sustained rate.
	Refill float64
	// Cost is what one request takes. Above one for operations that are
	// expensive out of proportion to their size -- a login runs argon2 over
	// 19MB and is worth more than a timeline read.
	Cost float64
}

// Valid reports whether a rule can be enforced.
func (r Rule) Valid() bool {
	return r.Capacity > 0 && r.Refill > 0 && r.Cost > 0 && r.Cost <= r.Capacity
}

// Decision is the answer for one request.
type Decision struct {
	Allowed bool
	// Remaining tokens after this request, for the response header that lets
	// a well-behaved client slow down before it is refused.
	Remaining int
	// RetryAfter is how long until the bucket holds enough for this request.
	// Zero when allowed.
	RetryAfter time.Duration
}

// Metrics are the limiter's instruments.
type Metrics struct {
	// Decisions is labelled by scope and outcome. The ratio is what tells an
	// operator whether a limit is protecting the service or breaking a
	// legitimate client, which a raw count of rejections cannot.
	Decisions *prometheus.CounterVec
	// Failures counts requests the limiter could not decide, which are
	// allowed through. See Allow.
	Failures prometheus.Counter
}

// NewMetrics registers the limiter's instruments.
func NewMetrics(registry prometheus.Registerer) *Metrics {
	m := &Metrics{
		Decisions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "murmur", Subsystem: "ratelimit", Name: "decisions_total",
			Help: "Rate limit decisions by scope and outcome.",
		}, []string{"scope", "outcome"}),
		Failures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "murmur", Subsystem: "ratelimit", Name: "failures_total",
			Help: "Requests allowed because the limiter could not be reached.",
		}),
	}
	registry.MustRegister(m.Decisions, m.Failures)
	return m
}

// Limiter enforces rules against Redis.
type Limiter struct {
	client  *redis.Client
	script  *redis.Script
	now     func() time.Time
	metrics *Metrics
}

// New builds a limiter over an existing Redis client.
func New(client *redis.Client, metrics *Metrics, now func() time.Time) *Limiter {
	if now == nil {
		now = time.Now
	}
	return &Limiter{
		client:  client,
		script:  redis.NewScript(takeScript),
		now:     now,
		metrics: metrics,
	}
}

// Prepare loads the script into Redis so the hot path can use EVALSHA.
//
// Explicit, for the reason phase 3 learned the hard way: go-redis cannot fall
// back from EVALSHA to EVAL inside a pipeline, so relying on the automatic
// fallback works until the first time Redis restarts and then fails an entire
// batch. Loading at startup and handling NOSCRIPT ourselves is the only
// arrangement that survives a Redis restart under load.
func (l *Limiter) Prepare(ctx context.Context) error {
	if err := l.script.Load(ctx, l.client).Err(); err != nil {
		return fmt.Errorf("load rate limit script: %w", err)
	}
	return nil
}

// takeScript refills the bucket and takes from it, atomically.
//
// Atomicity is the entire reason this is a script rather than a read, some
// arithmetic in Go, and a write. Two gateway replicas serving the same user at
// the same instant would both read the same token count, both decide there was
// room, and both write back -- and the limit would be per replica again, which
// is what using Redis was supposed to fix.
//
// The clock comes from the caller rather than from redis.call('TIME'). Redis's
// own clock would remove replica skew entirely, and the reason not to use it
// is testability: a limiter whose time cannot be controlled can only be tested
// with sleeps. The skew this trades away is milliseconds of NTP drift against
// buckets that refill over seconds.
const takeScript = `
local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill   = tonumber(ARGV[2])
local now_ms   = tonumber(ARGV[3])
local cost     = tonumber(ARGV[4])
local ttl_ms   = tonumber(ARGV[5])

local bucket  = redis.call('HMGET', key, 'tokens', 'updated')
local tokens  = tonumber(bucket[1])
local updated = tonumber(bucket[2])

if tokens == nil or updated == nil then
  tokens  = capacity
  updated = now_ms
end

-- A clock that went backwards must not create tokens.
local elapsed = now_ms - updated
if elapsed < 0 then elapsed = 0 end

tokens = math.min(capacity, tokens + (elapsed * refill / 1000.0))

local allowed = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
end

redis.call('HSET', key, 'tokens', tokens, 'updated', now_ms)
-- The key expires once the bucket would have refilled completely, so an idle
-- caller costs nothing. Letting them accumulate would mean one key per user
-- forever, which is a memory leak with a slow fuse.
redis.call('PEXPIRE', key, ttl_ms)

local retry_ms = 0
if allowed == 0 then
  retry_ms = math.ceil(((cost - tokens) / refill) * 1000.0)
end

return {allowed, math.floor(tokens), retry_ms}
`

// Key builds a bucket key. Scope separates the limits so a user hammering
// login does not spend the allowance they need to read their timeline.
func Key(scope, identity string) string {
	return "rl:" + scope + ":" + identity
}

// Allow takes one request's cost from a bucket.
//
// It fails open. A limiter that cannot reach Redis has a choice between
// refusing every request and allowing every request, and refusing turns a
// cache outage into a total outage -- the rate limiter, a protective measure,
// becomes the thing that takes the service down. Allowing degrades to the
// behaviour of the previous six phases, which was to have no limiter at all.
//
// This is a real trade and not a free one: while Redis is down there is no
// limit. The failure is counted so it is visible rather than silent, and the
// alert belongs on that counter.
func (l *Limiter) Allow(ctx context.Context, scope, identity string, rule Rule) (Decision, error) {
	if !rule.Valid() {
		return Decision{Allowed: true}, fmt.Errorf("ratelimit: rule %+v is not enforceable", rule)
	}

	nowMS := l.now().UnixMilli()
	// Long enough for the bucket to refill from empty, so a key only outlives
	// the state it holds.
	ttlMS := int64((rule.Capacity/rule.Refill)*1000) + 1000

	args := []any{rule.Capacity, rule.Refill, nowMS, rule.Cost, ttlMS}
	keys := []string{Key(scope, identity)}

	raw, err := l.client.EvalSha(ctx, l.script.Hash(), keys, args...).Result()
	if err != nil && isNoScript(err) {
		// Redis restarted and forgot the script. Reload and retry once.
		raw, err = l.script.Eval(ctx, l.client, keys, args...).Result()
	}
	if err != nil {
		if l.metrics != nil {
			l.metrics.Failures.Inc()
		}
		return Decision{Allowed: true}, fmt.Errorf("ratelimit: %w", err)
	}

	decision, err := parse(raw)
	if err != nil {
		if l.metrics != nil {
			l.metrics.Failures.Inc()
		}
		return Decision{Allowed: true}, err
	}

	if l.metrics != nil {
		outcome := "allowed"
		if !decision.Allowed {
			outcome = "limited"
		}
		l.metrics.Decisions.WithLabelValues(scope, outcome).Inc()
	}
	return decision, nil
}

func parse(raw any) (Decision, error) {
	values, ok := raw.([]any)
	if !ok || len(values) != 3 {
		return Decision{Allowed: true}, errors.New("ratelimit: unexpected script result")
	}

	allowed, ok1 := values[0].(int64)
	remaining, ok2 := values[1].(int64)
	retryMS, ok3 := values[2].(int64)
	if !ok1 || !ok2 || !ok3 {
		return Decision{Allowed: true}, errors.New("ratelimit: unexpected script result types")
	}

	return Decision{
		Allowed:    allowed == 1,
		Remaining:  int(remaining),
		RetryAfter: time.Duration(retryMS) * time.Millisecond,
	}, nil
}

// isNoScript reports whether Redis has forgotten the cached script, which
// happens on restart and on SCRIPT FLUSH.
func isNoScript(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOSCRIPT")
}
