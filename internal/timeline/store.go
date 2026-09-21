// Package timeline owns the materialised per-user feeds in Redis.
//
// Nothing here is authoritative. Every timeline can be rebuilt from Postgres,
// which is what makes it safe to flush the whole keyspace, to cap each feed at
// a few hundred entries, and to let Redis evict under memory pressure. A
// timeline going missing is a latency event, never a data-loss event.
package timeline

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

// DefaultCap is how many post IDs a timeline keeps.
//
// Nobody scrolls eight hundred posts back. Beyond the cap the read path falls
// through to the author's own history in Postgres, so the cap trades a rare
// slow deep-scroll for bounded, predictable memory across every user — which
// is the trade that lets a feed service size its cache at all.
const DefaultCap = 800

// Store reads and writes timelines.
type Store struct {
	client *redis.Client
	cap    int

	push *redis.Script

	// loaded guards the script cache. See ensureLoaded.
	mu     sync.Mutex
	loaded bool
}

// NewStore returns a store over an existing Redis client.
func NewStore(client *redis.Client, capacity int) *Store {
	if capacity <= 0 {
		capacity = DefaultCap
	}
	return &Store{
		client: client,
		cap:    capacity,
		push:   redis.NewScript(pushScript),
	}
}

// ensureLoaded makes sure Redis has the script cached.
//
// Outside a pipeline, go-redis runs a script by trying EVALSHA and falling
// back to EVAL when Redis answers NOSCRIPT. Inside a pipeline it cannot: the
// commands are queued and the NOSCRIPT only surfaces at Exec, long after the
// chance to substitute EVAL has passed. So the script is loaded explicitly,
// once, and Push sends EVALSHA — which is also what keeps a thousand-follower
// fanout from shipping a thousand copies of the script body.
func (s *Store) ensureLoaded(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.loaded {
		return nil
	}
	if err := s.push.Load(ctx, s.client).Err(); err != nil {
		return fmt.Errorf("load timeline script: %w", err)
	}
	s.loaded = true
	return nil
}

// forget marks the script cache stale, so the next Push reloads it.
func (s *Store) forget() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loaded = false
}

// Cap reports the configured timeline length.
func (s *Store) Cap() int { return s.cap }

// Key returns the Redis key holding one user's timeline.
func Key(userID string) string { return "tl:" + userID }

// pushScript adds a post to a timeline and trims it back to the cap.
//
// A script rather than two commands, because ZADD and ZREMRANGEBYRANK are
// separate round trips and a fanout worker is not the only writer. Two workers
// interleaving their adds and trims can leave a timeline above the cap — not
// catastrophic, but it is an invariant that either holds or does not, and Redis
// runs a script to completion without interleaving anything else.
//
// ZADD of a member already present updates its score and adds nothing. That
// single property is what makes redelivery from JetStream safe: replaying an
// event re-runs this script, and the timeline is unchanged. No dedup table, no
// processed-message set, no coordination.
const pushScript = `
local key   = KEYS[1]
local limit = tonumber(ARGV[1])
local score = ARGV[2]
local id    = ARGV[3]

redis.call('ZADD', key, score, id)
redis.call('ZREMRANGEBYRANK', key, 0, -limit - 1)
return redis.call('ZCARD', key)
`

// Push adds one post to many timelines in a single pipeline.
//
// Pipelining matters more than it looks: a thousand-follower fanout is a
// thousand script invocations, and at one round trip each that is a thousand
// network waits. Pipelined, it is one.
func (s *Store) Push(ctx context.Context, followerIDs []string, postID string, score int64) error {
	if len(followerIDs) == 0 {
		return nil
	}

	if err := s.ensureLoaded(ctx); err != nil {
		return err
	}

	err := s.pipelinePush(ctx, followerIDs, postID, score)
	if err != nil && isNoScript(err) {
		// Redis restarted, or somebody ran SCRIPT FLUSH. The cache is gone,
		// not the data. Reload and try once more rather than failing the event
		// and waiting for redelivery.
		s.forget()
		if loadErr := s.ensureLoaded(ctx); loadErr != nil {
			return loadErr
		}
		err = s.pipelinePush(ctx, followerIDs, postID, score)
	}
	if err != nil {
		return fmt.Errorf("push to %d timelines: %w", len(followerIDs), err)
	}
	return nil
}

func (s *Store) pipelinePush(ctx context.Context, followerIDs []string, postID string, score int64) error {
	scoreArg := strconv.FormatInt(score, 10)
	limitArg := strconv.Itoa(s.cap)
	sha := s.push.Hash()

	pipe := s.client.Pipeline()
	for _, follower := range followerIDs {
		pipe.EvalSha(ctx, sha, []string{Key(follower)}, limitArg, scoreArg, postID)
	}

	// Exec reports the first command error. There is nothing useful to do with
	// a partial failure here: the event has not been acknowledged yet, so
	// returning an error redelivers it, and redelivery re-runs the same writes
	// to no effect.
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	return nil
}

// isNoScript reports whether Redis has forgotten the cached script.
func isNoScript(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOSCRIPT")
}

// Range returns post IDs from a timeline, newest first.
//
// after is a ULID cursor; the page continues from strictly below it. Because
// ULIDs sort by creation time and the ZSET score is that same time, the cursor
// needs no separate score lookup — but scores can collide within a
// millisecond, so the ID itself is used to break the tie rather than trusting
// the score alone.
func (s *Store) Range(ctx context.Context, userID string, after string, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}

	key := Key(userID)

	if after == "" {
		ids, err := s.client.ZRangeArgs(ctx, redis.ZRangeArgs{
			Key:   key,
			Start: 0,
			Stop:  int64(limit - 1),
			Rev:   true,
		}).Result()
		if err != nil {
			return nil, fmt.Errorf("read timeline %s: %w", userID, err)
		}
		return ids, nil
	}

	// Over-fetch, then drop everything at or above the cursor. A score-bounded
	// range cannot do this correctly on its own: several posts can share a
	// millisecond, and ZRevRangeByScore would either re-show them or skip them
	// depending on which bound is exclusive.
	fetch := int64(limit) * 2
	if fetch < 64 {
		fetch = 64
	}

	candidates, err := s.client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     key,
		Start:   "+inf",
		Stop:    "-inf",
		ByScore: true,
		Rev:     true,
		Count:   fetch,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("read timeline %s: %w", userID, err)
	}

	out := make([]string, 0, limit)
	past := false
	for _, id := range candidates {
		if !past {
			if id == after {
				past = true
			}
			continue
		}
		out = append(out, id)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

// Len reports how many posts a timeline holds.
func (s *Store) Len(ctx context.Context, userID string) (int64, error) {
	n, err := s.client.ZCard(ctx, Key(userID)).Result()
	if err != nil {
		return 0, fmt.Errorf("timeline length %s: %w", userID, err)
	}
	return n, nil
}

// Contains reports whether a post is in a timeline, and how many copies.
//
// A sorted set cannot hold the same member twice, so the answer is only ever
// zero or one — which is precisely what the crash-recovery test asserts, and
// why it can assert it without deduplicating anything first.
func (s *Store) Contains(ctx context.Context, userID, postID string) (bool, error) {
	err := s.client.ZScore(ctx, Key(userID), postID).Err()
	switch {
	case err == redis.Nil:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("check timeline %s: %w", userID, err)
	default:
		return true, nil
	}
}

// Clear removes a timeline. For tests and for phase 4's rebuild path.
func (s *Store) Clear(ctx context.Context, userIDs ...string) error {
	if len(userIDs) == 0 {
		return nil
	}
	keys := make([]string, len(userIDs))
	for i, id := range userIDs {
		keys[i] = Key(id)
	}
	if err := s.client.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("clear timelines: %w", err)
	}
	return nil
}
