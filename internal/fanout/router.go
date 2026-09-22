package fanout

import (
	"context"
	"sync"
	"time"

	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
)

// Mode is how a post reaches the people who follow its author.
type Mode string

const (
	// ModePush writes the post into every follower's timeline at write time.
	// One publish, N writes; reads are then a single range.
	ModePush Mode = "push"

	// ModePull writes nothing to followers. The post lives only in the
	// author's own recent feed, and readers merge it in at read time.
	// One publish, one write; reads pay instead.
	ModePull Mode = "pull"
)

// Router decides how to fan a post out, based on how many followers its author
// has.
//
// The decision is per author rather than per post, and it is a cost decision
// rather than a product one: a reader cannot tell which mode produced their
// timeline, and that is the requirement the merge in package timeline exists
// to satisfy.
type Router struct {
	social    socialv1.SocialServiceClient
	threshold int64
	ttl       time.Duration
	now       func() time.Time

	mu    sync.RWMutex
	cache map[string]cachedCount
}

type cachedCount struct {
	followers int64
	expires   time.Time
}

// NewRouter builds a router. A threshold of zero disables pull entirely, which
// is how the benchmark measures the pure-push baseline it is comparing against.
func NewRouter(social socialv1.SocialServiceClient, threshold int64, ttl time.Duration, now func() time.Time) *Router {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	if now == nil {
		now = time.Now
	}
	return &Router{
		social:    social,
		threshold: threshold,
		ttl:       ttl,
		now:       now,
		cache:     make(map[string]cachedCount, 256),
	}
}

// Threshold reports the configured follower count at which push stops.
func (r *Router) Threshold() int64 { return r.threshold }

// Route returns the mode for an author, and the follower count it decided on.
//
// The count is cached for a short TTL because it is asked on every post and
// changes slowly: an account near the threshold might flip a few seconds late,
// and the consequence of being late is that one post takes the other path.
// Both paths are correct, so staleness costs efficiency rather than
// correctness — which is what makes a cache acceptable here at all.
func (r *Router) Route(ctx context.Context, authorID string) (Mode, int64, error) {
	if r.threshold <= 0 {
		// Pull disabled. Still needs the count, for the latency histogram's
		// bucket label.
		count, err := r.followerCount(ctx, authorID)
		return ModePush, count, err
	}

	count, err := r.followerCount(ctx, authorID)
	if err != nil {
		return "", 0, err
	}
	if count >= r.threshold {
		return ModePull, count, nil
	}
	return ModePush, count, nil
}

func (r *Router) followerCount(ctx context.Context, authorID string) (int64, error) {
	now := r.now()

	r.mu.RLock()
	entry, ok := r.cache[authorID]
	r.mu.RUnlock()
	if ok && now.Before(entry.expires) {
		return entry.followers, nil
	}

	resp, err := r.social.GetFollowerCount(ctx, &socialv1.GetFollowerCountRequest{UserId: authorID})
	if err != nil {
		// A stale entry beats no answer: the alternative is failing the event
		// and retrying, and a slightly old follower count routes a single post
		// down a path that is merely less efficient.
		if ok {
			return entry.followers, nil
		}
		return 0, err
	}

	count := resp.GetFollowers()

	r.mu.Lock()
	// Bound the cache. Authors are unbounded and this map is not; a fanout
	// worker running for a week would otherwise hold every author it has ever
	// seen. Dropping everything is crude and fine — the entries cost one RPC
	// each to rebuild, and the hot authors rebuild immediately.
	if len(r.cache) >= maxRouterCache {
		r.cache = make(map[string]cachedCount, 256)
	}
	r.cache[authorID] = cachedCount{followers: count, expires: now.Add(r.ttl)}
	r.mu.Unlock()

	return count, nil
}

const maxRouterCache = 50_000
