package timeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/types/known/timestamppb"

	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
)

// PostCache hydrates post IDs into posts, through three tiers.
//
//	in-process LRU  →  Redis  →  BatchGetPosts over gRPC
//
// Each tier answers a different question. The LRU removes the network entirely
// for the handful of posts everyone is reading right now. Redis shares that
// work across every gateway and timeline replica, so a post fetched by one
// process is free for the others. The gRPC call is the only tier that can
// produce a post nobody has read before, and it is the one this exists to
// avoid.
//
// singleflight sits in front of the misses. Without it, a post that suddenly
// becomes popular produces one downstream fetch per concurrent reader — a
// thousand readers, a thousand identical queries for the same row, all issued
// in the instant after it expires. That is the stampede, and it is worst
// exactly when the system is busiest.
type PostCache struct {
	local  *lru.Cache[string, cachedPost]
	client *redis.Client
	social socialv1.SocialServiceClient
	log    *slog.Logger

	localTTL time.Duration
	redisTTL time.Duration
	now      func() time.Time

	// group collapses concurrent misses for the same ID into one fetch.
	group singleflight.Group

	metrics *CacheMetrics
}

type cachedPost struct {
	post    *socialv1.Post
	expires time.Time
}

// CacheMetrics are the cache's instruments.
type CacheMetrics struct {
	// Lookups is labelled by which tier answered, which is the only way to
	// tell a cache that is working from one that is merely present.
	Lookups *prometheus.CounterVec
	// Collapsed counts misses that singleflight absorbed — requests that
	// wanted a downstream fetch and got somebody else's instead. Under a
	// viral post this is the difference between flat and linear.
	Collapsed prometheus.Counter
	LocalSize prometheus.Gauge
}

// NewCacheMetrics registers the cache's instruments.
func NewCacheMetrics(registry prometheus.Registerer) *CacheMetrics {
	m := &CacheMetrics{
		Lookups: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "murmur", Subsystem: "postcache", Name: "lookups_total",
			Help: "Post lookups by the tier that answered.",
		}, []string{"tier"}),
		Collapsed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "murmur", Subsystem: "postcache", Name: "collapsed_total",
			Help: "Concurrent misses absorbed by singleflight.",
		}),
		LocalSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "murmur", Subsystem: "postcache", Name: "local_entries",
			Help: "Posts held in the in-process tier.",
		}),
	}
	registry.MustRegister(m.Lookups, m.Collapsed, m.LocalSize)
	return m
}

// CacheOptions configure the cache.
type CacheOptions struct {
	LocalEntries int
	// LocalTTL bounds how stale the in-process tier can be.
	//
	// This is the one tier invalidation cannot reach synchronously: a delete
	// reaches Redis immediately and every other replica's memory only via an
	// event. The TTL is the guarantee that holds even if that event is lost,
	// which is why it is short and why the bound is documented rather than
	// assumed.
	LocalTTL time.Duration
	RedisTTL time.Duration
	Log      *slog.Logger
	Metrics  *CacheMetrics
	Now      func() time.Time
}

// NewPostCache builds the cache.
func NewPostCache(client *redis.Client, social socialv1.SocialServiceClient, opts CacheOptions) (*PostCache, error) {
	if opts.LocalEntries <= 0 {
		opts.LocalEntries = 10_000
	}
	if opts.LocalTTL <= 0 {
		opts.LocalTTL = 5 * time.Second
	}
	if opts.RedisTTL <= 0 {
		opts.RedisTTL = 10 * time.Minute
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	local, err := lru.New[string, cachedPost](opts.LocalEntries)
	if err != nil {
		return nil, fmt.Errorf("create post cache: %w", err)
	}

	return &PostCache{
		local:    local,
		client:   client,
		social:   social,
		log:      opts.Log,
		localTTL: opts.LocalTTL,
		redisTTL: opts.RedisTTL,
		now:      opts.Now,
		metrics:  opts.Metrics,
	}, nil
}

// LocalTTL reports how long the in-process tier may serve a stale post.
func (c *PostCache) LocalTTL() time.Duration { return c.localTTL }

// postKey returns the Redis key holding one hydrated post.
func postKey(id string) string { return "post:" + id }

// Get hydrates many post IDs, returning them keyed by ID.
//
// Missing IDs are simply absent: a post deleted since it was written into a
// timeline must not fail the page it appears on.
func (c *PostCache) Get(ctx context.Context, ids []string) (map[string]*socialv1.Post, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	out := make(map[string]*socialv1.Post, len(ids))
	missing := make([]string, 0, len(ids))
	now := c.now()

	// Tier 1: process memory.
	for _, id := range ids {
		if entry, ok := c.local.Get(id); ok && now.Before(entry.expires) {
			out[id] = entry.post
			c.count("local")
			continue
		}
		missing = append(missing, id)
	}
	if len(missing) == 0 {
		c.observeSize()
		return out, nil
	}

	// Tier 2: Redis, in one pipeline rather than one round trip per miss.
	found, stillMissing, err := c.fromRedis(ctx, missing)
	if err != nil {
		// Redis being unavailable must degrade to the source of truth, not
		// fail the read. A cache that can take the service down with it is
		// worse than no cache.
		c.log.Warn("post cache: redis lookup failed, falling through", "error", err)
		stillMissing = missing
	}
	for id, post := range found {
		out[id] = post
		c.rememberLocal(id, post, now)
		c.count("redis")
	}

	if len(stillMissing) == 0 {
		c.observeSize()
		return out, nil
	}

	// Tier 3: the source of truth, behind singleflight.
	fetched, err := c.fromSource(ctx, stillMissing)
	if err != nil {
		return out, err
	}
	for id, post := range fetched {
		out[id] = post
	}

	c.observeSize()
	return out, nil
}

func (c *PostCache) fromRedis(ctx context.Context, ids []string) (map[string]*socialv1.Post, []string, error) {
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = postKey(id)
	}

	values, err := c.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, nil, fmt.Errorf("mget posts: %w", err)
	}

	found := make(map[string]*socialv1.Post, len(ids))
	missing := make([]string, 0, len(ids))

	for i, raw := range values {
		text, ok := raw.(string)
		if !ok {
			missing = append(missing, ids[i])
			continue
		}
		post, err := decodePost(text)
		if err != nil {
			// A corrupt entry is a miss, not a failure. It will be overwritten
			// by the fetch below.
			missing = append(missing, ids[i])
			continue
		}
		found[ids[i]] = post
	}
	return found, missing, nil
}

// fromSource fetches from the social service, collapsing concurrent misses.
//
// Keyed per post ID rather than per batch: two readers wanting overlapping but
// unequal sets of posts should still share the work on the posts they have in
// common, and a batch-shaped key would make them miss each other.
func (c *PostCache) fromSource(ctx context.Context, ids []string) (map[string]*socialv1.Post, error) {
	type result struct {
		id   string
		post *socialv1.Post
		err  error
	}

	results := make(chan result, len(ids))
	for _, id := range ids {
		go func() {
			// led is set only in the goroutine whose closure actually ran, so
			// it distinguishes the one real fetch from everyone who waited on
			// it.
			//
			// singleflight reports shared=true to *every* participant,
			// including the leader. Branching on shared alone therefore counts
			// the leader as a collapsed request and never records the fetch it
			// performed — which reads as a source-tier counter of zero while
			// the service is very much talking to the source.
			led := false

			value, err, shared := c.group.Do(id, func() (any, error) {
				led = true
				c.count("source")

				resp, fetchErr := c.social.BatchGetPosts(ctx, &socialv1.BatchGetPostsRequest{Ids: []string{id}})
				if fetchErr != nil {
					return nil, fetchErr
				}
				post := resp.GetPosts()[id]
				if post != nil {
					c.store(ctx, id, post)
				}
				return post, nil
			})
			if shared && !led {
				c.countCollapsed()
			}

			post, _ := value.(*socialv1.Post)
			results <- result{id: id, post: post, err: err}
		}()
	}

	out := make(map[string]*socialv1.Post, len(ids))
	var firstErr error
	for range ids {
		r := <-results
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		if r.post != nil {
			out[r.id] = r.post
		}
	}
	return out, firstErr
}

// store writes a post into both cache tiers.
func (c *PostCache) store(ctx context.Context, id string, post *socialv1.Post) {
	c.rememberLocal(id, post, c.now())

	encoded, err := encodePost(post)
	if err != nil {
		return
	}
	if err := c.client.Set(ctx, postKey(id), encoded, c.redisTTL).Err(); err != nil {
		// A cache write failing is not a reason to fail the read; the post is
		// already in hand.
		c.log.Debug("post cache: could not write to redis", "post_id", id, "error", err)
	}
}

func (c *PostCache) rememberLocal(id string, post *socialv1.Post, now time.Time) {
	c.local.Add(id, cachedPost{post: post, expires: now.Add(c.localTTL)})
}

// Invalidate drops a post from both tiers of this process.
//
// Redis is shared, so removing it there serves every replica. The in-process
// tier is not, so each replica must be told — which is what the post.deleted
// event is for. A replica that never hears the event still stops serving the
// stale copy when its TTL expires, and that TTL is the bound the invalidation
// test asserts.
func (c *PostCache) Invalidate(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}

	for _, id := range ids {
		c.local.Remove(id)
	}

	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = postKey(id)
	}
	if err := c.client.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("invalidate posts: %w", err)
	}
	return nil
}

// InvalidateLocal drops a post from this process's memory only. Used by the
// event consumer, where Redis has already been cleared by whoever published.
func (c *PostCache) InvalidateLocal(ids ...string) {
	for _, id := range ids {
		c.local.Remove(id)
	}
}

func (c *PostCache) count(tier string) {
	if c.metrics != nil {
		c.metrics.Lookups.WithLabelValues(tier).Inc()
	}
}

func (c *PostCache) countCollapsed() {
	if c.metrics != nil {
		c.metrics.Collapsed.Inc()
	}
}

func (c *PostCache) observeSize() {
	if c.metrics != nil {
		c.metrics.LocalSize.Set(float64(c.local.Len()))
	}
}

// wirePost is the cached representation.
//
// JSON rather than the protobuf wire format, because a cached post is read far
// more often than written and being able to inspect one with redis-cli during
// an incident is worth more than the bytes. Unlike the outbox — where the
// stored bytes must be exactly what gets published — nothing downstream
// consumes this encoding.
type wirePost struct {
	ID        string    `json:"id"`
	AuthorID  string    `json:"author_id"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
}

func encodePost(post *socialv1.Post) (string, error) {
	encoded, err := json.Marshal(wirePost{
		ID:        post.GetId(),
		AuthorID:  post.GetAuthorId(),
		Body:      post.GetBody(),
		CreatedAt: post.GetCreatedAt().AsTime(),
	})
	if err != nil {
		return "", fmt.Errorf("encode post: %w", err)
	}
	return string(encoded), nil
}

func decodePost(text string) (*socialv1.Post, error) {
	var w wirePost
	if err := json.Unmarshal([]byte(text), &w); err != nil {
		return nil, fmt.Errorf("decode post: %w", err)
	}
	return &socialv1.Post{
		Id:        w.ID,
		AuthorId:  w.AuthorID,
		Body:      w.Body,
		CreatedAt: timestamppb.New(w.CreatedAt),
	}, nil
}
