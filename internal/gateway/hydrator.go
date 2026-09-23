package gateway

import (
	"context"
	"fmt"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
)

// Hydrator turns a post ID into a post once per post, rather than once per
// subscriber.
//
// It exists because of a number the first capacity run produced. Two thousand
// sockets on one replica, twenty posts published: forty thousand updates
// delivered, and — because the subscription resolver fetched each one on its
// own — forty thousand identical GetPost calls, for twenty distinct rows.
// Delivery latency went from 31 ms to 702 ms, and the cost was not the sockets
// but the fanout of work behind them.
//
// This is the same N+1 the read path spent phase 5 removing, reappearing on
// the live path in a shape the request-scoped loaders could not reach: a
// DataLoader batches within one request, and here every delivery is its own
// request on its own goroutine, arriving within microseconds of every other.
// There is nothing to batch and everything to share, so the answer is a cache
// keyed by post with singleflight in front of it.
//
// Deliberately not the three-tier cache the timeline service uses. The posts
// this holds are seconds old and every subscriber wants the same handful at
// the same instant, so the tier that matters is the one with no network in it;
// a Redis hop would add latency to the exact path this exists to make fast.
type Hydrator struct {
	social socialv1.SocialServiceClient
	local  *lru.Cache[string, hydrated]
	// group collapses the concurrent misses. Without it the first delivery of
	// a new post produces one fetch per connected socket, because none of them
	// has populated the cache yet — the stampede is guaranteed rather than
	// merely possible, since a broadcast wakes every subscriber at once.
	group   singleflight.Group
	ttl     time.Duration
	now     func() time.Time
	metrics *HydratorMetrics
}

// hydrateTimeout bounds the detached fetch below. Generous relative to a
// healthy lookup and far short of the socket's lifetime: a subscription has no
// request deadline to inherit, so this is the only thing stopping a wedged
// downstream call from holding every waiter on it indefinitely.
const hydrateTimeout = 5 * time.Second

// hydrated is one cached answer. A nil post is a remembered NotFound: without
// it, a post deleted between the notification and the lookup would produce a
// failed fetch per subscriber, which is the stampede again with nothing to
// show for it.
type hydrated struct {
	post    *socialv1.Post
	expires time.Time
}

// HydratorMetrics are the hydrator's instruments.
type HydratorMetrics struct {
	// Lookups is labelled by what answered: the local tier, a downstream
	// fetch, or a remembered miss. The ratio is the claim — "cached" is not a
	// measurement, "one source fetch per twenty thousand deliveries" is.
	Lookups *prometheus.CounterVec
	// Collapsed counts lookups singleflight absorbed. On a broadcast this is
	// very nearly the connection count, which is the point.
	Collapsed prometheus.Counter
	Entries   prometheus.Gauge
}

// NewHydratorMetrics registers the hydrator's instruments.
func NewHydratorMetrics(registry prometheus.Registerer) *HydratorMetrics {
	m := &HydratorMetrics{
		Lookups: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "murmur", Subsystem: "hydrator", Name: "lookups_total",
			Help: "Live-update hydrations by what answered them.",
		}, []string{"tier"}),
		Collapsed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "murmur", Subsystem: "hydrator", Name: "collapsed_total",
			Help: "Concurrent hydrations absorbed by singleflight.",
		}),
		Entries: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "murmur", Subsystem: "hydrator", Name: "entries",
			Help: "Posts held in the hydrator's cache.",
		}),
	}
	registry.MustRegister(m.Lookups, m.Collapsed, m.Entries)
	return m
}

// HydratorOptions configure the hydrator.
type HydratorOptions struct {
	Entries int
	// TTL bounds how long a delivered post may be stale.
	//
	// There is no invalidation subscriber behind it, and it does not need one:
	// an entry here is written when a post is broadcast and read by every
	// socket within milliseconds of that, so the window in which a delete
	// could race it is the window in which the post was being delivered
	// anyway. The read path's cache, which serves posts for as long as people
	// scroll past them, is the one that earns an invalidator.
	TTL     time.Duration
	Metrics *HydratorMetrics
	Now     func() time.Time
}

// NewHydrator builds a hydrator over the social client.
func NewHydrator(social socialv1.SocialServiceClient, opts HydratorOptions) (*Hydrator, error) {
	if opts.Entries <= 0 {
		opts.Entries = 10_000
	}
	if opts.TTL <= 0 {
		opts.TTL = 5 * time.Second
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	local, err := lru.New[string, hydrated](opts.Entries)
	if err != nil {
		return nil, fmt.Errorf("create hydrator cache: %w", err)
	}

	return &Hydrator{
		social:  social,
		local:   local,
		ttl:     opts.TTL,
		now:     opts.Now,
		metrics: opts.Metrics,
	}, nil
}

// Get returns one post, from memory when it can and from the social service
// when it must.
//
// A NotFound is returned as a NotFound, so the caller can tell a deleted post
// from a failed lookup and drop it rather than failing the stream.
func (h *Hydrator) Get(ctx context.Context, postID string) (*socialv1.Post, error) {
	if entry, ok := h.local.Get(postID); ok && h.now().Before(entry.expires) {
		if entry.post == nil {
			h.observe("missing")
			return nil, status.Error(codes.NotFound, "post not found")
		}
		h.observe("local")
		return entry.post, nil
	}

	// fetched is set inside the closure, so it is true only for the goroutine
	// that actually made the call. singleflight reports shared=true to the
	// leader as well as the followers, which would make a source counter read
	// zero on exactly the request that caused the fetch.
	var fetched bool
	result, err, _ := h.group.Do(postID, func() (any, error) {
		fetched = true

		// Detached from the caller's context on purpose. Every waiter on this
		// call is a different socket, and the one that happened to arrive
		// first has no special claim on the work: if its client disconnects
		// mid-fetch, cancelling would fail the lookup for every other
		// subscriber waiting behind it. The deadline replaces the one that was
		// dropped, so the call is still bounded.
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), hydrateTimeout)
		defer cancel()

		resp, err := h.social.GetPost(fetchCtx, &socialv1.GetPostRequest{Id: postID})
		if err != nil {
			if status.Code(err) == codes.NotFound {
				h.remember(postID, nil)
			}
			return nil, err
		}

		post := resp.GetPost()
		h.remember(postID, post)
		return post, nil
	})

	if !fetched && h.metrics != nil {
		h.metrics.Collapsed.Inc()
	}
	if err != nil {
		if status.Code(err) == codes.NotFound {
			h.observe("missing")
		} else {
			h.observe("error")
		}
		return nil, err
	}

	if fetched {
		h.observe("source")
	} else {
		h.observe("shared")
	}
	post, _ := result.(*socialv1.Post)
	return post, nil
}

// remember stores an answer, including the absence of one.
func (h *Hydrator) remember(postID string, post *socialv1.Post) {
	h.local.Add(postID, hydrated{post: post, expires: h.now().Add(h.ttl)})
	if h.metrics != nil {
		h.metrics.Entries.Set(float64(h.local.Len()))
	}
}

func (h *Hydrator) observe(tier string) {
	if h.metrics != nil {
		h.metrics.Lookups.WithLabelValues(tier).Inc()
	}
}
