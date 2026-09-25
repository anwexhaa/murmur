package timeline

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
)

// Breaker settings.
const (
	// breakerThreshold is how many consecutive failures open the circuit.
	// Small, because the failure this guards against is not subtle: Redis is
	// either answering or it is not, and three requests in a row failing is
	// not a coincidence worth waiting out.
	breakerThreshold = 3

	// breakerCooldown is how long the materialised path is skipped entirely.
	// Short enough that recovery is noticed in seconds; long enough that the
	// cost of discovering the outage is paid once per window rather than once
	// per request.
	breakerCooldown = 5 * time.Second
)

// breaker skips the materialised path while it is known to be failing.
//
// It exists because of a number. With Redis scaled to zero, every read paid
// the full cost of discovering that again -- a dial timeout, sometimes twice,
// measured at 0.5 to 2.5 seconds per request against a downstream budget of
// three. Some requests fit inside it and some did not, so the same outage
// produced a 78% success rate rather than either a clean degradation or a
// clean failure.
//
// The fallback was correct the whole time. What it lacked was a way to stop
// paying for a fact it had already established.
type breaker struct {
	mu        sync.Mutex
	failures  int
	openUntil time.Time
	now       func() time.Time
}

func newBreaker(now func() time.Time) *breaker {
	if now == nil {
		now = time.Now
	}
	return &breaker{now: now}
}

// allow reports whether the materialised path should be attempted.
//
// While open it returns false, and one request per cooldown is let through to
// find out whether the outage is over -- the half-open probe. Without it the
// circuit would either never reopen or would reopen blindly for everybody at
// once, which is a thundering herd aimed at a server that has just come back.
func (b *breaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.openUntil.IsZero() || b.now().After(b.openUntil) {
		// Closed, or the cooldown has expired: this caller is the probe.
		b.openUntil = time.Time{}
		return true
	}
	return false
}

// succeed closes the circuit.
func (b *breaker) succeed() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.openUntil = time.Time{}
}

// fail records a failure and opens the circuit once they stop looking like
// bad luck.
func (b *breaker) fail() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.failures++
	if b.failures >= breakerThreshold {
		b.openUntil = b.now().Add(breakerCooldown)
	}
}

// open reports whether the circuit is currently open, for the metric.
func (b *breaker) open() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.openUntil.IsZero() && !b.now().After(b.openUntil)
}

// ReadPathMetrics report which path served a timeline.
type ReadPathMetrics struct {
	// Reads is labelled by the path that answered: "materialised" for the
	// Redis timeline, "source" for the Postgres fallback. A ratio, not a
	// count, because "the fallback fired 4,000 times" means nothing without
	// knowing whether that was four percent of traffic or all of it.
	Reads *prometheus.CounterVec
	// Duration separates the two, so the cost of degrading is a number rather
	// than a claim.
	Duration *prometheus.HistogramVec
	// Degraded is 1 while the materialised path is failing. A gauge rather
	// than a counter because the question an operator asks at 3am is "is it
	// happening now", and a counter can only answer "has it ever".
	Degraded prometheus.Gauge
}

// NewReadPathMetrics registers the read path's instruments.
func NewReadPathMetrics(registry prometheus.Registerer) *ReadPathMetrics {
	m := &ReadPathMetrics{
		Reads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "murmur", Subsystem: "timeline", Name: "reads_total",
			Help: "Timeline reads by the path that served them.",
		}, []string{"path"}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "murmur", Subsystem: "timeline", Name: "read_seconds",
			Help:    "Time to assemble one timeline page, by path.",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
		}, []string{"path"}),
		Degraded: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "murmur", Subsystem: "timeline", Name: "degraded",
			Help: "1 while the materialised read path is unavailable.",
		}),
	}
	registry.MustRegister(m.Reads, m.Duration, m.Degraded)
	return m
}

// Read paths, as they appear in the metric label.
const (
	PathMaterialised = "materialised"
	PathSource       = "source"
	// PathSourceFast is the source path taken without trying Redis at all,
	// because the breaker is open. Labelled separately so the difference
	// between "we fell back" and "we stopped bothering" is visible, and so
	// the latency of the two is not averaged into one meaningless number.
	PathSourceFast = "source-open-circuit"
)

// fromSource assembles a timeline page out of Postgres, through the social
// service.
//
// This is the phase 2 design, and keeping it is the point. Everything in Redis
// is derived from Postgres, so the derived view is allowed to be unavailable
// while the data it derives from is not. Falling back costs latency; returning
// an error would cost the read entirely, and the data was there the whole
// time.
//
// The page is assembled by one SQL query rather than by the gateway's old
// fan-out of one call per followed account, because "the same answer computed
// badly" is not a fallback anybody should ship. See Store.ListFollowedPosts.
func (s *Service) fromSource(ctx context.Context, userID, pageToken string, limit int) ([]*socialv1.Post, string, error) {
	resp, err := s.social.ListFollowedPosts(ctx, &socialv1.ListFollowedPostsRequest{
		UserId:    userID,
		PageSize:  int32(limit),
		PageToken: pageToken,
	})
	if err != nil {
		return nil, "", err
	}
	return resp.GetPosts(), resp.GetNextPageToken(), nil
}

// observe records which path served a read and what it cost.
func (s *Service) observe(path string, started time.Time) {
	if s.metrics == nil {
		return
	}
	s.metrics.Reads.WithLabelValues(path).Inc()
	s.metrics.Duration.WithLabelValues(path).Observe(time.Since(started).Seconds())
}

// setDegraded moves the gauge, which is what an alert watches.
func (s *Service) setDegraded(degraded bool) {
	if s.metrics == nil {
		return
	}
	if degraded {
		s.metrics.Degraded.Set(1)
		return
	}
	s.metrics.Degraded.Set(0)
}

// Degraded reports whether this service is currently serving from the source,
// for tests and for the readiness endpoint's detail.
func (s *Service) Degraded() bool { return s.breaker.open() }

// shouldFallBack reports whether an error from the materialised path is the
// kind the source path can rescue.
//
// A caller that gave up, or a deadline that has already passed, is not a Redis
// problem and re-running the whole read against Postgres would only spend
// somebody else's capacity on a request nobody is waiting for. Everything else
// -- connection refused, timeout, a Lua error, a cluster failover -- means the
// derived view is unavailable and the source is not.
func shouldFallBack(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctx.Err() != nil {
		return false
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}
