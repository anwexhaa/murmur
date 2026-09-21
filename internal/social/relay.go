package social

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/anwexhaa/murmur/internal/platform/bus"
	"github.com/anwexhaa/murmur/internal/platform/lifecycle"
)

// RelayOptions configure the outbox relay.
type RelayOptions struct {
	Batch    int
	Interval time.Duration
	Log      *slog.Logger
	Metrics  *RelayMetrics
}

// RelayMetrics are the relay's instruments.
type RelayMetrics struct {
	Published prometheus.Counter
	Failed    prometheus.Counter
	// Backlog and Age together say whether the relay is keeping up. Depth
	// alone cannot: a thousand events that arrived this second are healthy,
	// and one event from ten minutes ago is not.
	Backlog   prometheus.Gauge
	OldestAge prometheus.Gauge
	Duration  prometheus.Histogram
}

// NewRelayMetrics registers the relay's instruments.
func NewRelayMetrics(registry prometheus.Registerer) *RelayMetrics {
	m := &RelayMetrics{
		Published: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "murmur", Subsystem: "outbox", Name: "published_total",
			Help: "Events successfully published to the bus.",
		}),
		Failed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "murmur", Subsystem: "outbox", Name: "publish_failures_total",
			Help: "Publish attempts that failed and will be retried.",
		}),
		Backlog: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "murmur", Subsystem: "outbox", Name: "pending_events",
			Help: "Events written but not yet published.",
		}),
		OldestAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "murmur", Subsystem: "outbox", Name: "oldest_pending_seconds",
			Help: "Age of the oldest unpublished event.",
		}),
		Duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "murmur", Subsystem: "outbox", Name: "batch_duration_seconds",
			Help:    "Time to claim, publish and mark one batch.",
			Buckets: prometheus.DefBuckets,
		}),
	}
	registry.MustRegister(m.Published, m.Failed, m.Backlog, m.OldestAge, m.Duration)
	return m
}

// Relay moves events from the outbox table to the bus.
//
// It is the second half of the transactional outbox: the write path commits a
// post and its event together, and this drains the table. Delivery is
// at-least-once by construction — a publish that succeeds just before the
// process dies will be published again — and that is the correct trade. The
// alternative, marking the row sent before publishing, would make delivery
// at-most-once and reintroduce exactly the silent loss the outbox exists to
// prevent.
type Relay struct {
	store *Store
	js    jetstream.JetStream
	opts  RelayOptions
}

// NewRelay builds a relay.
func NewRelay(store *Store, js jetstream.JetStream, opts RelayOptions) *Relay {
	if opts.Batch <= 0 {
		opts.Batch = 100
	}
	if opts.Interval <= 0 {
		opts.Interval = 250 * time.Millisecond
	}
	return &Relay{store: store, js: js, opts: opts}
}

// Component returns the relay as a lifecycle component.
func (r *Relay) Component() lifecycle.Component {
	return lifecycle.Component{
		Name:  "outbox-relay",
		Start: r.Run,
	}
}

// Run polls the outbox until the context is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.opts.Interval)
	defer ticker.Stop()

	r.opts.Log.Info("outbox relay started",
		"batch", r.opts.Batch, "interval", r.opts.Interval)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		// Drain rather than process one batch per tick. A burst of writes
		// should be cleared as fast as the bus accepts them, not metered out
		// at one batch every interval — otherwise the relay's own schedule
		// becomes the publish latency floor.
		for {
			published, err := r.drainOnce(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				r.opts.Log.Error("outbox relay batch failed", "error", err)
				break
			}
			if published < r.opts.Batch {
				break
			}
		}

		r.observeBacklog(ctx)
	}
}

// drainOnce claims a batch, publishes it, and marks it. It returns how many
// events were published.
func (r *Relay) drainOnce(ctx context.Context) (int, error) {
	start := time.Now()

	tx, records, err := r.store.ClaimOutbox(ctx, r.opts.Batch)
	if err != nil {
		return 0, err
	}
	if len(records) == 0 {
		// Nothing to do, but the transaction holds a snapshot open; end it.
		return 0, r.store.MarkPublished(ctx, tx, nil)
	}

	published := make([]int64, 0, len(records))
	failed := make([]int64, 0)

	for _, record := range records {
		subject, ok := subjectFor(record.Type)
		if !ok {
			// An event type nobody knows how to route would otherwise be
			// claimed and released forever. Mark it delivered and say so
			// loudly: it is a deploy mistake, not a runtime condition.
			r.opts.Log.Error("outbox event has an unroutable type, dropping",
				"id", record.ID, "type", record.Type, "aggregate_id", record.AggregateID)
			published = append(published, record.ID)
			continue
		}

		// PublishMsg with a message ID gives JetStream a deduplication window,
		// so a republish after a crash does not append a second copy. Belt and
		// braces: the fanout worker is idempotent anyway, but a stream without
		// duplicates is far easier to reason about when reading it by hand.
		_, err := r.js.Publish(ctx, subject, record.Payload,
			jetstream.WithMsgID(msgID(record)))
		if err != nil {
			r.opts.Log.Warn("publishing outbox event failed, will retry",
				"id", record.ID, "attempts", record.Attempts, "error", err)
			failed = append(failed, record.ID)
			continue
		}
		published = append(published, record.ID)
	}

	if len(failed) > 0 {
		if r.opts.Metrics != nil {
			r.opts.Metrics.Failed.Add(float64(len(failed)))
		}
		// Some of the batch published and some did not. Roll the whole claim
		// back rather than committing a partial result: the published ones will
		// be republished under the same message ID, which JetStream discards.
		if err := r.store.RecordAttempt(ctx, tx, failed); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("published %d of %d events", len(published), len(records))
	}

	if err := r.store.MarkPublished(ctx, tx, published); err != nil {
		return 0, err
	}

	if r.opts.Metrics != nil {
		r.opts.Metrics.Published.Add(float64(len(published)))
		r.opts.Metrics.Duration.Observe(time.Since(start).Seconds())
	}
	return len(published), nil
}

func (r *Relay) observeBacklog(ctx context.Context) {
	if r.opts.Metrics == nil {
		return
	}

	pending, err := r.store.PendingOutbox(ctx)
	if err != nil {
		return
	}
	r.opts.Metrics.Backlog.Set(float64(pending))

	age, err := r.store.OldestPending(ctx)
	if err != nil {
		return
	}
	r.opts.Metrics.OldestAge.Set(age.Seconds())
}

func subjectFor(t EventType) (string, bool) {
	switch t {
	case PostCreatedType:
		return bus.SubjectPostCreated, true
	default:
		return "", false
	}
}

// msgID is the deduplication key JetStream uses. The outbox row ID is unique
// and stable across retries, which is exactly what is needed.
func msgID(record OutboxRecord) string {
	return fmt.Sprintf("outbox-%d", record.ID)
}
