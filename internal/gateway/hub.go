package gateway

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/anwexhaa/murmur/api/gen/murmur/events/v1"
	"github.com/anwexhaa/murmur/internal/platform/bus"
)

// Hub delivers live timeline updates to connected clients.
//
// The routing lives in NATS, not in this process. A gateway subscribes only to
// the subjects for the users currently connected to *it*, and the fanout
// worker publishes one notification per follower without knowing or caring
// where anyone is connected. That is what makes any replica able to serve any
// user: there is no shared in-memory registry to keep consistent, no sticky
// sessions, and no reason for a publish on one replica to know about a socket
// on another.
//
// An in-memory hub — the obvious first design — works perfectly with one
// replica and silently delivers nothing the moment there are two.
type Hub struct {
	nc      *nats.Conn
	log     *slog.Logger
	metrics *HubMetrics

	bufferSize int
	// maxDrops is how many consecutive dropped updates a connection may
	// accumulate before it is closed. See deliver.
	maxDrops int64

	connections atomic.Int64
}

// HubMetrics are the hub's instruments.
type HubMetrics struct {
	// Connections is the number this phase's capacity claim is about.
	Connections prometheus.Gauge
	Delivered   prometheus.Counter
	// Dropped counts updates a slow client could not keep up with. Non-zero is
	// not a failure — it is the backpressure policy working — but a rising
	// rate means clients are slower than the timeline is moving.
	Dropped prometheus.Counter
	// Evicted counts connections closed for falling too far behind.
	Evicted prometheus.Counter
	// BufferDepth is sampled on delivery, so its high-water mark says how
	// close connections run to their limit.
	BufferDepth prometheus.Histogram
	// Latency is publish-to-client: how long a post took to travel from the
	// fanout worker, through NATS, to a socket. Across replicas, because the
	// timestamp travels in the event.
	Latency prometheus.Histogram
}

// NewHubMetrics registers the hub's instruments.
func NewHubMetrics(registry prometheus.Registerer) *HubMetrics {
	m := &HubMetrics{
		Connections: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "murmur", Subsystem: "subscriptions", Name: "connections",
			Help: "Live subscription connections on this replica.",
		}),
		Delivered: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "murmur", Subsystem: "subscriptions", Name: "delivered_total",
			Help: "Updates written to a client's send buffer.",
		}),
		Dropped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "murmur", Subsystem: "subscriptions", Name: "dropped_total",
			Help: "Updates dropped because a client's buffer was full.",
		}),
		Evicted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "murmur", Subsystem: "subscriptions", Name: "evicted_total",
			Help: "Connections closed for falling too far behind.",
		}),
		BufferDepth: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "murmur", Subsystem: "subscriptions", Name: "buffer_depth",
			Help:    "Send-buffer occupancy at delivery time.",
			Buckets: []float64{0, 1, 2, 4, 8, 16, 32, 64, 128},
		}),
		Latency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "murmur", Subsystem: "subscriptions", Name: "delivery_seconds",
			Help:    "Time from the post being written to it reaching a client's buffer.",
			Buckets: []float64{.001, .005, .01, .05, .1, .25, .5, 1, 2.5, 5},
		}),
	}
	registry.MustRegister(m.Connections, m.Delivered, m.Dropped, m.Evicted, m.BufferDepth, m.Latency)
	return m
}

// HubOptions configure the hub.
type HubOptions struct {
	// BufferSize is how many updates a connection may fall behind by.
	//
	// Small on purpose. The buffer exists to absorb a brief stall, not to
	// store a backlog: a client that is persistently slower than its timeline
	// is moving cannot be helped by a bigger buffer, only delayed, and memory
	// spent on it is memory multiplied by every connection.
	BufferSize int
	MaxDrops   int64
	Log        *slog.Logger
	Metrics    *HubMetrics
}

// NewHub builds a hub over an existing NATS connection.
func NewHub(nc *nats.Conn, opts HubOptions) *Hub {
	if opts.BufferSize <= 0 {
		opts.BufferSize = 64
	}
	if opts.MaxDrops <= 0 {
		opts.MaxDrops = 128
	}
	return &Hub{
		nc:         nc,
		log:        opts.Log,
		metrics:    opts.Metrics,
		bufferSize: opts.BufferSize,
		maxDrops:   opts.MaxDrops,
	}
}

// flushTimeout bounds how long Subscribe waits for the server to acknowledge
// the subscription. Short: a NATS round trip is sub-millisecond, and a client
// waiting seconds to open a stream is already a failure.
const flushTimeout = 2 * time.Second

// Connections reports how many subscriptions this replica is serving.
func (h *Hub) Connections() int64 { return h.connections.Load() }

// Update is one live notification.
type Update struct {
	PostID   string
	AuthorID string
	// Gap is set when updates were dropped before this one.
	Gap bool
}

// Subscription is one client's stream.
type Subscription struct {
	Updates <-chan Update

	hub     *Hub
	natsSub *nats.Subscription
	ch      chan Update
	closeCh chan struct{}

	// gapped records that something was dropped, so the next update that does
	// get through can say so.
	gapped  atomic.Bool
	dropped atomic.Int64
	once    sync.Once
}

// Subscribe opens a live stream for one user.
//
// The returned subscription must be closed by the caller; the resolver does it
// when the request context ends, which is when the WebSocket closes.
func (h *Hub) Subscribe(userID string) (*Subscription, error) {
	sub := &Subscription{
		hub:     h,
		ch:      make(chan Update, h.bufferSize),
		closeCh: make(chan struct{}),
	}
	sub.Updates = sub.ch

	// Subscribing only to this user's subject is the whole routing strategy:
	// this replica is told about exactly the users it is serving, and NATS
	// does the rest.
	natsSub, err := h.nc.Subscribe(bus.TimelineSubject(userID), func(msg *nats.Msg) {
		sub.handle(msg)
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe to timeline of %s: %w", userID, err)
	}
	sub.natsSub = natsSub

	// Wait for the server to have the subscription before returning.
	//
	// nats.Subscribe only buffers the SUB protocol message; it is not live
	// until the connection flushes. Returning early would mean a caller that
	// subscribes and then reads its timeline could miss anything published in
	// between — the exact gap the replay-then-live handoff exists to close,
	// reopened one layer down.
	//
	// Invisible on a single connection, because a publish on the same
	// connection flushes the pending SUB along with it. It shows up the moment
	// the publisher is a different process, which is to say in production.
	if err := h.nc.FlushTimeout(flushTimeout); err != nil {
		_ = natsSub.Unsubscribe()
		return nil, fmt.Errorf("establish subscription for %s: %w", userID, err)
	}

	h.connections.Add(1)
	if h.metrics != nil {
		h.metrics.Connections.Inc()
	}
	return sub, nil
}

// handle runs on a NATS delivery goroutine and must never block.
//
// Blocking here would stall delivery for every other subject on the same
// connection, which means one slow client could stop updates for every other
// client on this replica. That is the failure this whole design exists to
// prevent, so the send is non-blocking and a full buffer is dropped rather
// than waited on.
func (s *Subscription) handle(msg *nats.Msg) {
	var event eventsv1.PostCreated
	if err := proto.Unmarshal(msg.Data, &event); err != nil {
		return
	}

	update := Update{PostID: event.GetPostId(), AuthorID: event.GetAuthorId()}
	// Claim the gap flag, so it travels with this update rather than being
	// lost. If the send below fails, it is put back.
	if s.gapped.CompareAndSwap(true, false) {
		update.Gap = true
	}

	select {
	case s.ch <- update:
		s.dropped.Store(0)
		if m := s.hub.metrics; m != nil {
			m.Delivered.Inc()
			m.BufferDepth.Observe(float64(len(s.ch)))
			if created := event.GetCreatedAt(); created != nil {
				m.Latency.Observe(time.Since(created.AsTime()).Seconds())
			}
		}

	case <-s.closeCh:
		return

	default:
		// The client is not keeping up. Put the gap back so the next update it
		// does receive tells it to refetch.
		s.gapped.Store(true)
		if m := s.hub.metrics; m != nil {
			m.Dropped.Inc()
		}

		// A client that never drains would otherwise hold a buffer and a NATS
		// subscription forever while receiving nothing. Past the threshold it
		// is closed: a disconnect is a signal the client can act on, an
		// indefinitely stalled stream is not.
		if s.dropped.Add(1) >= s.hub.maxDrops {
			s.hub.log.Warn("closing a subscription that fell too far behind",
				"dropped", s.dropped.Load())
			if m := s.hub.metrics; m != nil {
				m.Evicted.Inc()
			}
			s.Close()
		}
	}
}

// Dropped reports how many consecutive updates this subscription has dropped.
func (s *Subscription) Dropped() int64 { return s.dropped.Load() }

// Done is closed when the subscription ends, whether by the caller or by
// eviction. The consumer selects on it to know the stream is over.
func (s *Subscription) Done() <-chan struct{} { return s.closeCh }

// Close unsubscribes and releases the stream. Safe to call more than once,
// which matters because both the consumer's defer and the eviction path inside
// the NATS handler can reach it.
//
// It deliberately does not close the update channel. A NATS delivery goroutine
// may be inside handle at this very moment, and closing a channel that another
// goroutine is selecting a send on is a panic, not a race the detector would
// even have to find. The sender never closes; the consumer watches Done
// instead, which is the only arrangement that is safe without a lock on the
// hot path.
func (s *Subscription) Close() {
	s.once.Do(func() {
		close(s.closeCh)

		if s.natsSub != nil {
			if err := s.natsSub.Unsubscribe(); err != nil {
				s.hub.log.Debug("unsubscribing from nats", "error", err)
			}
		}

		s.hub.connections.Add(-1)
		if m := s.hub.metrics; m != nil {
			m.Connections.Dec()
		}
	})
}

// Closed reports whether this subscription has been closed.
func (s *Subscription) Closed() bool {
	select {
	case <-s.closeCh:
		return true
	default:
		return false
	}
}
