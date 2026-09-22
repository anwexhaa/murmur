// Package fanout consumes post.created and writes post IDs into the
// timelines of everyone who follows the author.
//
// The correctness argument for this package is one sentence: ZADD of a member
// already present changes nothing, so re-processing an event is harmless, so
// at-least-once delivery is safe without any deduplication state. Everything
// else here — the bounded pool, the pipelining, the paging — is about speed.
// That one property is about correctness, and it is why a worker can be killed
// mid-fanout without anyone losing a post or seeing one twice.
package fanout

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/anwexhaa/murmur/api/gen/murmur/events/v1"
	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
	"github.com/anwexhaa/murmur/internal/platform/bus"
	"github.com/anwexhaa/murmur/internal/platform/lifecycle"
	"github.com/anwexhaa/murmur/internal/timeline"
)

// Options configure the worker.
type Options struct {
	// Concurrency is how many events are fanned out at once. Bounded on
	// purpose: an unbounded goroutine per message turns a backlog into an
	// out-of-memory kill, and the pool is what keeps a burst from becoming an
	// outage.
	Concurrency int
	// FollowerPage is how many follower IDs are fetched per call. Larger than
	// a human-facing page because this walks edges by the hundred thousand and
	// every page is a round trip.
	FollowerPage int
	// MaxDeliver must match the consumer's setting so the handler can
	// recognise a message's last chance and dead-letter it itself.
	MaxDeliver int
	AckWait    time.Duration

	// Threshold is the follower count at or above which a post stops being
	// pushed. Zero means push everything, which is the phase 3 behaviour and
	// the baseline the benchmark compares against.
	Threshold     int64
	RouteCacheTTL time.Duration

	Log     *slog.Logger
	Metrics *Metrics
}

// Metrics are the worker's instruments.
type Metrics struct {
	// Duration is labelled by follower-count bucket rather than being one
	// histogram. A hundred-follower fanout and a hundred-thousand-follower
	// fanout are different operations that happen to share code, and averaging
	// them together describes neither.
	Duration  *prometheus.HistogramVec
	Followers prometheus.Counter
	Events    *prometheus.CounterVec
	// Routed counts posts by the path they took, which is the number that says
	// whether the threshold is where it should be: almost everything should be
	// pushed, and the handful that are not should be the accounts big enough
	// to matter.
	Routed *prometheus.CounterVec

	// Outstanding is undelivered plus in-flight, not just undelivered.
	//
	// JetStream's NumPending counts only messages it has not handed out. A
	// fanout that takes two minutes is invisible to it: the message was
	// delivered, so NumPending is zero while the work is very much not done.
	// Measuring the wrong one made a 500,000-follower fanout look instant.
	Outstanding prometheus.Gauge
	// Redelivered is the signal that a fanout is outrunning its ack deadline,
	// which is the failure mode that turns slow into catastrophic.
	Redelivered prometheus.Gauge
}

// NewMetrics registers the worker's instruments.
func NewMetrics(registry prometheus.Registerer) *Metrics {
	m := &Metrics{
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "murmur", Subsystem: "fanout", Name: "duration_seconds",
			Help: "Time to fan one post out to every follower.",
			// Spanning a millisecond to half a minute: a small account is at
			// the bottom of this range and a 500k-follower account is off the
			// top of it, which is the finding phase 4 is built to produce.
			Buckets: []float64{.001, .005, .01, .05, .1, .5, 1, 5, 10, 30},
		}, []string{"bucket"}),
		Followers: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "murmur", Subsystem: "fanout", Name: "timeline_writes_total",
			Help: "Timelines written to. This is the write amplification.",
		}),
		Events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "murmur", Subsystem: "fanout", Name: "events_total",
			Help: "Events handled, by outcome.",
		}, []string{"outcome"}),
		Routed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "murmur", Subsystem: "fanout", Name: "routed_total",
			Help: "Posts by fanout mode and author size.",
		}, []string{"mode", "bucket"}),
		Outstanding: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "murmur", Subsystem: "fanout", Name: "consumer_outstanding",
			Help: "Messages the consumer still owes: undelivered plus in flight.",
		}),
		Redelivered: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "murmur", Subsystem: "fanout", Name: "consumer_redelivered",
			Help: "Messages currently being redelivered after a missed acknowledgement.",
		}),
	}
	registry.MustRegister(m.Duration, m.Followers, m.Events, m.Routed, m.Outstanding, m.Redelivered)
	return m
}

// followerBucket labels a fanout by order of magnitude.
func followerBucket(n int) string {
	switch {
	case n < 100:
		return "lt_100"
	case n < 1_000:
		return "100_1k"
	case n < 10_000:
		return "1k_10k"
	case n < 100_000:
		return "10k_100k"
	default:
		return "gte_100k"
	}
}

// Worker consumes post.created and materialises timelines.
type Worker struct {
	consumer jetstream.Consumer
	js       jetstream.JetStream
	social   socialv1.SocialServiceClient
	timeline *timeline.Store
	router   *Router
	opts     Options
}

// New builds a worker.
func New(
	consumer jetstream.Consumer,
	js jetstream.JetStream,
	social socialv1.SocialServiceClient,
	timelines *timeline.Store,
	opts Options,
) *Worker {
	if opts.Concurrency <= 0 {
		opts.Concurrency = 8
	}
	if opts.FollowerPage <= 0 {
		opts.FollowerPage = 1000
	}
	if opts.MaxDeliver <= 0 {
		opts.MaxDeliver = 5
	}
	return &Worker{
		consumer: consumer,
		js:       js,
		social:   social,
		timeline: timelines,
		router:   NewRouter(social, opts.Threshold, opts.RouteCacheTTL, nil),
		opts:     opts,
	}
}

// Component returns the worker as a lifecycle component.
func (w *Worker) Component() lifecycle.Component {
	return lifecycle.Component{Name: "fanout-consumer", Start: w.Run}
}

// Run consumes until the context is cancelled.
//
// Messages are pulled in batches and handed to a bounded pool. Nothing is
// acknowledged until its fanout has finished, so a worker killed at any point
// leaves every message it had not completed on the stream.
func (w *Worker) Run(ctx context.Context) error {
	w.opts.Log.Info("fanout worker started",
		"concurrency", w.opts.Concurrency,
		"follower_page", w.opts.FollowerPage,
		"timeline_cap", w.timeline.Cap(),
		"threshold", w.router.Threshold())

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(w.opts.Concurrency)

	go w.pollPending(groupCtx)

	for {
		if groupCtx.Err() != nil {
			break
		}

		batch, err := w.consumer.Fetch(w.opts.Concurrency, jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			if groupCtx.Err() != nil {
				break
			}
			w.opts.Log.Warn("fetch failed, retrying", "error", err)
			select {
			case <-groupCtx.Done():
			case <-time.After(time.Second):
			}
			continue
		}

		for msg := range batch.Messages() {
			group.Go(func() error {
				w.handle(groupCtx, msg)
				// Handler failures are expressed by not acknowledging, which
				// redelivers. Returning an error here would tear the group
				// down and stop the worker for a single bad message.
				return nil
			})
		}
		if err := batch.Error(); err != nil && groupCtx.Err() == nil {
			w.opts.Log.Warn("batch ended with an error", "error", err)
		}
	}

	// Wait for in-flight fanouts before returning, so a graceful shutdown
	// finishes the work it started rather than relying on redelivery.
	if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return ctx.Err()
}

// handle fans one event out.
func (w *Worker) handle(ctx context.Context, msg jetstream.Msg) {
	start := time.Now()

	var event eventsv1.PostCreated
	if err := proto.Unmarshal(msg.Data(), &event); err != nil {
		// Unparseable. Retrying cannot help, so dead-letter it immediately
		// rather than spending the whole delivery budget on it.
		w.deadLetter(ctx, msg, "malformed payload", err)
		return
	}

	if event.GetPostId() == "" || event.GetAuthorId() == "" {
		w.deadLetter(ctx, msg, "event is missing identifiers", nil)
		return
	}

	written, err := w.fanout(ctx, &event)
	if err != nil {
		if ctx.Err() != nil {
			// Shutting down. Leave it unacknowledged; another worker, or this
			// one after a restart, will take it.
			return
		}

		if bus.IsLastDelivery(msg, w.opts.MaxDeliver) {
			w.deadLetter(ctx, msg, "delivery budget exhausted", err)
			return
		}

		w.opts.Log.Warn("fanout failed, leaving for redelivery",
			"post_id", event.GetPostId(), "error", err)
		w.countEvent("retry")
		if nakErr := msg.Nak(); nakErr != nil {
			w.opts.Log.Warn("nak failed", "error", nakErr)
		}
		return
	}

	if err := msg.Ack(); err != nil {
		// The work is done but the ack did not land, so this will be
		// redelivered. That is safe — re-running the fanout writes the same
		// members to the same sorted sets and changes nothing.
		w.opts.Log.Warn("ack failed; the event will be redelivered harmlessly",
			"post_id", event.GetPostId(), "error", err)
	}

	elapsed := time.Since(start)
	if w.opts.Metrics != nil {
		w.opts.Metrics.Duration.WithLabelValues(followerBucket(written)).Observe(elapsed.Seconds())
		w.opts.Metrics.Followers.Add(float64(written))
	}
	w.countEvent("ok")

	w.opts.Log.Debug("fanned out",
		"post_id", event.GetPostId(),
		"author_id", event.GetAuthorId(),
		"timelines", written,
		"duration_ms", float64(elapsed.Microseconds())/1000)
}

// fanout materialises one post, by whichever route its author's size calls
// for. It returns how many follower timelines were written.
func (w *Worker) fanout(ctx context.Context, event *eventsv1.PostCreated) (int, error) {
	score := event.GetCreatedAt().AsTime().UnixMilli()

	// The author's own feed is written for every post regardless of mode.
	//
	// It costs one ZADD whatever the follower count, and it buys the two
	// transitions for free. An account crossing the threshold upward needs no
	// backfill, because its recent posts are already here for readers to merge.
	// An account crossing back down is safe for the same reason: the posts it
	// published while heavy are recoverable rather than stranded.
	if err := w.timeline.PushAuthor(ctx, event.GetAuthorId(), event.GetPostId(), score); err != nil {
		return 0, err
	}

	mode, followers, err := w.router.Route(ctx, event.GetAuthorId())
	if err != nil {
		return 0, fmt.Errorf("route post %s: %w", event.GetPostId(), err)
	}

	if w.opts.Metrics != nil {
		w.opts.Metrics.Routed.WithLabelValues(string(mode), followerBucket(int(followers))).Inc()
	}

	if mode == ModePull {
		// Deliberately nothing. This is the entire cutover: an account above
		// the threshold stops paying N writes per post, and its readers pay a
		// merge instead. The post is already in the author's feed above, so
		// every follower will see it on their next read.
		w.opts.Log.Debug("post routed to pull",
			"post_id", event.GetPostId(),
			"author_id", event.GetAuthorId(),
			"followers", followers)
		return 0, nil
	}

	return w.push(ctx, event, score)
}

// push writes the post into every follower's timeline, a page at a time.
func (w *Worker) push(ctx context.Context, event *eventsv1.PostCreated, score int64) (int, error) {
	cursor := ""
	written := 0

	for {
		resp, err := w.social.ListFollowers(ctx, &socialv1.ListFollowersRequest{
			UserId:    event.GetAuthorId(),
			PageSize:  int32(w.opts.FollowerPage),
			PageToken: cursor,
		})
		if err != nil {
			return written, fmt.Errorf("list followers of %s: %w", event.GetAuthorId(), err)
		}

		followers := resp.GetFollowerIds()
		if len(followers) == 0 {
			break
		}

		if err := w.timeline.Push(ctx, followers, event.GetPostId(), score); err != nil {
			return written, fmt.Errorf("push to timelines: %w", err)
		}
		written += len(followers)

		cursor = resp.GetNextPageToken()
		if cursor == "" {
			break
		}
	}

	return written, nil
}

// deadLetter moves a message that cannot succeed onto the dead-letter subject
// and acknowledges it.
//
// Acknowledging a failure feels wrong and is right. The alternative is a
// message that is redelivered forever, and on an ordered consumer that means
// one bad event stops every good event behind it. Parking it costs one lost
// fanout; not parking it costs all of them.
func (w *Worker) deadLetter(ctx context.Context, msg jetstream.Msg, reason string, cause error) {
	sequence, _ := bus.StreamSequence(msg)

	w.opts.Log.Error("dead-lettering event",
		"reason", reason, "stream_seq", sequence, "error", cause)

	if _, err := w.js.Publish(ctx, bus.SubjectPostsDead, msg.Data()); err != nil {
		// Publishing the dead letter failed. Do not ack: better to retry the
		// whole thing than to lose the event with no record anywhere.
		w.opts.Log.Error("could not publish to the dead-letter subject; leaving the message unacked",
			"stream_seq", sequence, "error", err)
		w.countEvent("dead_letter_failed")
		return
	}

	if err := msg.Ack(); err != nil {
		w.opts.Log.Warn("ack after dead-letter failed", "error", err)
	}
	w.countEvent("dead_lettered")
}

func (w *Worker) countEvent(outcome string) {
	if w.opts.Metrics != nil {
		w.opts.Metrics.Events.WithLabelValues(outcome).Inc()
	}
}

// pollPending keeps the consumer-lag gauges current.
//
// Lag is the number that says whether the fanout is keeping up with the
// writes, and it is the signal phase 8 scales the worker pool on.
func (w *Worker) pollPending(ctx context.Context) {
	if w.opts.Metrics == nil {
		return
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		info, err := w.consumer.Info(ctx)
		if err != nil {
			continue
		}
		w.opts.Metrics.Outstanding.Set(float64(info.NumPending) + float64(info.NumAckPending))
		w.opts.Metrics.Redelivered.Set(float64(info.NumRedelivered))
	}
}
