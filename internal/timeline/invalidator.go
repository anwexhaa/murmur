package timeline

import (
	"context"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/anwexhaa/murmur/api/gen/murmur/events/v1"
	"github.com/anwexhaa/murmur/internal/platform/bus"
)

// Invalidator drops deleted posts from this process's cache.
//
// Redis is shared, so one delete clears it for every replica. Process memory
// is not, so each replica has to be told — and every replica must be told,
// which is why this is an ephemeral per-instance consumer rather than a
// durable shared one. A durable queue-group consumer would deliver each event
// to exactly one replica, leaving the others serving a post that no longer
// exists until their TTL ran out. Here the requirement is the opposite of
// work-sharing: everyone needs the message.
//
// The TTL remains the backstop. If this consumer is down, or the event is
// lost, a replica still stops serving the stale post when its local entry
// expires — which is the bound TestLocalTierExpiresWithinItsTTL asserts.
type Invalidator struct {
	nc    *bus.Conn
	cache *PostCache
	log   *slog.Logger
}

// NewInvalidator builds the consumer.
func NewInvalidator(conn *bus.Conn, cache *PostCache, log *slog.Logger) *Invalidator {
	return &Invalidator{nc: conn, cache: cache, log: log}
}

// Run consumes post.deleted until the context is cancelled.
func (i *Invalidator) Run(ctx context.Context) error {
	// An ordered ephemeral consumer: no durable name, so every replica gets
	// its own cursor and its own copy of every event.
	consumer, err := i.nc.JS.OrderedConsumer(ctx, bus.PostsStream, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{bus.SubjectPostDeleted},
		// Only deletions from now on. Replaying weeks of them at startup would
		// evict a freshly warmed cache to forget posts that are already gone.
		DeliverPolicy: jetstream.DeliverNewPolicy,
	})
	if err != nil {
		return err
	}

	i.log.Info("cache invalidator started", "subject", bus.SubjectPostDeleted)

	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		var event eventsv1.PostDeleted
		if err := proto.Unmarshal(msg.Data(), &event); err != nil {
			i.log.Warn("cache invalidator: malformed post.deleted", "error", err)
			return
		}
		if event.GetPostId() == "" {
			return
		}

		i.cache.InvalidateLocal(event.GetPostId())
		i.log.Debug("dropped a deleted post from the local cache", "post_id", event.GetPostId())
	})
	if err != nil {
		return err
	}
	defer consumeCtx.Stop()

	<-ctx.Done()
	return ctx.Err()
}

// Component returns the invalidator as a lifecycle component.
func (i *Invalidator) Component() (string, func(context.Context) error) {
	return "cache-invalidator", i.Run
}

// WaitForDrain gives the consumer a moment to deliver in-flight messages on
// shutdown. Nothing depends on it — the TTL covers anything missed — but it
// keeps a graceful restart from leaving a needlessly stale window.
func (i *Invalidator) WaitForDrain() { time.Sleep(50 * time.Millisecond) }
