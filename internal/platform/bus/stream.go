package bus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Stream and subject names. Constants rather than configuration: a producer
// and a consumer that disagree about the subject fail silently — the publish
// succeeds, nothing consumes it, and the only symptom is timelines that never
// update.
const (
	PostsStream = "POSTS"

	SubjectPostCreated = "post.created"
	// Events that exhausted their redelivery budget land here instead of
	// blocking the consumer. Nothing consumes this yet; that is deliberate.
	// A dead-letter subject nobody reads is still infinitely better than a
	// poison message that stops every other event behind it.
	SubjectPostsDead = "post.dead"

	ConsumerFanout = "fanout"
)

// EnsureStreams creates or updates the streams Murmur needs.
//
// Idempotent, and run by every service that touches the bus at startup. That
// means there is no ordering requirement between services and no separate
// provisioning step to forget.
func EnsureStreams(ctx context.Context, js jetstream.JetStream) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     PostsStream,
		Subjects: []string{SubjectPostCreated, SubjectPostsDead},
		// File storage, not memory. The entire reason this is JetStream rather
		// than core NATS is that a restart must not lose events, and a
		// memory-backed stream would give up exactly that.
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		// Events are worth keeping long enough to replay a bad deploy, and no
		// longer: a post.created from last month describes a timeline that has
		// long since been rebuilt.
		MaxAge:     7 * 24 * time.Hour,
		Discard:    jetstream.DiscardOld,
		Duplicates: 2 * time.Minute,
	})
	if err != nil {
		return fmt.Errorf("ensure stream %s: %w", PostsStream, err)
	}
	return nil
}

// FanoutConsumerConfig is the durable consumer the fanout workers share.
//
// Durable and shared, not one per worker: every worker pulls from the same
// consumer, so adding a worker adds throughput rather than duplicating work.
// The consumer's cursor lives in JetStream, which is what lets a worker die
// mid-batch and another pick up exactly where it stopped.
func FanoutConsumerConfig(maxDeliver int, ackWait time.Duration) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       ConsumerFanout,
		FilterSubject: SubjectPostCreated,
		// Explicit acks are the whole safety property. A message is redelivered
		// unless a worker says it finished, so a process that dies holding a
		// message loses nothing but time.
		AckPolicy:  jetstream.AckExplicitPolicy,
		AckWait:    ackWait,
		MaxDeliver: maxDeliver,
		// Deliver from the start of the stream, so a consumer created after
		// events were published still processes them rather than silently
		// skipping everything older than itself.
		DeliverPolicy: jetstream.DeliverAllPolicy,
		// Backoff between redeliveries. A message failing because Postgres is
		// down should not be retried at full speed while Postgres is down.
		BackOff: []time.Duration{time.Second, 5 * time.Second, 15 * time.Second},
	}
}

// EnsureFanoutConsumer creates or updates the shared consumer.
func EnsureFanoutConsumer(ctx context.Context, js jetstream.JetStream, maxDeliver int, ackWait time.Duration) (jetstream.Consumer, error) {
	consumer, err := js.CreateOrUpdateConsumer(ctx, PostsStream, FanoutConsumerConfig(maxDeliver, ackWait))
	if err != nil {
		return nil, fmt.Errorf("ensure consumer %s: %w", ConsumerFanout, err)
	}
	return consumer, nil
}

// IsLastDelivery reports whether this delivery is the message's final chance.
//
// A handler that fails here must dead-letter the message itself: once the
// delivery budget is spent, JetStream stops redelivering and the event simply
// vanishes. Asking the message how many times it has been tried is the only
// way to notice that moment.
func IsLastDelivery(msg jetstream.Msg, maxDeliver int) bool {
	meta, err := msg.Metadata()
	if err != nil {
		// Without metadata there is no way to know, and treating an unknown as
		// "final" would discard a message that had attempts left.
		return false
	}
	return int(meta.NumDelivered) >= maxDeliver
}

// ErrNoMetadata is returned when a message carries no JetStream metadata,
// which means it did not come from a stream.
var ErrNoMetadata = errors.New("message has no jetstream metadata")

// StreamSequence returns a message's position in the stream, for logging.
func StreamSequence(msg jetstream.Msg) (uint64, error) {
	meta, err := msg.Metadata()
	if err != nil {
		return 0, ErrNoMetadata
	}
	return meta.Sequence.Stream, nil
}
