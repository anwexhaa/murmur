package fanout_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/anwexhaa/murmur/api/gen/murmur/events/v1"
	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
	"github.com/anwexhaa/murmur/internal/fanout"
	"github.com/anwexhaa/murmur/internal/timeline"
)

// These tests need a real Redis and a real NATS with JetStream. Both come from
// the compose stack; there is no mock, because the properties under test —
// redelivery after a crash, script atomicity, dead-lettering — are properties
// of those systems and a mock would only re-assert this file's own beliefs.
var (
	testRedis *redis.Client
	testNC    *nats.Conn
	testJS    jetstream.JetStream
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}

	redisAddr := os.Getenv("MURMUR_TEST_REDIS_ADDR")
	natsURL := os.Getenv("MURMUR_TEST_NATS_URL")
	if redisAddr == "" || natsURL == "" {
		log.Println("skipping fanout integration tests: set MURMUR_TEST_REDIS_ADDR and MURMUR_TEST_NATS_URL")
		os.Exit(m.Run())
	}

	testRedis = redis.NewClient(&redis.Options{Addr: redisAddr, DB: testRedisDB()})

	var err error
	testNC, err = nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("connect nats: %v", err)
	}
	testJS, err = jetstream.New(testNC)
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	code := m.Run()

	_ = testRedis.Close()
	testNC.Close()
	os.Exit(code)
}

// testRedisDB keeps these tests off the development keyspace; they flush it.
func testRedisDB() int {
	if raw := os.Getenv("MURMUR_TEST_REDIS_DB"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			return n
		}
	}
	return 1
}

func requireStack(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test: -short")
	}
	if testRedis == nil || testJS == nil {
		t.Skip("skipping: no redis or nats configured")
	}
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeSocial serves follower pages from an in-memory list.
//
// The follow graph is not what these tests are about — it is exercised
// thoroughly against real Postgres in the social package. Here it is a fixture,
// and making it one keeps each test's setup to a single line.
// The worker fans several events out concurrently, so this fake is called
// from several goroutines at once and has to be safe for that. Found by
// -race, which is the point of running it.
type fakeSocial struct {
	socialv1.SocialServiceClient

	followers []string
	pageSize  int

	// failUntil makes the first N calls fail, to drive the retry path.
	failUntil int

	// followerCount is what the router sees. Zero keeps every post on the
	// push path, which is what these tests are about; the routing decision
	// itself is covered in router_test.go without any infrastructure.
	followerCount int64

	mu    sync.Mutex
	calls int
}

// GetFollowerCount serves the router. The worker asks per post from phase 4,
// so a fake that does not answer it panics on a nil embedded client.
func (f *fakeSocial) GetFollowerCount(
	_ context.Context,
	_ *socialv1.GetFollowerCountRequest,
	_ ...grpc.CallOption,
) (*socialv1.GetFollowerCountResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &socialv1.GetFollowerCountResponse{Followers: f.followerCount}, nil
}

func (f *fakeSocial) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeSocial) ListFollowers(
	_ context.Context,
	req *socialv1.ListFollowersRequest,
	_ ...grpc.CallOption,
) (*socialv1.ListFollowersResponse, error) {
	f.mu.Lock()
	f.calls++
	shouldFail := f.calls <= f.failUntil
	f.mu.Unlock()

	if shouldFail {
		return nil, errors.New("social service is unavailable")
	}

	start := 0
	if token := req.GetPageToken(); token != "" {
		for i, id := range f.followers {
			if id == token {
				start = i + 1
				break
			}
		}
	}

	size := f.pageSize
	if size <= 0 {
		size = int(req.GetPageSize())
	}
	end := min(start+size, len(f.followers))

	page := f.followers[start:end]
	token := ""
	if end < len(f.followers) {
		token = page[len(page)-1]
	}
	return &socialv1.ListFollowersResponse{FollowerIds: page, NextPageToken: token}, nil
}

// uniqueStream gives each test its own stream and consumer, so a test that
// leaves messages behind cannot fail the next one.
func uniqueStream(t *testing.T) (jetstream.Consumer, string) {
	t.Helper()
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	streamName := "TEST_POSTS_" + suffix
	subject := "test." + suffix + ".post.created"

	_, err := testJS.CreateStream(ctx, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{subject, subject + ".dead"},
		Storage:   jetstream.MemoryStorage,
		Retention: jetstream.LimitsPolicy,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	t.Cleanup(func() { _ = testJS.DeleteStream(context.Background(), streamName) })

	consumer, err := testJS.CreateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       "test",
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       2 * time.Second,
		MaxDeliver:    3,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	return consumer, subject
}

func publishPostCreated(t *testing.T, subject, postID, authorID string, created time.Time) {
	t.Helper()

	payload, err := proto.Marshal(&eventsv1.PostCreated{
		PostId:    postID,
		AuthorId:  authorID,
		CreatedAt: timestamppb.New(created),
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	if _, err := testJS.Publish(t.Context(), subject, payload); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func newTimelineStore(t *testing.T, capacity int) *timeline.Store {
	t.Helper()
	if err := testRedis.FlushDB(t.Context()).Err(); err != nil {
		t.Fatalf("flush redis: %v", err)
	}
	return timeline.NewStore(testRedis, capacity)
}

// runWorker starts a worker and returns a function that stops it.
func runWorker(t *testing.T, consumer jetstream.Consumer, social socialv1.SocialServiceClient, store *timeline.Store) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	worker := fanout.New(consumer, testJS, testNC, social, store, fanout.Options{
		Concurrency:  4,
		FollowerPage: 100,
		MaxDeliver:   3,
		Log:          quietLogger(),
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = worker.Run(ctx)
	}()

	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("worker did not stop within 10s")
		}
	}
}

// waitForTimeout is a hang detector, not a performance assertion.
//
// What these tests assert is that the work happens at all; how fast it happens
// is what murmur_fanout_duration_seconds is for. Those are different questions,
// and a deadline tight enough to double as the second one fails for reasons
// that have nothing to do with the code — a cold Docker VM running every
// package in parallel under -race once blew a 20-second wait on a fanout that
// takes two seconds warm. Generous here costs a slow failure in the rare case
// where something really is wedged, and buys a suite that fails only when
// something is wrong.
const waitForTimeout = 90 * time.Second

func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(waitForTimeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func followerList(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("follower-%05d", i)
	}
	return out
}

// The phase's headline claim: a post reaches every follower, verified by
// reading all of them rather than by sampling.
func TestPostReachesEveryFollower(t *testing.T) {
	requireStack(t)

	const followers = 1000
	store := newTimelineStore(t, 800)
	consumer, subject := uniqueStream(t)
	social := &fakeSocial{followers: followerList(followers), pageSize: 100}

	stop := runWorker(t, consumer, social, store)
	defer stop()

	publishPostCreated(t, subject, "01POST0000000000000000000", "author", time.Now())

	waitFor(t, "the last follower to receive the post", func() bool {
		got, err := store.Contains(context.Background(), fmt.Sprintf("follower-%05d", followers-1), "01POST0000000000000000000")
		return err == nil && got
	})

	// Every one, not a sample.
	missing := 0
	for i := range followers {
		present, err := store.Contains(t.Context(), fmt.Sprintf("follower-%05d", i), "01POST0000000000000000000")
		if err != nil {
			t.Fatalf("Contains() = %v", err)
		}
		if !present {
			missing++
		}
	}
	if missing != 0 {
		t.Errorf("%d of %d followers never received the post", missing, followers)
	}
}

// The reason the outbox and explicit acks exist. A worker killed mid-fanout
// must lose nothing, and the redelivery that follows must not double anything.
func TestKillingTheWorkerLosesNothingAndDuplicatesNothing(t *testing.T) {
	requireStack(t)

	const (
		followers = 400
		posts     = 10
		cycles    = 10
	)

	store := newTimelineStore(t, 800)
	consumer, subject := uniqueStream(t)
	social := &fakeSocial{followers: followerList(followers), pageSize: 50}

	postIDs := make([]string, posts)
	for i := range postIDs {
		postIDs[i] = fmt.Sprintf("01POST%020d", i)
		publishPostCreated(t, subject, postIDs[i], "author", time.Now().Add(time.Duration(i)*time.Millisecond))
	}

	// Start and kill the worker repeatedly. Each cycle is short enough to land
	// mid-fanout, which is exactly the window being tested.
	for range cycles {
		stop := runWorker(t, consumer, social, store)
		time.Sleep(120 * time.Millisecond)
		stop()
	}

	// Then let it finish.
	stop := runWorker(t, consumer, social, store)
	defer stop()

	last := fmt.Sprintf("follower-%05d", followers-1)
	waitFor(t, "every post to reach the last follower", func() bool {
		n, err := store.Len(context.Background(), last)
		return err == nil && n == posts
	})

	// Nothing lost: every follower has every post. Nothing duplicated: a
	// sorted set cannot hold a member twice, so a length of exactly `posts`
	// proves both at once.
	lost, extra := 0, 0
	for i := range followers {
		follower := fmt.Sprintf("follower-%05d", i)

		n, err := store.Len(t.Context(), follower)
		if err != nil {
			t.Fatalf("Len() = %v", err)
		}
		switch {
		case n < posts:
			lost += posts - int(n)
		case n > posts:
			extra += int(n) - posts
		}

		for _, id := range postIDs {
			present, err := store.Contains(t.Context(), follower, id)
			if err != nil {
				t.Fatalf("Contains() = %v", err)
			}
			if !present {
				lost++
			}
		}
	}

	if lost != 0 {
		t.Errorf("lost %d timeline entries across %d kill-restart cycles, want 0", lost, cycles)
	}
	if extra != 0 {
		t.Errorf("found %d duplicate entries, want 0", extra)
	}
}

// A poisoned event must not block everything behind it.
func TestMalformedEventIsDeadLetteredNotRetriedForever(t *testing.T) {
	requireStack(t)

	store := newTimelineStore(t, 800)
	consumer, subject := uniqueStream(t)
	social := &fakeSocial{followers: followerList(5), pageSize: 10}

	// Garbage that is not a PostCreated, followed by a valid event.
	if _, err := testJS.Publish(t.Context(), subject, []byte("this is not protobuf at all")); err != nil {
		t.Fatalf("publish garbage: %v", err)
	}
	publishPostCreated(t, subject, "01POST0000000000000000001", "author", time.Now())

	stop := runWorker(t, consumer, social, store)
	defer stop()

	// The good event gets through, which is the point: the bad one did not
	// wedge the consumer.
	waitFor(t, "the valid event to be processed despite the poisoned one", func() bool {
		got, err := store.Contains(context.Background(), "follower-00000", "01POST0000000000000000001")
		return err == nil && got
	})

	waitFor(t, "the consumer to drain", func() bool {
		info, err := consumer.Info(context.Background())
		return err == nil && info.NumPending == 0 && info.NumAckPending == 0
	})
}

// An event missing its identifiers is unprocessable in the same way, and must
// take the same route.
func TestEventWithoutIdentifiersIsDeadLettered(t *testing.T) {
	requireStack(t)

	store := newTimelineStore(t, 800)
	consumer, subject := uniqueStream(t)
	social := &fakeSocial{followers: followerList(5), pageSize: 10}

	payload, err := proto.Marshal(&eventsv1.PostCreated{CreatedAt: timestamppb.Now()})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := testJS.Publish(t.Context(), subject, payload); err != nil {
		t.Fatalf("publish: %v", err)
	}

	stop := runWorker(t, consumer, social, store)
	defer stop()

	waitFor(t, "the empty event to be parked", func() bool {
		info, err := consumer.Info(context.Background())
		return err == nil && info.NumPending == 0 && info.NumAckPending == 0
	})
}

// A transient downstream failure must be retried, not parked. This is the
// difference between "Postgres blinked" and "this message is broken".
func TestTransientFailureIsRetriedAndThenSucceeds(t *testing.T) {
	requireStack(t)

	store := newTimelineStore(t, 800)
	consumer, subject := uniqueStream(t)

	// Fail the first call, succeed afterwards.
	social := &fakeSocial{followers: followerList(10), pageSize: 100, failUntil: 1}

	publishPostCreated(t, subject, "01POST0000000000000000002", "author", time.Now())

	stop := runWorker(t, consumer, social, store)
	defer stop()

	waitFor(t, "the retried event to succeed", func() bool {
		got, err := store.Contains(context.Background(), "follower-00000", "01POST0000000000000000002")
		return err == nil && got
	})

	if social.callCount() < 2 {
		t.Errorf("social service was called %d times, want a retry after the first failure", social.calls)
	}
}

// Paging matters because fanout walks follower edges by the hundred thousand.
// A worker that stopped at the first page would silently serve a fraction of
// the audience.
func TestFanoutPagesThroughEveryFollower(t *testing.T) {
	requireStack(t)

	const followers = 250
	store := newTimelineStore(t, 800)
	consumer, subject := uniqueStream(t)

	// A page size well below the follower count forces several pages.
	social := &fakeSocial{followers: followerList(followers), pageSize: 25}

	publishPostCreated(t, subject, "01POST0000000000000000003", "author", time.Now())

	stop := runWorker(t, consumer, social, store)
	defer stop()

	lastFollower := fmt.Sprintf("follower-%05d", followers-1)
	waitFor(t, "the follower on the last page to receive the post", func() bool {
		got, err := store.Contains(context.Background(), lastFollower, "01POST0000000000000000003")
		return err == nil && got
	})
}
