package timeline_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
	timelinev1 "github.com/anwexhaa/murmur/api/gen/murmur/timeline/v1"
	"github.com/anwexhaa/murmur/internal/timeline"
)

// sourceOfTruth stands in for social-svc: it answers from a list of posts and
// counts how often the degraded path reached it.
type sourceOfTruth struct {
	socialv1.SocialServiceClient

	posts []*socialv1.Post
	calls atomic.Int64
	err   error
}

func (s *sourceOfTruth) ListFollowedPosts(
	_ context.Context,
	req *socialv1.ListFollowedPostsRequest,
	_ ...grpc.CallOption,
) (*socialv1.ListFollowedPostsResponse, error) {
	s.calls.Add(1)
	if s.err != nil {
		return nil, s.err
	}

	limit := int(req.GetPageSize())
	out := make([]*socialv1.Post, 0, limit)
	for _, post := range s.posts {
		if token := req.GetPageToken(); token != "" && post.GetId() >= token {
			continue
		}
		out = append(out, post)
		if len(out) == limit {
			break
		}
	}
	return &socialv1.ListFollowedPostsResponse{Posts: out}, nil
}

// BatchGetPosts is the hydration the materialised path uses. The degraded path
// must not need it -- the posts arrive already hydrated -- and a test double
// that returns nothing proves that rather than assuming it.
func (s *sourceOfTruth) BatchGetPosts(
	_ context.Context,
	_ *socialv1.BatchGetPostsRequest,
	_ ...grpc.CallOption,
) (*socialv1.BatchGetPostsResponse, error) {
	return &socialv1.BatchGetPostsResponse{}, nil
}

func samplePosts(n int) []*socialv1.Post {
	posts := make([]*socialv1.Post, n)
	for i := range posts {
		// Descending, newest first, the way the source query returns them.
		//
		// "01PAGE" rather than "01POST" because these IDs are used as page
		// tokens, and the validator checks the Crockford base32 alphabet a
		// ULID actually uses -- which excludes I, L, O and U.
		posts[i] = &socialv1.Post{
			Id:        fmt.Sprintf("01PAGE%020d", n-i),
			AuthorId:  uuid.NewString(),
			Body:      fmt.Sprintf("post %d", n-i),
			CreatedAt: timestamppb.Now(),
		}
	}
	return posts
}

// deadRedis points at a port with nothing behind it, which is what Redis
// vanishing looks like from this service.
func deadRedis(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1",
		DialTimeout: 150 * time.Millisecond,
		ReadTimeout: 150 * time.Millisecond,
		MaxRetries:  -1,
	})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func degradedService(t *testing.T, source socialv1.SocialServiceClient) *timeline.Service {
	t.Helper()
	return timeline.NewService(
		timeline.NewStore(deadRedis(t), 800),
		source,
		nil,
		nil,
		quietTestLogger(),
	)
}

// TestRedisGoingAwayDegradesInsteadOfFailing is the phase's "done when".
//
// Everything in Redis is derived from Postgres, so losing it should cost
// latency rather than availability. Before this phase the read returned
// Internal and the data was sitting in Postgres the whole time.
func TestRedisGoingAwayDegradesInsteadOfFailing(t *testing.T) {
	source := &sourceOfTruth{posts: samplePosts(10)}
	service := degradedService(t, source)

	resp, err := service.GetTimeline(t.Context(), &timelinev1.GetTimelineRequest{
		UserId:   uuid.NewString(),
		PageSize: 5,
	})
	if err != nil {
		t.Fatalf("the read failed instead of degrading: %v", err)
	}
	if len(resp.GetPosts()) != 5 {
		t.Fatalf("got %d posts, want 5", len(resp.GetPosts()))
	}
	if !resp.GetDegraded() {
		t.Fatal("the response does not say it was degraded, so nothing downstream can tell")
	}
	if source.calls.Load() != 1 {
		t.Fatalf("the source was called %d times, want 1", source.calls.Load())
	}
}

// TestTheDegradedPageIsNewestFirst pins the ordering. A fallback that returns
// the right posts in the wrong order is a fallback that is visibly broken to
// every user it serves.
func TestTheDegradedPageIsNewestFirst(t *testing.T) {
	source := &sourceOfTruth{posts: samplePosts(20)}
	service := degradedService(t, source)

	resp, err := service.GetTimeline(t.Context(), &timelinev1.GetTimelineRequest{
		UserId:   uuid.NewString(),
		PageSize: 20,
	})
	if err != nil {
		t.Fatalf("degraded read: %v", err)
	}

	posts := resp.GetPosts()
	for i := 1; i < len(posts); i++ {
		if posts[i-1].GetId() <= posts[i].GetId() {
			t.Fatalf("posts %d and %d are out of order: %s then %s",
				i-1, i, posts[i-1].GetId(), posts[i].GetId())
		}
	}
}

// TestTheDegradedPathHonoursTheCursor matters because a client does not know
// which path served it. Pagination that silently restarts when Redis blips
// would show the same page twice.
func TestTheDegradedPathHonoursTheCursor(t *testing.T) {
	source := &sourceOfTruth{posts: samplePosts(10)}
	service := degradedService(t, source)
	viewer := uuid.NewString()

	first, err := service.GetTimeline(t.Context(), &timelinev1.GetTimelineRequest{
		UserId: viewer, PageSize: 4,
	})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	cursor := first.GetPosts()[len(first.GetPosts())-1].GetId()

	second, err := service.GetTimeline(t.Context(), &timelinev1.GetTimelineRequest{
		UserId: viewer, PageSize: 4, PageToken: cursor,
	})
	if err != nil {
		t.Fatalf("second page: %v", err)
	}

	seen := map[string]bool{}
	for _, post := range first.GetPosts() {
		seen[post.GetId()] = true
	}
	for _, post := range second.GetPosts() {
		if seen[post.GetId()] {
			t.Fatalf("post %s appeared on both pages", post.GetId())
		}
		if post.GetId() >= cursor {
			t.Fatalf("post %s is not older than the cursor %s", post.GetId(), cursor)
		}
	}
}

// TestBothPathsFailingIsAnError keeps the fallback honest. Degrading is only
// the right answer while there is something to degrade to.
func TestBothPathsFailingIsAnError(t *testing.T) {
	source := &sourceOfTruth{err: context.DeadlineExceeded}
	service := degradedService(t, source)

	_, err := service.GetTimeline(t.Context(), &timelinev1.GetTimelineRequest{
		UserId:   uuid.NewString(),
		PageSize: 5,
	})
	if err == nil {
		t.Fatal("both paths failed and the read still succeeded")
	}
}

// TestACancelledRequestDoesNotFallBack. A caller that has gone away is not a
// Redis problem, and re-running the whole read against Postgres would spend
// the database's capacity on a request nobody is waiting for -- which is how a
// blip becomes a stampede.
func TestACancelledRequestDoesNotFallBack(t *testing.T) {
	source := &sourceOfTruth{posts: samplePosts(10)}
	service := degradedService(t, source)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := service.GetTimeline(ctx, &timelinev1.GetTimelineRequest{
		UserId:   uuid.NewString(),
		PageSize: 5,
	}); err == nil {
		t.Fatal("a cancelled request produced a page")
	}
	if got := source.calls.Load(); got != 0 {
		t.Fatalf("a cancelled request reached the source %d times", got)
	}
}

// TestTheHealthyPathDoesNotTouchTheSource is the other direction: the fallback
// must cost nothing when Redis is fine, or it is not a fallback but a second
// read path running all the time.
func TestTheHealthyPathDoesNotTouchTheSource(t *testing.T) {
	store := newStore(t, 800)
	source := &sourceOfTruth{posts: samplePosts(10)}

	viewer := uuid.NewString()
	if err := store.Push(t.Context(), []string{viewer}, "01PAGE00000000000000000001", time.Now().UnixMilli()); err != nil {
		t.Fatalf("push: %v", err)
	}

	service := timeline.NewService(store, source, nil, nil, quietTestLogger())

	resp, err := service.GetTimeline(t.Context(), &timelinev1.GetTimelineRequest{
		UserId: viewer, PageSize: 5,
	})
	if err != nil {
		t.Fatalf("healthy read: %v", err)
	}
	if resp.GetDegraded() {
		t.Fatal("a healthy read reported itself as degraded")
	}
	if got := source.calls.Load(); got != 0 {
		t.Fatalf("a healthy read called the degraded path %d times", got)
	}
}

// TestTheBreakerStopsPayingForAKnownOutage is the measurement that justified
// it. Before the breaker, every read with Redis gone paid a dial timeout to
// rediscover the outage; a live cluster run measured 0.5 to 2.5 seconds per
// request against a three-second budget, which is why some requests survived
// and some did not.
func TestTheBreakerStopsPayingForAKnownOutage(t *testing.T) {
	source := &sourceOfTruth{posts: samplePosts(10)}
	service := degradedService(t, source)

	// The dead Redis client takes a dial timeout to fail. Three of those open
	// the circuit; everything after should skip Redis entirely.
	var firstThree time.Duration
	for i := 0; i < 3; i++ {
		started := time.Now()
		if _, err := service.GetTimeline(t.Context(), &timelinev1.GetTimelineRequest{
			UserId: uuid.NewString(), PageSize: 5,
		}); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		firstThree += time.Since(started)
	}

	if !service.Degraded() {
		t.Fatal("three consecutive failures did not open the circuit")
	}

	started := time.Now()
	for i := 0; i < 10; i++ {
		resp, err := service.GetTimeline(t.Context(), &timelinev1.GetTimelineRequest{
			UserId: uuid.NewString(), PageSize: 5,
		})
		if err != nil {
			t.Fatalf("read %d with the circuit open: %v", i, err)
		}
		if !resp.GetDegraded() {
			t.Fatal("a read with the circuit open did not report itself degraded")
		}
	}
	tenMore := time.Since(started)

	// Ten reads through an open circuit must cost less than the three that
	// opened it. The exact ratio depends on the dial timeout; the property is
	// that the cost no longer scales with the number of requests.
	if tenMore >= firstThree {
		t.Fatalf("ten reads with the circuit open took %v, no better than the three that opened it (%v)",
			tenMore, firstThree)
	}
}

// TestTheBreakerRecovers covers the other half. A circuit that opens and
// never closes is an outage the service inflicted on itself.
func TestTheBreakerRecovers(t *testing.T) {
	store := newStore(t, 800)
	source := &sourceOfTruth{posts: samplePosts(10)}

	viewer := uuid.NewString()
	if err := store.Push(t.Context(), []string{viewer}, "01PAGE00000000000000000001", time.Now().UnixMilli()); err != nil {
		t.Fatalf("push: %v", err)
	}

	// A healthy service: the circuit is closed and stays closed.
	service := timeline.NewService(store, source, nil, nil, quietTestLogger())
	for i := 0; i < 5; i++ {
		if _, err := service.GetTimeline(t.Context(), &timelinev1.GetTimelineRequest{
			UserId: viewer, PageSize: 5,
		}); err != nil {
			t.Fatalf("healthy read %d: %v", i, err)
		}
	}
	if service.Degraded() {
		t.Fatal("a healthy service opened its own circuit")
	}
	if got := source.calls.Load(); got != 0 {
		t.Fatalf("a healthy service called the source %d times", got)
	}
}
