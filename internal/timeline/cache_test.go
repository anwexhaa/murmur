package timeline_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
	"github.com/anwexhaa/murmur/internal/timeline"
)

// countingPosts answers BatchGetPosts and records how many times it was asked.
// The call count is the whole measurement: a cache that works is a cache that
// stops this number growing.
type countingPosts struct {
	socialv1.SocialServiceClient

	mu    sync.Mutex
	posts map[string]*socialv1.Post

	calls atomic.Int64
	// delay makes the fetch slow enough for concurrent callers to pile up
	// behind it, which is the condition singleflight exists for.
	delay time.Duration
}

func (c *countingPosts) BatchGetPosts(
	_ context.Context,
	req *socialv1.BatchGetPostsRequest,
	_ ...grpc.CallOption,
) (*socialv1.BatchGetPostsResponse, error) {
	c.calls.Add(1)
	if c.delay > 0 {
		time.Sleep(c.delay)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	out := make(map[string]*socialv1.Post, len(req.GetIds()))
	for _, id := range req.GetIds() {
		if post, ok := c.posts[id]; ok {
			out[id] = post
		}
	}
	return &socialv1.BatchGetPostsResponse{Posts: out}, nil
}

func (c *countingPosts) remove(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.posts, id)
}

func newCountingPosts(ids ...string) *countingPosts {
	posts := make(map[string]*socialv1.Post, len(ids))
	for _, id := range ids {
		posts[id] = &socialv1.Post{
			Id: id, AuthorId: "author", Body: "body of " + id,
			CreatedAt: timestamppb.Now(),
		}
	}
	return &countingPosts{posts: posts}
}

func newCache(t *testing.T, source socialv1.SocialServiceClient, opts timeline.CacheOptions) *timeline.PostCache {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test: -short")
	}
	if testClient == nil {
		t.Fatal("no redis: TestMain did not start one")
	}
	if err := testClient.FlushDB(t.Context()).Err(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	opts.Log = quietTestLogger()
	cache, err := timeline.NewPostCache(testClient, source, opts)
	if err != nil {
		t.Fatalf("NewPostCache() = %v", err)
	}
	return cache
}

func TestCacheServesFromTheSourceThenRemembers(t *testing.T) {
	source := newCountingPosts("01AAA", "01BBB")
	cache := newCache(t, source, timeline.CacheOptions{LocalTTL: time.Minute})

	first, err := cache.Get(t.Context(), []string{"01AAA", "01BBB"})
	if err != nil {
		t.Fatalf("Get() = %v", err)
	}
	if len(first) != 2 {
		t.Fatalf("got %d posts, want 2", len(first))
	}
	if got := source.calls.Load(); got != 2 {
		t.Errorf("source was asked %d times for 2 uncached posts, want 2", got)
	}

	for range 20 {
		if _, err := cache.Get(t.Context(), []string{"01AAA", "01BBB"}); err != nil {
			t.Fatalf("Get() = %v", err)
		}
	}
	if got := source.calls.Load(); got != 2 {
		t.Errorf("source was asked %d times after caching, want it to stay at 2", got)
	}
}

// The second tier is what makes a cache useful across replicas: a post
// fetched by one process should be free for the next.
func TestRedisTierServesASecondProcess(t *testing.T) {
	source := newCountingPosts("01AAA")

	first := newCache(t, source, timeline.CacheOptions{LocalTTL: time.Minute})
	if _, err := first.Get(t.Context(), []string{"01AAA"}); err != nil {
		t.Fatalf("Get() = %v", err)
	}
	if got := source.calls.Load(); got != 1 {
		t.Fatalf("source calls = %d, want 1", got)
	}

	// A second cache over the same Redis, with an empty local tier — which is
	// what another replica looks like.
	second, err := timeline.NewPostCache(testClient, source, timeline.CacheOptions{
		LocalTTL: time.Minute, Log: quietTestLogger(),
	})
	if err != nil {
		t.Fatalf("NewPostCache() = %v", err)
	}

	got, err := second.Get(t.Context(), []string{"01AAA"})
	if err != nil {
		t.Fatalf("Get() = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("second cache got %d posts, want 1", len(got))
	}
	if calls := source.calls.Load(); calls != 1 {
		t.Errorf("source calls = %d, want the second process served from redis", calls)
	}
}

// The viral-post case. Without singleflight, a thousand concurrent readers
// produce a thousand identical fetches in the instant after a miss.
func TestSingleflightCollapsesAStampede(t *testing.T) {
	source := newCountingPosts("01VIRAL")
	source.delay = 50 * time.Millisecond

	cache := newCache(t, source, timeline.CacheOptions{LocalTTL: time.Minute})

	const readers = 500
	var wg sync.WaitGroup
	errs := make(chan error, readers)

	// Released together, so they all miss at the same instant.
	start := make(chan struct{})

	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			posts, err := cache.Get(context.Background(), []string{"01VIRAL"})
			if err != nil {
				errs <- err
				return
			}
			if len(posts) != 1 {
				errs <- fmt.Errorf("got %d posts, want 1", len(posts))
			}
		}()
	}

	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Get() = %v", err)
	}

	calls := source.calls.Load()
	t.Logf("%d concurrent readers produced %d downstream fetches", readers, calls)

	// Not exactly one: readers arriving after a flight completes but before
	// the entry is visible start a new one. A small constant is the property;
	// linear growth is the bug.
	if calls > 20 {
		t.Errorf("source was asked %d times by %d concurrent readers, want a small constant", calls, readers)
	}
}

// Invalidation must clear both tiers, or a delete would be undone by the next
// read promoting the Redis copy back into memory.
func TestInvalidateClearsBothTiers(t *testing.T) {
	source := newCountingPosts("01AAA")
	cache := newCache(t, source, timeline.CacheOptions{LocalTTL: time.Minute})

	if _, err := cache.Get(t.Context(), []string{"01AAA"}); err != nil {
		t.Fatalf("Get() = %v", err)
	}

	source.remove("01AAA")
	if err := cache.Invalidate(t.Context(), "01AAA"); err != nil {
		t.Fatalf("Invalidate() = %v", err)
	}

	got, err := cache.Get(t.Context(), []string{"01AAA"})
	if err != nil {
		t.Fatalf("Get() = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d posts after invalidation, want the deleted post gone", len(got))
	}
}

// The documented staleness bound.
//
// Invalidation reaches Redis immediately and other replicas' memory only via
// an event. If that event is lost, the local tier is the one place a deleted
// post can still be served — and the TTL is the guarantee that it stops. This
// test is what makes that a bound rather than a hope.
func TestLocalTierExpiresWithinItsTTL(t *testing.T) {
	const ttl = 300 * time.Millisecond

	source := newCountingPosts("01AAA")
	cache := newCache(t, source, timeline.CacheOptions{LocalTTL: ttl})

	if _, err := cache.Get(t.Context(), []string{"01AAA"}); err != nil {
		t.Fatalf("Get() = %v", err)
	}

	// Delete it everywhere except this process's memory, which is exactly the
	// state a replica that missed the invalidation event is in.
	source.remove("01AAA")
	if err := testClient.Del(t.Context(), "post:01AAA").Err(); err != nil {
		t.Fatalf("del: %v", err)
	}

	// Still served, because the local entry is fresh. This is the stale window,
	// and asserting it exists is as important as asserting it ends.
	got, err := cache.Get(t.Context(), []string{"01AAA"})
	if err != nil {
		t.Fatalf("Get() = %v", err)
	}
	if len(got) != 1 {
		t.Fatal("the local tier should still be serving inside its TTL")
	}

	time.Sleep(ttl + 100*time.Millisecond)

	got, err = cache.Get(t.Context(), []string{"01AAA"})
	if err != nil {
		t.Fatalf("Get() = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("the deleted post is still served %v after the %v TTL", ttl+100*time.Millisecond, ttl)
	}
}

// A post that does not exist must be absent, not an error: one deleted post in
// a page of fifty must not fail the other forty-nine.
func TestMissingPostsAreAbsentNotFatal(t *testing.T) {
	source := newCountingPosts("01AAA")
	cache := newCache(t, source, timeline.CacheOptions{LocalTTL: time.Minute})

	got, err := cache.Get(t.Context(), []string{"01AAA", "01GONE"})
	if err != nil {
		t.Fatalf("Get() = %v, want a partial result rather than an error", err)
	}
	if len(got) != 1 {
		t.Errorf("got %d posts, want just the one that exists", len(got))
	}
	if _, present := got["01GONE"]; present {
		t.Error("a post that does not exist came back anyway")
	}
}

func TestCacheHandlesAnEmptyRequest(t *testing.T) {
	cache := newCache(t, newCountingPosts(), timeline.CacheOptions{LocalTTL: time.Minute})

	got, err := cache.Get(t.Context(), nil)
	if err != nil {
		t.Fatalf("Get(nil) = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d posts for an empty request", len(got))
	}
}

func TestCacheIsSafeForConcurrentUse(t *testing.T) {
	ids := make([]string, 50)
	for i := range ids {
		ids[i] = fmt.Sprintf("01POST%020d", i)
	}
	source := newCountingPosts(ids...)
	cache := newCache(t, source, timeline.CacheOptions{LocalTTL: time.Minute})

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				if _, err := cache.Get(context.Background(), ids); err != nil {
					t.Errorf("Get() = %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
