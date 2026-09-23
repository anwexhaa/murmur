package gateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
)

// postSource counts GetPost calls, which is the whole measurement: the
// hydrator's claim is about how many of them a broadcast produces.
type postSource struct {
	socialv1.SocialServiceClient

	calls   atomic.Int64
	missing bool
	err     error

	// block holds every fetch open until it is closed, so a test can be
	// certain the callers really are concurrent rather than merely quick.
	block chan struct{}
}

func (p *postSource) GetPost(
	ctx context.Context,
	req *socialv1.GetPostRequest,
	_ ...grpc.CallOption,
) (*socialv1.GetPostResponse, error) {
	p.calls.Add(1)

	if p.block != nil {
		select {
		case <-p.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if p.err != nil {
		return nil, p.err
	}
	if p.missing {
		return nil, status.Error(codes.NotFound, "post not found")
	}
	return &socialv1.GetPostResponse{Post: &socialv1.Post{
		Id: req.GetId(), AuthorId: "author", Body: "body of " + req.GetId(),
		CreatedAt: timestamppb.Now(),
	}}, nil
}

func newHydrator(t *testing.T, source socialv1.SocialServiceClient, opts HydratorOptions) (*Hydrator, *prometheus.Registry) {
	t.Helper()
	registry := prometheus.NewRegistry()
	if opts.Metrics == nil {
		opts.Metrics = NewHydratorMetrics(registry)
	}
	hydrator, err := NewHydrator(source, opts)
	if err != nil {
		t.Fatalf("new hydrator: %v", err)
	}
	return hydrator, registry
}

func TestHydratorFetchesOnceAndServesFromMemory(t *testing.T) {
	source := &postSource{}
	hydrator, _ := newHydrator(t, source, HydratorOptions{})

	for i := 0; i < 50; i++ {
		post, err := hydrator.Get(context.Background(), "post-1")
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if post.GetId() != "post-1" {
			t.Fatalf("got post %q", post.GetId())
		}
	}

	if got := source.calls.Load(); got != 1 {
		t.Fatalf("fifty lookups made %d downstream calls, want 1", got)
	}
}

// TestHydratorCollapsesABroadcast is the test the capacity run asked for.
//
// A broadcast wakes every subscriber at once, so every one of them misses the
// cache simultaneously — the worst possible case for a cache without
// singleflight, and the one that actually happens every time somebody posts.
func TestHydratorCollapsesABroadcast(t *testing.T) {
	source := &postSource{block: make(chan struct{})}
	hydrator, registry := newHydrator(t, source, HydratorOptions{})

	const subscribers = 500

	var ready, done sync.WaitGroup
	ready.Add(subscribers)
	done.Add(subscribers)
	release := make(chan struct{})

	errs := make(chan error, subscribers)
	for i := 0; i < subscribers; i++ {
		go func() {
			defer done.Done()
			ready.Done()
			<-release

			if _, err := hydrator.Get(context.Background(), "viral"); err != nil {
				errs <- err
			}
		}()
	}

	ready.Wait()
	close(release)

	// Let the waiters pile up behind the one in-flight fetch before letting it
	// finish. Without this the test would pass on timing rather than on
	// singleflight.
	waitFor(t, func() bool { return source.calls.Load() == 1 })
	time.Sleep(20 * time.Millisecond)
	close(source.block)

	done.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("subscriber: %v", err)
	}

	if got := source.calls.Load(); got != 1 {
		t.Fatalf("%d subscribers produced %d downstream calls, want 1", subscribers, got)
	}
	if collapsed := counterValue(t, registry, "murmur_hydrator_collapsed_total", nil); collapsed == 0 {
		t.Fatal("singleflight collapsed nothing, so the counter is not measuring what it claims")
	}
	if source := counterValue(t, registry, "murmur_hydrator_lookups_total", map[string]string{"tier": "source"}); source != 1 {
		t.Fatalf("source tier counted %v fetches, want exactly 1", source)
	}
}

// TestHydratorRemembersAMissingPost covers the delete race: a post can vanish
// between the notification and the lookup, and every subscriber is about to
// ask for it.
func TestHydratorRemembersAMissingPost(t *testing.T) {
	source := &postSource{missing: true}
	hydrator, registry := newHydrator(t, source, HydratorOptions{})

	for i := 0; i < 20; i++ {
		_, err := hydrator.Get(context.Background(), "deleted")
		if status.Code(err) != codes.NotFound {
			t.Fatalf("get %d: want NotFound, got %v", i, err)
		}
	}

	if got := source.calls.Load(); got != 1 {
		t.Fatalf("twenty lookups for a deleted post made %d calls, want 1", got)
	}
	if missing := counterValue(t, registry, "murmur_hydrator_lookups_total", map[string]string{"tier": "missing"}); missing != 20 {
		t.Fatalf("missing tier counted %v, want 20", missing)
	}
}

// TestHydratorDoesNotCacheFailures separates "this post does not exist", which
// is stable, from "the lookup failed", which is not. Remembering the second
// would turn one blip into TTL seconds of empty updates.
func TestHydratorDoesNotCacheFailures(t *testing.T) {
	source := &postSource{err: errors.New("downstream is having a day")}
	hydrator, _ := newHydrator(t, source, HydratorOptions{})

	for i := 0; i < 3; i++ {
		if _, err := hydrator.Get(context.Background(), "post-1"); err == nil {
			t.Fatalf("get %d: expected an error", i)
		}
	}

	if got := source.calls.Load(); got != 3 {
		t.Fatalf("a failed lookup was cached: %d calls for 3 attempts", got)
	}
}

func TestHydratorExpiresItsEntries(t *testing.T) {
	source := &postSource{}

	now := time.Now()
	clock := func() time.Time { return now }
	hydrator, _ := newHydrator(t, source, HydratorOptions{TTL: time.Second, Now: clock})

	if _, err := hydrator.Get(context.Background(), "post-1"); err != nil {
		t.Fatalf("first get: %v", err)
	}
	now = now.Add(999 * time.Millisecond)
	if _, err := hydrator.Get(context.Background(), "post-1"); err != nil {
		t.Fatalf("within ttl: %v", err)
	}
	if got := source.calls.Load(); got != 1 {
		t.Fatalf("a lookup inside the TTL refetched: %d calls", got)
	}

	now = now.Add(2 * time.Millisecond)
	if _, err := hydrator.Get(context.Background(), "post-1"); err != nil {
		t.Fatalf("past ttl: %v", err)
	}
	if got := source.calls.Load(); got != 2 {
		t.Fatalf("a lookup past the TTL was served stale: %d calls", got)
	}
}

// TestHydratorOutlivesTheCallerThatTriggeredIt is the reason the fetch runs on
// a detached context.
//
// The first subscriber to ask is only first by luck. If its socket closes
// while the shared fetch is in flight, cancelling that fetch would fail the
// lookup for every other subscriber waiting behind it — a disconnect on one
// connection silently dropping an update on hundreds of others.
func TestHydratorOutlivesTheCallerThatTriggeredIt(t *testing.T) {
	source := &postSource{block: make(chan struct{})}
	hydrator, _ := newHydrator(t, source, HydratorOptions{})

	leaderCtx, cancelLeader := context.WithCancel(context.Background())

	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		_, _ = hydrator.Get(leaderCtx, "post-1")
	}()

	waitFor(t, func() bool { return source.calls.Load() == 1 })

	followerResult := make(chan error, 1)
	go func() {
		_, err := hydrator.Get(context.Background(), "post-1")
		followerResult <- err
	}()

	// Give the follower a moment to join the flight, then abandon the leader.
	time.Sleep(20 * time.Millisecond)
	cancelLeader()

	// The leader is still inside singleflight.Do and stays there until the
	// fetch it owns returns — cancelling its context detaches the work from it
	// but does not hand the work to somebody else. Releasing the fetch first is
	// therefore the only order that does not deadlock, and the fact that this
	// is the only order says something true about the design: a caller that
	// triggers a shared fetch is committed to it, bounded by hydrateTimeout.
	close(source.block)

	select {
	case err := <-followerResult:
		if err != nil {
			t.Fatalf("the follower was failed by the leader's disconnect: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the follower never returned")
	}

	<-leaderDone

	if got := source.calls.Load(); got != 1 {
		t.Fatalf("expected one shared fetch, got %d", got)
	}
}

func TestHydratorIsSafeUnderConcurrentKeys(t *testing.T) {
	source := &postSource{}
	hydrator, _ := newHydrator(t, source, HydratorOptions{})

	const (
		posts   = 20
		readers = 50
	)

	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < posts; j++ {
				if _, err := hydrator.Get(context.Background(), postID(j)); err != nil {
					t.Errorf("get: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	// Not "exactly posts": a reader can arrive after an entry is written but
	// before another reader's flight for the same key has registered, which is
	// a legal outcome for any cache. The claim is that the work is bounded by
	// the number of distinct posts and not by the number of readers.
	calls := source.calls.Load()
	if calls < posts {
		t.Fatalf("%d calls for %d distinct posts", calls, posts)
	}
	if calls > posts*2 {
		t.Fatalf("%d calls for %d posts across %d readers: the sharing is not working", calls, posts, readers)
	}
}

func postID(i int) string { return "post-" + string(rune('a'+i)) }

// waitFor polls until cond holds, so a test can synchronise on a condition
// rather than on a sleep long enough to be reliable and slow enough to hurt.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition never became true")
}

// counterValue reads one counter out of a registry, optionally matching
// labels.
func counterValue(t *testing.T, registry *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if !matchesLabels(metric.GetLabel(), labels) {
				continue
			}
			return metric.GetCounter().GetValue()
		}
	}
	return 0
}

type labelPair interface {
	GetName() string
	GetValue() string
}

func matchesLabels[T labelPair](pairs []T, want map[string]string) bool {
	for name, value := range want {
		found := false
		for _, pair := range pairs {
			if pair.GetName() == name && pair.GetValue() == value {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
