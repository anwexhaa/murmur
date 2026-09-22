package fanout

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
)

// countingSocial answers follower-count lookups and records how many it was
// asked, which is how the cache is observed.
type countingSocial struct {
	socialv1.SocialServiceClient

	mu     sync.Mutex
	counts map[string]int64
	calls  int
	err    error
}

func (c *countingSocial) GetFollowerCount(
	_ context.Context,
	req *socialv1.GetFollowerCountRequest,
	_ ...grpc.CallOption,
) (*socialv1.GetFollowerCountResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.calls++
	if c.err != nil {
		return nil, c.err
	}
	return &socialv1.GetFollowerCountResponse{Followers: c.counts[req.GetUserId()]}, nil
}

func (c *countingSocial) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *countingSocial) set(id string, n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[id] = n
}

func newCountingSocial(counts map[string]int64) *countingSocial {
	return &countingSocial{counts: counts}
}

func TestRouteByThreshold(t *testing.T) {
	social := newCountingSocial(map[string]int64{
		"tiny":       12,
		"just_under": 9_999,
		"exactly":    10_000,
		"whale":      500_000,
	})
	router := NewRouter(social, 10_000, time.Minute, nil)

	tests := []struct {
		author string
		want   Mode
	}{
		{"tiny", ModePush},
		{"just_under", ModePush},
		// At the threshold, not merely above it: a boundary that reads one way
		// in the config and another in the code is the kind of thing nobody
		// notices until the numbers stop adding up.
		{"exactly", ModePull},
		{"whale", ModePull},
	}

	for _, tt := range tests {
		mode, _, err := router.Route(t.Context(), tt.author)
		if err != nil {
			t.Fatalf("Route(%s) = %v", tt.author, err)
		}
		if mode != tt.want {
			t.Errorf("Route(%s) = %q, want %q", tt.author, mode, tt.want)
		}
	}
}

// A threshold of zero is the phase 3 behaviour, and the baseline the benchmark
// compares against. It must push everything, however large.
func TestZeroThresholdPushesEverything(t *testing.T) {
	social := newCountingSocial(map[string]int64{"whale": 5_000_000})
	router := NewRouter(social, 0, time.Minute, nil)

	mode, followers, err := router.Route(t.Context(), "whale")
	if err != nil {
		t.Fatalf("Route() = %v", err)
	}
	if mode != ModePush {
		t.Errorf("mode = %q, want push when the threshold is disabled", mode)
	}
	// The count is still fetched, because the latency histogram is labelled by
	// author size and a baseline run has to be comparable to a routed one.
	if followers != 5_000_000 {
		t.Errorf("followers = %d, want the real count even when routing is off", followers)
	}
}

// The count is asked for on every post and changes slowly, so it is cached.
func TestRouteCachesTheFollowerCount(t *testing.T) {
	social := newCountingSocial(map[string]int64{"a": 5})
	router := NewRouter(social, 10_000, time.Minute, nil)

	for range 50 {
		if _, _, err := router.Route(t.Context(), "a"); err != nil {
			t.Fatalf("Route() = %v", err)
		}
	}

	if got := social.callCount(); got != 1 {
		t.Errorf("asked the social service %d times for 50 posts, want 1", got)
	}
}

func TestRouteCacheExpires(t *testing.T) {
	social := newCountingSocial(map[string]int64{"a": 5})

	now := time.Now()
	clock := func() time.Time { return now }
	router := NewRouter(social, 10_000, 30*time.Second, clock)

	if _, _, err := router.Route(t.Context(), "a"); err != nil {
		t.Fatalf("Route() = %v", err)
	}

	// The account grows past the threshold while the entry is still fresh.
	social.set("a", 20_000)

	mode, _, err := router.Route(t.Context(), "a")
	if err != nil {
		t.Fatalf("Route() = %v", err)
	}
	if mode != ModePush {
		t.Errorf("mode = %q, want the cached decision to still be push", mode)
	}

	now = now.Add(31 * time.Second)

	mode, followers, err := router.Route(t.Context(), "a")
	if err != nil {
		t.Fatalf("Route() = %v", err)
	}
	if mode != ModePull {
		t.Errorf("mode = %q, want pull once the cache expired", mode)
	}
	if followers != 20_000 {
		t.Errorf("followers = %d, want the refreshed 20000", followers)
	}
}

// A stale count beats no answer. Both routes are correct, so failing the event
// over a lookup error would trade a cheap inefficiency for an expensive retry.
func TestRouteFallsBackToAStaleCount(t *testing.T) {
	social := newCountingSocial(map[string]int64{"a": 50_000})

	now := time.Now()
	router := NewRouter(social, 10_000, time.Second, func() time.Time { return now })

	if _, _, err := router.Route(t.Context(), "a"); err != nil {
		t.Fatalf("Route() = %v", err)
	}

	social.mu.Lock()
	social.err = errors.New("social service is down")
	social.mu.Unlock()
	now = now.Add(time.Hour)

	mode, followers, err := router.Route(t.Context(), "a")
	if err != nil {
		t.Fatalf("Route() = %v, want the stale entry rather than an error", err)
	}
	if mode != ModePull || followers != 50_000 {
		t.Errorf("Route() = (%q, %d), want the stale (pull, 50000)", mode, followers)
	}
}

// With no cached entry and a failing lookup there is nothing to fall back to,
// and the event must be retried rather than routed on a guess.
func TestRouteFailsWithNoCachedEntry(t *testing.T) {
	social := newCountingSocial(nil)
	social.err = errors.New("social service is down")
	router := NewRouter(social, 10_000, time.Minute, nil)

	if _, _, err := router.Route(t.Context(), "unknown"); err == nil {
		t.Error("Route() = nil, want an error when there is nothing cached to fall back to")
	}
}

// The worker routes several posts at once, so the cache is written from
// several goroutines. This fails under -race if the lock is removed.
func TestRouterIsSafeForConcurrentUse(t *testing.T) {
	counts := make(map[string]int64, 64)
	for i := range 64 {
		counts[string(rune('a'+i%26))+string(rune('a'+i/26))] = int64(i * 500)
	}
	social := newCountingSocial(counts)
	router := NewRouter(social, 10_000, time.Minute, nil)

	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			author := string(rune('a'+i%26)) + string(rune('a'+i/26))
			for range 100 {
				if _, _, err := router.Route(t.Context(), author); err != nil {
					t.Errorf("Route() = %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
