package gateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
)

// batchSpy records the batches it was handed, which is the only thing these
// tests actually care about: not that the answers are right — the services are
// tested elsewhere — but that fifty resolver calls became one call with fifty
// keys, and that duplicate keys collapsed.
type batchSpy struct {
	socialv1.SocialServiceClient

	mu           sync.Mutex
	userBatches  [][]string
	countBatches [][]string
	filterCalls  [][]string
	filterViewer string

	following map[string]bool
	err       error
	calls     atomic.Int64
}

func newBatchSpy() *batchSpy {
	return &batchSpy{following: map[string]bool{}}
}

func (b *batchSpy) BatchGetUsers(
	_ context.Context,
	req *socialv1.BatchGetUsersRequest,
	_ ...grpc.CallOption,
) (*socialv1.BatchGetUsersResponse, error) {
	b.calls.Add(1)
	b.mu.Lock()
	b.userBatches = append(b.userBatches, req.GetIds())
	b.mu.Unlock()

	if b.err != nil {
		return nil, b.err
	}

	users := make(map[string]*socialv1.User, len(req.GetIds()))
	for _, id := range req.GetIds() {
		if id == "missing" {
			continue
		}
		users[id] = &socialv1.User{
			Id: id, Handle: "h_" + id, DisplayName: "D " + id,
			CreatedAt: timestamppb.Now(),
		}
	}
	return &socialv1.BatchGetUsersResponse{Users: users}, nil
}

func (b *batchSpy) BatchGetFollowerCounts(
	_ context.Context,
	req *socialv1.BatchGetFollowerCountsRequest,
	_ ...grpc.CallOption,
) (*socialv1.BatchGetFollowerCountsResponse, error) {
	b.calls.Add(1)
	b.mu.Lock()
	b.countBatches = append(b.countBatches, req.GetUserIds())
	b.mu.Unlock()

	counts := make(map[string]int64, len(req.GetUserIds()))
	for i, id := range req.GetUserIds() {
		counts[id] = int64(i + 1)
	}
	return &socialv1.BatchGetFollowerCountsResponse{Followers: counts}, nil
}

func (b *batchSpy) FilterFollowing(
	_ context.Context,
	req *socialv1.FilterFollowingRequest,
	_ ...grpc.CallOption,
) (*socialv1.FilterFollowingResponse, error) {
	b.calls.Add(1)
	b.mu.Lock()
	b.filterCalls = append(b.filterCalls, req.GetCandidateIds())
	b.filterViewer = req.GetFollowerId()
	b.mu.Unlock()

	var followed []string
	for _, id := range req.GetCandidateIds() {
		if b.following[id] {
			followed = append(followed, id)
		}
	}
	return &socialv1.FilterFollowingResponse{FolloweeIds: followed}, nil
}

func (b *batchSpy) batches() ([][]string, [][]string, [][]string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.userBatches, b.countBatches, b.filterCalls
}

func spyClients(spy *batchSpy) *Clients {
	return &Clients{Social: spy, Timeline: nil}
}

// The headline behaviour: many concurrent Load calls collapse into very few
// downstream calls, and no key is ever fetched twice.
//
// Not "exactly one call", which is what this test asserted at first and why it
// failed about two runs in five. A loader batches the keys that arrive inside
// its wait window, and goroutine scheduling can spread fifty Load calls across
// more than 500µs — so a second batch is the loader working as designed, not a
// regression. Asserting the stricter thing made the test flaky without making
// the code any more correct.
//
// The two properties that actually matter are both exact:
//
//   - every distinct key is fetched exactly once, so batching never
//     duplicates work
//   - the number of calls is a small constant rather than one per Load, which
//     is the whole point
func TestUserLoaderBatchesConcurrentLoads(t *testing.T) {
	spy := newBatchSpy()
	loaders := NewLoaders(spyClients(spy), "viewer")

	const loads = 50
	ids := make([]string, loads)
	distinct := map[string]struct{}{}
	for i := range ids {
		ids[i] = string(rune('a' + i%26))
		distinct[ids[i]] = struct{}{}
	}

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := loaders.Users.Load(context.Background(), id); err != nil {
				t.Errorf("Load(%s) = %v", id, err)
			}
		}()
	}
	wg.Wait()

	userBatches, _, _ := spy.batches()

	fetched := map[string]int{}
	for _, batch := range userBatches {
		for _, id := range batch {
			fetched[id]++
		}
	}

	for id, times := range fetched {
		if times != 1 {
			t.Errorf("key %q was fetched %d times, want exactly once", id, times)
		}
	}
	if len(fetched) != len(distinct) {
		t.Errorf("fetched %d distinct keys, want %d", len(fetched), len(distinct))
	}
	if len(userBatches) >= loads/5 {
		t.Errorf("made %d downstream calls for %d concurrent loads, want a small constant",
			len(userBatches), loads)
	}
}

// Fifty posts by one author is one key, not fifty. This is the case a timeline
// hits constantly and the reason deduplication matters as much as batching.
func TestLoaderDeduplicatesRepeatedKeys(t *testing.T) {
	spy := newBatchSpy()
	loaders := NewLoaders(spyClients(spy), "viewer")

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := loaders.Users.Load(context.Background(), "same-author"); err != nil {
				t.Errorf("Load() = %v", err)
			}
		}()
	}
	wg.Wait()

	userBatches, _, _ := spy.batches()

	// However the batches fall, the author is fetched once in total. Same
	// reasoning as the test above: the number of batches is scheduling, the
	// number of fetches is the guarantee.
	total := 0
	for _, batch := range userBatches {
		total += len(batch)
	}
	if total != 1 {
		t.Errorf("fetched the same author %d times across %d batches, want once",
			total, len(userBatches))
	}
}

// A deleted author must not fail the other forty-nine posts on the page.
func TestUserLoaderReturnsNilForAMissingUser(t *testing.T) {
	spy := newBatchSpy()
	loaders := NewLoaders(spyClients(spy), "viewer")

	user, err := loaders.Users.Load(t.Context(), "missing")
	if err != nil {
		t.Fatalf("Load() = %v, want nil for an absent user", err)
	}
	if user != nil {
		t.Errorf("Load() = %v, want nil", user)
	}
}

func TestUserLoaderPropagatesFailures(t *testing.T) {
	spy := newBatchSpy()
	spy.err = errors.New("social service is down")
	loaders := NewLoaders(spyClients(spy), "viewer")

	if _, err := loaders.Users.Load(t.Context(), "a"); err == nil {
		t.Error("Load() = nil, want the downstream failure surfaced")
	}
}

func TestFollowerCountLoaderBatches(t *testing.T) {
	spy := newBatchSpy()
	loaders := NewLoaders(spyClients(spy), "viewer")

	var wg sync.WaitGroup
	for i := range 30 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := loaders.FollowerCounts.Load(context.Background(), string(rune('a'+i))); err != nil {
				t.Errorf("Load() = %v", err)
			}
		}()
	}
	wg.Wait()

	_, countBatches, _ := spy.batches()
	if len(countBatches) != 1 {
		t.Errorf("made %d calls for 30 concurrent loads, want 1", len(countBatches))
	}
}

// viewerFollows batches through FilterFollowing, which phase 4 already needed
// for the read path. One query answers every resolver on the page.
func TestViewerFollowsBatchesThroughFilterFollowing(t *testing.T) {
	spy := newBatchSpy()
	spy.following["followed"] = true
	loaders := NewLoaders(spyClients(spy), "viewer")

	var (
		wg      sync.WaitGroup
		results sync.Map
	)
	for _, id := range []string{"followed", "stranger", "another"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			follows, err := loaders.ViewerFollows.Load(context.Background(), id)
			if err != nil {
				t.Errorf("Load(%s) = %v", id, err)
				return
			}
			results.Store(id, follows)
		}()
	}
	wg.Wait()

	_, _, filterCalls := spy.batches()
	if len(filterCalls) != 1 {
		t.Fatalf("made %d FilterFollowing calls, want 1", len(filterCalls))
	}
	if spy.filterViewer != "viewer" {
		t.Errorf("filtered for %q, want the request's viewer", spy.filterViewer)
	}

	if v, _ := results.Load("followed"); v != true {
		t.Error("a followed account came back as not followed")
	}
	if v, _ := results.Load("stranger"); v != false {
		t.Error("a stranger came back as followed")
	}
}

// Asking whether you follow yourself wastes a slot in the candidate list, and
// the answer is fixed.
func TestViewerFollowsExcludesTheViewerFromCandidates(t *testing.T) {
	spy := newBatchSpy()
	loaders := NewLoaders(spyClients(spy), "viewer")

	var wg sync.WaitGroup
	for _, id := range []string{"viewer", "other"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = loaders.ViewerFollows.Load(context.Background(), id)
		}()
	}
	wg.Wait()

	_, _, filterCalls := spy.batches()
	if len(filterCalls) != 1 {
		t.Fatalf("made %d calls, want 1", len(filterCalls))
	}
	for _, id := range filterCalls[0] {
		if id == "viewer" {
			t.Error("the viewer was included in its own candidate list")
		}
	}
}

// No viewer means no downstream call at all, rather than a call with an empty
// follower that the service would have to reject.
func TestViewerFollowsMakesNoCallWithoutAViewer(t *testing.T) {
	spy := newBatchSpy()
	loaders := NewLoaders(spyClients(spy), "")

	follows, err := loaders.ViewerFollows.Load(t.Context(), "someone")
	if err != nil {
		t.Fatalf("Load() = %v", err)
	}
	if follows {
		t.Error("Load() = true with no viewer")
	}
	if got := spy.calls.Load(); got != 0 {
		t.Errorf("made %d downstream calls with no viewer, want 0", got)
	}
}

// Loaders are per request because viewerFollows depends on who is asking.
// Sharing one across requests would serve one viewer's answer to another —
// a data-leak bug, not a performance bug.
func TestLoadersAreScopedToTheirRequest(t *testing.T) {
	spy := newBatchSpy()
	spy.following["target"] = true

	alice := NewLoaders(spyClients(spy), "alice")
	bob := NewLoaders(spyClients(spy), "bob")

	if _, err := alice.ViewerFollows.Load(t.Context(), "target"); err != nil {
		t.Fatalf("Load() = %v", err)
	}
	if _, err := bob.ViewerFollows.Load(t.Context(), "target"); err != nil {
		t.Fatalf("Load() = %v", err)
	}

	_, _, filterCalls := spy.batches()
	if len(filterCalls) != 2 {
		t.Errorf("two viewers produced %d calls, want 2 — a shared loader would have answered the second from the first's cache", len(filterCalls))
	}
}

func TestLoadersFromWithoutAContext(t *testing.T) {
	if LoadersFrom(context.Background()) != nil {
		t.Error("LoadersFrom on a bare context should return nil")
	}

	loaders := NewLoaders(spyClients(newBatchSpy()), "viewer")
	ctx := WithLoaders(context.Background(), loaders)
	if LoadersFrom(ctx) != loaders {
		t.Error("LoadersFrom did not return the loaders that were attached")
	}
}
