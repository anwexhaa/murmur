package timeline

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type fakeHeavySource struct {
	socialv1.SocialServiceClient

	mu    sync.Mutex
	ids   []string
	err   error
	calls int
}

func (f *fakeHeavySource) ListHeavyAuthors(
	_ context.Context,
	_ *socialv1.ListHeavyAuthorsRequest,
	_ ...grpc.CallOption,
) (*socialv1.ListHeavyAuthorsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return &socialv1.ListHeavyAuthorsResponse{UserIds: f.ids}, nil
}

func (f *fakeHeavySource) set(ids []string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids, f.err = ids, err
}

func TestHeavySetRefreshes(t *testing.T) {
	source := &fakeHeavySource{ids: []string{"whale", "star"}}
	set := NewHeavySet(source, 10_000, time.Minute, quietLogger())

	if err := set.Refresh(t.Context()); err != nil {
		t.Fatalf("Refresh() = %v", err)
	}
	if got := len(set.IDs()); got != 2 {
		t.Errorf("IDs() has %d entries, want 2", got)
	}
}

// A threshold of zero turns the pull side off entirely, which is the phase 3
// behaviour and the baseline the benchmark compares against.
func TestDisabledHeavySetNeverQueries(t *testing.T) {
	source := &fakeHeavySource{ids: []string{"whale"}}
	set := NewHeavySet(source, 0, time.Minute, quietLogger())

	if set.Enabled() {
		t.Error("Enabled() = true with a zero threshold")
	}
	if err := set.Refresh(t.Context()); err != nil {
		t.Fatalf("Refresh() = %v", err)
	}
	if len(set.IDs()) != 0 {
		t.Error("a disabled set should stay empty")
	}
	if source.calls != 0 {
		t.Errorf("a disabled set made %d calls, want 0", source.calls)
	}
}

// The failure that matters: an empty heavy set silently makes every large
// account's posts invisible, because nothing pushed them and nothing merges
// them. So a failed refresh must keep the last good copy rather than clear it.
func TestFailedRefreshKeepsThePreviousSet(t *testing.T) {
	source := &fakeHeavySource{ids: []string{"whale", "star"}}
	set := NewHeavySet(source, 10_000, time.Minute, quietLogger())

	if err := set.Refresh(t.Context()); err != nil {
		t.Fatalf("Refresh() = %v", err)
	}

	source.set(nil, errors.New("social service is down"))

	if err := set.Refresh(t.Context()); err == nil {
		t.Fatal("Refresh() = nil, want the error surfaced to the caller")
	}
	if got := len(set.IDs()); got != 2 {
		t.Errorf("IDs() has %d entries after a failed refresh, want the previous 2 kept", got)
	}
}

func TestHeavySetRunRefreshesImmediately(t *testing.T) {
	source := &fakeHeavySource{ids: []string{"whale"}}
	set := NewHeavySet(source, 10_000, time.Hour, quietLogger())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = set.Run(ctx)
	}()

	// The first fetch must not wait for the first tick, or the service serves
	// un-merged timelines for a whole interval after every restart.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && len(set.IDs()) == 0 {
		time.Sleep(10 * time.Millisecond)
	}

	if got := len(set.IDs()); got != 1 {
		t.Errorf("IDs() has %d entries shortly after Run started, want 1", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("Run did not return after cancellation")
	}
}

func TestHeavySetRunStopsWhenDisabled(t *testing.T) {
	set := NewHeavySet(&fakeHeavySource{}, 0, time.Hour, quietLogger())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = set.Run(ctx)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("a disabled Run did not return after cancellation")
	}
}

// The read path reads IDs while the refresher replaces them.
func TestHeavySetIsSafeForConcurrentUse(t *testing.T) {
	source := &fakeHeavySource{ids: []string{"a", "b", "c"}}
	set := NewHeavySet(source, 10_000, time.Minute, quietLogger())

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = set.Refresh(context.Background())
			}
		}
	}()

	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				_ = len(set.IDs())
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
}
