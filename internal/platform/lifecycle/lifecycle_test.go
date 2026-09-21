package lifecycle

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// blocker is a component that runs until its context is cancelled, which is
// how every real server behaves.
func blocker(name string, started chan<- struct{}) Component {
	return Component{
		Name: name,
		Start: func(ctx context.Context) error {
			if started != nil {
				started <- struct{}{}
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}
}

func TestRunStopsEveryComponentWhenParentIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var stopped [3]atomic.Bool
	components := make([]Component, 3)
	for i := range components {
		components[i] = blocker(string(rune('a'+i)), nil)
		components[i].Stop = func(context.Context) error {
			stopped[i].Store(true)
			return nil
		}
	}

	done := make(chan error, 1)
	go func() { done <- Run(ctx, quietLogger(), time.Second, components...) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() = %v, want nil on a clean cancellation", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run() did not return within 3s of cancellation")
	}

	for i := range stopped {
		if !stopped[i].Load() {
			t.Errorf("component %d was never stopped", i)
		}
	}
}

// The reason this package exists: one component finishing must not leave the
// others running. A half-dead process still passes a liveness probe.
func TestOneComponentExitingDrainsTheRest(t *testing.T) {
	survivorStopped := make(chan struct{})

	quitter := Component{
		Name: "quitter",
		Start: func(context.Context) error {
			return nil
		},
	}
	survivor := blocker("survivor", nil)
	survivor.Stop = func(context.Context) error {
		close(survivorStopped)
		return nil
	}

	done := make(chan error, 1)
	go func() { done <- Run(context.Background(), quietLogger(), time.Second, quitter, survivor) }()

	select {
	case <-survivorStopped:
	case <-time.After(3 * time.Second):
		t.Fatal("the surviving component was never drained")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run() did not return")
	}
}

func TestFailingComponentStopsTheProcessAndSurfacesTheError(t *testing.T) {
	boom := errors.New("listener died")

	failing := Component{
		Name:  "failing",
		Start: func(context.Context) error { return boom },
	}

	err := Run(context.Background(), quietLogger(), time.Second, failing, blocker("other", nil))
	if !errors.Is(err, boom) {
		t.Fatalf("Run() = %v, want it to wrap %v", err, boom)
	}
}

// Components are torn down in reverse start order, so a server that depends on
// a connection pool is stopped before the pool it uses.
func TestStopRunsInReverseOrder(t *testing.T) {
	var mu sync.Mutex
	var order []string

	record := func(name string) func(context.Context) error {
		return func(context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			order = append(order, name)
			return nil
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	first, second, third := blocker("first", nil), blocker("second", nil), blocker("third", nil)
	first.Stop, second.Stop, third.Stop = record("first"), record("second"), record("third")

	done := make(chan error, 1)
	go func() { done <- Run(ctx, quietLogger(), time.Second, first, second, third) }()

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	want := []string{"third", "second", "first"}
	if len(order) != len(want) {
		t.Fatalf("stop order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("stop order = %v, want %v", order, want)
		}
	}
}

// A component that refuses to stop must not hold the process open past the
// drain deadline.
func TestHungComponentCannotOutlastTheDrainDeadline(t *testing.T) {
	hung := blocker("hung", nil)
	hung.Stop = func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, quietLogger(), 100*time.Millisecond, hung) }()

	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	cancel()

	select {
	case <-done:
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("shutdown took %v, want it bounded by the 100ms drain deadline", elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a hung Stop held the process open past its deadline")
	}
}

// Stop is handed a live context even though the context that triggered the
// shutdown is already cancelled — otherwise nothing could ever drain.
func TestStopReceivesAUsableContext(t *testing.T) {
	stopCtxErr := make(chan error, 1)

	c := blocker("c", nil)
	c.Stop = func(ctx context.Context) error {
		stopCtxErr <- ctx.Err()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	if err := Run(ctx, quietLogger(), time.Second, c); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}

	if err := <-stopCtxErr; err != nil {
		t.Fatalf("Stop received an already-cancelled context (%v); it cannot drain with one", err)
	}
}

func TestRunRejectsAnEmptyComponentSet(t *testing.T) {
	if err := Run(context.Background(), quietLogger(), time.Second); err == nil {
		t.Fatal("Run() with no components = nil, want an error")
	}
}
