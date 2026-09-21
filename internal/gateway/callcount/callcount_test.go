package callcount

import (
	"context"
	"errors"
	"sync"
	"testing"

	"google.golang.org/grpc"
)

const getUser = "/murmur.social.v1.SocialService/GetUser"

func TestCounterTalliesByMethod(t *testing.T) {
	counter := New()
	counter.Record(getUser)
	counter.Record(getUser)
	counter.Record("/murmur.social.v1.SocialService/ListFollowing")

	if got := counter.Total(); got != 3 {
		t.Errorf("Total() = %d, want 3", got)
	}

	breakdown := counter.Breakdown()
	if breakdown["GetUser"] != 2 {
		t.Errorf("GetUser = %d, want 2", breakdown["GetUser"])
	}
	if breakdown["ListFollowing"] != 1 {
		t.Errorf("ListFollowing = %d, want 1", breakdown["ListFollowing"])
	}
}

// The summary leads with the worst offender, which is the whole point of
// printing a breakdown rather than a total.
func TestSummaryOrdersByFrequency(t *testing.T) {
	counter := New()
	counter.Record("/s/ListFollowing")
	for range 50 {
		counter.Record(getUser)
	}
	for range 11 {
		counter.Record("/s/ListAuthorPosts")
	}

	want := "50 GetUser + 11 ListAuthorPosts + 1 ListFollowing"
	if got := counter.Summary(); got != want {
		t.Errorf("Summary() = %q, want %q", got, want)
	}
}

func TestSummaryOfNothing(t *testing.T) {
	if got := New().Summary(); got != "none" {
		t.Errorf("Summary() = %q, want %q", got, "none")
	}
}

// Calls made outside a request must not panic. Health checks and connection
// warmup happen on contexts that never saw an HTTP request.
func TestNilCounterIsSafe(t *testing.T) {
	var counter *Counter
	counter.Record(getUser)

	if got := counter.Total(); got != 0 {
		t.Errorf("Total() on a nil counter = %d, want 0", got)
	}
	if got := counter.Summary(); got != "none" {
		t.Errorf("Summary() on a nil counter = %q, want %q", got, "none")
	}
	if counter.Breakdown() != nil {
		t.Error("Breakdown() on a nil counter should be nil")
	}
}

func TestFromContextWithoutACounter(t *testing.T) {
	if FromContext(context.Background()) != nil {
		t.Error("FromContext on a bare context should return nil")
	}
}

// GraphQL resolves sibling fields concurrently, so the counter is written to
// from many goroutines at once. This fails under -race if the lock goes away.
func TestCounterIsSafeForConcurrentUse(t *testing.T) {
	ctx, counter := NewContext(context.Background())

	const goroutines, each = 32, 100
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range each {
				FromContext(ctx).Record(getUser)
			}
		}()
	}
	wg.Wait()

	if got := counter.Total(); got != goroutines*each {
		t.Errorf("Total() = %d, want %d", got, goroutines*each)
	}
}

// The interceptor is what makes resolvers unable to forget to count.
func TestInterceptorCountsEveryCall(t *testing.T) {
	ctx, counter := NewContext(context.Background())
	interceptor := UnaryClientInterceptor()

	invoked := 0
	invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		invoked++
		return nil
	}

	for range 7 {
		if err := interceptor(ctx, getUser, nil, nil, nil, invoker); err != nil {
			t.Fatalf("interceptor returned %v", err)
		}
	}

	if invoked != 7 {
		t.Errorf("the underlying call ran %d times, want 7", invoked)
	}
	if got := counter.Total(); got != 7 {
		t.Errorf("counted %d calls, want 7", got)
	}
}

// A failed call still cost a round trip, so it still counts.
func TestInterceptorCountsFailedCalls(t *testing.T) {
	ctx, counter := NewContext(context.Background())
	boom := errors.New("unavailable")

	invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return boom
	}

	err := UnaryClientInterceptor()(ctx, getUser, nil, nil, nil, invoker)
	if !errors.Is(err, boom) {
		t.Errorf("interceptor swallowed the error: %v", err)
	}
	if got := counter.Total(); got != 1 {
		t.Errorf("counted %d calls, want the failed one counted", got)
	}
}

func TestInterceptorOutsideARequestDoesNotPanic(t *testing.T) {
	invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return nil
	}
	if err := UnaryClientInterceptor()(context.Background(), getUser, nil, nil, nil, invoker); err != nil {
		t.Errorf("interceptor returned %v outside a request", err)
	}
}
