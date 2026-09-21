package grpcx

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

var testInfo = &grpc.UnaryServerInfo{FullMethod: "/murmur.social.v1.SocialService/GetUser"}

// A correlation ID that arrives from the caller must survive the hop. One
// GraphQL query becomes several gRPC calls, and they are only traceable as a
// single unit of work if the ID is adopted rather than replaced.
func TestRequestIDIsAdoptedFromTheCaller(t *testing.T) {
	const incoming = "caller-supplied-id"

	var seen string
	handler := func(ctx context.Context, _ any) (any, error) {
		seen = RequestID(ctx)
		return nil, nil
	}

	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(RequestIDKey, incoming))

	if _, err := UnaryRequestID()(ctx, nil, testInfo, handler); err != nil {
		t.Fatalf("interceptor returned %v", err)
	}
	if seen != incoming {
		t.Errorf("RequestID = %q, want the caller's %q", seen, incoming)
	}
}

func TestRequestIDIsMintedWhenAbsent(t *testing.T) {
	var seen string
	handler := func(ctx context.Context, _ any) (any, error) {
		seen = RequestID(ctx)
		return nil, nil
	}

	if _, err := UnaryRequestID()(context.Background(), nil, testInfo, handler); err != nil {
		t.Fatalf("interceptor returned %v", err)
	}
	if seen == "" {
		t.Error("RequestID is empty; the interceptor should have minted one")
	}
}

// The point of the guard: work that cannot finish before the caller gives up
// must not consume a database connection on its way to being discarded.
func TestDeadlineGuardRefusesWorkThatCannotFinish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	called := false
	handler := func(context.Context, any) (any, error) {
		called = true
		return nil, nil
	}

	_, err := UnaryDeadlineGuard(50*time.Millisecond)(ctx, nil, testInfo, handler)

	if called {
		t.Error("the handler ran despite having less than the minimum time left")
	}
	if got := status.Code(err); got != codes.DeadlineExceeded {
		t.Errorf("code = %v, want DeadlineExceeded", got)
	}
	if err != nil && !strings.Contains(err.Error(), testInfo.FullMethod) {
		t.Errorf("error should name the refused method, got: %v", err)
	}
}

func TestDeadlineGuardAllowsWorkWithTimeToSpare(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	called := false
	handler := func(context.Context, any) (any, error) {
		called = true
		return "ok", nil
	}

	if _, err := UnaryDeadlineGuard(50*time.Millisecond)(ctx, nil, testInfo, handler); err != nil {
		t.Fatalf("interceptor returned %v, want nil", err)
	}
	if !called {
		t.Error("the handler did not run despite having a full second")
	}
}

// A request with no deadline is not this interceptor's problem. Inventing one
// here would silently cap every call that deliberately has none.
func TestDeadlineGuardIgnoresRequestsWithoutADeadline(t *testing.T) {
	called := false
	handler := func(context.Context, any) (any, error) {
		called = true
		return nil, nil
	}

	if _, err := UnaryDeadlineGuard(time.Hour)(context.Background(), nil, testInfo, handler); err != nil {
		t.Fatalf("interceptor returned %v, want nil", err)
	}
	if !called {
		t.Error("the handler was refused even though the request had no deadline")
	}
}

// One bad request must not take down a process serving thousands of good ones.
func TestRecoveryTurnsAPanicIntoAnInternalError(t *testing.T) {
	panicking := func(context.Context, any) (any, error) {
		panic("something went very wrong")
	}

	resp, err := UnaryRecovery(quietLogger())(context.Background(), nil, testInfo, panicking)

	if resp != nil {
		t.Errorf("resp = %v, want nil after a panic", resp)
	}
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("code = %v, want Internal", got)
	}
}

// The stack trace goes to the log. A client learns that something broke, not
// where in the source it broke.
func TestRecoveryDoesNotLeakThePanicToTheClient(t *testing.T) {
	panicking := func(context.Context, any) (any, error) {
		panic("connection string: postgres://murmur:hunter2@db:5432")
	}

	_, err := UnaryRecovery(quietLogger())(context.Background(), nil, testInfo, panicking)

	if err == nil {
		t.Fatal("err = nil, want an error")
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("the panic value reached the client: %v", err)
	}
}

func TestRecoveryLeavesNormalErrorsAlone(t *testing.T) {
	sentinel := status.Error(codes.NotFound, "no such user")
	handler := func(context.Context, any) (any, error) { return nil, sentinel }

	_, err := UnaryRecovery(quietLogger())(context.Background(), nil, testInfo, handler)
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want the handler's own error untouched", err)
	}
}

// Client mistakes are warnings, not errors. A stream of InvalidArgument from
// one broken client should not make the service look unhealthy.
func TestLoggingLevelMatchesTheOutcome(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want slog.Level
	}{
		{"success", nil, slog.LevelInfo},
		{"client sent nonsense", status.Error(codes.InvalidArgument, "bad"), slog.LevelWarn},
		{"not found", status.Error(codes.NotFound, "gone"), slog.LevelWarn},
		{"server broke", status.Error(codes.Internal, "boom"), slog.LevelError},
		{"dependency down", status.Error(codes.Unavailable, "no"), slog.LevelError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			capture := &levelCapture{}
			log := slog.New(capture)

			handler := func(context.Context, any) (any, error) { return nil, tt.err }
			_, _ = UnaryLogging(log)(context.Background(), nil, testInfo, handler)

			if capture.level != tt.want {
				t.Errorf("logged at %v, want %v", capture.level, tt.want)
			}
		})
	}
}

// levelCapture records the level of the last record it handled.
type levelCapture struct {
	level slog.Level
}

func (c *levelCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c *levelCapture) Handle(_ context.Context, record slog.Record) error {
	c.level = record.Level
	return nil
}

func (c *levelCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *levelCapture) WithGroup(string) slog.Handler      { return c }
