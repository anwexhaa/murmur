package grpcx

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// RequestIDKey is the metadata key carrying a request's correlation ID across
// process boundaries.
const RequestIDKey = "x-request-id"

type contextKey struct{ name string }

var requestIDContextKey = contextKey{"request-id"}

// RequestID returns the correlation ID for this request, or "" if there is
// none. Handlers use it to tie their own logs to the request log line.
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDContextKey).(string)
	return id
}

// UnaryRequestID adopts the caller's request ID, or mints one.
//
// Adopting rather than always minting is the point: a single GraphQL query
// fans out to several gRPC calls, and they are only traceable as one unit of
// work if the ID survives the hop.
func UnaryRequestID() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		id := ""
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if values := md.Get(RequestIDKey); len(values) > 0 {
				id = values[0]
			}
		}
		if id == "" {
			id = uuid.NewString()
		}
		return handler(context.WithValue(ctx, requestIDContextKey, id), req)
	}
}

// UnaryDeadlineGuard refuses work that cannot plausibly finish.
//
// When a caller's deadline is nearly spent, running the handler anyway burns a
// database connection and a goroutine to produce a result nobody is still
// waiting for. Under load that is exactly the wrong thing to spend capacity
// on, and it is how a slow dependency turns into a collapsed service. Failing
// immediately sheds the work while it is still cheap.
func UnaryDeadlineGuard(minRemaining time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			// No deadline at all. Not this interceptor's problem to invent one.
			return handler(ctx, req)
		}

		if remaining := time.Until(deadline); remaining < minRemaining {
			return nil, status.Errorf(codes.DeadlineExceeded,
				"refusing %s: %v left, need at least %v", info.FullMethod, remaining.Truncate(time.Millisecond), minRemaining)
		}
		return handler(ctx, req)
	}
}

// UnaryRecovery converts a panicking handler into an Internal error.
//
// One bad request must not take down a process that is serving thousands of
// good ones. The stack goes to the log, never to the client.
func UnaryRecovery(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Error("handler panicked",
					"method", info.FullMethod,
					"request_id", RequestID(ctx),
					"panic", fmt.Sprint(recovered),
					"stack", string(debug.Stack()),
				)
				err = status.Error(codes.Internal, "internal error")
				resp = nil
			}
		}()
		return handler(ctx, req)
	}
}

// UnaryLogging emits one line per RPC, at a level that matches the outcome.
//
// Client mistakes are not warnings. A stream of InvalidArgument from a
// misbehaving client should not look like the service is unhealthy, so only
// server-side failures rise above Info.
func UnaryLogging(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)

		code := status.Code(err)
		attrs := []any{
			"method", info.FullMethod,
			"code", code.String(),
			"duration_ms", float64(time.Since(start).Microseconds()) / 1000,
			"request_id", RequestID(ctx),
		}
		if err != nil {
			attrs = append(attrs, "error", err.Error())
		}

		switch code {
		case codes.OK:
			log.Info("rpc", attrs...)
		case codes.Internal, codes.Unknown, codes.DataLoss, codes.Unavailable:
			log.Error("rpc", attrs...)
		default:
			log.Warn("rpc", attrs...)
		}
		return resp, err
	}
}
