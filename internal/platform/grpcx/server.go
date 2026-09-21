// Package grpcx adapts gRPC servers to the lifecycle contract and carries the
// interceptors every Murmur service shares.
package grpcx

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"github.com/anwexhaa/murmur/internal/platform/lifecycle"
)

// Options configure a server.
type Options struct {
	Name string
	Addr string
	Log  *slog.Logger

	// Register attaches the service implementations. It runs before the
	// listener opens, so a registration panic fails at startup.
	Register func(*grpc.Server)

	// Reflection serves the server reflection API, which is what lets grpcurl
	// call this service without a copy of the .proto. Useful in development,
	// and turned off in production where it is free schema disclosure.
	Reflection bool

	// MinRemaining is the deadline floor. See UnaryDeadlineGuard.
	MinRemaining time.Duration
}

// Server builds a gRPC server as a lifecycle component. Stop performs a
// graceful shutdown: the listener closes, in-flight RPCs run to completion,
// and new ones are refused. If the drain deadline passes first, it escalates.
func Server(opts Options) lifecycle.Component {
	if opts.MinRemaining == 0 {
		opts.MinRemaining = 50 * time.Millisecond
	}

	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			UnaryRecovery(opts.Log),
			UnaryRequestID(),
			UnaryDeadlineGuard(opts.MinRemaining),
			UnaryLogging(opts.Log),
		),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			// Reap connections whose peer has vanished without a FIN, which
			// otherwise sit in the pool until the OS notices.
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             15 * time.Second,
			PermitWithoutStream: true,
		}),
	)

	opts.Register(srv)
	if opts.Reflection {
		reflection.Register(srv)
	}

	return lifecycle.Component{
		Name: opts.Name,
		Start: func(ctx context.Context) error {
			listener, err := net.Listen("tcp", opts.Addr)
			if err != nil {
				return fmt.Errorf("listen on %s: %w", opts.Addr, err)
			}
			opts.Log.Info("grpc server listening",
				"component", opts.Name, "addr", opts.Addr, "reflection", opts.Reflection)
			return srv.Serve(listener)
		},
		Stop: func(ctx context.Context) error {
			stopped := make(chan struct{})
			go func() {
				srv.GracefulStop()
				close(stopped)
			}()

			select {
			case <-stopped:
				return nil
			case <-ctx.Done():
				// The drain window is gone. Cut the remaining RPCs rather than
				// let a single slow call hold the whole process open.
				opts.Log.Warn("grpc drain deadline passed, forcing stop", "component", opts.Name)
				srv.Stop()
				<-stopped
				return nil
			}
		},
	}
}
