package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	authv1 "github.com/anwexhaa/murmur/api/gen/murmur/auth/v1"
	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
	timelinev1 "github.com/anwexhaa/murmur/api/gen/murmur/timeline/v1"
	"github.com/anwexhaa/murmur/internal/auth"
	"github.com/anwexhaa/murmur/internal/gateway/callcount"
	"github.com/anwexhaa/murmur/internal/platform/grpcx"
)

// assertionSubject reads the signed-in account out of a request context, for
// the assertion signer to name.
//
// Deliberately the gateway's own viewer rather than a second context key
// belonging to grpcx: two places recording who the caller is would eventually
// disagree, and the one that disagreed would be the one the backends believe.
func assertionSubject(ctx context.Context) (string, string) {
	return Viewer(ctx), ViewerHandle(ctx)
}

// Clients holds the gateway's outbound connections.
//
// One connection per backend, opened at startup and held for the life of the
// process. gRPC multiplexes concurrent RPCs over a single HTTP/2 connection,
// so dialling per request would add a TCP and TLS handshake to every call and
// gain nothing — and under the load phase 5 applies, it would exhaust
// ephemeral ports long before it exhausted the database.
type Clients struct {
	Social   socialv1.SocialServiceClient
	Timeline timelinev1.TimelineServiceClient
	// Auth shares social-svc's connection: it is the same process, and a
	// second connection to the same address would double the keepalive
	// traffic to prove a point about package layout.
	Auth authv1.AuthServiceClient

	conns []*grpc.ClientConn
}

// Dial opens the backend connections. The returned function closes them.
//
// grpc.NewClient does not block on a reachable server: the connection is
// established lazily and re-established automatically. A backend that is
// briefly down therefore delays the first request rather than preventing the
// gateway from starting, which is what lets the whole stack come up in any
// order.
func Dial(_ context.Context, socialAddr, timelineAddr string, signer *auth.Signer, log *slog.Logger) (*Clients, func(), error) {
	dial := func(addr string) (*grpc.ClientConn, error) {
		return grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
			grpc.WithChainUnaryInterceptor(
				callcount.UnaryClientInterceptor(),
				forwardRequestID(),
				// Every outbound call carries a fresh assertion naming the
				// user it is for. This is what the backends verify, and it is
				// what replaced trusting a plaintext header.
				grpcx.UnaryAssertionSigner(signer, assertionSubject),
				perCallDeadline(3*time.Second),
			),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                30 * time.Second,
				Timeout:             10 * time.Second,
				PermitWithoutStream: true,
			}),
		)
	}

	socialConn, err := dial(socialAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("dial social service at %s: %w", socialAddr, err)
	}

	timelineConn, err := dial(timelineAddr)
	if err != nil {
		_ = socialConn.Close()
		return nil, nil, fmt.Errorf("dial timeline service at %s: %w", timelineAddr, err)
	}

	log.Info("grpc clients ready", "social", socialAddr, "timeline", timelineAddr)

	clients := &Clients{
		Social:   socialv1.NewSocialServiceClient(socialConn),
		Timeline: timelinev1.NewTimelineServiceClient(timelineConn),
		Auth:     authv1.NewAuthServiceClient(socialConn),
		conns:    []*grpc.ClientConn{socialConn, timelineConn},
	}
	return clients, func() {
		for _, c := range clients.conns {
			if err := c.Close(); err != nil {
				log.Warn("closing grpc client", "error", err)
			}
		}
		log.Info("grpc clients closed")
	}, nil
}

// forwardRequestID copies this request's correlation ID into the outgoing
// metadata, which is the other half of the interceptor phase 1 put on the
// server. One GraphQL query becomes dozens of gRPC calls; without this they
// appear in the logs as dozens of unrelated events.
func forwardRequestID() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if id := grpcx.RequestID(ctx); id != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, grpcx.RequestIDKey, id)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// perCallDeadline caps how long any single downstream call may take.
//
// The inbound request already carries a deadline, and that deadline is
// inherited here — but it covers the whole query, and a GraphQL query is many
// calls. Without a per-call cap, one slow lookup can spend the entire budget
// and starve the fifty after it. The cap never extends the request's own
// deadline, only shortens it.
func perCallDeadline(max time.Duration) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > max {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, max)
			defer cancel()
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
