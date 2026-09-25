package grpcx

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/anwexhaa/murmur/internal/auth"
)

// AssertionKey is the metadata key carrying the caller's signed identity.
const AssertionKey = "x-murmur-assertion"

// AssertionTTL is how long an assertion is valid.
//
// Seconds, because it only has to survive one hop. An assertion is minted per
// call and thrown away; anything longer is a replay window bought for no
// reason, and the cost of minting a fresh one is a single Ed25519 signature --
// tens of microseconds against a network round trip measured in milliseconds.
const AssertionTTL = 30 * time.Second

type callerKey struct{}

// Caller is who a verified assertion says is calling.
type Caller struct {
	// UserID is the account the call is on behalf of. Empty for a call made
	// for nobody -- a signed-out visitor reading a public profile still has to
	// cross this boundary.
	UserID string
	Handle string
	// Service is the process that signed the assertion.
	Service string
}

// CallerFrom returns the verified caller, and whether there was one.
func CallerFrom(ctx context.Context) (Caller, bool) {
	caller, ok := ctx.Value(callerKey{}).(Caller)
	return caller, ok
}

// WithCaller attaches a caller, for tests and for the interceptor below.
func WithCaller(ctx context.Context, caller Caller) context.Context {
	return context.WithValue(ctx, callerKey{}, caller)
}

// UnaryAssertion verifies the caller's signed assertion and rejects the call
// without one.
//
// This is the boundary the phase exists to draw. Until now a backend believed
// whatever identity arrived in a plaintext header, which means anyone who
// could reach port 9081 could be anybody -- and in a cluster, "anyone who can
// reach the port" includes every other pod, every sidecar, and anything that
// gets a shell in the namespace. Here the caller has to present something
// signed by a key the backends do not have, so reaching the port is no longer
// the same as being trusted.
//
// exempt names methods that are reachable without one. Health checking is the
// realistic case: a kubelet probing readiness has no key and no identity, and
// requiring one would mean the pod could never become ready.
func UnaryAssertion(verifier *auth.Verifier, exempt ...string) grpc.UnaryServerInterceptor {
	skip := make(map[string]bool, len(exempt))
	for _, method := range exempt {
		skip[method] = true
	}

	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if verifier == nil {
			// No verifier configured means the boundary is off, which is a
			// development convenience and never a production state. The
			// service logs a warning at startup rather than here, because a
			// line per request would be its own denial of service.
			return handler(ctx, req)
		}
		if skip[info.FullMethod] || isHealthMethod(info.FullMethod) {
			return handler(ctx, req)
		}

		raw := metadataValue(ctx, AssertionKey)
		if raw == "" {
			return nil, status.Error(codes.Unauthenticated, "no caller assertion")
		}

		claims, err := verifier.Parse(raw)
		if err != nil {
			// The reason is deliberately not returned. "Expired" and
			// "wrong signature" are different facts about the attacker's own
			// token, and neither helps a legitimate caller, which has neither
			// problem.
			return nil, status.Error(codes.Unauthenticated, "invalid caller assertion")
		}

		return handler(WithCaller(ctx, Caller{
			UserID:  claims.UserID(),
			Handle:  claims.Handle,
			Service: claims.SignedBy(),
		}), req)
	}
}

// SubjectFunc reads the account a call is being made on behalf of, out of the
// caller's own request context.
//
// A function rather than a context key defined here, because the gateway
// already has its own notion of "the viewer" and threading a second key
// through every handler to say the same thing would be two sources of truth
// for one fact.
type SubjectFunc func(context.Context) (userID, handle string)

// UnaryAssertionSigner mints a fresh assertion for every outbound call.
//
// Per call rather than per connection: the assertion names the user the call
// is for, and one connection carries requests for many users. Caching it per
// connection would mean the first user's identity on every subsequent
// request -- a cross-account data leak dressed as an optimisation.
func UnaryAssertionSigner(signer *auth.Signer, subject SubjectFunc) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		conn *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if signer == nil {
			return invoker(ctx, method, req, reply, conn, opts...)
		}

		var userID, handle string
		if subject != nil {
			userID, handle = subject(ctx)
		}

		assertion, err := signer.Sign(userID, handle, auth.AudienceInternal, AssertionTTL)
		if err != nil {
			return status.Error(codes.Internal, "could not sign caller assertion")
		}

		ctx = metadata.AppendToOutgoingContext(ctx, AssertionKey, assertion)
		return invoker(ctx, method, req, reply, conn, opts...)
	}
}

// isHealthMethod reports whether a method belongs to the gRPC health service,
// which probes call before anything has an identity.
func isHealthMethod(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, "/grpc.health.v1.Health/")
}

func metadataValue(ctx context.Context, key string) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if values := md.Get(key); len(values) > 0 {
		return values[0]
	}
	return ""
}
