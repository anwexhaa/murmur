package grpcx_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/anwexhaa/murmur/internal/auth"
	"github.com/anwexhaa/murmur/internal/platform/grpcx"
)

// The interceptor is tested through a real gRPC connection rather than by
// calling it directly. What is being asserted is that a call made without the
// key is refused, and a unit test that hands the interceptor a context it
// built itself proves nothing about metadata actually travelling.

const echoMethod = "/murmur.test.v1.Echo/Say"

type echoServer struct {
	saw grpcx.Caller
	ok  bool
}

func (e *echoServer) handler(ctx context.Context, _ any) (any, error) {
	e.saw, e.ok = grpcx.CallerFrom(ctx)
	return "pong", nil
}

// serve starts a gRPC server over an in-memory listener with the given
// interceptor, and returns a connection to it.
func serve(t *testing.T, interceptor grpc.UnaryServerInterceptor, server *echoServer) *grpc.ClientConn {
	t.Helper()

	listener := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(interceptor),
		// Both ends speak the string codec below, so the test needs no
		// generated protobuf types to prove a point about metadata.
		grpc.ForceServerCodec(stringCodec{}),
	)
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "murmur.test.v1.Echo",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Say",
			Handler: func(_ any, ctx context.Context, dec func(any) error, chain grpc.UnaryServerInterceptor) (any, error) {
				var in string
				if err := dec(&in); err != nil {
					return nil, err
				}
				info := &grpc.UnaryServerInfo{FullMethod: echoMethod}
				if chain == nil {
					return server.handler(ctx, in)
				}
				return chain(ctx, in, info, server.handler)
			},
		}},
	}, server)

	go func() { _ = srv.Serve(listener) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(stringCodec{})),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// stringCodec keeps the test free of protobuf plumbing: the payload is a
// string and what is under test is the metadata beside it.
type stringCodec struct{}

func (stringCodec) Marshal(v any) ([]byte, error) {
	s, _ := v.(string)
	return []byte(s), nil
}

func (stringCodec) Unmarshal(data []byte, v any) error {
	if p, ok := v.(*string); ok {
		*p = string(data)
	}
	return nil
}

func (stringCodec) Name() string { return "murmur-test-string" }

func call(ctx context.Context, conn *grpc.ClientConn) error {
	var out string
	return conn.Invoke(ctx, echoMethod, "ping", &out)
}

func keys(t *testing.T) (*auth.Signer, *auth.Verifier) {
	t.Helper()
	key, err := auth.GenerateKey()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	signer, err := auth.NewSigner(key, auth.ServiceGateway)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	verifier, err := auth.NewVerifier(signer.PublicKey(), auth.AudienceInternal)
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	return signer, verifier
}

// TestACallWithNoAssertionIsRefused is the phase's "done when": a backend
// refuses a call that did not come through the gateway.
func TestACallWithNoAssertionIsRefused(t *testing.T) {
	_, verifier := keys(t)
	server := &echoServer{}
	conn := serve(t, grpcx.UnaryAssertion(verifier), server)

	err := call(t.Context(), conn)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("call = %v, want Unauthenticated", err)
	}
	if server.ok {
		t.Fatal("the handler ran for a call with no assertion")
	}
}

func TestACallWithAValidAssertionCarriesTheCaller(t *testing.T) {
	signer, verifier := keys(t)
	server := &echoServer{}
	conn := serve(t, grpcx.UnaryAssertion(verifier), server)

	assertion, err := signer.Sign("user-1", "anwexhaa", auth.AudienceInternal, time.Minute)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	ctx := metadata.AppendToOutgoingContext(t.Context(), grpcx.AssertionKey, assertion)

	if err := call(ctx, conn); err != nil {
		t.Fatalf("call: %v", err)
	}
	if !server.ok {
		t.Fatal("the handler saw no caller")
	}
	if server.saw.UserID != "user-1" || server.saw.Handle != "anwexhaa" {
		t.Fatalf("caller = %+v, want user-1/anwexhaa", server.saw)
	}
	if server.saw.Service != auth.ServiceGateway {
		t.Fatalf("service = %q, want %q", server.saw.Service, auth.ServiceGateway)
	}
}

// TestAnAssertionFromAnUnknownKeyIsRefused is the property the whole scheme
// rests on: reaching the port is not the same as being trusted.
func TestAnAssertionFromAnUnknownKeyIsRefused(t *testing.T) {
	_, verifier := keys(t)
	attacker, _ := keys(t)

	server := &echoServer{}
	conn := serve(t, grpcx.UnaryAssertion(verifier), server)

	assertion, err := attacker.Sign("administrator", "", auth.AudienceInternal, time.Minute)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	ctx := metadata.AppendToOutgoingContext(t.Context(), grpcx.AssertionKey, assertion)

	if err := call(ctx, conn); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("call = %v, want Unauthenticated", err)
	}
	if server.ok {
		t.Fatal("the handler ran for an assertion signed by an unknown key")
	}
}

// TestAClientTokenCannotBeUsedAsAnAssertion is the audience check at the
// boundary rather than in the token package. A user's own access token is
// signed by the same key the backends verify with; only the audience
// distinguishes it.
func TestAClientTokenCannotBeUsedAsAnAssertion(t *testing.T) {
	signer, verifier := keys(t)
	server := &echoServer{}
	conn := serve(t, grpcx.UnaryAssertion(verifier), server)

	clientToken, err := signer.Sign("user-1", "anwexhaa", auth.AudienceClient, time.Minute)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	ctx := metadata.AppendToOutgoingContext(t.Context(), grpcx.AssertionKey, clientToken)

	if err := call(ctx, conn); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("call = %v, want Unauthenticated", err)
	}
	if server.ok {
		t.Fatal("a browser's own access token was accepted as an internal assertion")
	}
}

func TestAnExpiredAssertionIsRefused(t *testing.T) {
	signer, verifier := keys(t)
	server := &echoServer{}
	conn := serve(t, grpcx.UnaryAssertion(verifier), server)

	assertion, err := signer.Sign("user-1", "", auth.AudienceInternal, time.Nanosecond)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	time.Sleep(2 * time.Millisecond)

	ctx := metadata.AppendToOutgoingContext(t.Context(), grpcx.AssertionKey, assertion)
	if err := call(ctx, conn); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("call = %v, want Unauthenticated", err)
	}
}

// TestAnExemptMethodNeedsNoAssertion covers the kubelet: a readiness probe has
// no key and no identity, and requiring one would mean the pod never becomes
// ready.
func TestAnExemptMethodNeedsNoAssertion(t *testing.T) {
	_, verifier := keys(t)
	server := &echoServer{}
	conn := serve(t, grpcx.UnaryAssertion(verifier, echoMethod), server)

	if err := call(t.Context(), conn); err != nil {
		t.Fatalf("an exempt method was refused: %v", err)
	}
	if server.ok {
		t.Fatal("an exempt call produced a verified caller out of nothing")
	}
}

// TestNoVerifierLeavesTheBoundaryOpen documents the development setting
// explicitly rather than leaving it as an accident of a nil check.
func TestNoVerifierLeavesTheBoundaryOpen(t *testing.T) {
	server := &echoServer{}
	conn := serve(t, grpcx.UnaryAssertion(nil), server)

	if err := call(t.Context(), conn); err != nil {
		t.Fatalf("call: %v", err)
	}
}

// TestTheSignerNamesTheCallerPerCall is why the assertion is minted per call
// rather than cached on the connection. One connection carries requests for
// many users, and a cached assertion would put the first user's identity on
// every one of them.
func TestTheSignerNamesTheCallerPerCall(t *testing.T) {
	signer, verifier := keys(t)
	server := &echoServer{}

	var subject string
	interceptor := grpcx.UnaryAssertionSigner(signer, func(context.Context) (string, string) {
		return subject, subject + "-handle"
	})

	listener := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(grpcx.UnaryAssertion(verifier)),
		grpc.ForceServerCodec(stringCodec{}),
	)
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "murmur.test.v1.Echo",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Say",
			Handler: func(_ any, ctx context.Context, dec func(any) error, chain grpc.UnaryServerInterceptor) (any, error) {
				var in string
				if err := dec(&in); err != nil {
					return nil, err
				}
				return chain(ctx, in, &grpc.UnaryServerInfo{FullMethod: echoMethod}, server.handler)
			},
		}},
	}, server)
	go func() { _ = srv.Serve(listener) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(stringCodec{})),
		grpc.WithChainUnaryInterceptor(interceptor),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	for _, want := range []string{"alice", "bob", "carol"} {
		subject = want
		if err := call(t.Context(), conn); err != nil {
			t.Fatalf("call as %s: %v", want, err)
		}
		if server.saw.UserID != want {
			t.Fatalf("the backend saw %q on a call made as %q", server.saw.UserID, want)
		}
	}
}
