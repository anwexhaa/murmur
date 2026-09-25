package gateway

import (
	"context"
	"net"
	"net/http"
	"strconv"

	"github.com/vektah/gqlparser/v2/gqlerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/anwexhaa/murmur/internal/platform/ratelimit"
)

// Rate limit scopes. Separate buckets, because an attacker spending one must
// not spend the others: failing a password a few times should not also cost
// the account its ability to read a timeline.
const (
	ScopeLogin       = "login"
	ScopeLoginSource = "login-src"
	ScopeRegister    = "register"
	ScopeRefresh     = "refresh"
	ScopeRequest     = "request"
)

// Rules are the limits the gateway enforces.
//
// The numbers are not arbitrary and the ratios are the argument. A login costs
// argon2 over 19MB and roughly fifty milliseconds of CPU; a timeline read
// costs four batched gRPC calls against warm caches. Pricing them the same
// would mean the login path could be used to exhaust the machine while staying
// comfortably inside a limit set for reads.
var (
	// Five bad passwords in a row, then one more every twelve seconds.
	// Generous enough that a person mistyping does not notice, tight enough
	// that guessing a password at this rate takes centuries.
	LoginRule = ratelimit.Rule{Capacity: 5, Refill: 1.0 / 12.0, Cost: 1}

	// Per client address, and looser: one address is a whole office or a
	// mobile carrier's NAT, so this is sized to stop stuffing rather than to
	// stop people.
	LoginSourceRule = ratelimit.Rule{Capacity: 30, Refill: 0.5, Cost: 1}

	// Registration is rarer than login and more expensive to undo.
	RegisterRule = ratelimit.Rule{Capacity: 3, Refill: 1.0 / 60.0, Cost: 1}

	// A refresh is cheap, and a client with a flapping network may retry.
	RefreshRule = ratelimit.Rule{Capacity: 30, Refill: 1, Cost: 1}

	// The blanket per-identity limit on everything else: 120 requests of
	// burst, sustained at 20 a second. Well above what a person browsing
	// produces and well below what one client should be able to take from a
	// replica.
	RequestRule = ratelimit.Rule{Capacity: 120, Refill: 20, Cost: 1}
)

type clientKeyCtx struct{}

// WithClientKey records the address a request arrived from.
func WithClientKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, clientKeyCtx{}, key)
}

// clientKey returns the caller's address, or "unknown" when the middleware did
// not record one.
//
// "unknown" is a shared bucket on purpose. It is reached only when the
// middleware could not parse a remote address, which should not happen; having
// those requests share one narrow allowance is a safer failure than giving
// each of them a private one.
func clientKey(ctx context.Context) string {
	if key, ok := ctx.Value(clientKeyCtx{}).(string); ok && key != "" {
		return key
	}
	return "unknown"
}

// ClientKeyFromRequest picks the identity a per-address limit applies to.
//
// The remote address, never X-Forwarded-For. A header the client controls is
// not an identity: an attacker sets a different one per request and every
// request gets a fresh bucket, which is a limiter that cannot limit. Behind a
// trusted proxy this needs the proxy's own forwarded header and a list of
// trusted hops, which is deployment configuration rather than something to
// guess at here.
func ClientKeyFromRequest(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// rulesByScope maps a scope to its rule.
func rulesByScope(scope string) ratelimit.Rule {
	switch scope {
	case ScopeLogin:
		return LoginRule
	case ScopeLoginSource:
		return LoginSourceRule
	case ScopeRegister:
		return RegisterRule
	case ScopeRefresh:
		return RefreshRule
	default:
		return RequestRule
	}
}

// throttle takes one token and turns a refusal into a GraphQL error.
func (r *Resolver) throttle(ctx context.Context, scope, identity string) error {
	if r.Limiter == nil {
		return nil
	}

	decision, err := r.Limiter.Allow(ctx, scope, identity, rulesByScope(scope))
	if err != nil {
		// Allow returns Allowed on failure, and it has already counted the
		// failure. Logging keeps it visible without failing the request.
		r.Log.Warn("rate limiter unavailable; allowing the request", "scope", scope, "error", err)
		return nil
	}
	if decision.Allowed {
		return nil
	}

	return &gqlerror.Error{
		Message: "too many requests; slow down",
		Extensions: map[string]any{
			"code":       codes.ResourceExhausted.String(),
			"retryAfter": strconv.Itoa(int(decision.RetryAfter.Seconds() + 0.5)),
		},
	}
}

// errNoSessions means the gateway was built without a signing key, which can
// only be a startup mistake.
var errNoSessions = status.Error(codes.Unimplemented, "authentication is not enabled on this gateway")
