// Package callcount counts the downstream calls one inbound request makes.
//
// This is the instrument phase 2 exists to build. The gateway is deliberately
// written the obvious way — a resolver per field, each doing its own lookup —
// and the claim that this is expensive is worth nothing without a number
// attached. So every gRPC call is counted, by method, for the life of one
// HTTP request.
//
// The counting lives in a client interceptor rather than in the resolvers.
// Resolvers therefore cannot forget to count, cannot double-count, and do not
// mention counting at all — which matters because phase 5 rewrites them to
// batch, and the measurement has to survive that rewrite unchanged to be a
// fair comparison.
package callcount

import (
	"context"
	"sort"
	"strings"
	"sync"

	"google.golang.org/grpc"
)

type contextKey struct{}

// Counter tallies downstream calls for a single request.
//
// GraphQL resolves sibling fields concurrently, so several goroutines record
// into the same counter. The mutex is held only to bump an integer.
type Counter struct {
	mu       sync.Mutex
	total    int
	byMethod map[string]int
}

// New returns an empty counter.
func New() *Counter {
	return &Counter{byMethod: make(map[string]int, 8)}
}

// NewContext attaches a fresh counter to ctx and returns both.
func NewContext(ctx context.Context) (context.Context, *Counter) {
	counter := New()
	return context.WithValue(ctx, contextKey{}, counter), counter
}

// FromContext returns the counter for this request, or nil outside one.
func FromContext(ctx context.Context) *Counter {
	counter, _ := ctx.Value(contextKey{}).(*Counter)
	return counter
}

// Record counts one call to the given gRPC method.
func (c *Counter) Record(method string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.total++
	c.byMethod[shortMethod(method)]++
}

// Total returns how many downstream calls this request has made.
func (c *Counter) Total() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.total
}

// Breakdown returns the per-method tallies, which is the half of the number
// that explains itself: "62 calls" is a fact, "1 ListFollowing + 11
// ListAuthorPosts + 50 GetUser" is a diagnosis.
func (c *Counter) Breakdown() map[string]int {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make(map[string]int, len(c.byMethod))
	for method, n := range c.byMethod {
		out[method] = n
	}
	return out
}

// Summary renders the breakdown as a stable, sorted, log-friendly string.
func (c *Counter) Summary() string {
	breakdown := c.Breakdown()
	if len(breakdown) == 0 {
		return "none"
	}

	methods := make([]string, 0, len(breakdown))
	for method := range breakdown {
		methods = append(methods, method)
	}
	// Most frequent first, then alphabetical, so the worst offender leads and
	// the string is identical for identical inputs.
	sort.Slice(methods, func(i, j int) bool {
		if breakdown[methods[i]] != breakdown[methods[j]] {
			return breakdown[methods[i]] > breakdown[methods[j]]
		}
		return methods[i] < methods[j]
	})

	var b strings.Builder
	for i, method := range methods {
		if i > 0 {
			b.WriteString(" + ")
		}
		b.WriteString(itoa(breakdown[method]))
		b.WriteByte(' ')
		b.WriteString(method)
	}
	return b.String()
}

// UnaryClientInterceptor records every outbound call against the counter in
// the call's context. Calls made outside a request context are not counted
// and not an error — health checks and warmup are not part of anyone's page.
func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		FromContext(ctx).Record(method)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// shortMethod turns "/murmur.social.v1.SocialService/GetUser" into "GetUser".
func shortMethod(fullMethod string) string {
	if i := strings.LastIndexByte(fullMethod, '/'); i >= 0 && i+1 < len(fullMethod) {
		return fullMethod[i+1:]
	}
	return fullMethod
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
