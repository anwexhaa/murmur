package gateway

import (
	"bufio"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/anwexhaa/murmur/internal/auth"
	"github.com/anwexhaa/murmur/internal/gateway/callcount"
	"github.com/anwexhaa/murmur/internal/platform/grpcx"
)

type viewerKey struct{}
type viewerHandleKey struct{}

// AuthorizationHeader carries the access token.
//
// Phase 7 replaced X-Murmur-User, a plaintext header naming whoever the caller
// said they were, with a signature. The old header was honest scaffolding --
// it let the read path be built and measured before auth existed -- and it was
// also a complete authentication bypass for anyone who could reach the port.
// Nothing outside this file ever knew the difference, which was the point of
// confining it to one function.
const AuthorizationHeader = "Authorization"

// bearerPrefix is the scheme, matched case-insensitively because RFC 7235 says
// the scheme is case-insensitive and some clients send "bearer".
const bearerPrefix = "bearer "

// Viewer returns the signed-in account's ID, or "" when nobody is.
func Viewer(ctx context.Context) string {
	id, _ := ctx.Value(viewerKey{}).(string)
	return id
}

// ViewerHandle returns the signed-in account's handle, or "".
func ViewerHandle(ctx context.Context) string {
	handle, _ := ctx.Value(viewerHandleKey{}).(string)
	return handle
}

// WithViewer attaches a viewer ID, for tests and for the auth middleware.
func WithViewer(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, viewerKey{}, userID)
}

// WithViewerHandle attaches the viewer's handle.
func WithViewerHandle(ctx context.Context, handle string) context.Context {
	return context.WithValue(ctx, viewerHandleKey{}, handle)
}

// BearerToken pulls the access token out of an Authorization header value.
// It returns "" for anything that is not a bearer credential.
func BearerToken(header string) string {
	if len(header) < len(bearerPrefix) {
		return ""
	}
	if !strings.EqualFold(header[:len(bearerPrefix)], bearerPrefix) {
		return ""
	}
	return strings.TrimSpace(header[len(bearerPrefix):])
}

// Authenticate verifies an access token and returns the context a request
// should run under.
//
// A token that fails to verify produces an anonymous context rather than an
// error. That is deliberate: the schema has public fields, and a client
// holding an expired token should be able to read a public profile while it
// refreshes rather than have the whole request rejected. Fields that require a
// viewer return Unauthenticated on their own, which is a more useful answer
// than a blanket 401 on a query that asked for one private field among twenty
// public ones.
func Authenticate(ctx context.Context, verifier *auth.Verifier, header string) context.Context {
	if verifier == nil {
		return ctx
	}
	token := BearerToken(header)
	if token == "" {
		return ctx
	}
	claims, err := verifier.Parse(token)
	if err != nil || claims.UserID() == "" {
		return ctx
	}
	return WithViewerHandle(WithViewer(ctx, claims.UserID()), claims.Handle)
}

// Metrics are the gateway's own instruments.
type Metrics struct {
	// DownstreamCalls is the measurement phase 2 exists to produce. A
	// histogram rather than a counter, because the interesting question is
	// the shape of the distribution — the median request is cheap and the
	// timeline request is not, and an average would hide both.
	DownstreamCalls *prometheus.HistogramVec
	RequestDuration *prometheus.HistogramVec
}

// NewMetrics registers the gateway's instruments.
func NewMetrics(registry prometheus.Registerer) *Metrics {
	m := &Metrics{
		DownstreamCalls: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "murmur",
			Subsystem: "gateway",
			Name:      "downstream_calls_per_request",
			Help:      "gRPC calls made while serving one inbound request.",
			// Deliberately spanning 1 to 256: phase 2 lands near the top of
			// this range and phase 5 should land at the bottom, and the same
			// buckets have to hold both for the comparison to be readable.
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256},
		}, []string{"operation"}),

		RequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "murmur",
			Subsystem: "gateway",
			Name:      "request_duration_seconds",
			Help:      "Wall time to serve one inbound request.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"operation"}),
	}

	registry.MustRegister(m.DownstreamCalls, m.RequestDuration)
	return m
}

// Instrument wraps a handler with everything one request needs: a correlation
// ID, a deadline, a viewer, per-request loaders, a downstream-call counter,
// and a log line at the end carrying all of it.
//
// clients may be nil, in which case no loaders are built — which is only ever
// the case in tests of this middleware itself.
func Instrument(next http.Handler, clients *Clients, verifier *auth.Verifier, metrics *Metrics, log *slog.Logger, timeout time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		requestID := r.Header.Get(grpcx.RequestIDKey)
		if requestID == "" {
			requestID = uuid.NewString()
		}

		// Every downstream call inherits this deadline, so a request that the
		// client has given up on stops costing the backends money.
		//
		// Except a WebSocket, which is meant to stay open. Applying the request
		// timeout to an upgrade would cancel the connection's context on a
		// fixed clock and kill every subscription fifteen seconds in — the
		// deadline that protects a query is exactly the wrong thing for a
		// stream. A subscription's lifetime is bounded by the socket instead,
		// and by the keepalive ping that notices when the peer is gone.
		ctx := r.Context()
		if isWebSocketUpgrade(r) {
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(ctx)
			defer cancel()
		} else {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		ctx = grpcx.WithRequestID(ctx, requestID)

		ctx = Authenticate(ctx, verifier, r.Header.Get(AuthorizationHeader))
		viewer := Viewer(ctx)

		// The address a per-source rate limit applies to. Recorded here
		// because it is the last point at which the *HTTP* request exists; a
		// resolver sees only a context.
		ctx = WithClientKey(ctx, ClientKeyFromRequest(r))

		// Loaders are built here, per request, and die with the context.
		//
		// They must not be shared: viewerFollows depends on who is asking, so a
		// loader outliving its request would serve one viewer's answer to
		// another. That is a data-leak bug rather than a performance bug, and
		// building them at exactly this point is what prevents it.
		if clients != nil {
			ctx = WithLoaders(ctx, NewLoaders(clients, viewer))
		}

		ctx, counter := callcount.NewContext(ctx)

		holder := &operationHolder{}
		ctx = context.WithValue(ctx, operationKey{}, holder)

		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		recorder.Header().Set(grpcx.RequestIDKey, requestID)

		next.ServeHTTP(recorder, r.WithContext(ctx))

		elapsed := time.Since(start)
		operation := operationName(r, holder)

		metrics.DownstreamCalls.WithLabelValues(operation).Observe(float64(counter.Total()))
		metrics.RequestDuration.WithLabelValues(operation).Observe(elapsed.Seconds())

		log.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"operation", operation,
			"status", recorder.status,
			"duration_ms", float64(elapsed.Microseconds())/1000,
			"downstream_calls", counter.Total(),
			"downstream_breakdown", counter.Summary(),
			"request_id", requestID,
			"viewer", Viewer(ctx),
		)
	})
}

type operationKey struct{}

// operationHolder carries the GraphQL operation name back out to the HTTP
// middleware.
//
// The name arrives in the request body, which the middleware must not consume
// — that is the executor's to read. gqlgen knows the name once it has parsed
// the query, but by then it is inside a derived context whose values cannot
// travel back up. A pointer placed in the context on the way in solves it:
// the executor fills it, the middleware reads it after ServeHTTP returns.
type operationHolder struct {
	mu   sync.Mutex
	name string
}

func (h *operationHolder) set(name string) {
	if h == nil || name == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.name = name
}

func (h *operationHolder) get() string {
	if h == nil {
		return ""
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.name
}

// RecordOperationName reports the operation gqlgen parsed, for metric labels.
// Called from the executor's operation middleware; a no-op outside a request.
func RecordOperationName(ctx context.Context, name string) {
	holder, _ := ctx.Value(operationKey{}).(*operationHolder)
	holder.set(name)
}

// isWebSocketUpgrade reports whether this request is starting a WebSocket.
//
// Both headers are checked because either alone is ambiguous: Connection can
// carry several comma-separated tokens, and a stray Upgrade header without a
// Connection token is not an upgrade.
func isWebSocketUpgrade(r *http.Request) bool {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return false
	}
	for _, token := range strings.Split(r.Header.Get("Connection"), ",") {
		if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
			return true
		}
	}
	return false
}

// operationName labels metrics by GraphQL operation rather than by path.
//
// Every GraphQL request is a POST to the same URL, so the path carries no
// information at all. Unnamed queries are labelled "anonymous" rather than
// given a generated label, which would turn one metric into thousands of
// useless series.
func operationName(r *http.Request, holder *operationHolder) string {
	if name := holder.get(); name != "" {
		return name
	}
	if r.URL.Path != "/query" {
		return r.URL.Path
	}
	return "anonymous"
}

// statusRecorder remembers the status code so it can be logged. net/http
// offers no way to read back what a handler wrote.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.wroteHeader {
		return
	}
	s.status = code
	s.wroteHeader = true
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if !s.wroteHeader {
		s.WriteHeader(http.StatusOK)
	}
	return s.ResponseWriter.Write(b)
}

// Flush forwards to the underlying writer when it supports flushing, which
// gqlgen needs for deferred and streamed responses.
func (s *statusRecorder) Flush() {
	if flusher, ok := s.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// Hijack hands the raw connection to the caller, which is how a WebSocket
// upgrade takes the socket away from net/http.
//
// Wrapping a ResponseWriter silently removes every optional interface the
// original implemented — Flusher, Hijacker, ReaderFrom — because the wrapper
// satisfies only http.ResponseWriter and Go has no way to forward the rest.
// A middleware that logs status codes therefore breaks WebSockets, and the
// failure is invisible until something actually tries to upgrade: every query
// keeps working, and the subscription fails with a 501 that names the
// transport rather than the wrapper that removed it.
func (s *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := s.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("gateway: the underlying ResponseWriter does not support hijacking")
	}
	// The upgrade takes the connection over entirely, so nothing further will
	// be written through this recorder. Record the status the handshake
	// promises now, while there is still somewhere to record it.
	s.status = http.StatusSwitchingProtocols
	s.wroteHeader = true
	return hijacker.Hijack()
}
