package gateway

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/anwexhaa/murmur/internal/gateway/callcount"
	"github.com/anwexhaa/murmur/internal/platform/grpcx"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestMetrics(t *testing.T) (*Metrics, *prometheus.Registry) {
	t.Helper()
	registry := prometheus.NewRegistry()
	return NewMetrics(registry), registry
}

// histogramSum returns the observation sum for one operation label.
func histogramSum(t *testing.T, registry *prometheus.Registry, name, operation string) float64 {
	t.Helper()

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "operation" && label.GetValue() == operation {
					return metric.GetHistogram().GetSampleSum()
				}
			}
		}
	}
	t.Fatalf("no metric %s with operation=%q", name, operation)
	return 0
}

func histogramCount(t *testing.T, registry *prometheus.Registry, name, operation string) uint64 {
	t.Helper()

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "operation" && label.GetValue() == operation {
					return metric.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	return 0
}

func TestInstrumentAttachesRequestIDAndEchoesIt(t *testing.T) {
	metrics, _ := newTestMetrics(t)

	var seen string
	handler := Instrument(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = grpcx.RequestID(r.Context())
	}), metrics, quietLogger(), time.Second)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/query", nil))

	if seen == "" {
		t.Error("no request ID reached the handler")
	}
	if echoed := recorder.Header().Get(grpcx.RequestIDKey); echoed != seen {
		t.Errorf("response header = %q, want the request's own ID %q", echoed, seen)
	}
}

// A caller-supplied correlation ID must survive, so one client request stays
// traceable across the gateway and everything behind it.
func TestInstrumentAdoptsACallerSuppliedRequestID(t *testing.T) {
	metrics, _ := newTestMetrics(t)
	const supplied = "client-correlation-id"

	var seen string
	handler := Instrument(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = grpcx.RequestID(r.Context())
	}), metrics, quietLogger(), time.Second)

	req := httptest.NewRequest(http.MethodPost, "/query", nil)
	req.Header.Set(grpcx.RequestIDKey, supplied)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if seen != supplied {
		t.Errorf("request ID = %q, want the caller's %q", seen, supplied)
	}
}

func TestInstrumentCarriesTheViewer(t *testing.T) {
	metrics, _ := newTestMetrics(t)
	const viewer = "11111111-2222-3333-4444-555555555555"

	var seen string
	handler := Instrument(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = Viewer(r.Context())
	}), metrics, quietLogger(), time.Second)

	req := httptest.NewRequest(http.MethodPost, "/query", nil)
	req.Header.Set(ViewerHeader, viewer)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if seen != viewer {
		t.Errorf("viewer = %q, want %q", seen, viewer)
	}
}

func TestInstrumentLeavesViewerEmptyWhenNoHeader(t *testing.T) {
	metrics, _ := newTestMetrics(t)

	seen := "not-called"
	handler := Instrument(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = Viewer(r.Context())
	}), metrics, quietLogger(), time.Second)

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/query", nil))

	if seen != "" {
		t.Errorf("viewer = %q, want empty with no header", seen)
	}
}

// Every downstream call inherits the request's deadline, so a request the
// client has abandoned stops costing the backends anything.
func TestInstrumentImposesADeadline(t *testing.T) {
	metrics, _ := newTestMetrics(t)

	var deadline time.Time
	var hasDeadline bool
	handler := Instrument(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		deadline, hasDeadline = r.Context().Deadline()
	}), metrics, quietLogger(), 250*time.Millisecond)

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/query", nil))

	if !hasDeadline {
		t.Fatal("the handler's context has no deadline")
	}
	if remaining := time.Until(deadline); remaining <= 0 || remaining > 250*time.Millisecond {
		t.Errorf("deadline is %v away, want it within the 250ms budget", remaining)
	}
}

// This is the measurement phase 2 exists to produce, so it has a test.
func TestInstrumentRecordsTheDownstreamCallCount(t *testing.T) {
	metrics, registry := newTestMetrics(t)

	handler := Instrument(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		RecordOperationName(r.Context(), "HomeTimeline")
		counter := callcount.FromContext(r.Context())
		for range 163 {
			counter.Record("/murmur.social.v1.SocialService/GetUser")
		}
	}), metrics, quietLogger(), time.Second)

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/query", nil))

	if got := histogramSum(t, registry, "murmur_gateway_downstream_calls_per_request", "HomeTimeline"); got != 163 {
		t.Errorf("recorded %v calls, want 163", got)
	}
	if got := histogramCount(t, registry, "murmur_gateway_downstream_calls_per_request", "HomeTimeline"); got != 1 {
		t.Errorf("recorded %d observations, want 1", got)
	}
}

// Metrics are labelled by GraphQL operation because every request is a POST
// to the same path — the path carries no information at all.
func TestOperationLabelComesFromTheParsedQuery(t *testing.T) {
	metrics, registry := newTestMetrics(t)

	handler := Instrument(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		RecordOperationName(r.Context(), "UserProfile")
	}), metrics, quietLogger(), time.Second)

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/query", nil))

	if got := histogramCount(t, registry, "murmur_gateway_request_duration_seconds", "UserProfile"); got != 1 {
		t.Errorf("no observation labelled UserProfile")
	}
}

// An unnamed query must not invent a label. Generating one per request would
// turn a single metric into thousands of useless series.
func TestUnnamedOperationsShareOneLabel(t *testing.T) {
	metrics, registry := newTestMetrics(t)

	handler := Instrument(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
		metrics, quietLogger(), time.Second)

	for range 3 {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/query", nil))
	}

	if got := histogramCount(t, registry, "murmur_gateway_request_duration_seconds", "anonymous"); got != 3 {
		t.Errorf("anonymous observations = %d, want all 3 under one label", got)
	}
}

func TestInstrumentRecordsTheStatusCode(t *testing.T) {
	metrics, _ := newTestMetrics(t)

	handler := Instrument(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = io.WriteString(w, "nope")
	}), metrics, quietLogger(), time.Second)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/query", nil))

	if recorder.Code != http.StatusTeapot {
		t.Errorf("status = %d, want %d passed through", recorder.Code, http.StatusTeapot)
	}
	if body := recorder.Body.String(); !strings.Contains(body, "nope") {
		t.Errorf("body = %q, want the handler's own output", body)
	}
}

// A handler that writes without calling WriteHeader still produces a 200, and
// the recorder must not double-write the header.
func TestStatusRecorderDefaultsToOK(t *testing.T) {
	recorder := httptest.NewRecorder()
	wrapped := &statusRecorder{ResponseWriter: recorder, status: http.StatusOK}

	if _, err := io.WriteString(wrapped, "body"); err != nil {
		t.Fatalf("write: %v", err)
	}
	wrapped.WriteHeader(http.StatusInternalServerError)

	if wrapped.status != http.StatusOK {
		t.Errorf("status = %d, want the first write to win", wrapped.status)
	}
	if recorder.Code != http.StatusOK {
		t.Errorf("underlying status = %d, want 200", recorder.Code)
	}
}

func TestRecordOperationNameOutsideARequestIsSafe(t *testing.T) {
	RecordOperationName(context.Background(), "Whatever")
}

func TestWithViewerRoundTrips(t *testing.T) {
	ctx := WithViewer(context.Background(), "someone")
	if got := Viewer(ctx); got != "someone" {
		t.Errorf("Viewer() = %q, want %q", got, "someone")
	}
	if got := Viewer(context.Background()); got != "" {
		t.Errorf("Viewer() on a bare context = %q, want empty", got)
	}
}
