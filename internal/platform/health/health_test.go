package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func ok(context.Context) error { return nil }

func TestReadyIsHealthyWhenEveryCheckPasses(t *testing.T) {
	r := New(time.Second)
	r.Register("postgres", ok)
	r.Register("redis", ok)

	report := r.Run(context.Background())
	if !report.Healthy {
		t.Fatalf("Healthy = false, want true; checks: %v", report.Checks)
	}
	if report.Status != "ok" {
		t.Errorf("Status = %q, want %q", report.Status, "ok")
	}
}

func TestReadyReportsTheFailingDependencyByName(t *testing.T) {
	r := New(time.Second)
	r.Register("postgres", ok)
	r.Register("redis", func(context.Context) error { return errors.New("connection refused") })

	report := r.Run(context.Background())
	if report.Healthy {
		t.Fatal("Healthy = true, want false when a dependency is down")
	}
	if report.Checks["postgres"] != "ok" {
		t.Errorf("postgres = %q, want ok", report.Checks["postgres"])
	}
	if report.Checks["redis"] != "connection refused" {
		t.Errorf("redis = %q, want the underlying error", report.Checks["redis"])
	}
}

// Checks run in parallel, so the slowest one sets the total, not the sum.
func TestChecksRunInParallel(t *testing.T) {
	r := New(2 * time.Second)
	slow := func(ctx context.Context) error {
		select {
		case <-time.After(200 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, name := range []string{"a", "b", "c", "d"} {
		r.Register(name, slow)
	}

	start := time.Now()
	report := r.Run(context.Background())
	elapsed := time.Since(start)

	if !report.Healthy {
		t.Fatalf("Healthy = false, want true; checks: %v", report.Checks)
	}
	if elapsed > 600*time.Millisecond {
		t.Errorf("four 200ms checks took %v, which means they ran in sequence", elapsed)
	}
}

// A dependency that hangs must fail readiness on the registry's deadline
// rather than hold the probe open until the kubelet gives up.
func TestHangingCheckFailsOnTheDeadline(t *testing.T) {
	r := New(100 * time.Millisecond)
	r.Register("wedged", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})

	start := time.Now()
	report := r.Run(context.Background())

	if report.Healthy {
		t.Fatal("Healthy = true, want false when a check never returns")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Run() took %v, want it bounded by the 100ms timeout", elapsed)
	}
}

func TestReadyHandlerStatusCodes(t *testing.T) {
	tests := []struct {
		name  string
		check Check
		want  int
	}{
		{"healthy", ok, http.StatusOK},
		{"degraded", func(context.Context) error { return errors.New("down") }, http.StatusServiceUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New(time.Second)
			r.Register("dep", tt.check)

			rec := httptest.NewRecorder()
			r.ReadyHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

			if rec.Code != tt.want {
				t.Errorf("status = %d, want %d", rec.Code, tt.want)
			}

			var report Report
			if err := json.Unmarshal(rec.Body.Bytes(), &report); err != nil {
				t.Fatalf("response body is not JSON: %v", err)
			}
			if _, present := report.Checks["dep"]; !present {
				t.Error("response omits the dependency name")
			}
		})
	}
}

// Liveness must stay green while dependencies are down, or one slow database
// restarts every pod that talks to it.
func TestLivenessIgnoresDependencies(t *testing.T) {
	rec := httptest.NewRecorder()
	LiveHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

// Readiness with nothing registered is healthy: a service with no external
// dependencies is ready as soon as it serves.
func TestEmptyRegistryIsReady(t *testing.T) {
	report := New(time.Second).Run(context.Background())
	if !report.Healthy {
		t.Error("Healthy = false, want true for a service with no dependencies")
	}
}
