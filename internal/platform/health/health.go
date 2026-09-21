// Package health serves the two probes Kubernetes asks for, and keeps them
// meaningfully different.
//
// Liveness answers "is this process wedged?" and must not touch dependencies.
// A liveness probe that pings Postgres turns one slow database into a rolling
// restart of every pod that talks to it.
//
// Readiness answers "should traffic come here right now?" and is exactly where
// dependency checks belong: a pod whose Redis connection is gone should leave
// the load balancer, not die.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Check reports whether one dependency is usable. It must respect its context
// deadline; a check that blocks is a check that takes the pod down.
type Check func(ctx context.Context) error

// Registry holds the readiness checks for one service.
type Registry struct {
	timeout time.Duration

	mu     sync.RWMutex
	checks map[string]Check
}

// New returns a registry that gives every check the same deadline.
func New(timeout time.Duration) *Registry {
	return &Registry{
		timeout: timeout,
		checks:  make(map[string]Check),
	}
}

// Register adds a named dependency check, replacing any check with that name.
func (r *Registry) Register(name string, check Check) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checks[name] = check
}

// Report is the outcome of running every check once.
type Report struct {
	Status  string            `json:"status"`
	Checks  map[string]string `json:"checks"`
	Healthy bool              `json:"-"`
}

// Run executes every check in parallel under a shared deadline. Parallel
// matters: three dependencies with a two-second timeout should take two
// seconds in the worst case, not six.
func (r *Registry) Run(ctx context.Context) Report {
	r.mu.RLock()
	checks := make(map[string]Check, len(r.checks))
	for name, check := range r.checks {
		checks[name] = check
	}
	r.mu.RUnlock()

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	var (
		mu      sync.Mutex
		wg      sync.WaitGroup
		results = make(map[string]string, len(checks))
	)

	for name, check := range checks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status := "ok"
			if err := check(ctx); err != nil {
				status = err.Error()
			}
			mu.Lock()
			results[name] = status
			mu.Unlock()
		}()
	}
	wg.Wait()

	report := Report{Status: "ok", Checks: results, Healthy: true}
	for _, status := range results {
		if status != "ok" {
			report.Status = "degraded"
			report.Healthy = false
			break
		}
	}
	return report
}

// ReadyHandler serves readiness: 200 when every dependency answers, 503 with
// the failing check names otherwise.
func (r *Registry) ReadyHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		report := r.Run(req.Context())

		code := http.StatusOK
		if !report.Healthy {
			code = http.StatusServiceUnavailable
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(report)
	})
}

// LiveHandler serves liveness. It deliberately checks nothing: if this handler
// runs at all, the process is scheduling goroutines and serving HTTP, which is
// the whole question.
func LiveHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
}
