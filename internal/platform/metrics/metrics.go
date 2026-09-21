// Package metrics owns the Prometheus registry and its endpoint.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry is a private registry rather than the default global one.
//
// The default registry is package-level mutable state that any dependency can
// write to. A private one means every series on /metrics was registered
// deliberately, and two tests in the same binary cannot collide over a metric
// name.
type Registry struct {
	*prometheus.Registry
}

// New returns a registry carrying the Go runtime and process collectors,
// which cost nothing and answer the first question asked during any incident:
// whether the problem is the service or the machine under it.
func New() *Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return &Registry{Registry: reg}
}

// Handler serves the metrics endpoint.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.Registry, promhttp.HandlerOpts{
		// A scrape failure should be visible as a scrape failure, not as a
		// silently truncated exposition.
		ErrorHandling: promhttp.HTTPErrorOnError,
	})
}
