// Package httpx adapts net/http servers to the lifecycle contract.
package httpx

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/anwexhaa/murmur/internal/platform/lifecycle"
)

// Server wraps an http.Handler as a lifecycle component whose Stop performs a
// graceful shutdown: no new connections, in-flight requests allowed to finish.
//
// Note the timeouts it does not set. ReadHeaderTimeout closes the Slowloris
// hole, and IdleTimeout reaps dead keep-alives, but there is deliberately no
// WriteTimeout: from Phase 6 this same helper carries GraphQL subscriptions
// over WebSocket, and a write deadline would sever every long-lived stream on
// a fixed clock.
func Server(name, addr string, handler http.Handler, log *slog.Logger) lifecycle.Component {
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}

	return lifecycle.Component{
		Name: name,
		Start: func(context.Context) error {
			log.Info("http server listening", "component", name, "addr", addr)
			return srv.ListenAndServe()
		},
		Stop: func(ctx context.Context) error {
			return srv.Shutdown(ctx)
		},
	}
}
