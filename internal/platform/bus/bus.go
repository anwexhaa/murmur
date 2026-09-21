// Package bus opens the NATS connection and its JetStream context.
//
// JetStream, not core NATS: core pub/sub drops messages when nobody is
// listening, and a fanout worker that was restarting when a post was published
// would leave that post out of every timeline forever. JetStream persists the
// stream and redelivers unacknowledged messages, which turns a worker crash
// into a retry instead of silent data loss.
package bus

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/anwexhaa/murmur/internal/platform/config"
)

// Conn bundles the raw connection with its JetStream context. Publishers and
// consumers want JS; health checks and drain want NC.
type Conn struct {
	NC *nats.Conn
	JS jetstream.JetStream
}

// Open connects to NATS and derives a JetStream context. The returned function
// drains the connection, which delivers buffered publishes and lets in-flight
// message handlers finish before the socket closes.
func Open(_ context.Context, cfg config.NATS, log *slog.Logger) (*Conn, func(), error) {
	nc, err := nats.Connect(cfg.URL,
		nats.Name("murmur"),
		nats.Timeout(cfg.ConnectTimeout),
		nats.MaxReconnects(cfg.MaxReconnects),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Warn("nats disconnected", "error", err)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			log.Info("nats reconnected", "url", c.ConnectedUrl())
		}),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("connect nats at %s: %w", cfg.URL, err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("create jetstream context: %w", err)
	}

	log.Info("nats connected", "url", nc.ConnectedUrl())

	return &Conn{NC: nc, JS: js}, func() {
		// Drain rather than Close: it flushes pending publishes and waits for
		// subscription handlers to return. Close would abandon both.
		if err := nc.Drain(); err != nil {
			log.Warn("draining nats", "error", err)
			return
		}
		log.Info("nats drained")
	}, nil
}

// Healthy reports whether the connection is currently usable, for readiness.
// A reconnecting client is not ready: it will buffer publishes rather than
// fail them, which hides the outage instead of shedding traffic.
func (c *Conn) Healthy(context.Context) error {
	if !c.NC.IsConnected() {
		return fmt.Errorf("nats not connected (status %s)", c.NC.Status())
	}
	return nil
}
