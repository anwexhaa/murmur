// Package lifecycle runs a set of long-lived components and shuts them all
// down together.
//
// Every Murmur binary is a handful of things that block forever — an HTTP
// server, a gRPC server, a NATS consumer — and the interesting question is
// what happens when one of them stops. The contract here is:
//
//   - A SIGINT or SIGTERM begins a drain.
//   - Any component returning, for any reason, also begins a drain. A process
//     with a dead consumer and a live HTTP server is worse than a dead process,
//     because Kubernetes will happily keep routing to it.
//   - Stop is called on every component in reverse start order, under one
//     shared deadline, so a hung component cannot hold the process open
//     forever.
//   - The drain deadline is independent of the signal: the context that told
//     us to stop is already cancelled, so Stop gets a fresh one.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

// Component is one long-lived part of a service.
//
// Start blocks until the component finishes or its context is cancelled. Stop
// asks it to finish and blocks until it has, or until the drain deadline
// passes. Stop may be nil for components that need nothing beyond context
// cancellation.
type Component struct {
	Name  string
	Start func(ctx context.Context) error
	Stop  func(ctx context.Context) error
}

// Run starts every component and blocks until they have all stopped.
//
// It returns nil on a clean shutdown, or the joined errors of whatever failed.
// Errors that only mean "you asked me to stop" are not failures.
func Run(parent context.Context, log *slog.Logger, drain time.Duration, components ...Component) error {
	if len(components) == 0 {
		return errors.New("lifecycle: no components to run")
	}

	signalCtx, stopListening := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stopListening()

	// Our own cancel, below the signal context: a component whose Stop is nil
	// unwinds on context cancellation alone, and nothing else in this function
	// is in a position to cancel it once draining has finished.
	runCtx, cancelComponents := context.WithCancel(signalCtx)
	defer cancelComponents()

	group, groupCtx := errgroup.WithContext(runCtx)

	// Closed as soon as any component returns, so a component that finishes on
	// its own initiative drains the rest. Without this, Run would block
	// forever waiting on a context that nothing is going to cancel.
	exited := make(chan struct{})
	var once sync.Once
	noteExit := func(name string) {
		once.Do(func() {
			log.Info("component exited, draining the rest", "component", name)
			close(exited)
		})
	}

	for _, c := range components {
		group.Go(func() error {
			log.Info("component starting", "component", c.Name)
			defer noteExit(c.Name)

			if err := c.Start(groupCtx); err != nil && !isShutdown(err) {
				return fmt.Errorf("component %s: %w", c.Name, err)
			}
			return nil
		})
	}

	group.Go(func() error {
		select {
		case <-groupCtx.Done():
		case <-exited:
		}

		reason := "component exited"
		if signalCtx.Err() != nil {
			reason = "signal received"
		}
		log.Info("draining", "reason", reason, "timeout", drain)

		// WithoutCancel matters: groupCtx is already cancelled by the time we
		// get here, and a Stop handed an expired context cannot drain anything.
		drainCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), drain)
		defer cancel()

		var errs []error
		for i := len(components) - 1; i >= 0; i-- {
			c := components[i]
			if c.Stop == nil {
				continue
			}
			if err := c.Stop(drainCtx); err != nil && !isShutdown(err) {
				errs = append(errs, fmt.Errorf("stopping %s: %w", c.Name, err))
				log.Error("component did not stop cleanly", "component", c.Name, "error", err)
				continue
			}
			log.Info("component stopped", "component", c.Name)
		}

		// Every Stop has had its turn. Anything still blocked — a component
		// with no Stop, or one whose Stop only signalled intent — comes down
		// with the context now.
		cancelComponents()
		return errors.Join(errs...)
	})

	err := group.Wait()
	if err != nil {
		log.Error("shutdown completed with errors", "error", err)
		return err
	}
	log.Info("shutdown complete")
	return nil
}

// isShutdown reports whether err just means the component was asked to stop.
// These arrive on every clean shutdown and are not worth a non-zero exit code.
func isShutdown(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, http.ErrServerClosed)
}
