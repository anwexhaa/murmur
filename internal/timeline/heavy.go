package timeline

import (
	"context"
	"log/slog"
	"sync"
	"time"

	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
)

// HeavySet caches the accounts big enough that their posts are not pushed.
//
// The read path needs this list on every request, and it barely changes: an
// account crossing the threshold is a rare event on a human timescale. So it is
// fetched on a timer rather than per request, which turns a per-read query into
// one query every refresh interval across the whole service.
//
// Staleness is safe in one direction and briefly visible in the other. An
// account that just became heavy is still being pushed, so readers see its
// posts either way. An account that just stopped being heavy is no longer
// pushed but is still merged, which is also fine. The window where a reader
// could miss a post is the gap between the worker deciding to stop pushing and
// this cache learning about it — bounded by the refresh interval, and closed
// entirely by the author feed, which is written for every post in both modes.
type HeavySet struct {
	social    socialv1.SocialServiceClient
	threshold int64
	interval  time.Duration
	log       *slog.Logger

	mu        sync.RWMutex
	ids       []string
	fetchedAt time.Time
	// loaded distinguishes "no heavy accounts" from "never successfully
	// fetched", which matter differently: the first is a fact, the second is
	// a reason to keep trying.
	loaded bool
}

// NewHeavySet builds the cache. A threshold of zero disables the pull side
// entirely and the set stays empty.
func NewHeavySet(social socialv1.SocialServiceClient, threshold int64, interval time.Duration, log *slog.Logger) *HeavySet {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	return &HeavySet{social: social, threshold: threshold, interval: interval, log: log}
}

// Enabled reports whether the pull side is in use at all.
func (h *HeavySet) Enabled() bool { return h.threshold > 0 }

// IDs returns the cached heavy account IDs.
func (h *HeavySet) IDs() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.ids
}

// Refresh fetches the heavy set once.
func (h *HeavySet) Refresh(ctx context.Context) error {
	if !h.Enabled() {
		return nil
	}

	resp, err := h.social.ListHeavyAuthors(ctx, &socialv1.ListHeavyAuthorsRequest{
		Threshold: h.threshold,
	})
	if err != nil {
		return err
	}

	ids := resp.GetUserIds()

	h.mu.Lock()
	previous := len(h.ids)
	h.ids = ids
	h.fetchedAt = time.Now()
	h.loaded = true
	h.mu.Unlock()

	if previous != len(ids) {
		h.log.Info("heavy set changed",
			"threshold", h.threshold, "accounts", len(ids), "previously", previous)
	}
	return nil
}

// Run refreshes on a timer until the context is cancelled. It is a
// lifecycle component in timeline-svc.
func (h *HeavySet) Run(ctx context.Context) error {
	if !h.Enabled() {
		h.log.Info("pull side disabled: every post is pushed")
		<-ctx.Done()
		return ctx.Err()
	}

	// One fetch immediately, so the service is not serving un-merged timelines
	// for the first interval after it starts.
	if err := h.Refresh(ctx); err != nil && ctx.Err() == nil {
		h.log.Warn("first heavy-set refresh failed; retrying on the timer", "error", err)
	}

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		if err := h.Refresh(ctx); err != nil && ctx.Err() == nil {
			// Keep serving from the last good copy. A refresh failure must not
			// empty the set, because an empty set silently turns every heavy
			// account's posts invisible.
			h.log.Warn("heavy-set refresh failed, keeping the previous set", "error", err)
		}
	}
}
