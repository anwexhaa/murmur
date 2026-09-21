package domain

import (
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

// IDGenerator produces monotonically increasing ULIDs.
//
// Monotonic matters more than it looks. Two posts created in the same
// millisecond would otherwise get IDs ordered by random entropy, which means
// a timeline could show them in an order that flips between reads, and the
// keyset cursor could skip one. The monotonic source increments instead of
// re-randomising within a millisecond, so ties break consistently and forever.
//
// ulid's monotonic entropy is not safe for concurrent use — it mutates an
// internal counter — so every call is serialised here. The lock is held for
// the duration of an increment, not an I/O operation, and only contends
// between posts created in the same millisecond.
type IDGenerator struct {
	mu      sync.Mutex
	entropy *ulid.MonotonicEntropy
}

// NewIDGenerator returns a generator seeded from the system CSPRNG.
//
// Reading from crypto/rand is not a hot path here despite appearances: the
// monotonic source only draws fresh entropy when the millisecond advances, so
// this reads at most a thousand times a second no matter how many posts are
// created.
func NewIDGenerator() *IDGenerator {
	return &IDGenerator{entropy: ulid.Monotonic(rand.Reader, 0)}
}

// New returns a ULID for the given instant.
//
// It fails only on monotonic overflow, which needs more than 2^80 IDs inside
// one millisecond. That is unreachable in practice, but it is returned rather
// than panicked on: a service that drops one write is better than a service
// that takes the process down.
func (g *IDGenerator) New(t time.Time) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	id, err := ulid.New(ulid.Timestamp(t), g.entropy)
	if err != nil {
		return "", fmt.Errorf("generate ulid: %w", err)
	}
	return id.String(), nil
}

// ValidatePostID checks that an identifier is a well-formed ULID, matching the
// CHECK constraint on the posts table.
func ValidatePostID(field, raw string) error {
	if raw == "" {
		return fmt.Errorf("%w: %s is required", ErrInvalid, field)
	}
	if _, err := ulid.ParseStrict(raw); err != nil {
		return fmt.Errorf("%w: %s is not a valid post id", ErrInvalid, field)
	}
	return nil
}
