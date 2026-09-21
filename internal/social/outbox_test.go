package social_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/anwexhaa/murmur/internal/domain"
	"github.com/anwexhaa/murmur/internal/social"
)

// Every ClaimOutbox in this file closes its transaction, including the ones
// whose result is only inspected. A claim holds row locks and a pooled
// connection until it is committed or rolled back, and the next test's
// TRUNCATE needs an exclusive lock on the same table — so a leaked claim does
// not fail its own test, it hangs the one after it.

func makePost(t *testing.T, store *social.Store, author domain.User) domain.Post {
	t.Helper()

	id, err := domain.NewIDGenerator().New(time.Now())
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	post := domain.Post{
		ID:        id,
		AuthorID:  author.ID,
		Body:      "a post",
		CreatedAt: time.Now().UTC(),
	}
	if err := store.CreatePostWithEvent(t.Context(), post, []byte(`{"post_id":"`+id+`"}`)); err != nil {
		t.Fatalf("CreatePostWithEvent() = %v", err)
	}
	return post
}

// The whole reason the outbox exists: a post and its event are one atomic
// fact, so there is no state in which the post is visible but no timeline
// will ever hear about it.
func TestPostAndEventCommitTogether(t *testing.T) {
	store := newStore(t)
	author := makeUser(t, store, "author")

	post := makePost(t, store, author)

	// The post is readable.
	if _, err := store.GetPost(t.Context(), post.ID); err != nil {
		t.Fatalf("GetPost() = %v", err)
	}

	// And its event is waiting.
	pending, err := store.PendingOutbox(t.Context())
	if err != nil {
		t.Fatalf("PendingOutbox() = %v", err)
	}
	if pending != 1 {
		t.Errorf("pending events = %d, want 1", pending)
	}
}

// A post that cannot be written must not leave an orphan event behind, or the
// fanout worker would chase a post that does not exist.
func TestAFailedPostWritesNoEvent(t *testing.T) {
	store := newStore(t)

	id, err := domain.NewIDGenerator().New(time.Now())
	if err != nil {
		t.Fatalf("New() = %v", err)
	}

	err = store.CreatePostWithEvent(t.Context(), domain.Post{
		ID:        id,
		AuthorID:  uuid.New(), // nobody
		Body:      "orphan",
		CreatedAt: time.Now().UTC(),
	}, []byte(`{}`))
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("CreatePostWithEvent() = %v, want ErrNotFound", err)
	}

	pending, err := store.PendingOutbox(t.Context())
	if err != nil {
		t.Fatalf("PendingOutbox() = %v", err)
	}
	if pending != 0 {
		t.Errorf("pending events = %d after a failed post, want 0 — the transaction should have taken the event with it", pending)
	}
}

func TestClaimAndMarkPublished(t *testing.T) {
	store := newStore(t)
	author := makeUser(t, store, "author")
	for range 3 {
		makePost(t, store, author)
	}

	tx, records, err := store.ClaimOutbox(t.Context(), 10)
	if err != nil {
		t.Fatalf("ClaimOutbox() = %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("claimed %d events, want 3", len(records))
	}
	for _, r := range records {
		if r.Type != social.PostCreatedType {
			t.Errorf("event type = %q, want %q", r.Type, social.PostCreatedType)
		}
		if len(r.Payload) == 0 {
			t.Error("event has an empty payload")
		}
	}

	ids := make([]int64, len(records))
	for i, r := range records {
		ids[i] = r.ID
	}
	if err := store.MarkPublished(t.Context(), tx, ids); err != nil {
		t.Fatalf("MarkPublished() = %v", err)
	}

	pending, err := store.PendingOutbox(t.Context())
	if err != nil {
		t.Fatalf("PendingOutbox() = %v", err)
	}
	if pending != 0 {
		t.Errorf("pending events = %d after marking all published, want 0", pending)
	}
}

// SKIP LOCKED is what lets several relay instances run at once. Without it a
// second relay would queue behind the first, and running only one would make
// the relay a single point of failure for every write in the system.
func TestConcurrentClaimsDoNotOverlap(t *testing.T) {
	store := newStore(t)
	author := makeUser(t, store, "author")
	for range 6 {
		makePost(t, store, author)
	}

	firstTx, firstBatch, err := store.ClaimOutbox(t.Context(), 3)
	if err != nil {
		t.Fatalf("first ClaimOutbox() = %v", err)
	}
	defer func() { _ = firstTx.Rollback(t.Context()) }()

	// A second claim, while the first still holds its locks, must skip those
	// rows rather than block on them.
	secondTx, secondBatch, err := store.ClaimOutbox(t.Context(), 3)
	if err != nil {
		t.Fatalf("second ClaimOutbox() = %v", err)
	}
	defer func() { _ = secondTx.Rollback(t.Context()) }()

	if len(firstBatch) != 3 || len(secondBatch) != 3 {
		t.Fatalf("claims returned %d and %d events, want 3 each", len(firstBatch), len(secondBatch))
	}

	seen := make(map[int64]struct{}, 6)
	for _, r := range append(append([]social.OutboxRecord{}, firstBatch...), secondBatch...) {
		if _, duplicate := seen[r.ID]; duplicate {
			t.Fatalf("event %d was claimed by both relays", r.ID)
		}
		seen[r.ID] = struct{}{}
	}
}

// A claim that is rolled back must leave its events available, or a relay
// crash would strand them.
func TestRolledBackClaimReleasesEvents(t *testing.T) {
	store := newStore(t)
	author := makeUser(t, store, "author")
	makePost(t, store, author)

	tx, records, err := store.ClaimOutbox(t.Context(), 10)
	if err != nil {
		t.Fatalf("ClaimOutbox() = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("claimed %d events, want 1", len(records))
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatalf("Rollback() = %v", err)
	}

	againTx, again, err := store.ClaimOutbox(t.Context(), 10)
	if err != nil {
		t.Fatalf("second ClaimOutbox() = %v", err)
	}
	defer func() { _ = againTx.Rollback(t.Context()) }()

	if len(again) != 1 {
		t.Errorf("claimed %d events after a rollback, want the event to be available again", len(again))
	}
}

// A publish that keeps failing must leave a durable trace. Without the
// counter, a permanently unpublishable event is retried forever at full speed
// and looks exactly like an idle relay.
func TestRecordAttemptIncrementsAndReleases(t *testing.T) {
	store := newStore(t)
	author := makeUser(t, store, "author")
	makePost(t, store, author)

	tx, records, err := store.ClaimOutbox(t.Context(), 10)
	if err != nil {
		t.Fatalf("ClaimOutbox() = %v", err)
	}
	if err := store.RecordAttempt(t.Context(), tx, []int64{records[0].ID}); err != nil {
		t.Fatalf("RecordAttempt() = %v", err)
	}

	againTx, again, err := store.ClaimOutbox(t.Context(), 10)
	if err != nil {
		t.Fatalf("second ClaimOutbox() = %v", err)
	}
	defer func() { _ = againTx.Rollback(t.Context()) }()

	if len(again) != 1 {
		t.Fatalf("claimed %d events, want the failed one to still be pending", len(again))
	}
	if again[0].Attempts != 1 {
		t.Errorf("attempts = %d, want 1", again[0].Attempts)
	}
}

// Backlog depth alone cannot distinguish a busy relay from a stuck one. Age
// can.
func TestOldestPendingReportsAge(t *testing.T) {
	store := newStore(t)

	age, err := store.OldestPending(t.Context())
	if err != nil {
		t.Fatalf("OldestPending() = %v", err)
	}
	if age != 0 {
		t.Errorf("age = %v on an empty outbox, want 0", age)
	}

	author := makeUser(t, store, "author")
	makePost(t, store, author)

	age, err = store.OldestPending(t.Context())
	if err != nil {
		t.Fatalf("OldestPending() = %v", err)
	}
	if age < 0 || age > time.Minute {
		t.Errorf("age = %v for an event just written, want something near zero", age)
	}
}

func TestClaimOnAnEmptyOutbox(t *testing.T) {
	store := newStore(t)

	tx, records, err := store.ClaimOutbox(t.Context(), 10)
	if err != nil {
		t.Fatalf("ClaimOutbox() = %v", err)
	}
	if len(records) != 0 {
		t.Errorf("claimed %d events from an empty outbox", len(records))
	}
	if err := store.MarkPublished(t.Context(), tx, nil); err != nil {
		t.Errorf("MarkPublished(nil) = %v, want it to close the transaction cleanly", err)
	}
}
