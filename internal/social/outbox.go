package social

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/anwexhaa/murmur/internal/domain"
)

// EventType names an event in the outbox.
type EventType string

// PostCreatedType is the only event Murmur publishes so far.
const PostCreatedType EventType = "post.created"

// OutboxRecord is one undelivered event.
type OutboxRecord struct {
	ID          int64
	AggregateID string
	Type        EventType
	Payload     []byte
	Attempts    int
}

// CreatePostWithEvent writes the post and its event in one transaction.
//
// This is the whole point of the outbox. Either both rows exist or neither
// does, so there is no state in which a post is visible but no timeline will
// ever be told about it. The relay picks the event up afterwards; if the
// process dies between the commit and the publish, the row is still there and
// the next relay pass finds it.
func (s *Store) CreatePostWithEvent(ctx context.Context, post domain.Post, payload []byte) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	// Rollback after a successful commit is a no-op, so this is safe
	// unconditionally and covers every early return below.
	defer func() { _ = tx.Rollback(ctx) }()

	_, err = tx.Exec(ctx,
		`INSERT INTO posts (id, author_id, body, created_at) VALUES ($1, $2, $3, $4)`,
		post.ID, post.AuthorID, post.Body, post.CreatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			case codeForeignKeyViolation:
				return fmt.Errorf("%w: author %s does not exist", domain.ErrNotFound, post.AuthorID)
			case codeUniqueViolation:
				return fmt.Errorf("%w: post %s already exists", domain.ErrAlreadyExists, post.ID)
			}
		}
		return fmt.Errorf("insert post: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO outbox (aggregate_id, type, payload) VALUES ($1, $2, $3)`,
		post.ID, string(PostCreatedType), payload)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// ClaimOutbox locks and returns up to limit undelivered events.
//
// FOR UPDATE SKIP LOCKED is what lets several relays run at once: each takes
// rows the others have not locked instead of queueing behind them. Without it,
// a second relay instance would be pure contention, and running one instance
// would make the relay a single point of failure for every write in the system.
//
// The returned transaction stays open and holds the locks. The caller must
// finish it — MarkPublished commits, and anything else should roll back so the
// rows are immediately available to another pass.
func (s *Store) ClaimOutbox(ctx context.Context, limit int) (pgx.Tx, []OutboxRecord, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("begin outbox claim: %w", err)
	}

	rows, err := tx.Query(ctx,
		`SELECT id, aggregate_id, type, payload, attempts
		 FROM outbox
		 WHERE published_at IS NULL
		 ORDER BY id
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, nil, fmt.Errorf("claim outbox: %w", err)
	}

	var records []OutboxRecord
	for rows.Next() {
		var r OutboxRecord
		var eventType string
		if err := rows.Scan(&r.ID, &r.AggregateID, &eventType, &r.Payload, &r.Attempts); err != nil {
			rows.Close()
			_ = tx.Rollback(ctx)
			return nil, nil, fmt.Errorf("scan outbox row: %w", err)
		}
		r.Type = EventType(eventType)
		records = append(records, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		_ = tx.Rollback(ctx)
		return nil, nil, fmt.Errorf("read outbox rows: %w", err)
	}

	return tx, records, nil
}

// MarkPublished marks the given events delivered and commits the claim.
func (s *Store) MarkPublished(ctx context.Context, tx pgx.Tx, ids []int64) error {
	if len(ids) == 0 {
		return tx.Commit(ctx)
	}

	_, err := tx.Exec(ctx,
		`UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids)
	if err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("mark outbox published: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit outbox claim: %w", err)
	}
	return nil
}

// RecordAttempt counts a failed publish and releases the claim.
//
// The attempt counter is the only durable record that an event is struggling.
// Without it a permanently unpublishable row is retried forever at full speed
// and looks, from the outside, exactly like an idle relay.
func (s *Store) RecordAttempt(ctx context.Context, tx pgx.Tx, ids []int64) error {
	if len(ids) == 0 {
		_ = tx.Rollback(ctx)
		return nil
	}

	_, err := tx.Exec(ctx,
		`UPDATE outbox SET attempts = attempts + 1 WHERE id = ANY($1)`, ids)
	if err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("record outbox attempt: %w", err)
	}
	return tx.Commit(ctx)
}

// PendingOutbox reports how many events are waiting. It backs the relay's
// backlog gauge, which is the number that says whether the relay is keeping up.
func (s *Store) PendingOutbox(ctx context.Context) (int64, error) {
	var pending int64
	err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&pending)
	if err != nil {
		return 0, fmt.Errorf("count pending outbox: %w", err)
	}
	return pending, nil
}

// OldestPending returns the age of the oldest undelivered event.
//
// Depth alone cannot tell a busy relay from a stuck one: a thousand events
// that arrived this second are healthy, and one event from ten minutes ago is
// not. Age distinguishes them.
func (s *Store) OldestPending(ctx context.Context) (time.Duration, error) {
	var seconds *float64
	err := s.pool.QueryRow(ctx,
		`SELECT extract(epoch FROM now() - min(created_at)) FROM outbox WHERE published_at IS NULL`).
		Scan(&seconds)
	if err != nil {
		return 0, fmt.Errorf("oldest pending outbox: %w", err)
	}
	if seconds == nil {
		return 0, nil
	}
	return time.Duration(*seconds * float64(time.Second)), nil
}

// CopyOutbox bulk-loads outbox events with COPY.
//
// The seeder uses this so seeded posts behave exactly like posts written
// through the API: they get an event, the relay publishes it, and the fanout
// worker materialises the timelines. A seeded dataset whose posts never fanned
// out would look right in Postgres and be invisible in every feed.
func (s *Store) CopyOutbox(ctx context.Context, records []OutboxRecord) (int64, error) {
	n, err := s.pool.CopyFrom(ctx,
		pgx.Identifier{"outbox"},
		[]string{"aggregate_id", "type", "payload"},
		pgx.CopyFromSlice(len(records), func(i int) ([]any, error) {
			r := records[i]
			return []any{r.AggregateID, string(r.Type), r.Payload}, nil
		}))
	if err != nil {
		return n, fmt.Errorf("copy outbox: %w", err)
	}
	return n, nil
}
