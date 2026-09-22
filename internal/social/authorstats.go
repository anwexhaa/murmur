package social

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/anwexhaa/murmur/internal/domain"
)

// FollowWithStats creates an edge and adjusts the followee's count atomically.
//
// One transaction, not two statements: a follow that committed without its
// count would drift the routing decision permanently, and the drift would be
// invisible — the graph would be right and the fanout would be wrong. The
// count is only bumped when the edge was actually new, which is what makes
// this safe to retry.
func (s *Store) FollowWithStats(ctx context.Context, follower, followee uuid.UUID) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin follow: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx,
		`INSERT INTO follows (follower_id, followee_id) VALUES ($1, $2)
		 ON CONFLICT (follower_id, followee_id) DO NOTHING`,
		follower, followee)
	if err != nil {
		return false, translateFollowError(err)
	}

	created := tag.RowsAffected() == 1
	if created {
		if err := adjustFollowerCount(ctx, tx, followee, 1); err != nil {
			return false, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit follow: %w", err)
	}
	return created, nil
}

// UnfollowWithStats removes an edge and adjusts the count atomically.
func (s *Store) UnfollowWithStats(ctx context.Context, follower, followee uuid.UUID) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin unfollow: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx,
		`DELETE FROM follows WHERE follower_id = $1 AND followee_id = $2`,
		follower, followee)
	if err != nil {
		return false, fmt.Errorf("delete follow: %w", err)
	}

	removed := tag.RowsAffected() == 1
	if removed {
		if err := adjustFollowerCount(ctx, tx, followee, -1); err != nil {
			return false, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit unfollow: %w", err)
	}
	return removed, nil
}

// adjustFollowerCount moves a count by delta, creating the row if needed.
//
// GREATEST(0, ...) rather than trusting the arithmetic: the CHECK constraint
// would reject a negative count and fail the whole unfollow, turning a
// bookkeeping drift into a user-visible error. Clamping keeps the user's
// action working and leaves the drift for RecomputeAuthorStats to repair.
func adjustFollowerCount(ctx context.Context, tx pgx.Tx, user uuid.UUID, delta int64) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO author_stats (user_id, follower_count, updated_at)
		 VALUES ($1, GREATEST(0, $2), now())
		 ON CONFLICT (user_id) DO UPDATE
		 SET follower_count = GREATEST(0, author_stats.follower_count + $2),
		     updated_at = now()`,
		user, delta)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == codeForeignKeyViolation {
			return fmt.Errorf("%w: user %s does not exist", domain.ErrNotFound, user)
		}
		return fmt.Errorf("adjust follower count: %w", err)
	}
	return nil
}

// GetFollowerCountFast reads the maintained count.
//
// This is what the fanout worker asks per post. It is a primary-key lookup
// rather than an aggregate over the follow edges, which is the difference
// between a microsecond and scanning half a million rows.
func (s *Store) GetFollowerCountFast(ctx context.Context, user uuid.UUID) (int64, error) {
	var count int64
	err := s.pool.QueryRow(ctx,
		`SELECT follower_count FROM author_stats WHERE user_id = $1`, user).Scan(&count)
	if errors.Is(err, pgx.ErrNoRows) {
		// No row means nobody has ever followed them, which is a count of zero
		// rather than a missing user.
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get follower count: %w", err)
	}
	return count, nil
}

// ListHeavyAuthors returns every account at or above the threshold.
//
// The result is small by construction — that is what "heavy" means in a power
// law — so the read path can hold the whole list in memory and refresh it on a
// timer rather than querying per request.
func (s *Store) ListHeavyAuthors(ctx context.Context, threshold int64, limit int) ([]uuid.UUID, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT user_id FROM author_stats
		 WHERE follower_count >= $1
		 ORDER BY follower_count DESC
		 LIMIT $2`, threshold, limit)
	if err != nil {
		return nil, fmt.Errorf("list heavy authors: %w", err)
	}
	defer rows.Close()

	ids := make([]uuid.UUID, 0, 64)
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan heavy author: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// FilterFollowing returns the subset of candidates that the user follows.
//
// This is how the read path finds "which heavy accounts does this viewer
// follow" without ever listing everyone they follow. The candidate list is the
// heavy set — small — and the query rides the follows primary key, so the cost
// is bounded by the number of heavy accounts rather than by how many people the
// viewer follows.
func (s *Store) FilterFollowing(ctx context.Context, follower uuid.UUID, candidates []uuid.UUID) ([]uuid.UUID, error) {
	if len(candidates) == 0 {
		return nil, nil
	}

	rows, err := s.pool.Query(ctx,
		`SELECT followee_id FROM follows
		 WHERE follower_id = $1 AND followee_id = ANY($2::uuid[])`,
		follower, uuidStrings(candidates))
	if err != nil {
		return nil, fmt.Errorf("filter following: %w", err)
	}
	defer rows.Close()

	ids := make([]uuid.UUID, 0, len(candidates))
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan followed author: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// RecomputeAuthorStats rebuilds every count from the follow edges.
//
// Two callers. The seeder, because COPY bypasses the incremental path
// entirely — maintaining counts row by row through a two-million-edge bulk
// load would take longer than the load itself. And operations, because any
// maintained counter can drift, and a counter with no way to check it is a
// counter nobody should trust.
//
// One statement, so there is no window in which the table is half-rebuilt.
func (s *Store) RecomputeAuthorStats(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO author_stats (user_id, follower_count, updated_at)
		SELECT u.id, coalesce(f.n, 0), now()
		FROM users u
		LEFT JOIN (
		    SELECT followee_id, count(*) AS n FROM follows GROUP BY followee_id
		) f ON f.followee_id = u.id
		ON CONFLICT (user_id) DO UPDATE
		SET follower_count = excluded.follower_count,
		    updated_at = excluded.updated_at`)
	if err != nil {
		return 0, fmt.Errorf("recompute author stats: %w", err)
	}
	return tag.RowsAffected(), nil
}

// AuthorStatsDrift reports accounts whose maintained count disagrees with the
// edges. It should always return zero; a non-zero answer is a bug in the
// incremental path, and this is how it would be found.
func (s *Store) AuthorStatsDrift(ctx context.Context) (int64, error) {
	var drifted int64
	err := s.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM author_stats s
		LEFT JOIN (
		    SELECT followee_id, count(*) AS n FROM follows GROUP BY followee_id
		) f ON f.followee_id = s.user_id
		WHERE s.follower_count <> coalesce(f.n, 0)`).Scan(&drifted)
	if err != nil {
		return 0, fmt.Errorf("check author stats drift: %w", err)
	}
	return drifted, nil
}
