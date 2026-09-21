// Package social owns the write path: users, the follow graph and posts.
//
// The store speaks SQL directly through pgx. No ORM, for two reasons that
// matter later: fanout pages through hundreds of thousands of follow edges and
// needs the query plan to stay an index-only scan, and the bulk loader in
// cmd/seed needs COPY, which ORMs generally hide.
package social

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anwexhaa/murmur/internal/domain"
)

// Postgres error codes worth distinguishing. Everything else is a genuine
// failure and travels up as-is.
const (
	codeUniqueViolation     = "23505"
	codeForeignKeyViolation = "23503"
	codeCheckViolation      = "23514"
)

const (
	userColumns = `id, handle, display_name, created_at`
	postColumns = `id, author_id, body, created_at`
)

// Store reads and writes the source of truth.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore returns a store over an existing pool. The pool's lifetime belongs
// to the caller, which is what lets one process share it across components.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// ---------------------------------------------------------------- users

// CreateUser inserts a user, reporting a taken handle as domain.ErrAlreadyExists.
func (s *Store) CreateUser(ctx context.Context, user domain.User) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO users (id, handle, display_name, created_at) VALUES ($1, $2, $3, $4)`,
		user.ID, user.Handle, user.DisplayName, user.CreatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == codeUniqueViolation {
			return fmt.Errorf("%w: handle %q is taken", domain.ErrAlreadyExists, user.Handle)
		}
		return fmt.Errorf("insert user: %w", err)
	}
	return nil
}

// GetUser fetches one user by ID.
func (s *Store) GetUser(ctx context.Context, id uuid.UUID) (domain.User, error) {
	return scanUser(s.pool.QueryRow(ctx,
		`SELECT `+userColumns+` FROM users WHERE id = $1`, id))
}

// GetUserByHandle fetches one user by handle. The column is citext, so the
// comparison is case-insensitive without a function index.
func (s *Store) GetUserByHandle(ctx context.Context, handle string) (domain.User, error) {
	return scanUser(s.pool.QueryRow(ctx,
		`SELECT `+userColumns+` FROM users WHERE handle = $1`, handle))
}

// BatchGetUsers fetches many users in one round trip.
//
// Missing IDs are simply absent from the result rather than an error: this is
// the primitive the gateway's DataLoader sits on in phase 5, and a batch that
// failed because one of fifty IDs had been deleted would be useless to it.
func (s *Store) BatchGetUsers(ctx context.Context, ids []uuid.UUID) ([]domain.User, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := s.pool.Query(ctx,
		`SELECT `+userColumns+` FROM users WHERE id = ANY($1::uuid[])`, uuidStrings(ids))
	if err != nil {
		return nil, fmt.Errorf("query users: %w", err)
	}
	defer rows.Close()

	users := make([]domain.User, 0, len(ids))
	for rows.Next() {
		var user domain.User
		if err := rows.Scan(&user.ID, &user.Handle, &user.DisplayName, &user.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan user: %w", err)
		}
		users = append(users, user)
	}
	return users, rows.Err()
}

// --------------------------------------------------------- follow graph

// Follow creates an edge, reporting whether it was new.
//
// ON CONFLICT DO NOTHING makes this idempotent: a client retrying after a
// timeout gets success, not a duplicate-key error it would have to interpret.
func (s *Store) Follow(ctx context.Context, follower, followee uuid.UUID) (bool, error) {
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO follows (follower_id, followee_id) VALUES ($1, $2)
		 ON CONFLICT (follower_id, followee_id) DO NOTHING`,
		follower, followee)
	if err != nil {
		return false, translateFollowError(err)
	}
	return tag.RowsAffected() == 1, nil
}

// Unfollow removes an edge, reporting whether one was there.
func (s *Store) Unfollow(ctx context.Context, follower, followee uuid.UUID) (bool, error) {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM follows WHERE follower_id = $1 AND followee_id = $2`,
		follower, followee)
	if err != nil {
		return false, fmt.Errorf("delete follow: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// ListFollowers pages through the users following the given account.
//
// Keyset pagination on follower_id, which is the second column of
// follows_by_followee — so this is an index-only scan that resumes exactly
// where the last page stopped. OFFSET would re-walk every row it skips, and
// fanout for a large account walks a great many rows.
func (s *Store) ListFollowers(ctx context.Context, followee uuid.UUID, after string, limit int) ([]uuid.UUID, error) {
	cursor, err := parseCursor(after)
	if err != nil {
		return nil, err
	}
	return s.listEdge(ctx,
		`SELECT follower_id FROM follows
		 WHERE followee_id = $1 AND ($2::uuid IS NULL OR follower_id > $2::uuid)
		 ORDER BY follower_id
		 LIMIT $3`,
		followee, cursor, limit)
}

// ListFollowing pages through the accounts a user follows. This one rides the
// primary key, which is already (follower_id, followee_id).
func (s *Store) ListFollowing(ctx context.Context, follower uuid.UUID, after string, limit int) ([]uuid.UUID, error) {
	cursor, err := parseCursor(after)
	if err != nil {
		return nil, err
	}
	return s.listEdge(ctx,
		`SELECT followee_id FROM follows
		 WHERE follower_id = $1 AND ($2::uuid IS NULL OR followee_id > $2::uuid)
		 ORDER BY followee_id
		 LIMIT $3`,
		follower, cursor, limit)
}

// CountFollowers is used by the seeder and, from phase 4, by the push/pull
// routing decision.
func (s *Store) CountFollowers(ctx context.Context, followee uuid.UUID) (int64, error) {
	var count int64
	err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM follows WHERE followee_id = $1`, followee).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count followers: %w", err)
	}
	return count, nil
}

// BatchCountFollowers counts followers for many accounts in one round trip.
//
// Accounts with no followers do not appear in the grouped result, so they are
// filled in as zero here. A caller asking about fifty accounts should get
// fifty answers, not "some of them".
func (s *Store) BatchCountFollowers(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]int64, error) {
	counts := make(map[uuid.UUID]int64, len(ids))
	for _, id := range ids {
		counts[id] = 0
	}
	if len(ids) == 0 {
		return counts, nil
	}

	rows, err := s.pool.Query(ctx,
		`SELECT followee_id, count(*) FROM follows
		 WHERE followee_id = ANY($1::uuid[])
		 GROUP BY followee_id`, uuidStrings(ids))
	if err != nil {
		return nil, fmt.Errorf("batch count followers: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id    uuid.UUID
			count int64
		)
		if err := rows.Scan(&id, &count); err != nil {
			return nil, fmt.Errorf("scan follower count: %w", err)
		}
		counts[id] = count
	}
	return counts, rows.Err()
}

// IsFollowing reports whether one account follows another. EXISTS rather than
// a count: the planner can stop at the first matching row.
func (s *Store) IsFollowing(ctx context.Context, follower, followee uuid.UUID) (bool, error) {
	var following bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM follows WHERE follower_id = $1 AND followee_id = $2)`,
		follower, followee).Scan(&following)
	if err != nil {
		return false, fmt.Errorf("is following: %w", err)
	}
	return following, nil
}

func (s *Store) listEdge(ctx context.Context, query string, owner uuid.UUID, cursor any, limit int) ([]uuid.UUID, error) {
	rows, err := s.pool.Query(ctx, query, owner, cursor, limit)
	if err != nil {
		return nil, fmt.Errorf("query follow edges: %w", err)
	}
	defer rows.Close()

	ids := make([]uuid.UUID, 0, limit)
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan follow edge: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ---------------------------------------------------------------- posts

// CreatePost inserts a post. A missing author is reported as ErrNotFound
// rather than a foreign-key error, so the transport layer has one thing to map.
func (s *Store) CreatePost(ctx context.Context, post domain.Post) error {
	_, err := s.pool.Exec(ctx,
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
	return nil
}

// GetPost fetches one post. Soft-deleted posts read as missing.
func (s *Store) GetPost(ctx context.Context, id string) (domain.Post, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+postColumns+` FROM posts WHERE id = $1 AND deleted_at IS NULL`, id)

	var post domain.Post
	if err := row.Scan(&post.ID, &post.AuthorID, &post.Body, &post.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Post{}, domain.ErrNotFound
		}
		return domain.Post{}, fmt.Errorf("scan post: %w", err)
	}
	return post, nil
}

// BatchGetPosts fetches many posts in one round trip. This is the call the
// timeline read path makes once per page, after the ID list comes back from
// Redis — the single most latency-sensitive query in the system.
func (s *Store) BatchGetPosts(ctx context.Context, ids []string) ([]domain.Post, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := s.pool.Query(ctx,
		`SELECT `+postColumns+` FROM posts WHERE id = ANY($1) AND deleted_at IS NULL`, ids)
	if err != nil {
		return nil, fmt.Errorf("query posts: %w", err)
	}
	defer rows.Close()

	return collectPosts(rows, len(ids))
}

// ListAuthorPosts pages one author's posts, newest first. The cursor is the
// post ID: ULIDs already sort by creation time, so no second column is needed
// and no tie-break is possible.
func (s *Store) ListAuthorPosts(ctx context.Context, author uuid.UUID, after string, limit int) ([]domain.Post, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+postColumns+` FROM posts
		 WHERE author_id = $1 AND deleted_at IS NULL AND ($2 = '' OR id < $2)
		 ORDER BY id DESC
		 LIMIT $3`,
		author, after, limit)
	if err != nil {
		return nil, fmt.Errorf("query author posts: %w", err)
	}
	defer rows.Close()

	return collectPosts(rows, limit)
}

// ------------------------------------------------------------ bulk load

// FollowEdge is one row for the bulk loader.
type FollowEdge struct {
	FollowerID uuid.UUID
	FolloweeID uuid.UUID
}

// CopyUsers bulk-loads users with COPY. Inserting the seed graph one row at a
// time would take hours; COPY takes seconds.
func (s *Store) CopyUsers(ctx context.Context, users []domain.User) (int64, error) {
	n, err := s.pool.CopyFrom(ctx,
		pgx.Identifier{"users"},
		[]string{"id", "handle", "display_name", "created_at"},
		pgx.CopyFromSlice(len(users), func(i int) ([]any, error) {
			u := users[i]
			return []any{u.ID, u.Handle, u.DisplayName, u.CreatedAt}, nil
		}))
	if err != nil {
		return n, fmt.Errorf("copy users: %w", err)
	}
	return n, nil
}

// CopyFollows bulk-loads follow edges with COPY.
func (s *Store) CopyFollows(ctx context.Context, edges []FollowEdge) (int64, error) {
	n, err := s.pool.CopyFrom(ctx,
		pgx.Identifier{"follows"},
		[]string{"follower_id", "followee_id"},
		pgx.CopyFromSlice(len(edges), func(i int) ([]any, error) {
			return []any{edges[i].FollowerID, edges[i].FolloweeID}, nil
		}))
	if err != nil {
		return n, fmt.Errorf("copy follows: %w", err)
	}
	return n, nil
}

// CopyPosts bulk-loads posts with COPY.
func (s *Store) CopyPosts(ctx context.Context, posts []domain.Post) (int64, error) {
	n, err := s.pool.CopyFrom(ctx,
		pgx.Identifier{"posts"},
		[]string{"id", "author_id", "body", "created_at"},
		pgx.CopyFromSlice(len(posts), func(i int) ([]any, error) {
			p := posts[i]
			return []any{p.ID, p.AuthorID, p.Body, p.CreatedAt}, nil
		}))
	if err != nil {
		return n, fmt.Errorf("copy posts: %w", err)
	}
	return n, nil
}

// --------------------------------------------------------------- shared

func scanUser(row pgx.Row) (domain.User, error) {
	var user domain.User
	if err := row.Scan(&user.ID, &user.Handle, &user.DisplayName, &user.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.User{}, domain.ErrNotFound
		}
		return domain.User{}, fmt.Errorf("scan user: %w", err)
	}
	return user, nil
}

func collectPosts(rows pgx.Rows, capacity int) ([]domain.Post, error) {
	posts := make([]domain.Post, 0, capacity)
	for rows.Next() {
		var post domain.Post
		if err := rows.Scan(&post.ID, &post.AuthorID, &post.Body, &post.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan post: %w", err)
		}
		posts = append(posts, post)
	}
	return posts, rows.Err()
}

// parseCursor turns an opaque page token into a NULL-able uuid argument.
// An empty token means "start at the beginning".
func parseCursor(after string) (any, error) {
	if after == "" {
		return nil, nil
	}
	id, err := uuid.Parse(after)
	if err != nil {
		return nil, fmt.Errorf("%w: page token is malformed", domain.ErrInvalid)
	}
	return id, nil
}

func uuidStrings(ids []uuid.UUID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	return out
}

func translateFollowError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case codeForeignKeyViolation:
			return fmt.Errorf("%w: follower or followee does not exist", domain.ErrNotFound)
		case codeCheckViolation:
			return fmt.Errorf("%w: a user cannot follow themselves", domain.ErrInvalid)
		}
	}
	return fmt.Errorf("insert follow: %w", err)
}

// TruncateAll empties every table. It exists for the seeder's -reset flag and
// for integration tests, which need a clean slate between cases without
// paying to recreate the schema. RESTART IDENTITY CASCADE takes the dependent
// tables with it, so the order of this list does not matter.
//
// outbox is listed explicitly because nothing references it: it has no foreign
// key to posts, deliberately, so that an event outlives the row it describes
// and a delete cannot strand the fanout. CASCADE therefore does not reach it,
// and leaving it out here leaks events between tests.
func (s *Store) TruncateAll(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `TRUNCATE users, follows, posts, outbox RESTART IDENTITY CASCADE`)
	if err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	return nil
}

// Distribution summarises how followers are spread across accounts.
type Distribution struct {
	Accounts int64
	Max      int64
	P50      int64
	P90      int64
	P99      int64
}

// FollowerDistribution reports the shape of the follow graph.
//
// It exists for the seeder and, from phase 4, for choosing the fanout
// threshold: the crossover point is only meaningful relative to where the
// real accounts sit, so the benchmark needs to know the percentiles rather
// than guess at them.
func (s *Store) FollowerDistribution(ctx context.Context) (Distribution, error) {
	var d Distribution
	err := s.pool.QueryRow(ctx, `
		WITH counts AS (
		    SELECT followee_id, count(*) AS followers
		    FROM follows
		    GROUP BY followee_id
		)
		SELECT
		    count(*),
		    coalesce(max(followers), 0),
		    coalesce(percentile_disc(0.50) WITHIN GROUP (ORDER BY followers), 0),
		    coalesce(percentile_disc(0.90) WITHIN GROUP (ORDER BY followers), 0),
		    coalesce(percentile_disc(0.99) WITHIN GROUP (ORDER BY followers), 0)
		FROM counts`).Scan(&d.Accounts, &d.Max, &d.P50, &d.P90, &d.P99)
	if err != nil {
		return Distribution{}, fmt.Errorf("follower distribution: %w", err)
	}
	return d, nil
}
