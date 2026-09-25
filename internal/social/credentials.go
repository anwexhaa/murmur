package social

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/anwexhaa/murmur/internal/domain"
)

// ErrTokenReused means a refresh token was presented after it had already been
// spent, and the family has been revoked as a result.
//
// Distinct from "not found" because the two mean opposite things. A token that
// was never issued is noise -- a typo, an old bookmark, a scanner. A token that
// was issued, used once, and is now being used again means two parties hold a
// copy of a secret that was meant to have one holder, and exactly one of them
// is the legitimate client.
var ErrTokenReused = errors.New("refresh token reused")

// RefreshRecord is one row of the rotation chain.
type RefreshRecord struct {
	ID        uuid.UUID
	UserID    uuid.UUID
	FamilyID  uuid.UUID
	ExpiresAt time.Time
}

// ---------------------------------------------------------------- credentials

// CreateUserWithCredential inserts a user and its password hash atomically.
//
// One transaction, because the alternative has a failure mode with no good
// recovery: a user row with no credential is an account nobody -- including
// its owner -- can ever sign into, and it holds the handle forever.
func (s *Store) CreateUserWithCredential(ctx context.Context, user domain.User, passwordHash string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, err = tx.Exec(ctx,
		`INSERT INTO users (id, handle, display_name, created_at) VALUES ($1, $2, $3, $4)`,
		user.ID, user.Handle, user.DisplayName, user.CreatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			case codeUniqueViolation:
				return fmt.Errorf("%w: handle %q is taken", domain.ErrAlreadyExists, user.Handle)
			case codeCheckViolation:
				return fmt.Errorf("%w: handle %q is not a valid shape", domain.ErrInvalid, user.Handle)
			}
		}
		return fmt.Errorf("insert user: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO user_credentials (user_id, password_hash, updated_at) VALUES ($1, $2, $3)`,
		user.ID, passwordHash, user.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert credential: %w", err)
	}

	return tx.Commit(ctx)
}

// CredentialByHandle returns the stored hash and the user it belongs to.
func (s *Store) CredentialByHandle(ctx context.Context, handle string) (uuid.UUID, string, string, error) {
	var (
		userID uuid.UUID
		stored string
		actual string
	)
	err := s.pool.QueryRow(ctx,
		`SELECT u.id, u.handle, c.password_hash
		   FROM users u JOIN user_credentials c ON c.user_id = u.id
		  WHERE u.handle = $1`, handle).Scan(&userID, &actual, &stored)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, "", "", fmt.Errorf("%w: no such account", domain.ErrNotFound)
	}
	if err != nil {
		return uuid.Nil, "", "", fmt.Errorf("read credential: %w", err)
	}
	return userID, actual, stored, nil
}

// CredentialByUser returns one user's stored hash.
func (s *Store) CredentialByUser(ctx context.Context, userID uuid.UUID) (string, error) {
	var stored string
	err := s.pool.QueryRow(ctx,
		`SELECT password_hash FROM user_credentials WHERE user_id = $1`, userID).Scan(&stored)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("%w: no such account", domain.ErrNotFound)
	}
	if err != nil {
		return "", fmt.Errorf("read credential: %w", err)
	}
	return stored, nil
}

// UpdateCredential replaces a stored hash.
//
// Used both by a deliberate password change and by the login path's silent
// upgrade when the cost parameters have been raised since the hash was made.
func (s *Store) UpdateCredential(ctx context.Context, userID uuid.UUID, passwordHash string, now time.Time) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE user_credentials SET password_hash = $2, updated_at = $3 WHERE user_id = $1`,
		userID, passwordHash, now)
	if err != nil {
		return fmt.Errorf("update credential: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: no such account", domain.ErrNotFound)
	}
	return nil
}

// ---------------------------------------------------------------- sessions

// IssueRefreshToken starts a new family.
func (s *Store) IssueRefreshToken(
	ctx context.Context,
	userID uuid.UUID,
	tokenHash string,
	now time.Time,
	ttl time.Duration,
) (RefreshRecord, error) {
	record := RefreshRecord{
		ID:        uuid.New(),
		UserID:    userID,
		FamilyID:  uuid.New(),
		ExpiresAt: now.Add(ttl),
	}

	_, err := s.pool.Exec(ctx,
		`INSERT INTO refresh_tokens (id, user_id, token_hash, family_id, created_at, expires_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		record.ID, record.UserID, tokenHash, record.FamilyID, now, record.ExpiresAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == codeForeignKeyViolation {
			return RefreshRecord{}, fmt.Errorf("%w: no such account", domain.ErrNotFound)
		}
		return RefreshRecord{}, fmt.Errorf("insert refresh token: %w", err)
	}
	return record, nil
}

// RotateRefreshToken spends one token and issues its successor, in one
// transaction.
//
// The transaction is the whole design, and it is worth being precise about
// why. Two concurrent refreshes with the same token must not both succeed:
// that would produce two live successors in one family, and the next rotation
// of either would look like reuse of the other. The row is therefore locked
// with FOR UPDATE, so the second caller waits, sees used_at already set, and
// takes the reuse path -- which is the correct answer, because a token really
// was presented twice.
//
// On reuse the whole family is revoked and ErrTokenReused is returned. The
// legitimate client is logged out too. That is the intended outcome rather
// than collateral damage: from the server's position the two presentations are
// indistinguishable, and the safe reading of "somebody has a copy of this
// secret" is that the session is compromised.
func (s *Store) RotateRefreshToken(
	ctx context.Context,
	presentedHash, successorHash string,
	now time.Time,
	ttl time.Duration,
) (RefreshRecord, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return RefreshRecord{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		id        uuid.UUID
		userID    uuid.UUID
		familyID  uuid.UUID
		expiresAt time.Time
		usedAt    *time.Time
		revokedAt *time.Time
	)
	err = tx.QueryRow(ctx,
		`SELECT id, user_id, family_id, expires_at, used_at, revoked_at
		   FROM refresh_tokens WHERE token_hash = $1 FOR UPDATE`,
		presentedHash).Scan(&id, &userID, &familyID, &expiresAt, &usedAt, &revokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		// Never issued, or issued so long ago it has been swept. Nothing to
		// revoke and nothing to learn: this is noise, not an attack signal.
		return RefreshRecord{}, fmt.Errorf("%w: unknown refresh token", domain.ErrNotFound)
	}
	if err != nil {
		return RefreshRecord{}, fmt.Errorf("read refresh token: %w", err)
	}

	if usedAt != nil {
		// The detection. Revoke every token in the family, including the
		// successor the legitimate client is currently holding.
		if _, err := tx.Exec(ctx,
			`UPDATE refresh_tokens SET revoked_at = $2
			  WHERE family_id = $1 AND revoked_at IS NULL`,
			familyID, now); err != nil {
			return RefreshRecord{}, fmt.Errorf("revoke family: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return RefreshRecord{}, fmt.Errorf("commit revocation: %w", err)
		}
		return RefreshRecord{}, fmt.Errorf("%w: family %s revoked", ErrTokenReused, familyID)
	}

	if revokedAt != nil {
		return RefreshRecord{}, fmt.Errorf("%w: session revoked", domain.ErrForbidden)
	}
	if !now.Before(expiresAt) {
		return RefreshRecord{}, fmt.Errorf("%w: refresh token expired", domain.ErrForbidden)
	}

	if _, err := tx.Exec(ctx,
		`UPDATE refresh_tokens SET used_at = $2 WHERE id = $1`, id, now); err != nil {
		return RefreshRecord{}, fmt.Errorf("mark spent: %w", err)
	}

	// The successor inherits the family, which is what makes the chain
	// traceable back to the login that started it.
	successor := RefreshRecord{
		ID:        uuid.New(),
		UserID:    userID,
		FamilyID:  familyID,
		ExpiresAt: now.Add(ttl),
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO refresh_tokens (id, user_id, token_hash, family_id, created_at, expires_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		successor.ID, successor.UserID, successorHash, successor.FamilyID, now, successor.ExpiresAt,
	); err != nil {
		return RefreshRecord{}, fmt.Errorf("insert successor: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return RefreshRecord{}, fmt.Errorf("commit rotation: %w", err)
	}
	return successor, nil
}

// RevokeFamilyByToken ends the session a token belongs to. This is logout.
func (s *Store) RevokeFamilyByToken(ctx context.Context, tokenHash string, now time.Time) (int, error) {
	var familyID uuid.UUID
	err := s.pool.QueryRow(ctx,
		`SELECT family_id FROM refresh_tokens WHERE token_hash = $1`, tokenHash).Scan(&familyID)
	if errors.Is(err, pgx.ErrNoRows) {
		// Logging out with a token nobody recognises is not an error. The
		// caller wanted to be logged out and they are.
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read refresh token: %w", err)
	}
	return s.RevokeFamily(ctx, familyID, now)
}

// RevokeFamily ends one rotation chain.
func (s *Store) RevokeFamily(ctx context.Context, familyID uuid.UUID, now time.Time) (int, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE refresh_tokens SET revoked_at = $2 WHERE family_id = $1 AND revoked_at IS NULL`,
		familyID, now)
	if err != nil {
		return 0, fmt.Errorf("revoke family: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// RevokeAllSessions ends every family a user has. This is what a password
// change does, and what "log out everywhere" does.
func (s *Store) RevokeAllSessions(ctx context.Context, userID uuid.UUID, now time.Time) (int, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE refresh_tokens SET revoked_at = $2 WHERE user_id = $1 AND revoked_at IS NULL`,
		userID, now)
	if err != nil {
		return 0, fmt.Errorf("revoke sessions: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// DeleteExpiredTokens sweeps rows that can no longer be presented.
//
// Kept well past expiry rather than deleted on use, because a deleted row and
// a never-issued one are indistinguishable on replay, and only one of them
// means a breach. The sweep runs on a grace period so the reuse window
// outlives the token itself.
func (s *Store) DeleteExpiredTokens(ctx context.Context, before time.Time) (int, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM refresh_tokens WHERE expires_at < $1`, before)
	if err != nil {
		return 0, fmt.Errorf("sweep refresh tokens: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// CountActiveSessions reports how many live tokens a user has, for tests and
// for an account page.
func (s *Store) CountActiveSessions(ctx context.Context, userID uuid.UUID, now time.Time) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM refresh_tokens
		  WHERE user_id = $1 AND revoked_at IS NULL AND used_at IS NULL AND expires_at > $2`,
		userID, now).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count sessions: %w", err)
	}
	return count, nil
}
