// Package domain holds Murmur's entities and the rules that govern them.
//
// Nothing here imports infrastructure — no pgx, no Redis, no protobuf. That
// constraint is the point: the rules about what a valid handle is, or how long
// a post may be, stay unit-testable without a container, and stay in one place
// rather than being reimplemented at each edge.
package domain

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

// Sentinel errors. Callers match on these with errors.Is; the transport layer
// is what turns them into gRPC status codes.
var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrInvalid       = errors.New("invalid argument")
)

// User is an account. Handles are case-insensitive: the column is citext, so
// "Anwesha" and "anwesha" are the same handle and only one can exist.
type User struct {
	ID          uuid.UUID
	Handle      string
	DisplayName string
	CreatedAt   time.Time
}

// Post is one piece of content. ID is a ULID rather than a UUID, which makes
// it sort by creation time as a plain string — the Redis timeline score, the
// pagination cursor and the merge comparison all fall out of that one choice.
type Post struct {
	ID        string
	AuthorID  uuid.UUID
	Body      string
	CreatedAt time.Time
}

// Limits, mirrored by CHECK constraints in the schema. Both exist on purpose:
// the constraint is the guarantee, this is the error message.
const (
	HandleMinLen      = 2
	HandleMaxLen      = 30
	DisplayNameMaxLen = 80
	BodyMaxLen        = 2000
)

var handlePattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// ValidateHandle checks a handle against the same shape the database enforces.
func ValidateHandle(handle string) error {
	switch {
	case handle == "":
		return fmt.Errorf("%w: handle is required", ErrInvalid)
	case utf8.RuneCountInString(handle) < HandleMinLen:
		return fmt.Errorf("%w: handle must be at least %d characters", ErrInvalid, HandleMinLen)
	case utf8.RuneCountInString(handle) > HandleMaxLen:
		return fmt.Errorf("%w: handle must be at most %d characters", ErrInvalid, HandleMaxLen)
	case !handlePattern.MatchString(handle):
		return fmt.Errorf("%w: handle may contain only letters, digits and underscores", ErrInvalid)
	}
	return nil
}

// ValidateDisplayName checks a display name. Unlike a handle it may hold any
// printable text, so the only rules are that it is present and bounded.
func ValidateDisplayName(name string) error {
	trimmed := strings.TrimSpace(name)
	switch {
	case trimmed == "":
		return fmt.Errorf("%w: display name is required", ErrInvalid)
	case utf8.RuneCountInString(trimmed) > DisplayNameMaxLen:
		return fmt.Errorf("%w: display name must be at most %d characters", ErrInvalid, DisplayNameMaxLen)
	}
	return nil
}

// ValidateBody checks post content.
//
// The bound is measured in runes, not bytes, because the database CHECK uses
// char_length. Counting bytes here would reject a 2,000-character post written
// in any script that needs more than one byte per character.
func ValidateBody(body string) error {
	trimmed := strings.TrimSpace(body)
	switch {
	case trimmed == "":
		return fmt.Errorf("%w: post body is required", ErrInvalid)
	case utf8.RuneCountInString(trimmed) > BodyMaxLen:
		return fmt.Errorf("%w: post body must be at most %d characters", ErrInvalid, BodyMaxLen)
	}
	return nil
}

// ParseUserID converts a wire-format identifier, reporting a domain error
// rather than uuid's own so callers have one error kind to match on.
func ParseUserID(field, raw string) (uuid.UUID, error) {
	if raw == "" {
		return uuid.Nil, fmt.Errorf("%w: %s is required", ErrInvalid, field)
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: %s is not a valid id", ErrInvalid, field)
	}
	return id, nil
}

// NewUser builds a validated user. Callers cannot construct an invalid one
// through this path, which is the only path the service uses.
func NewUser(handle, displayName string, now time.Time) (User, error) {
	if err := ValidateHandle(handle); err != nil {
		return User{}, err
	}
	if err := ValidateDisplayName(displayName); err != nil {
		return User{}, err
	}
	return User{
		ID:          uuid.New(),
		Handle:      handle,
		DisplayName: strings.TrimSpace(displayName),
		CreatedAt:   now.UTC(),
	}, nil
}
