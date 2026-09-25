// Package auth holds Murmur's credential handling: password hashing, the
// tokens the gateway issues to clients, and the assertions it signs for the
// backend services.
//
// Nothing here imports infrastructure. Hashing a password and verifying a
// signature are pure functions of their inputs, and keeping them that way
// means they can be tested exhaustively without a container, which is what
// you want for the code where a mistake is a breach rather than a bug.
package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// ErrMismatch means the password does not match the hash.
//
// Deliberately one error for every failure mode a caller is allowed to
// distinguish. "No such user", "wrong password" and "malformed stored hash"
// must look identical from outside, or the difference becomes an oracle for
// enumerating accounts.
var ErrMismatch = errors.New("auth: password does not match")

// ErrInvalidHash means the stored string is not a hash this package wrote.
// It is for logs and tests; callers answering a login request return
// ErrMismatch regardless.
var ErrInvalidHash = errors.New("auth: malformed password hash")

// HashParams are the argon2id cost parameters.
//
// They are stored alongside every hash rather than compiled in, which is what
// makes them changeable: raising the cost does not invalidate existing hashes,
// because each one still carries the settings it was made with. NeedsRehash
// then lets the login path upgrade them one user at a time, at the only moment
// the plaintext is available.
type HashParams struct {
	// Memory in KiB. The dominant cost, and the one that actually resists
	// GPU attack -- iterations can be parallelised away, memory cannot.
	Memory uint32
	// Time is the number of passes over that memory.
	Time uint32
	// Parallelism is the number of lanes.
	Parallelism uint8
	SaltLength  uint32
	KeyLength   uint32
}

// DefaultParams follows the OWASP argon2id recommendation: 19 MiB, two
// passes, one lane.
//
// Chosen over RFC 9106's 64 MiB profile because the memory is per concurrent
// login, not per process: at 64 MiB, a hundred simultaneous logins would ask
// for 6.4 GB and the login path would become the easiest way to take the
// service down. The rate limiter in front of it is part of this calculation,
// not a separate feature.
var DefaultParams = HashParams{
	Memory:      19 * 1024,
	Time:        2,
	Parallelism: 1,
	SaltLength:  16,
	KeyLength:   32,
}

// Hasher turns passwords into storable hashes and checks them again.
type Hasher struct {
	params HashParams
}

// NewHasher builds a hasher. Zero-valued parameters fall back to the defaults
// field by field, so a caller can raise the memory cost without restating the
// rest.
func NewHasher(params HashParams) *Hasher {
	if params.Memory == 0 {
		params.Memory = DefaultParams.Memory
	}
	if params.Time == 0 {
		params.Time = DefaultParams.Time
	}
	if params.Parallelism == 0 {
		params.Parallelism = DefaultParams.Parallelism
	}
	if params.SaltLength == 0 {
		params.SaltLength = DefaultParams.SaltLength
	}
	if params.KeyLength == 0 {
		params.KeyLength = DefaultParams.KeyLength
	}
	return &Hasher{params: params}
}

// Hash returns a PHC-encoded argon2id hash of the password.
//
// The salt is fresh per call and is stored in the output, so two users with
// the same password get different hashes. That is what stops one cracked hash
// from being a lookup table for every account that shares the password.
func (h *Hasher) Hash(password string) (string, error) {
	salt := make([]byte, h.params.SaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("auth: read salt: %w", err)
	}

	key := argon2.IDKey(
		[]byte(password), salt,
		h.params.Time, h.params.Memory, h.params.Parallelism, h.params.KeyLength,
	)

	return encode(h.params, salt, key), nil
}

// Verify checks a password against a stored hash.
//
// The parameters come from the stored string, not from this hasher, which is
// the only way a hash made under older settings stays verifiable after the
// settings change.
func Verify(password, encoded string) error {
	params, salt, want, err := decode(encoded)
	if err != nil {
		return err
	}

	got := argon2.IDKey(
		[]byte(password), salt,
		params.Time, params.Memory, params.Parallelism, params.KeyLength,
	)

	// Constant time, so the comparison cannot be turned into a byte-at-a-time
	// oracle by measuring how long a wrong answer takes to be rejected.
	if subtle.ConstantTimeCompare(got, want) != 1 {
		return ErrMismatch
	}
	return nil
}

// NeedsRehash reports whether a stored hash was made with weaker settings than
// this hasher uses now.
//
// Login is the only moment the plaintext exists, so it is the only moment an
// upgrade is possible. A deployment that raises its cost parameters therefore
// migrates its users as they sign in, rather than all at once or never.
func (h *Hasher) NeedsRehash(encoded string) bool {
	params, _, _, err := decode(encoded)
	if err != nil {
		// Unreadable is a reason to replace it, not to keep it.
		return true
	}
	return params.Memory < h.params.Memory ||
		params.Time < h.params.Time ||
		params.KeyLength < h.params.KeyLength
}

// Params reports the parameters this hasher writes with.
func (h *Hasher) Params() HashParams { return h.params }

// encode writes the PHC string format, which is what every other argon2
// implementation reads:
//
//	$argon2id$v=19$m=19456,t=2,p=1$<salt>$<hash>
func encode(params HashParams, salt, key []byte) string {
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version,
		params.Memory, params.Time, params.Parallelism,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(key),
	)
}

func decode(encoded string) (HashParams, []byte, []byte, error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[0] != "" {
		return HashParams{}, nil, nil, ErrInvalidHash
	}
	if parts[1] != "argon2id" {
		// argon2i and argon2d are real algorithms and this is not a general
		// PHC reader. Refusing them is better than quietly verifying against
		// a variant with different resistance properties.
		return HashParams{}, nil, nil, ErrInvalidHash
	}

	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return HashParams{}, nil, nil, ErrInvalidHash
	}
	if version != argon2.Version {
		return HashParams{}, nil, nil, ErrInvalidHash
	}

	var params HashParams
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d",
		&params.Memory, &params.Time, &params.Parallelism); err != nil {
		return HashParams{}, nil, nil, ErrInvalidHash
	}

	salt, err := base64.RawStdEncoding.Strict().DecodeString(parts[4])
	if err != nil {
		return HashParams{}, nil, nil, ErrInvalidHash
	}
	key, err := base64.RawStdEncoding.Strict().DecodeString(parts[5])
	if err != nil {
		return HashParams{}, nil, nil, ErrInvalidHash
	}

	params.SaltLength = uint32(len(salt))
	params.KeyLength = uint32(len(key))
	return params, salt, key, nil
}
