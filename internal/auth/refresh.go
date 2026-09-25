package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
)

// refreshTokenBytes is the entropy in a refresh token.
//
// 256 bits, which is not a number anyone will guess and is the reason the
// stored hash can be a fast one. See HashRefreshToken.
const refreshTokenBytes = 32

// RefreshToken is opaque on purpose.
//
// It could have been a JWT, and that would have been worse. A refresh token
// has to be revocable -- reuse detection is the whole feature -- and a
// self-contained signed token is valid until it expires whether the server
// likes it or not. Revoking one means keeping a list of revoked tokens, at
// which point the database lookup that a JWT exists to avoid is happening
// anyway, and the token is carrying claims nobody reads.
//
// So: a random string, and the truth lives in Postgres.
type RefreshToken struct {
	// Plaintext goes to the client and is never stored.
	Plaintext string
	// Hash is what the database holds.
	Hash string
}

// NewRefreshToken mints one.
func NewRefreshToken() (RefreshToken, error) {
	raw := make([]byte, refreshTokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return RefreshToken{}, fmt.Errorf("auth: read refresh token: %w", err)
	}

	plaintext := base64.RawURLEncoding.EncodeToString(raw)
	return RefreshToken{Plaintext: plaintext, Hash: HashRefreshToken(plaintext)}, nil
}

// HashRefreshToken is what the database stores and what rotation looks up by.
//
// SHA-256, and the contrast with the password column is the point. Argon2 is
// slow so that guessing a human-chosen secret is expensive; there is nothing
// to guess here, because the token is 256 bits of CSPRNG output and an
// attacker who can brute-force that can brute-force the signing key too. What
// hashing buys is the same thing it buys for passwords -- a database dump does
// not hand over live credentials -- and a fast hash buys all of it.
//
// Spending 19MB and two passes on every refresh to defend against an attack
// nobody can mount would be a cost with no corresponding benefit, and the
// kind of thing that looks rigorous until somebody asks what it is for.
func HashRefreshToken(plaintext string) string {
	sum := sha256.Sum256([]byte(plaintext))
	return hex.EncodeToString(sum[:])
}
