package auth_test

import (
	"encoding/base64"
	"testing"

	"github.com/anwexhaa/murmur/internal/auth"
)

func TestRefreshTokensAreUnique(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		token, err := auth.NewRefreshToken()
		if err != nil {
			t.Fatalf("mint %d: %v", i, err)
		}
		if seen[token.Plaintext] {
			t.Fatal("two refresh tokens collided")
		}
		seen[token.Plaintext] = true
	}
}

func TestRefreshTokenCarriesItsOwnHash(t *testing.T) {
	token, err := auth.NewRefreshToken()
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if got := auth.HashRefreshToken(token.Plaintext); got != token.Hash {
		t.Fatalf("hash = %q, want %q", got, token.Hash)
	}
}

// TestTheHashIsNotThePlaintext is the property that makes a database dump
// survivable: the stored value cannot be presented as a token.
func TestTheHashIsNotThePlaintext(t *testing.T) {
	token, err := auth.NewRefreshToken()
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if token.Hash == token.Plaintext {
		t.Fatal("the stored hash is the token itself")
	}
	if auth.HashRefreshToken(token.Hash) == token.Hash {
		t.Fatal("hashing is not doing anything")
	}
}

func TestHashingIsDeterministic(t *testing.T) {
	first, second := auth.HashRefreshToken("abc"), auth.HashRefreshToken("abc")
	if first != second {
		t.Fatal("the same token hashed to two different values, so rotation could never find it")
	}
	if other := auth.HashRefreshToken("abd"); other == first {
		t.Fatal("two different tokens hashed the same")
	}
}

// TestTheTokenCarriesFullEntropy is what licenses the fast hash. If the token
// were short or structured, SHA-256 over it would be brute-forceable from a
// dump and argon2 would be the right answer instead.
func TestTheTokenCarriesFullEntropy(t *testing.T) {
	token, err := auth.NewRefreshToken()
	if err != nil {
		t.Fatalf("mint: %v", err)
	}

	raw, err := base64.RawURLEncoding.DecodeString(token.Plaintext)
	if err != nil {
		t.Fatalf("the token is not base64url: %v", err)
	}
	if len(raw) != 32 {
		t.Fatalf("token carries %d bytes, want 32", len(raw))
	}
}

// TestTheTokenIsURLSafe matters because it travels in headers and bodies and,
// for some clients, in a cookie. Padding and '+' would need escaping at every
// one of those, and one place that forgets is a login that fails confusingly.
func TestTheTokenIsURLSafe(t *testing.T) {
	for i := 0; i < 200; i++ {
		token, err := auth.NewRefreshToken()
		if err != nil {
			t.Fatalf("mint: %v", err)
		}
		for _, r := range token.Plaintext {
			safe := (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') ||
				(r >= '0' && r <= '9') || r == '-' || r == '_'
			if !safe {
				t.Fatalf("token %q contains %q, which needs escaping somewhere", token.Plaintext, r)
			}
		}
	}
}
