package auth

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Audiences separate the two kinds of token this package signs.
//
// Both are signed with the same key, and the audience is what stops one being
// used as the other. An access token is handed to a browser; an internal
// assertion says "this call came through the gateway" and is the entire basis
// on which a backend trusts a caller's identity. Without an audience check,
// anyone holding their own access token could present it straight to
// social-svc and be believed -- the signature is valid, after all. The claim
// that differs is the one that has to be verified.
const (
	// AudienceClient is on tokens given to users.
	AudienceClient = "murmur-client"
	// AudienceInternal is on assertions sent between services.
	AudienceInternal = "murmur-internal"
)

// Issuer is on every token this deployment signs.
//
// One value rather than one per service, because verification has to be a
// constant-time comparison against a known string, not a lookup in a list that
// grows every time a service is added. Which service signed a given assertion
// is recorded in the Service claim instead, where it is useful for logs and
// irrelevant to trust.
const Issuer = "murmur"

// Service names the process that signed an assertion.
//
// Note which processes appear here and which do not. The gateway, the fanout
// worker and timeline-svc make internal calls and therefore hold the signing
// key. social-svc -- the service that owns the user table, the credentials and
// the follow graph -- makes none, holds only the public key, and can verify an
// assertion without being able to produce one. Compromising the largest store
// of data in the system does not yield the ability to impersonate a user to
// anything else, and that is what the asymmetric key buys that a shared secret
// would not.
const (
	ServiceGateway  = "gateway"
	ServiceFanout   = "fanout-worker"
	ServiceTimeline = "timeline-svc"
)

// Errors a caller may act on. Everything else collapses into ErrInvalidToken,
// because a client learning *why* its token failed learns something about the
// server it does not need.
var (
	ErrInvalidToken = errors.New("auth: invalid token")
	ErrExpiredToken = errors.New("auth: token expired")
)

// Claims is what a Murmur token asserts.
type Claims struct {
	jwt.RegisteredClaims

	// Handle travels with the token so the gateway can log and authorise
	// without a lookup. It is a convenience, never an identifier: the subject
	// is the user ID, which does not change, and handles can.
	Handle string `json:"handle,omitempty"`

	// Service names the process that signed this token. Observability, not
	// authorisation: it says who claims to have called, and the signature is
	// what makes the claim worth anything.
	Service string `json:"svc,omitempty"`
}

// SignedBy reports which service signed the token.
func (c *Claims) SignedBy() string { return c.Service }

// UserID returns the account the token speaks for, or "" for an assertion made
// on nobody's behalf.
func (c *Claims) UserID() string { return c.Subject }

// Signer issues tokens. Only the gateway holds one.
//
// Ed25519 rather than HMAC, and that asymmetry is the point. With a shared
// secret, every backend able to verify an assertion is also able to mint one,
// so compromising the least important service would yield the ability to
// impersonate any user to every other service. Here the backends hold only the
// public key: they can check that the gateway said something, and they cannot
// say anything themselves.
type Signer struct {
	key     ed25519.PrivateKey
	service string
	now     func() time.Time
}

// Verifier checks tokens. Every service holds one.
type Verifier struct {
	key    ed25519.PublicKey
	parser *jwt.Parser
}

// NewSigner builds a signer over an Ed25519 private key. service names the
// process doing the signing and travels in every token it issues.
func NewSigner(key ed25519.PrivateKey, service string) (*Signer, error) {
	if len(key) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("auth: private key is %d bytes, want %d", len(key), ed25519.PrivateKeySize)
	}
	if service == "" {
		return nil, errors.New("auth: signer needs a service name")
	}
	return &Signer{key: key, service: service, now: time.Now}, nil
}

// PublicKey returns the key verifiers need.
func (s *Signer) PublicKey() ed25519.PublicKey {
	return s.key.Public().(ed25519.PublicKey)
}

// NewVerifier builds a verifier over an Ed25519 public key.
func NewVerifier(key ed25519.PublicKey, audience string) (*Verifier, error) {
	if len(key) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("auth: public key is %d bytes, want %d", len(key), ed25519.PublicKeySize)
	}
	if audience == "" {
		return nil, errors.New("auth: verifier needs an audience")
	}

	return &Verifier{
		key: key,
		parser: jwt.NewParser(
			// Pinning the algorithm is not optional. A parser that accepts
			// whatever the token's own header names will happily verify a
			// token whose header says "none", and will treat an HMAC token as
			// if the public key were a shared secret -- a public key that,
			// being public, the attacker has. Both are famous, both are
			// one missing line, and this is that line.
			jwt.WithValidMethods([]string{jwt.SigningMethodEdDSA.Alg()}),
			jwt.WithIssuer(Issuer),
			jwt.WithAudience(audience),
			jwt.WithExpirationRequired(),
			// No leeway. These tokens live for minutes and both ends run NTP;
			// accepting an expired one to be polite widens the window in which
			// a stolen token still works, for no benefit anyone asked for.
			jwt.WithLeeway(0),
		),
	}, nil
}

// Sign issues a token for one audience.
//
// subject may be empty, which is how the gateway asserts "this request came
// from me" for a call made on nobody's behalf -- a public profile lookup by a
// signed-out visitor still has to cross the trust boundary.
func (s *Signer) Sign(subject, handle, audience string, ttl time.Duration) (string, error) {
	if audience == "" {
		return "", errors.New("auth: token needs an audience")
	}
	if ttl <= 0 {
		return "", errors.New("auth: token needs a positive lifetime")
	}

	now := s.now()
	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    Issuer,
			Subject:   subject,
			Audience:  jwt.ClaimStrings{audience},
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		},
		Handle:  handle,
		Service: s.service,
	}

	token, err := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims).SignedString(s.key)
	if err != nil {
		return "", fmt.Errorf("auth: sign token: %w", err)
	}
	return token, nil
}

// Parse verifies a token and returns its claims.
func (v *Verifier) Parse(raw string) (*Claims, error) {
	claims := &Claims{}
	_, err := v.parser.ParseWithClaims(raw, claims, func(*jwt.Token) (any, error) {
		return v.key, nil
	})
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrExpiredToken
		}
		return nil, fmt.Errorf("%w: %s", ErrInvalidToken, err)
	}
	return claims, nil
}

// EncodeSeed renders the 32-byte Ed25519 seed as base64, which is what goes in
// configuration. The seed rather than the 64-byte expanded key: it is the only
// part that is actually secret, and the rest is derived from it.
func EncodeSeed(key ed25519.PrivateKey) string {
	return base64.StdEncoding.EncodeToString(key.Seed())
}

// DecodeSeed rebuilds a private key from configuration.
func DecodeSeed(encoded string) (ed25519.PrivateKey, error) {
	seed, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("auth: decode signing key: %w", err)
	}
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("auth: signing key is %d bytes, want %d", len(seed), ed25519.SeedSize)
	}
	return ed25519.NewKeyFromSeed(seed), nil
}

// EncodePublicKey renders a public key for configuration.
func EncodePublicKey(key ed25519.PublicKey) string {
	return base64.StdEncoding.EncodeToString(key)
}

// DecodePublicKey reads a public key from configuration.
func DecodePublicKey(encoded string) (ed25519.PublicKey, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("auth: decode verifying key: %w", err)
	}
	if len(raw) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("auth: verifying key is %d bytes, want %d", len(raw), ed25519.PublicKeySize)
	}
	return ed25519.PublicKey(raw), nil
}

// GenerateKey makes a fresh keypair, for tests and for a development stack
// that has no key configured.
func GenerateKey() (ed25519.PrivateKey, error) {
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, fmt.Errorf("auth: generate signing key: %w", err)
	}
	return key, nil
}
