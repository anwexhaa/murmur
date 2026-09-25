package auth_test

import (
	"crypto/ed25519"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/anwexhaa/murmur/internal/auth"
)

func newSigner(t *testing.T) (*auth.Signer, ed25519.PublicKey) {
	t.Helper()
	key, err := auth.GenerateKey()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	signer, err := auth.NewSigner(key, auth.ServiceGateway)
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}
	return signer, signer.PublicKey()
}

func newVerifier(t *testing.T, pub ed25519.PublicKey, audience string) *auth.Verifier {
	t.Helper()
	verifier, err := auth.NewVerifier(pub, audience)
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	return verifier
}

func TestSignAndParseRoundTrip(t *testing.T) {
	signer, pub := newSigner(t)
	verifier := newVerifier(t, pub, auth.AudienceClient)

	raw, err := signer.Sign("user-1", "anwexhaa", auth.AudienceClient, time.Minute)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	claims, err := verifier.Parse(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if claims.UserID() != "user-1" {
		t.Fatalf("subject = %q, want user-1", claims.UserID())
	}
	if claims.Handle != "anwexhaa" {
		t.Fatalf("handle = %q, want anwexhaa", claims.Handle)
	}
}

// TestAClientTokenIsNotAnInternalAssertion is the audience check earning its
// place.
//
// Both tokens are signed by the same key, so the signature on a user's own
// access token is perfectly valid to a backend service. If the backend did not
// insist on the internal audience, any signed-in user could take the token
// their browser was given, present it straight to social-svc, and be believed
// as whoever the token names -- which is themselves, but the same reasoning
// makes every other claim in it trusted too.
func TestAClientTokenIsNotAnInternalAssertion(t *testing.T) {
	signer, pub := newSigner(t)
	backend := newVerifier(t, pub, auth.AudienceInternal)

	clientToken, err := signer.Sign("user-1", "anwexhaa", auth.AudienceClient, time.Minute)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	if _, err := backend.Parse(clientToken); !errors.Is(err, auth.ErrInvalidToken) {
		t.Fatalf("a backend accepted a client token: %v", err)
	}
}

func TestAnInternalAssertionIsNotAClientToken(t *testing.T) {
	signer, pub := newSigner(t)
	edge := newVerifier(t, pub, auth.AudienceClient)

	assertion, err := signer.Sign("user-1", "anwexhaa", auth.AudienceInternal, time.Minute)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	if _, err := edge.Parse(assertion); !errors.Is(err, auth.ErrInvalidToken) {
		t.Fatalf("the edge accepted an internal assertion: %v", err)
	}
}

// TestAlgorithmConfusionIsRejected is the attack the WithValidMethods line
// exists for, and it is worth spelling out because it looks like nothing.
//
// The verifying key is public -- every backend has it, and anything public is
// available to an attacker. A parser that trusts the token's own `alg` header
// will, when handed an HS256 token, use the key material as an HMAC secret.
// The attacker therefore holds the "secret" and can mint any claims they like.
// The signature verifies. Nothing looks wrong in a log.
func TestAlgorithmConfusionIsRejected(t *testing.T) {
	signer, pub := newSigner(t)
	verifier := newVerifier(t, pub, auth.AudienceClient)

	// Forged by someone who has only the public key.
	forged := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss": auth.Issuer,
		"sub": "somebody-elses-account",
		"aud": auth.AudienceClient,
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
	})
	raw, err := forged.SignedString([]byte(pub))
	if err != nil {
		t.Fatalf("sign forgery: %v", err)
	}

	if _, err := verifier.Parse(raw); err == nil {
		t.Fatal("an HS256 token signed with the public key was accepted")
	}

	// And the genuine article still works, so the check is not simply
	// rejecting everything.
	good, err := signer.Sign("user-1", "", auth.AudienceClient, time.Minute)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if _, err := verifier.Parse(good); err != nil {
		t.Fatalf("a genuine token was rejected: %v", err)
	}
}

// TestAlgNoneIsRejected is the other half of the same family: a token that
// claims to need no signature at all.
func TestAlgNoneIsRejected(t *testing.T) {
	_, pub := newSigner(t)
	verifier := newVerifier(t, pub, auth.AudienceClient)

	unsigned := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims{
		"iss": auth.Issuer,
		"sub": "somebody-elses-account",
		"aud": auth.AudienceClient,
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	raw, err := unsigned.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("sign unsigned token: %v", err)
	}

	if _, err := verifier.Parse(raw); err == nil {
		t.Fatal("an alg=none token was accepted")
	}
}

func TestAnotherKeyIsRejected(t *testing.T) {
	signer, _ := newSigner(t)
	_, otherPub := newSigner(t)
	verifier := newVerifier(t, otherPub, auth.AudienceClient)

	raw, err := signer.Sign("user-1", "", auth.AudienceClient, time.Minute)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if _, err := verifier.Parse(raw); err == nil {
		t.Fatal("a token signed by a different key was accepted")
	}
}

func TestAnExpiredTokenIsRejected(t *testing.T) {
	signer, pub := newSigner(t)
	verifier := newVerifier(t, pub, auth.AudienceClient)

	raw, err := signer.Sign("user-1", "", auth.AudienceClient, time.Nanosecond)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	time.Sleep(2 * time.Millisecond)

	if _, err := verifier.Parse(raw); !errors.Is(err, auth.ErrExpiredToken) {
		t.Fatalf("parse = %v, want ErrExpiredToken", err)
	}
}

// TestATamperedPayloadIsRejected confirms the claims are signed rather than
// merely encoded. Base64 is not a security boundary and a JWT's payload is
// readable by anyone; what stops it being *writable* is the signature.
func TestATamperedPayloadIsRejected(t *testing.T) {
	signer, pub := newSigner(t)
	verifier := newVerifier(t, pub, auth.AudienceClient)

	raw, err := signer.Sign("user-1", "", auth.AudienceClient, time.Minute)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		t.Fatalf("token has %d segments, want 3", len(parts))
	}
	// Re-sign the same claims with a different key and splice in that payload:
	// the signature no longer matches the body it is supposed to cover.
	other, _ := newSigner(t)
	forged, err := other.Sign("administrator", "", auth.AudienceClient, time.Minute)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	forgedParts := strings.Split(forged, ".")

	spliced := parts[0] + "." + forgedParts[1] + "." + parts[2]
	if _, err := verifier.Parse(spliced); err == nil {
		t.Fatal("a token with a swapped payload was accepted")
	}
}

// TestAnAssertionCanSpeakForNobody covers the signed-out visitor: the call
// still crosses the trust boundary and still has to be proven to come from the
// gateway, even though there is no user behind it.
func TestAnAssertionCanSpeakForNobody(t *testing.T) {
	signer, pub := newSigner(t)
	backend := newVerifier(t, pub, auth.AudienceInternal)

	raw, err := signer.Sign("", "", auth.AudienceInternal, time.Minute)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	claims, err := backend.Parse(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if claims.UserID() != "" {
		t.Fatalf("subject = %q, want empty", claims.UserID())
	}
}

func TestSignRejectsMissingAudienceOrLifetime(t *testing.T) {
	signer, _ := newSigner(t)

	if _, err := signer.Sign("user-1", "", "", time.Minute); err == nil {
		t.Fatal("signed a token with no audience")
	}
	if _, err := signer.Sign("user-1", "", auth.AudienceClient, 0); err == nil {
		t.Fatal("signed a token that was already expired")
	}
}

func TestKeyEncodingRoundTrips(t *testing.T) {
	key, err := auth.GenerateKey()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	decoded, err := auth.DecodeSeed(auth.EncodeSeed(key))
	if err != nil {
		t.Fatalf("decode seed: %v", err)
	}
	if !decoded.Equal(key) {
		t.Fatal("the decoded private key differs from the original")
	}

	pub := key.Public().(ed25519.PublicKey)
	decodedPub, err := auth.DecodePublicKey(auth.EncodePublicKey(pub))
	if err != nil {
		t.Fatalf("decode public key: %v", err)
	}
	if !decodedPub.Equal(pub) {
		t.Fatal("the decoded public key differs from the original")
	}
}

func TestKeyDecodingRejectsTheWrongLength(t *testing.T) {
	if _, err := auth.DecodeSeed("c2hvcnQ="); err == nil {
		t.Fatal("accepted a seed that is not 32 bytes")
	}
	if _, err := auth.DecodePublicKey("c2hvcnQ="); err == nil {
		t.Fatal("accepted a public key that is not 32 bytes")
	}
	if _, err := auth.DecodeSeed("not base64 at all"); err == nil {
		t.Fatal("accepted a seed that is not base64")
	}
}
