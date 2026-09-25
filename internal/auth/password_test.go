package auth_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/anwexhaa/murmur/internal/auth"
)

// cheapParams keep the tests fast. Every property they assert is independent
// of the cost, and 19 MiB per hash across a few dozen cases is a minute of CI
// spent proving nothing the small parameters do not already prove.
var cheapParams = auth.HashParams{Memory: 64, Time: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32}

func TestHashAndVerifyRoundTrip(t *testing.T) {
	hasher := auth.NewHasher(cheapParams)

	encoded, err := hasher.Hash("correct horse battery staple")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	if err := auth.Verify("correct horse battery staple", encoded); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

func TestVerifyRejectsTheWrongPassword(t *testing.T) {
	hasher := auth.NewHasher(cheapParams)

	encoded, err := hasher.Hash("hunter2")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	if err := auth.Verify("hunter3", encoded); !errors.Is(err, auth.ErrMismatch) {
		t.Fatalf("verify = %v, want ErrMismatch", err)
	}
}

// TestTheSamePasswordHashesDifferently is the salt doing its job. Without it,
// one cracked hash is a lookup table for every account that shares the
// password, and the whole database falls to a single dictionary run.
func TestTheSamePasswordHashesDifferently(t *testing.T) {
	hasher := auth.NewHasher(cheapParams)

	seen := map[string]bool{}
	for i := 0; i < 20; i++ {
		encoded, err := hasher.Hash("same password every time")
		if err != nil {
			t.Fatalf("hash %d: %v", i, err)
		}
		if seen[encoded] {
			t.Fatal("two hashes of the same password collided, so the salt is not random")
		}
		seen[encoded] = true

		if err := auth.Verify("same password every time", encoded); err != nil {
			t.Fatalf("hash %d does not verify: %v", i, err)
		}
	}
}

// TestAHashSurvivesAParameterChange is the reason the parameters live in the
// stored string rather than in the binary. Raising the cost must not lock
// every existing user out.
func TestAHashSurvivesAParameterChange(t *testing.T) {
	old := auth.NewHasher(auth.HashParams{Memory: 64, Time: 1, Parallelism: 1})

	encoded, err := old.Hash("unchanged")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}

	// The deployment raises its cost parameters.
	stronger := auth.NewHasher(auth.HashParams{Memory: 256, Time: 3, Parallelism: 1})

	if err := auth.Verify("unchanged", encoded); err != nil {
		t.Fatalf("an old hash stopped verifying after the parameters changed: %v", err)
	}
	if !stronger.NeedsRehash(encoded) {
		t.Fatal("NeedsRehash did not notice the old hash is weaker than the current settings")
	}
	if old.NeedsRehash(encoded) {
		t.Fatal("NeedsRehash wants to rewrite a hash made with the current settings")
	}
}

func TestNeedsRehashWantsToReplaceGarbage(t *testing.T) {
	if !auth.NewHasher(cheapParams).NeedsRehash("not a hash at all") {
		t.Fatal("an unreadable hash should be replaced, not kept")
	}
}

// TestVerifyRejectsMalformedHashes covers the parser, which is the part an
// attacker can reach with arbitrary input if a stored hash is ever
// attacker-controlled.
func TestVerifyRejectsMalformedHashes(t *testing.T) {
	hasher := auth.NewHasher(cheapParams)
	good, err := hasher.Hash("password")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	parts := strings.Split(good, "$")

	cases := map[string]string{
		"empty":              "",
		"not phc":            "plaintextpassword",
		"too few fields":     "$argon2id$v=19$m=64,t=1,p=1$c2FsdA",
		"too many fields":    good + "$extra",
		"unknown algorithm":  strings.Replace(good, "argon2id", "scrypt", 1),
		"wrong version":      strings.Replace(good, "v=19", "v=18", 1),
		"unparseable params": "$argon2id$v=19$memory=64$" + parts[4] + "$" + parts[5],
		"bad salt encoding":  "$argon2id$v=19$m=64,t=1,p=1$not!base64$" + parts[5],
		"bad hash encoding":  "$argon2id$v=19$m=64,t=1,p=1$" + parts[4] + "$not!base64",
	}

	for name, encoded := range cases {
		t.Run(name, func(t *testing.T) {
			if err := auth.Verify("password", encoded); err == nil {
				t.Fatal("a malformed hash verified")
			}
		})
	}
}

// TestArgon2iIsRefused matters because argon2i and argon2id have different
// resistance properties. Accepting whichever variant the stored string names
// would let a downgrade ride in on data rather than on code.
func TestArgon2iIsRefused(t *testing.T) {
	hasher := auth.NewHasher(cheapParams)
	encoded, err := hasher.Hash("password")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}

	downgraded := strings.Replace(encoded, "argon2id", "argon2i", 1)
	if err := auth.Verify("password", downgraded); !errors.Is(err, auth.ErrInvalidHash) {
		t.Fatalf("verify = %v, want ErrInvalidHash", err)
	}
}

// TestAFlippedByteFailsToVerify is the bluntest form of tamper detection: the
// stored hash is not a checksum anyone can adjust.
func TestAFlippedByteFailsToVerify(t *testing.T) {
	hasher := auth.NewHasher(cheapParams)
	encoded, err := hasher.Hash("password")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}

	raw := []byte(encoded)
	// The last character is inside the hash segment.
	if raw[len(raw)-1] == 'A' {
		raw[len(raw)-1] = 'B'
	} else {
		raw[len(raw)-1] = 'A'
	}

	if err := auth.Verify("password", string(raw)); err == nil {
		t.Fatal("a tampered hash verified")
	}
}

func TestZeroParamsFallBackToTheDefaults(t *testing.T) {
	got := auth.NewHasher(auth.HashParams{}).Params()
	if got != auth.DefaultParams {
		t.Fatalf("params = %+v, want %+v", got, auth.DefaultParams)
	}
}

func TestPartialParamsKeepTheOtherDefaults(t *testing.T) {
	got := auth.NewHasher(auth.HashParams{Memory: 128}).Params()
	if got.Memory != 128 {
		t.Fatalf("memory = %d, want 128", got.Memory)
	}
	if got.Time != auth.DefaultParams.Time || got.KeyLength != auth.DefaultParams.KeyLength {
		t.Fatalf("raising the memory cost disturbed the other parameters: %+v", got)
	}
}

// TestTheEncodingIsStandardPHC keeps the stored format readable by other
// argon2 implementations, which is what makes a migration away from this code
// possible without a password reset for everybody.
func TestTheEncodingIsStandardPHC(t *testing.T) {
	encoded, err := auth.NewHasher(cheapParams).Hash("password")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	if !strings.HasPrefix(encoded, "$argon2id$v=19$m=64,t=1,p=1$") {
		t.Fatalf("encoded = %q, which is not the PHC layout", encoded)
	}
}
