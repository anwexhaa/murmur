package social_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/anwexhaa/murmur/internal/auth"
	"github.com/anwexhaa/murmur/internal/domain"
	"github.com/anwexhaa/murmur/internal/social"
)

const testTTL = 24 * time.Hour

// cheapHasher keeps these tests about the session logic rather than about
// argon2, which has its own tests and its own reasons to be slow.
func cheapHasher() *auth.Hasher {
	return auth.NewHasher(auth.HashParams{Memory: 64, Time: 1, Parallelism: 1})
}

func newAccount(t *testing.T, store *social.Store, handle string) domain.User {
	t.Helper()

	user, err := domain.NewUser(handle, "Display "+handle, time.Now().UTC().Truncate(time.Microsecond))
	if err != nil {
		t.Fatalf("new user: %v", err)
	}
	hash, err := cheapHasher().Hash("a perfectly adequate password")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	if err := store.CreateUserWithCredential(t.Context(), user, hash); err != nil {
		t.Fatalf("create: %v", err)
	}
	return user
}

func TestCredentialsAreWrittenWithTheUser(t *testing.T) {
	store := newStore(t)
	user := newAccount(t, store, "alice")

	userID, handle, stored, err := store.CredentialByHandle(t.Context(), "alice")
	if err != nil {
		t.Fatalf("read credential: %v", err)
	}
	if userID != user.ID {
		t.Fatalf("user id = %s, want %s", userID, user.ID)
	}
	if handle != "alice" {
		t.Fatalf("handle = %q, want alice", handle)
	}
	if err := auth.Verify("a perfectly adequate password", stored); err != nil {
		t.Fatalf("the stored hash does not verify: %v", err)
	}
}

// TestATakenHandleWritesNoCredential is why registration is one transaction. A
// user row with no credential would be an account nobody can sign into that
// holds the handle forever; a credential with no user is an orphan.
func TestATakenHandleWritesNoCredential(t *testing.T) {
	store := newStore(t)
	newAccount(t, store, "alice")

	second, err := domain.NewUser("alice", "Someone Else", time.Now().UTC())
	if err != nil {
		t.Fatalf("new user: %v", err)
	}
	err = store.CreateUserWithCredential(t.Context(), second, "$argon2id$irrelevant")
	if !errors.Is(err, domain.ErrAlreadyExists) {
		t.Fatalf("create = %v, want ErrAlreadyExists", err)
	}

	// The original credential is untouched.
	_, _, stored, err := store.CredentialByHandle(t.Context(), "alice")
	if err != nil {
		t.Fatalf("read credential: %v", err)
	}
	if stored == "$argon2id$irrelevant" {
		t.Fatal("the failed registration overwrote the existing credential")
	}
}

func TestAnUnknownHandleHasNoCredential(t *testing.T) {
	store := newStore(t)

	_, _, _, err := store.CredentialByHandle(t.Context(), "nobody")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("read = %v, want ErrNotFound", err)
	}
}

func TestRotationReturnsANewTokenEachTime(t *testing.T) {
	store := newStore(t)
	user := newAccount(t, store, "alice")
	now := time.Now().UTC()

	first, err := auth.NewRefreshToken()
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	issued, err := store.IssueRefreshToken(t.Context(), user.ID, first.Hash, now, testTTL)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}

	current := first
	family := issued.FamilyID

	for i := 0; i < 5; i++ {
		next, err := auth.NewRefreshToken()
		if err != nil {
			t.Fatalf("mint %d: %v", i, err)
		}
		record, err := store.RotateRefreshToken(t.Context(), current.Hash, next.Hash, now, testTTL)
		if err != nil {
			t.Fatalf("rotate %d: %v", i, err)
		}
		if record.UserID != user.ID {
			t.Fatalf("rotation %d returned user %s, want %s", i, record.UserID, user.ID)
		}
		// The family is the chain back to the login. Losing it would lose the
		// ability to revoke everything that descends from one compromise.
		if record.FamilyID != family {
			t.Fatalf("rotation %d changed the family from %s to %s", i, family, record.FamilyID)
		}
		current = next
	}
}

// TestAReusedTokenRevokesTheWholeFamily is the phase's second "done when".
//
// The scenario is the one that matters: an attacker steals a refresh token,
// the legitimate client refreshes first, and the attacker then presents the
// stolen copy. The server cannot tell which of the two is legitimate -- both
// hold a token it issued -- so it assumes the worst and ends the session.
func TestAReusedTokenRevokesTheWholeFamily(t *testing.T) {
	store := newStore(t)
	user := newAccount(t, store, "alice")
	now := time.Now().UTC()

	login, _ := auth.NewRefreshToken()
	if _, err := store.IssueRefreshToken(t.Context(), user.ID, login.Hash, now, testTTL); err != nil {
		t.Fatalf("issue: %v", err)
	}

	// The legitimate client refreshes.
	second, _ := auth.NewRefreshToken()
	if _, err := store.RotateRefreshToken(t.Context(), login.Hash, second.Hash, now, testTTL); err != nil {
		t.Fatalf("first rotation: %v", err)
	}

	// The attacker presents the token it stole before that rotation.
	third, _ := auth.NewRefreshToken()
	_, err := store.RotateRefreshToken(t.Context(), login.Hash, third.Hash, now, testTTL)
	if !errors.Is(err, social.ErrTokenReused) {
		t.Fatalf("replay = %v, want ErrTokenReused", err)
	}

	// And the legitimate client's live token is dead too. That is the
	// intended outcome: from here the two are indistinguishable, and the safe
	// reading is that the session is compromised.
	fourth, _ := auth.NewRefreshToken()
	_, err = store.RotateRefreshToken(t.Context(), second.Hash, fourth.Hash, now, testTTL)
	if !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("the survivor rotated after a reuse: %v", err)
	}

	active, err := store.CountActiveSessions(t.Context(), user.ID, now)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if active != 0 {
		t.Fatalf("%d sessions survived a reuse", active)
	}
}

// TestOnlyTheReusedFamilyDies keeps the blast radius honest. Somebody signed
// in on a phone and a laptop should lose the compromised session, not both.
func TestOnlyTheReusedFamilyDies(t *testing.T) {
	store := newStore(t)
	user := newAccount(t, store, "alice")
	now := time.Now().UTC()

	phone, _ := auth.NewRefreshToken()
	if _, err := store.IssueRefreshToken(t.Context(), user.ID, phone.Hash, now, testTTL); err != nil {
		t.Fatalf("issue phone: %v", err)
	}
	laptop, _ := auth.NewRefreshToken()
	if _, err := store.IssueRefreshToken(t.Context(), user.ID, laptop.Hash, now, testTTL); err != nil {
		t.Fatalf("issue laptop: %v", err)
	}

	// The phone's token is rotated and then replayed.
	next, _ := auth.NewRefreshToken()
	if _, err := store.RotateRefreshToken(t.Context(), phone.Hash, next.Hash, now, testTTL); err != nil {
		t.Fatalf("rotate phone: %v", err)
	}
	replacement, _ := auth.NewRefreshToken()
	if _, err := store.RotateRefreshToken(t.Context(), phone.Hash, replacement.Hash, now, testTTL); !errors.Is(err, social.ErrTokenReused) {
		t.Fatalf("replay = %v, want ErrTokenReused", err)
	}

	// The laptop is untouched.
	laptopNext, _ := auth.NewRefreshToken()
	if _, err := store.RotateRefreshToken(t.Context(), laptop.Hash, laptopNext.Hash, now, testTTL); err != nil {
		t.Fatalf("the laptop's session died with the phone's: %v", err)
	}
}

// TestConcurrentRotationOfOneTokenProducesOneSuccessor is why rotation runs in
// a transaction with FOR UPDATE.
//
// Two tabs refreshing at the same instant would otherwise both succeed, and
// the family would hold two live successors -- after which rotating either one
// would look like reuse of the other, and a perfectly innocent client would be
// logged out at random.
func TestConcurrentRotationOfOneTokenProducesOneSuccessor(t *testing.T) {
	store := newStore(t)
	user := newAccount(t, store, "alice")
	now := time.Now().UTC()

	login, _ := auth.NewRefreshToken()
	if _, err := store.IssueRefreshToken(t.Context(), user.ID, login.Hash, now, testTTL); err != nil {
		t.Fatalf("issue: %v", err)
	}

	const racers = 8

	var succeeded, reused atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			successor, err := auth.NewRefreshToken()
			if err != nil {
				t.Errorf("mint: %v", err)
				return
			}
			<-start
			_, err = store.RotateRefreshToken(t.Context(), login.Hash, successor.Hash, now, testTTL)
			switch {
			case err == nil:
				succeeded.Add(1)
			case errors.Is(err, social.ErrTokenReused):
				reused.Add(1)
			default:
				t.Errorf("rotate: %v", err)
			}
		}()
	}

	close(start)
	wg.Wait()

	if got := succeeded.Load(); got != 1 {
		t.Fatalf("%d of %d concurrent rotations succeeded, want exactly 1", got, racers)
	}
	if got := reused.Load(); got != racers-1 {
		t.Fatalf("%d rotations reported reuse, want %d", got, racers-1)
	}
}

func TestAnExpiredTokenCannotRotate(t *testing.T) {
	store := newStore(t)
	user := newAccount(t, store, "alice")
	now := time.Now().UTC()

	token, _ := auth.NewRefreshToken()
	if _, err := store.IssueRefreshToken(t.Context(), user.ID, token.Hash, now, time.Hour); err != nil {
		t.Fatalf("issue: %v", err)
	}

	successor, _ := auth.NewRefreshToken()
	_, err := store.RotateRefreshToken(t.Context(), token.Hash, successor.Hash, now.Add(2*time.Hour), testTTL)
	if !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("rotate = %v, want ErrForbidden", err)
	}
}

func TestAnUnknownTokenIsNotFoundRatherThanReuse(t *testing.T) {
	store := newStore(t)
	now := time.Now().UTC()

	successor, _ := auth.NewRefreshToken()
	_, err := store.RotateRefreshToken(t.Context(), auth.HashRefreshToken("never issued"), successor.Hash, now, testTTL)
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("rotate = %v, want ErrNotFound", err)
	}
}

func TestLogoutEndsOneSession(t *testing.T) {
	store := newStore(t)
	user := newAccount(t, store, "alice")
	now := time.Now().UTC()

	phone, _ := auth.NewRefreshToken()
	if _, err := store.IssueRefreshToken(t.Context(), user.ID, phone.Hash, now, testTTL); err != nil {
		t.Fatalf("issue: %v", err)
	}
	laptop, _ := auth.NewRefreshToken()
	if _, err := store.IssueRefreshToken(t.Context(), user.ID, laptop.Hash, now, testTTL); err != nil {
		t.Fatalf("issue: %v", err)
	}

	revoked, err := store.RevokeFamilyByToken(t.Context(), phone.Hash, now)
	if err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if revoked != 1 {
		t.Fatalf("revoked %d tokens, want 1", revoked)
	}

	active, err := store.CountActiveSessions(t.Context(), user.ID, now)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if active != 1 {
		t.Fatalf("%d sessions remain, want 1", active)
	}
}

// TestLoggingOutWithAnUnknownTokenIsNotAnError: the caller wanted to be logged
// out, and they are. Returning an error would make a client retry something
// that already achieved its purpose.
func TestLoggingOutWithAnUnknownTokenIsNotAnError(t *testing.T) {
	store := newStore(t)
	now := time.Now().UTC()

	revoked, err := store.RevokeFamilyByToken(t.Context(), auth.HashRefreshToken("never issued"), now)
	if err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if revoked != 0 {
		t.Fatalf("revoked %d tokens for an unknown token", revoked)
	}
}

func TestRevokeAllSessionsEndsEveryFamily(t *testing.T) {
	store := newStore(t)
	user := newAccount(t, store, "alice")
	now := time.Now().UTC()

	for i := 0; i < 3; i++ {
		token, _ := auth.NewRefreshToken()
		if _, err := store.IssueRefreshToken(t.Context(), user.ID, token.Hash, now, testTTL); err != nil {
			t.Fatalf("issue %d: %v", i, err)
		}
	}

	revoked, err := store.RevokeAllSessions(t.Context(), user.ID, now)
	if err != nil {
		t.Fatalf("revoke all: %v", err)
	}
	if revoked != 3 {
		t.Fatalf("revoked %d, want 3", revoked)
	}

	active, err := store.CountActiveSessions(t.Context(), user.ID, now)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if active != 0 {
		t.Fatalf("%d sessions survived a revoke-all", active)
	}
}

func TestIssuingForAnUnknownUserFails(t *testing.T) {
	store := newStore(t)
	token, _ := auth.NewRefreshToken()

	_, err := store.IssueRefreshToken(t.Context(), uuid.New(), token.Hash, time.Now().UTC(), testTTL)
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("issue = %v, want ErrNotFound", err)
	}
}

// TestSpentTokensSurviveTheSweepUntilTheyExpire is what makes reuse detectable
// at all. Deleting a token when it is spent would make a replay
// indistinguishable from a token that was never issued, and only one of those
// means a breach.
func TestSpentTokensSurviveTheSweepUntilTheyExpire(t *testing.T) {
	store := newStore(t)
	user := newAccount(t, store, "alice")
	now := time.Now().UTC()

	login, _ := auth.NewRefreshToken()
	if _, err := store.IssueRefreshToken(t.Context(), user.ID, login.Hash, now, testTTL); err != nil {
		t.Fatalf("issue: %v", err)
	}
	successor, _ := auth.NewRefreshToken()
	if _, err := store.RotateRefreshToken(t.Context(), login.Hash, successor.Hash, now, testTTL); err != nil {
		t.Fatalf("rotate: %v", err)
	}

	// A sweep that only removes expired rows leaves the spent one alone.
	deleted, err := store.DeleteExpiredTokens(t.Context(), now)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("the sweep deleted %d live rows", deleted)
	}

	replacement, _ := auth.NewRefreshToken()
	if _, err := store.RotateRefreshToken(t.Context(), login.Hash, replacement.Hash, now, testTTL); !errors.Is(err, social.ErrTokenReused) {
		t.Fatalf("after the sweep, replay = %v, want ErrTokenReused", err)
	}

	// Past expiry, the rows go.
	if _, err := store.DeleteExpiredTokens(t.Context(), now.Add(2*testTTL)); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	var count int
	if err := testPool.QueryRow(t.Context(),
		`SELECT count(*) FROM refresh_tokens WHERE user_id = $1`, user.ID).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("%d expired rows survived the sweep", count)
	}
}

func TestChangingACredentialKeepsTheAccount(t *testing.T) {
	store := newStore(t)
	user := newAccount(t, store, "alice")

	replacement, err := cheapHasher().Hash("an entirely different password")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	if err := store.UpdateCredential(t.Context(), user.ID, replacement, time.Now().UTC()); err != nil {
		t.Fatalf("update: %v", err)
	}

	_, _, stored, err := store.CredentialByHandle(t.Context(), "alice")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := auth.Verify("an entirely different password", stored); err != nil {
		t.Fatalf("the new password does not verify: %v", err)
	}
	if err := auth.Verify("a perfectly adequate password", stored); err == nil {
		t.Fatal("the old password still verifies")
	}
}

func TestUpdatingAnUnknownCredentialFails(t *testing.T) {
	store := newStore(t)

	err := store.UpdateCredential(t.Context(), uuid.New(), "$argon2id$whatever", time.Now().UTC())
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("update = %v, want ErrNotFound", err)
	}
}
