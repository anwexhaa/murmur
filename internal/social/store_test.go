package social_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/anwexhaa/murmur/internal/domain"
	"github.com/anwexhaa/murmur/internal/social"
)

func makeUser(t *testing.T, store *social.Store, handle string) domain.User {
	t.Helper()

	user, err := domain.NewUser(handle, "Test "+handle, time.Now())
	if err != nil {
		t.Fatalf("NewUser(%q) = %v", handle, err)
	}
	if err := store.CreateUser(t.Context(), user); err != nil {
		t.Fatalf("CreateUser(%q) = %v", handle, err)
	}
	return user
}

func TestCreateAndGetUser(t *testing.T) {
	store := newStore(t)
	created := makeUser(t, store, "anwexhaa")

	byID, err := store.GetUser(t.Context(), created.ID)
	if err != nil {
		t.Fatalf("GetUser() = %v", err)
	}
	if byID.Handle != created.Handle {
		t.Errorf("Handle = %q, want %q", byID.Handle, created.Handle)
	}

	// The column is citext, so a handle differing only in case is the same
	// handle. If this ever fails, the migration lost the extension.
	byHandle, err := store.GetUserByHandle(t.Context(), "ANWEXHAA")
	if err != nil {
		t.Fatalf("GetUserByHandle() with different casing = %v", err)
	}
	if byHandle.ID != created.ID {
		t.Errorf("case-insensitive lookup returned a different user")
	}
}

func TestDuplicateHandleIsRejected(t *testing.T) {
	store := newStore(t)
	makeUser(t, store, "anwexhaa")

	duplicate, err := domain.NewUser("AnWeXhAa", "Impostor", time.Now())
	if err != nil {
		t.Fatalf("NewUser() = %v", err)
	}

	err = store.CreateUser(t.Context(), duplicate)
	if !errors.Is(err, domain.ErrAlreadyExists) {
		t.Fatalf("CreateUser() = %v, want ErrAlreadyExists — citext should make casing irrelevant", err)
	}
}

func TestGetUserReportsMissingAsNotFound(t *testing.T) {
	store := newStore(t)

	_, err := store.GetUser(t.Context(), uuid.New())
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("GetUser() = %v, want ErrNotFound", err)
	}
}

// Follow is idempotent so a client retrying after a timeout gets success
// rather than a duplicate-key error it would have to interpret.
func TestFollowIsIdempotent(t *testing.T) {
	store := newStore(t)
	alice := makeUser(t, store, "alice")
	bob := makeUser(t, store, "bob")

	created, err := store.Follow(t.Context(), alice.ID, bob.ID)
	if err != nil {
		t.Fatalf("Follow() = %v", err)
	}
	if !created {
		t.Error("first Follow() reported created=false")
	}

	created, err = store.Follow(t.Context(), alice.ID, bob.ID)
	if err != nil {
		t.Fatalf("second Follow() = %v, want success", err)
	}
	if created {
		t.Error("second Follow() reported created=true; the edge already existed")
	}

	count, err := store.CountFollowers(t.Context(), bob.ID)
	if err != nil {
		t.Fatalf("CountFollowers() = %v", err)
	}
	if count != 1 {
		t.Errorf("follower count = %d after two identical follows, want 1", count)
	}
}

func TestFollowRejectsSelfAndMissingUsers(t *testing.T) {
	store := newStore(t)
	alice := makeUser(t, store, "alice")

	if _, err := store.Follow(t.Context(), alice.ID, alice.ID); !errors.Is(err, domain.ErrInvalid) {
		t.Errorf("self-follow = %v, want ErrInvalid from the CHECK constraint", err)
	}
	if _, err := store.Follow(t.Context(), alice.ID, uuid.New()); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("follow of a missing user = %v, want ErrNotFound", err)
	}
}

func TestUnfollowReportsWhetherAnEdgeWasThere(t *testing.T) {
	store := newStore(t)
	alice := makeUser(t, store, "alice")
	bob := makeUser(t, store, "bob")

	if _, err := store.Follow(t.Context(), alice.ID, bob.ID); err != nil {
		t.Fatalf("Follow() = %v", err)
	}

	removed, err := store.Unfollow(t.Context(), alice.ID, bob.ID)
	if err != nil || !removed {
		t.Fatalf("Unfollow() = (%v, %v), want (true, nil)", removed, err)
	}

	removed, err = store.Unfollow(t.Context(), alice.ID, bob.ID)
	if err != nil {
		t.Fatalf("second Unfollow() = %v, want success", err)
	}
	if removed {
		t.Error("second Unfollow() reported removed=true; there was no edge left")
	}
}

// Keyset pagination is what fanout will page follower edges with, hundreds of
// thousands at a time. It must return every edge exactly once across pages.
func TestListFollowersPagesWithoutGapsOrDuplicates(t *testing.T) {
	store := newStore(t)
	star := makeUser(t, store, "star")

	const followerCount = 57
	want := make(map[uuid.UUID]struct{}, followerCount)
	for i := range followerCount {
		follower := makeUser(t, store, fmt.Sprintf("follower_%02d", i))
		if _, err := store.Follow(t.Context(), follower.ID, star.ID); err != nil {
			t.Fatalf("Follow() = %v", err)
		}
		want[follower.ID] = struct{}{}
	}

	const pageSize = 10
	got := make(map[uuid.UUID]struct{}, followerCount)
	cursor := ""
	pages := 0

	for {
		page, err := store.ListFollowers(t.Context(), star.ID, cursor, pageSize)
		if err != nil {
			t.Fatalf("ListFollowers() = %v", err)
		}
		if len(page) == 0 {
			break
		}
		for _, id := range page {
			if _, seen := got[id]; seen {
				t.Fatalf("follower %s appeared on more than one page", id)
			}
			got[id] = struct{}{}
		}
		pages++
		if pages > followerCount {
			t.Fatal("pagination did not terminate")
		}
		if len(page) < pageSize {
			break
		}
		cursor = page[len(page)-1].String()
	}

	if len(got) != followerCount {
		t.Fatalf("paged through %d followers, want %d", len(got), followerCount)
	}
	for id := range want {
		if _, found := got[id]; !found {
			t.Errorf("follower %s was never returned", id)
		}
	}
}

func TestListFollowersRejectsAMalformedCursor(t *testing.T) {
	store := newStore(t)
	star := makeUser(t, store, "star")

	_, err := store.ListFollowers(t.Context(), star.ID, "not-a-uuid", 10)
	if !errors.Is(err, domain.ErrInvalid) {
		t.Errorf("ListFollowers() with a bad token = %v, want ErrInvalid", err)
	}
}

func TestBatchGetUsersReturnsWhatExistsAndSkipsWhatDoesNot(t *testing.T) {
	store := newStore(t)
	alice := makeUser(t, store, "alice")
	bob := makeUser(t, store, "bob")
	missing := uuid.New()

	users, err := store.BatchGetUsers(t.Context(), []uuid.UUID{alice.ID, missing, bob.ID})
	if err != nil {
		t.Fatalf("BatchGetUsers() = %v, want a partial result rather than an error", err)
	}
	if len(users) != 2 {
		t.Fatalf("got %d users, want 2 (the missing id should simply be absent)", len(users))
	}

	found := map[uuid.UUID]bool{}
	for _, user := range users {
		found[user.ID] = true
	}
	if !found[alice.ID] || !found[bob.ID] {
		t.Errorf("batch did not return both existing users")
	}
}

func TestBatchGetUsersHandlesAnEmptyRequest(t *testing.T) {
	store := newStore(t)

	users, err := store.BatchGetUsers(t.Context(), nil)
	if err != nil {
		t.Fatalf("BatchGetUsers(nil) = %v, want nil", err)
	}
	if len(users) != 0 {
		t.Errorf("got %d users for an empty request", len(users))
	}
}

func TestPostsRoundTripAndPageNewestFirst(t *testing.T) {
	store := newStore(t)
	author := makeUser(t, store, "author")
	ids := domain.NewIDGenerator()

	const postCount = 25
	created := make([]string, postCount)
	instant := time.Now().UTC()

	for i := range postCount {
		instant = instant.Add(time.Millisecond)
		id, err := ids.New(instant)
		if err != nil {
			t.Fatalf("New() = %v", err)
		}
		post := domain.Post{
			ID:        id,
			AuthorID:  author.ID,
			Body:      fmt.Sprintf("post number %d", i),
			CreatedAt: instant,
		}
		if err := store.CreatePost(t.Context(), post); err != nil {
			t.Fatalf("CreatePost() = %v", err)
		}
		created[i] = id
	}

	page, err := store.ListAuthorPosts(t.Context(), author.ID, "", 10)
	if err != nil {
		t.Fatalf("ListAuthorPosts() = %v", err)
	}
	if len(page) != 10 {
		t.Fatalf("got %d posts, want 10", len(page))
	}

	// Newest first, which for ULIDs means descending string order.
	for i := 1; i < len(page); i++ {
		if page[i].ID >= page[i-1].ID {
			t.Fatalf("posts are not newest-first at index %d: %q then %q", i, page[i-1].ID, page[i].ID)
		}
	}
	if page[0].ID != created[postCount-1] {
		t.Errorf("first post = %q, want the most recent %q", page[0].ID, created[postCount-1])
	}

	// The second page must continue where the first stopped, with no overlap.
	next, err := store.ListAuthorPosts(t.Context(), author.ID, page[len(page)-1].ID, 10)
	if err != nil {
		t.Fatalf("ListAuthorPosts(page 2) = %v", err)
	}
	if len(next) != 10 {
		t.Fatalf("second page has %d posts, want 10", len(next))
	}
	if next[0].ID >= page[len(page)-1].ID {
		t.Errorf("second page starts at %q, which is not after the first page's last %q",
			next[0].ID, page[len(page)-1].ID)
	}
}

func TestCreatePostRequiresAnExistingAuthor(t *testing.T) {
	store := newStore(t)
	ids := domain.NewIDGenerator()

	id, err := ids.New(time.Now())
	if err != nil {
		t.Fatalf("New() = %v", err)
	}

	err = store.CreatePost(t.Context(), domain.Post{
		ID:        id,
		AuthorID:  uuid.New(),
		Body:      "orphan",
		CreatedAt: time.Now().UTC(),
	})
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("CreatePost() with a missing author = %v, want ErrNotFound", err)
	}
}

func TestBatchGetPosts(t *testing.T) {
	store := newStore(t)
	author := makeUser(t, store, "author")
	ids := domain.NewIDGenerator()

	wanted := make([]string, 3)
	for i := range wanted {
		id, err := ids.New(time.Now().Add(time.Duration(i) * time.Millisecond))
		if err != nil {
			t.Fatalf("New() = %v", err)
		}
		if err := store.CreatePost(t.Context(), domain.Post{
			ID: id, AuthorID: author.ID, Body: "hello", CreatedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("CreatePost() = %v", err)
		}
		wanted[i] = id
	}

	absent, err := ids.New(time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("New() = %v", err)
	}

	posts, err := store.BatchGetPosts(t.Context(), append(wanted, absent))
	if err != nil {
		t.Fatalf("BatchGetPosts() = %v", err)
	}
	if len(posts) != len(wanted) {
		t.Errorf("got %d posts, want %d", len(posts), len(wanted))
	}
}

// COPY is what makes the seeder finish in seconds instead of hours. It is also
// all-or-nothing, which is why the seeder must not generate duplicate edges.
func TestBulkCopyLoadsUsersAndFollows(t *testing.T) {
	store := newStore(t)

	const count = 500
	users := make([]domain.User, count)
	now := time.Now().UTC()
	for i := range users {
		users[i] = domain.User{
			ID:          uuid.New(),
			Handle:      fmt.Sprintf("bulk_%05d", i),
			DisplayName: "Bulk",
			CreatedAt:   now,
		}
	}

	loaded, err := store.CopyUsers(t.Context(), users)
	if err != nil {
		t.Fatalf("CopyUsers() = %v", err)
	}
	if loaded != count {
		t.Errorf("CopyUsers() loaded %d rows, want %d", loaded, count)
	}

	star := users[0]
	edges := make([]social.FollowEdge, 0, count-1)
	for _, follower := range users[1:] {
		edges = append(edges, social.FollowEdge{FollowerID: follower.ID, FolloweeID: star.ID})
	}

	loaded, err = store.CopyFollows(t.Context(), edges)
	if err != nil {
		t.Fatalf("CopyFollows() = %v", err)
	}
	if loaded != int64(len(edges)) {
		t.Errorf("CopyFollows() loaded %d rows, want %d", loaded, len(edges))
	}

	followers, err := store.CountFollowers(t.Context(), star.ID)
	if err != nil {
		t.Fatalf("CountFollowers() = %v", err)
	}
	if followers != int64(len(edges)) {
		t.Errorf("follower count = %d, want %d", followers, len(edges))
	}
}

func TestFollowerDistributionOnAnEmptyGraph(t *testing.T) {
	store := newStore(t)

	dist, err := store.FollowerDistribution(t.Context())
	if err != nil {
		t.Fatalf("FollowerDistribution() = %v, want zeroes rather than an error", err)
	}
	if dist.Accounts != 0 || dist.Max != 0 {
		t.Errorf("empty graph reported %+v, want all zeroes", dist)
	}
}
