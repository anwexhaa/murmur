package timeline_test

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/anwexhaa/murmur/internal/timeline"
)

var testClient *redis.Client

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}

	ctx := context.Background()

	// Reuse the development Redis when one is pointed at, otherwise start a
	// throwaway. Same arrangement as the social package's Postgres: fast
	// locally, self-contained in CI.
	addr := os.Getenv("MURMUR_TEST_REDIS_ADDR")

	var container *tcredis.RedisContainer
	if addr == "" {
		var err error
		container, err = tcredis.Run(ctx, "redis:7-alpine")
		if err != nil {
			log.Fatalf("start redis container: %v", err)
		}
		endpoint, err := container.ConnectionString(ctx)
		if err != nil {
			log.Fatalf("redis connection string: %v", err)
		}
		opts, err := redis.ParseURL(endpoint)
		if err != nil {
			log.Fatalf("parse redis url: %v", err)
		}
		addr = opts.Addr
	}

	testClient = redis.NewClient(&redis.Options{Addr: addr, DB: testRedisDB()})
	code := m.Run()

	_ = testClient.Close()
	if container != nil {
		if err := testcontainers.TerminateContainer(container); err != nil {
			log.Printf("terminate redis container: %v", err)
		}
	}
	os.Exit(code)
}

// testRedisDB keeps integration tests off the development keyspace.
//
// These tests call FlushDB, and pointing them at the same database the local
// stack uses would delete every timeline the seeder just built. Redis's
// numbered databases are the cheap equivalent of the separate Postgres
// database the social tests use.
func testRedisDB() int {
	if raw := os.Getenv("MURMUR_TEST_REDIS_DB"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			return n
		}
	}
	return 1
}

// quietTestLogger keeps test output to test failures.
func quietTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newStore(t *testing.T, capacity int) *timeline.Store {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test: -short")
	}
	if testClient == nil {
		t.Fatal("no redis: TestMain did not start one")
	}
	if err := testClient.FlushDB(t.Context()).Err(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return timeline.NewStore(testClient, capacity)
}

func TestPushAndRange(t *testing.T) {
	store := newStore(t, 100)
	ctx := t.Context()

	followers := []string{"alice", "bob", "cara"}
	for i, id := range []string{"01AAA", "01BBB", "01CCC"} {
		if err := store.Push(ctx, followers, id, int64(i)); err != nil {
			t.Fatalf("Push() = %v", err)
		}
	}

	for _, follower := range followers {
		ids, err := store.Range(ctx, follower, "", 10)
		if err != nil {
			t.Fatalf("Range(%s) = %v", follower, err)
		}
		want := []string{"01CCC", "01BBB", "01AAA"}
		if len(ids) != len(want) {
			t.Fatalf("%s got %v, want %v", follower, ids, want)
		}
		for i := range want {
			if ids[i] != want[i] {
				t.Fatalf("%s got %v, want newest-first %v", follower, ids, want)
			}
		}
	}
}

// The property the whole phase rests on. JetStream redelivers on any failure,
// so the same event is processed more than once — and that has to be a no-op.
func TestPushIsIdempotent(t *testing.T) {
	store := newStore(t, 100)
	ctx := t.Context()

	for range 10 {
		if err := store.Push(ctx, []string{"alice"}, "01AAA", 1000); err != nil {
			t.Fatalf("Push() = %v", err)
		}
	}

	n, err := store.Len(ctx, "alice")
	if err != nil {
		t.Fatalf("Len() = %v", err)
	}
	if n != 1 {
		t.Errorf("timeline holds %d entries after 10 identical pushes, want 1", n)
	}
}

// The cap is an invariant, not a target. Redis runs a script to completion, so
// no interleaving of two workers' adds and trims can leave a timeline over it.
func TestTimelineIsCappedExactly(t *testing.T) {
	const capacity = 10
	store := newStore(t, capacity)
	ctx := t.Context()

	for i := range 50 {
		id := fmt.Sprintf("01%03d", i)
		if err := store.Push(ctx, []string{"alice"}, id, int64(i)); err != nil {
			t.Fatalf("Push() = %v", err)
		}
	}

	n, err := store.Len(ctx, "alice")
	if err != nil {
		t.Fatalf("Len() = %v", err)
	}
	if n != capacity {
		t.Errorf("timeline holds %d entries, want exactly the cap of %d", n, capacity)
	}

	// The survivors must be the newest, not an arbitrary ten.
	ids, err := store.Range(ctx, "alice", "", capacity)
	if err != nil {
		t.Fatalf("Range() = %v", err)
	}
	if ids[0] != "01049" {
		t.Errorf("newest entry is %q, want the most recent 01049", ids[0])
	}
	if ids[len(ids)-1] != "01040" {
		t.Errorf("oldest surviving entry is %q, want 01040", ids[len(ids)-1])
	}
}

// Concurrent writers must not be able to push a timeline past its cap. This is
// the interleaving the Lua script exists to prevent.
func TestCapHoldsUnderConcurrentWriters(t *testing.T) {
	const capacity = 20
	store := newStore(t, capacity)
	ctx := t.Context()

	done := make(chan error, 8)
	for worker := range 8 {
		go func() {
			for i := range 100 {
				id := fmt.Sprintf("01%d%04d", worker, i)
				if err := store.Push(ctx, []string{"alice"}, id, int64(i)); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}()
	}
	for range 8 {
		if err := <-done; err != nil {
			t.Fatalf("concurrent Push() = %v", err)
		}
	}

	n, err := store.Len(ctx, "alice")
	if err != nil {
		t.Fatalf("Len() = %v", err)
	}
	if n != capacity {
		t.Errorf("timeline holds %d entries after concurrent writes, want the cap of %d", n, capacity)
	}
}

func TestRangePagesWithACursor(t *testing.T) {
	store := newStore(t, 100)
	ctx := t.Context()

	for i := range 25 {
		id := fmt.Sprintf("01%03d", i)
		if err := store.Push(ctx, []string{"alice"}, id, int64(i)); err != nil {
			t.Fatalf("Push() = %v", err)
		}
	}

	seen := make(map[string]struct{}, 25)
	cursor := ""
	pages := 0

	for {
		page, err := store.Range(ctx, "alice", cursor, 10)
		if err != nil {
			t.Fatalf("Range() = %v", err)
		}
		if len(page) == 0 {
			break
		}
		for _, id := range page {
			if _, duplicate := seen[id]; duplicate {
				t.Fatalf("post %s appeared on two pages", id)
			}
			seen[id] = struct{}{}
		}
		pages++
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
		if len(page) < 10 {
			break
		}
		cursor = page[len(page)-1]
	}

	if len(seen) != 25 {
		t.Errorf("paged through %d posts, want 25", len(seen))
	}
}

// Posts created in the same millisecond share a score. Paging must still not
// repeat or skip them, which is why the cursor compares IDs rather than
// trusting the score bound alone.
func TestPagingSurvivesIdenticalScores(t *testing.T) {
	store := newStore(t, 100)
	ctx := t.Context()

	const sameMilli = int64(1_700_000_000_000)
	for i := range 20 {
		id := fmt.Sprintf("01%03d", i)
		if err := store.Push(ctx, []string{"alice"}, id, sameMilli); err != nil {
			t.Fatalf("Push() = %v", err)
		}
	}

	first, err := store.Range(ctx, "alice", "", 8)
	if err != nil {
		t.Fatalf("Range() = %v", err)
	}
	second, err := store.Range(ctx, "alice", first[len(first)-1], 8)
	if err != nil {
		t.Fatalf("Range(page 2) = %v", err)
	}

	seen := make(map[string]struct{}, 16)
	for _, id := range append(append([]string{}, first...), second...) {
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("post %s appeared twice across pages that share a score", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != 16 {
		t.Errorf("two pages of 8 yielded %d distinct posts, want 16", len(seen))
	}
}

func TestContainsAndClear(t *testing.T) {
	store := newStore(t, 100)
	ctx := t.Context()

	if err := store.Push(ctx, []string{"alice"}, "01AAA", 1); err != nil {
		t.Fatalf("Push() = %v", err)
	}

	present, err := store.Contains(ctx, "alice", "01AAA")
	if err != nil || !present {
		t.Fatalf("Contains() = (%v, %v), want (true, nil)", present, err)
	}

	absent, err := store.Contains(ctx, "alice", "01ZZZ")
	if err != nil || absent {
		t.Fatalf("Contains(missing) = (%v, %v), want (false, nil)", absent, err)
	}

	if err := store.Clear(ctx, "alice"); err != nil {
		t.Fatalf("Clear() = %v", err)
	}
	n, err := store.Len(ctx, "alice")
	if err != nil {
		t.Fatalf("Len() = %v", err)
	}
	if n != 0 {
		t.Errorf("timeline holds %d entries after Clear, want 0", n)
	}
}

func TestPushToNobodyIsNotAnError(t *testing.T) {
	store := newStore(t, 100)
	if err := store.Push(t.Context(), nil, "01AAA", 1); err != nil {
		t.Errorf("Push() to zero followers = %v, want nil", err)
	}
}

func TestRangeOfAnEmptyTimeline(t *testing.T) {
	store := newStore(t, 100)
	ids, err := store.Range(t.Context(), "nobody", "", 10)
	if err != nil {
		t.Fatalf("Range() = %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("got %v, want an empty timeline", ids)
	}
}

// A thousand-follower fanout is one pipelined round trip, not a thousand.
func TestPushToManyFollowersIsOneRoundTrip(t *testing.T) {
	store := newStore(t, 800)
	ctx := t.Context()

	followers := make([]string, 1000)
	for i := range followers {
		followers[i] = fmt.Sprintf("follower-%04d", i)
	}

	start := time.Now()
	if err := store.Push(ctx, followers, "01AAA", 1); err != nil {
		t.Fatalf("Push() = %v", err)
	}
	elapsed := time.Since(start)

	for _, follower := range []string{followers[0], followers[500], followers[999]} {
		present, err := store.Contains(ctx, follower, "01AAA")
		if err != nil || !present {
			t.Errorf("%s did not receive the post", follower)
		}
	}

	// Generous, because CI is slow; a thousand sequential round trips would
	// not come close to this even on a fast machine.
	if elapsed > 3*time.Second {
		t.Errorf("pushing to 1000 timelines took %v, which suggests it was not pipelined", elapsed)
	}
}
