package fanout_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/anwexhaa/murmur/internal/timeline"
)

// The benchmark that chooses the fanout threshold.
//
// The question is not "how slow is a big fanout" — that is obvious. It is
// where the *total* cost of pushing stops being lower than the total cost of
// merging at read time, given that reads outnumber writes. Both sides are
// measured here against a real Redis, on the same machine, in the same run, so
// the comparison is between two measurements rather than between a measurement
// and an estimate.
//
//	make bench-threshold
//
// The write side is one post fanned out to F followers: F pipelined script
// invocations. The read side is one reader merging H author feeds into their
// timeline, which is what every reader pays on every read once an author stops
// being pushed to.
//
// Skipped without a Redis, because a benchmark against a mock would measure
// the mock.

func benchRedis(b *testing.B) *redis.Client {
	b.Helper()

	addr := os.Getenv("MURMUR_TEST_REDIS_ADDR")
	if addr == "" {
		b.Skip("set MURMUR_TEST_REDIS_ADDR to run the threshold benchmark")
	}

	client := redis.NewClient(&redis.Options{Addr: addr, DB: testRedisDB(), PoolSize: 64})
	if err := client.Ping(b.Context()).Err(); err != nil {
		b.Skipf("redis unreachable at %s: %v", addr, err)
	}
	b.Cleanup(func() { _ = client.Close() })
	return client
}

// BenchmarkPushFanout measures the write side: the cost of materialising one
// post into F follower timelines.
//
// This is the cost an author's publish imposes, and the cost that disappears
// when they cross the threshold.
func BenchmarkPushFanout(b *testing.B) {
	client := benchRedis(b)
	store := timeline.NewStore(client, 800)

	for _, followers := range []int{100, 1_000, 5_000, 10_000, 25_000, 50_000, 100_000} {
		b.Run(fmt.Sprintf("followers=%d", followers), func(b *testing.B) {
			ids := make([]string, followers)
			for i := range ids {
				ids[i] = fmt.Sprintf("bench-follower-%07d", i)
			}

			ctx := b.Context()
			if err := client.FlushDB(ctx).Err(); err != nil {
				b.Fatalf("flush: %v", err)
			}

			b.ResetTimer()
			b.ReportAllocs()

			post := 0
			for b.Loop() {
				post++
				if err := store.Push(ctx, ids, fmt.Sprintf("01BENCH%019d", post), int64(post)); err != nil {
					b.Fatalf("Push() = %v", err)
				}
			}

			b.StopTimer()
			// Per-follower cost, so the numbers are comparable across sizes.
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*followers), "ns/follower")
		})
	}
}

// BenchmarkPullRead measures the read side: the cost one reader pays to merge
// H author feeds into their own timeline.
//
// This is the cost that *appears* when an author crosses the threshold, and it
// is paid by every follower on every read — which is why the crossover is not
// simply "where pushing gets slow".
func BenchmarkPullRead(b *testing.B) {
	client := benchRedis(b)
	store := timeline.NewStore(client, 800)
	ctx := b.Context()

	const page = 50

	for _, heavyAuthors := range []int{1, 2, 5, 10, 20} {
		b.Run(fmt.Sprintf("heavy_authors=%d", heavyAuthors), func(b *testing.B) {
			if err := client.FlushDB(ctx).Err(); err != nil {
				b.Fatalf("flush: %v", err)
			}

			// One reader with a populated pushed timeline.
			reader := "bench-reader"
			for i := range 200 {
				if err := store.Push(ctx, []string{reader}, fmt.Sprintf("01PUSHED%018d", i), int64(i)); err != nil {
					b.Fatalf("seed timeline: %v", err)
				}
			}

			authors := make([]string, heavyAuthors)
			for a := range authors {
				authors[a] = fmt.Sprintf("bench-author-%03d", a)
				for i := range 200 {
					if err := store.PushAuthor(ctx, authors[a], fmt.Sprintf("01AUTH%02d%018d", a, i), int64(i)); err != nil {
						b.Fatalf("seed author feed: %v", err)
					}
				}
			}

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				pushed, err := store.Range(ctx, reader, "", page)
				if err != nil {
					b.Fatalf("Range() = %v", err)
				}
				streams, err := store.RangeAuthors(ctx, authors, page)
				if err != nil {
					b.Fatalf("RangeAuthors() = %v", err)
				}
				if got := timeline.MergeNewestFirst(append([][]string{pushed}, streams...), page); len(got) != page {
					b.Fatalf("merged %d ids, want %d", len(got), page)
				}
			}
		})
	}
}

// BenchmarkReadBaseline is the control: a reader whose timeline was fully
// pushed, merging nothing.
//
// The difference between this and BenchmarkPullRead is the price of the pull
// side, and it is the number the threshold decision divides into the price of
// the push side.
func BenchmarkReadBaseline(b *testing.B) {
	client := benchRedis(b)
	store := timeline.NewStore(client, 800)
	ctx := b.Context()

	if err := client.FlushDB(ctx).Err(); err != nil {
		b.Fatalf("flush: %v", err)
	}

	reader := "bench-reader"
	for i := range 200 {
		if err := store.Push(ctx, []string{reader}, fmt.Sprintf("01PUSHED%018d", i), int64(i)); err != nil {
			b.Fatalf("seed timeline: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		if _, err := store.Range(ctx, reader, "", 50); err != nil {
			b.Fatalf("Range() = %v", err)
		}
	}
}

// TestThresholdCrossover computes the crossover from the two costs above and
// prints the table that the ADR cites.
//
// It is a test rather than a benchmark because it needs to run both sides and
// compare them, which `go test -bench` cannot do on its own. It asserts
// nothing about the value — the whole point is that the number is measured,
// and asserting a particular answer would quietly turn the measurement into a
// restatement of an assumption.
func TestThresholdCrossover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: -short")
	}
	addr := os.Getenv("MURMUR_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set MURMUR_TEST_REDIS_ADDR to measure the crossover")
	}

	client := redis.NewClient(&redis.Options{Addr: addr, DB: testRedisDB(), PoolSize: 64})
	defer func() { _ = client.Close() }()

	ctx := t.Context()
	store := timeline.NewStore(client, 800)

	// The read-side premium: how much longer one read takes when it has to
	// merge one extra author feed.
	baseline := timeReads(t, ctx, store, nil, 200)
	withOne := timeReads(t, ctx, store, []string{"x-author"}, 200)
	premium := withOne - baseline

	t.Logf("read baseline (no merge)      : %v", baseline.Round(time.Microsecond))
	t.Logf("read with one merged author   : %v", withOne.Round(time.Microsecond))
	t.Logf("premium per merged author     : %v", premium.Round(time.Microsecond))

	t.Log("")
	t.Log("followers   push cost/post   reads to break even at 100:1 read:write")

	for _, followers := range []int{100, 1_000, 10_000, 50_000, 100_000} {
		pushCost := timePush(t, ctx, store, followers)

		// A post pushed to F followers costs pushCost once. Not pushing it
		// costs `premium` on every read by every follower. At R reads per
		// follower per post, pulling is cheaper when:
		//     pushCost > premium * F * R
		// Solving for the F where they are equal gives the crossover.
		var crossover float64
		if premium > 0 {
			crossover = float64(pushCost) / float64(premium)
		}

		t.Logf("%9d   %-14v   %.0f follower-reads", followers,
			pushCost.Round(time.Microsecond), crossover)
	}
}

func timePush(t *testing.T, ctx context.Context, store *timeline.Store, followers int) time.Duration {
	t.Helper()

	ids := make([]string, followers)
	for i := range ids {
		ids[i] = fmt.Sprintf("x-follower-%07d", i)
	}

	const rounds = 5
	start := time.Now()
	for r := range rounds {
		if err := store.Push(ctx, ids, fmt.Sprintf("01XPUSH%019d", r), int64(r)); err != nil {
			t.Fatalf("Push() = %v", err)
		}
	}
	return time.Since(start) / rounds
}

func timeReads(t *testing.T, ctx context.Context, store *timeline.Store, authors []string, rounds int) time.Duration {
	t.Helper()

	const reader = "x-reader"
	for i := range 200 {
		if err := store.Push(ctx, []string{reader}, fmt.Sprintf("01XREAD%019d", i), int64(i)); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	for _, a := range authors {
		for i := range 200 {
			if err := store.PushAuthor(ctx, a, fmt.Sprintf("01XAUTH%019d", i), int64(i)); err != nil {
				t.Fatalf("seed author: %v", err)
			}
		}
	}

	start := time.Now()
	for range rounds {
		pushed, err := store.Range(ctx, reader, "", 50)
		if err != nil {
			t.Fatalf("Range() = %v", err)
		}
		if len(authors) > 0 {
			streams, err := store.RangeAuthors(ctx, authors, 50)
			if err != nil {
				t.Fatalf("RangeAuthors() = %v", err)
			}
			timeline.MergeNewestFirst(append([][]string{pushed}, streams...), 50)
		}
	}
	return time.Since(start) / time.Duration(rounds)
}
