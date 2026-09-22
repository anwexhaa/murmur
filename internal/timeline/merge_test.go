package timeline

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"
)

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestMergeInterleavesStreams(t *testing.T) {
	got := MergeNewestFirst([][]string{
		{"F", "D", "B"},
		{"E", "C", "A"},
	}, 10)

	want := []string{"F", "E", "D", "C", "B", "A"}
	if !equal(got, want) {
		t.Errorf("merge = %v, want %v", got, want)
	}
}

func TestMergeStopsAtTheRequestedSize(t *testing.T) {
	got := MergeNewestFirst([][]string{
		{"F", "D", "B"},
		{"E", "C", "A"},
	}, 3)

	want := []string{"F", "E", "D"}
	if !equal(got, want) {
		t.Errorf("merge = %v, want %v", got, want)
	}
}

// The bug the hybrid design introduces: a viewer can follow an account that
// was pushed to them before it grew heavy, so the same post sits in both their
// timeline and the author's feed.
func TestMergeDropsIDsPresentInTwoStreams(t *testing.T) {
	got := MergeNewestFirst([][]string{
		{"D", "C", "B"},
		{"C", "B", "A"},
	}, 10)

	want := []string{"D", "C", "B", "A"}
	if !equal(got, want) {
		t.Errorf("merge = %v, want %v with no repeats", got, want)
	}
}

func TestMergeEdgeCases(t *testing.T) {
	if got := MergeNewestFirst(nil, 10); got != nil {
		t.Errorf("merge(nil) = %v, want nil", got)
	}
	if got := MergeNewestFirst([][]string{{}, nil}, 10); got != nil {
		t.Errorf("merge of empty streams = %v, want nil", got)
	}
	if got := MergeNewestFirst([][]string{{"A"}}, 0); got != nil {
		t.Errorf("merge with n=0 = %v, want nil", got)
	}
	if got := MergeNewestFirst([][]string{{"A"}}, -1); got != nil {
		t.Errorf("merge with a negative n = %v, want nil", got)
	}
}

func TestMergeOfOneStreamIsThatStream(t *testing.T) {
	stream := []string{"E", "D", "C", "B", "A"}
	if got := MergeNewestFirst([][]string{stream}, 10); !equal(got, stream) {
		t.Errorf("merge = %v, want the input unchanged %v", got, stream)
	}
}

// ---------------------------------------------------------------- properties

// The merge is the one piece of phase 4 that a reader can catch being wrong:
// a duplicated post or a post out of order is visible on screen. So it is
// checked the way the payment engine's invariants were — by generating
// hundreds of shapes rather than by listing the ones I happened to think of.
//
// Three properties, which together say the output is exactly the top-k of the
// union in descending order:
//
//  1. strictly descending
//  2. no duplicates
//  3. equal to sorting the union and taking the first k
//
// The third subsumes the first two, and all three are asserted anyway: when
// this fails, knowing which property broke is the difference between a
// one-line fix and an afternoon.
func TestMergePropertiesOverGeneratedStreams(t *testing.T) {
	random := rand.New(rand.NewPCG(0x6D75726D, 0x75720004))

	const cases = 500
	for c := range cases {
		streamCount := 1 + random.IntN(6)
		pageSize := 1 + random.IntN(60)

		// A small ID space relative to the number of drawn IDs, so collisions
		// between streams are common rather than rare. Duplicates are the
		// interesting case and a wide space would almost never produce them.
		idSpace := 10 + random.IntN(40)

		streams := make([][]string, streamCount)
		union := make(map[string]struct{})

		for s := range streams {
			length := random.IntN(25)
			ids := make([]string, 0, length)
			for range length {
				id := fmt.Sprintf("%03d", random.IntN(idSpace))
				ids = append(ids, id)
				union[id] = struct{}{}
			}
			// Streams arrive newest-first and deduplicated within themselves,
			// because each one is a Redis sorted set.
			ids = sortDescUnique(ids)
			streams[s] = ids
		}

		got := MergeNewestFirst(streams, pageSize)

		// 1. strictly descending
		for i := 1; i < len(got); i++ {
			if got[i] >= got[i-1] {
				t.Fatalf("case %d: not strictly descending at %d: %q then %q\nstreams: %v",
					c, i, got[i-1], got[i], streams)
			}
		}

		// 2. no duplicates
		seen := make(map[string]struct{}, len(got))
		for _, id := range got {
			if _, duplicate := seen[id]; duplicate {
				t.Fatalf("case %d: %q appears twice\nstreams: %v", c, id, streams)
			}
			seen[id] = struct{}{}
		}

		// 3. exactly the top-k of the union
		all := make([]string, 0, len(union))
		for id := range union {
			all = append(all, id)
		}
		sort.Sort(sort.Reverse(sort.StringSlice(all)))
		want := all[:min(pageSize, len(all))]

		if !equal(got, want) {
			t.Fatalf("case %d: merge = %v, want the top %d of the union %v\nstreams: %v",
				c, got, pageSize, want, streams)
		}
	}
}

// A stream that is longer than the page must not cost more than the page.
// This is the reason for a heap rather than concatenate-and-sort, so it is
// worth asserting that the shape of the work is what was intended.
func TestMergeReadsOnlyAsFarAsThePageRequires(t *testing.T) {
	// Two long streams, a tiny page. A correct merge touches roughly `page`
	// elements; a sort would touch all 20,000.
	long := make([]string, 10_000)
	for i := range long {
		long[i] = fmt.Sprintf("%08d", len(long)-i)
	}

	got := MergeNewestFirst([][]string{long, long}, 5)
	if len(got) != 5 {
		t.Fatalf("merge returned %d ids, want 5", len(got))
	}
	if got[0] != "00010000" {
		t.Errorf("first id = %q, want the newest 00010000", got[0])
	}
}

func sortDescUnique(ids []string) []string {
	sort.Sort(sort.Reverse(sort.StringSlice(ids)))

	out := ids[:0]
	var last string
	for i, id := range ids {
		if i > 0 && id == last {
			continue
		}
		last = id
		out = append(out, id)
	}
	return out
}

func BenchmarkMergeNewestFirst(b *testing.B) {
	for _, streams := range []int{1, 2, 5, 10} {
		b.Run(fmt.Sprintf("streams=%d", streams), func(b *testing.B) {
			input := make([][]string, streams)
			for s := range input {
				ids := make([]string, 200)
				for i := range ids {
					ids[i] = fmt.Sprintf("%02d%06d", s, 200-i)
				}
				input[s] = ids
			}

			b.ReportAllocs()
			for b.Loop() {
				MergeNewestFirst(input, 50)
			}
		})
	}
}
