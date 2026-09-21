package gateway

import (
	"testing"

	"github.com/anwexhaa/murmur/internal/gateway/gqlmodel"
)

func posts(ids ...string) []*gqlmodel.Post {
	out := make([]*gqlmodel.Post, len(ids))
	for i, id := range ids {
		out[i] = &gqlmodel.Post{ID: id}
	}
	return out
}

func ids(posts []*gqlmodel.Post) []string {
	out := make([]string, len(posts))
	for i, p := range posts {
		out[i] = p.ID
	}
	return out
}

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

// ULIDs sort by creation time as plain strings, which is the whole reason the
// merge needs no timestamps. These IDs are ordered A < B < C < D so the
// expectations read as "newest first" without a table of dates.
func TestMergeReturnsNewestFirstAcrossStreams(t *testing.T) {
	merged := mergePostsNewestFirst([][]*gqlmodel.Post{
		posts("D", "A"),
		posts("C", "B"),
	}, 10)

	want := []string{"D", "C", "B", "A"}
	if got := ids(merged); !equal(got, want) {
		t.Errorf("merge = %v, want %v", got, want)
	}
}

func TestMergeTruncatesToTheRequestedSize(t *testing.T) {
	merged := mergePostsNewestFirst([][]*gqlmodel.Post{
		posts("F", "E", "D"),
		posts("C", "B", "A"),
	}, 2)

	want := []string{"F", "E"}
	if got := ids(merged); !equal(got, want) {
		t.Errorf("merge = %v, want the two newest %v", got, want)
	}
}

// The same post can arrive from two streams — you can follow an account that
// another followed account reposted from. A timeline showing one post twice
// is a visible bug, so the merge deduplicates.
func TestMergeDropsDuplicates(t *testing.T) {
	merged := mergePostsNewestFirst([][]*gqlmodel.Post{
		posts("C", "B"),
		posts("C", "A"),
	}, 10)

	want := []string{"C", "B", "A"}
	if got := ids(merged); !equal(got, want) {
		t.Errorf("merge = %v, want %v with no repeat of C", got, want)
	}
}

func TestMergeHandlesEmptyInput(t *testing.T) {
	if got := mergePostsNewestFirst(nil, 10); len(got) != 0 {
		t.Errorf("merge(nil) = %v, want empty", ids(got))
	}
	if got := mergePostsNewestFirst([][]*gqlmodel.Post{nil, {}}, 10); len(got) != 0 {
		t.Errorf("merge of empty streams = %v, want empty", ids(got))
	}
}

// The merge result feeds a keyset cursor, so its order must be total and
// stable. If two streams could produce a different order on different runs,
// pagination would skip or repeat posts.
func TestMergeIsStrictlyDescending(t *testing.T) {
	merged := mergePostsNewestFirst([][]*gqlmodel.Post{
		posts("H", "E", "B"),
		posts("G", "D", "A"),
		posts("F", "C"),
	}, 100)

	for i := 1; i < len(merged); i++ {
		if merged[i].ID >= merged[i-1].ID {
			t.Fatalf("not strictly descending at %d: %q then %q", i, merged[i-1].ID, merged[i].ID)
		}
	}
	if len(merged) != 8 {
		t.Errorf("got %d posts, want all 8", len(merged))
	}
}

// A next-page cursor on a short page costs every client one extra round trip
// to discover there is nothing more.
func TestConnectionOnlyOffersACursorWhenThePageIsFull(t *testing.T) {
	full := connection(posts("C", "B", "A"), 3)
	if !full.PageInfo.HasNextPage {
		t.Error("a full page should report hasNextPage")
	}
	if full.PageInfo.EndCursor == nil || *full.PageInfo.EndCursor != "A" {
		t.Errorf("endCursor = %v, want the last id", full.PageInfo.EndCursor)
	}

	short := connection(posts("C", "B"), 3)
	if short.PageInfo.HasNextPage {
		t.Error("a short page should not report hasNextPage")
	}
}

func TestConnectionOnEmptyResults(t *testing.T) {
	empty := connection(nil, 50)

	if empty.PageInfo.HasNextPage {
		t.Error("an empty page should not report hasNextPage")
	}
	if empty.PageInfo.EndCursor != nil {
		t.Errorf("endCursor = %v, want nil on an empty page", *empty.PageInfo.EndCursor)
	}
	if empty.Edges == nil {
		t.Error("edges should be an empty slice, not nil: GraphQL non-null lists cannot be null")
	}
}

// Each edge's cursor is its own ID, which is what makes the cursor usable as
// a keyset "resume after this" token.
func TestConnectionCursorsAreTheNodeIDs(t *testing.T) {
	conn := connection(posts("C", "B", "A"), 10)

	for _, edge := range conn.Edges {
		if edge.Cursor != edge.Node.ID {
			t.Errorf("cursor %q does not match node id %q", edge.Cursor, edge.Node.ID)
		}
	}
}

// Out of range is clamped rather than rejected. An unbounded page is a
// denial-of-service vector; a friendly cap is better manners than an error.
func TestPageSizeClamping(t *testing.T) {
	ptr := func(n int) *int { return &n }

	tests := []struct {
		name  string
		first *int
		want  int
	}{
		{"unset falls back", nil, 20},
		{"zero falls back", ptr(0), 20},
		{"negative falls back", ptr(-5), 20},
		{"in range is honoured", ptr(35), 35},
		{"at the cap", ptr(100), 100},
		{"over the cap is clamped", ptr(10000), 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pageSize(tt.first, 20, 100); got != tt.want {
				t.Errorf("pageSize(%v) = %d, want %d", tt.first, got, tt.want)
			}
		})
	}
}

func TestFanoutFallsBackWhenUnset(t *testing.T) {
	if got := (&Resolver{}).fanout(); got != DefaultTimelineFanout {
		t.Errorf("fanout() = %d, want the default %d", got, DefaultTimelineFanout)
	}
	if got := (&Resolver{TimelineFanout: 7}).fanout(); got != 7 {
		t.Errorf("fanout() = %d, want the configured 7", got)
	}
}
