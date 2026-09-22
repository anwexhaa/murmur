package timeline

import "container/heap"

// MergeNewestFirst merges already-sorted ID streams and returns the top n.
//
// This is the read side of the hybrid design. One stream is the viewer's
// pushed timeline; the others are the recent feeds of the heavy accounts they
// follow, which were never pushed to anyone. Merged, they are indistinguishable
// from a timeline that had been fully materialised — which is the whole point:
// the split is a cost decision, not a product decision, and a reader must not
// be able to tell.
//
// Every input must already be sorted newest-first, which they are: each comes
// from a Redis sorted set read in descending score order.
//
// A heap rather than "concatenate and sort". Sorting costs O(total log total)
// and touches every element of every stream; the heap costs O(n log k) for n
// results across k streams and stops as soon as the page is full. With a
// hundred-entry page and a handful of streams that difference is small, but it
// is the difference between work proportional to the page and work
// proportional to everything the viewer could have read.
//
// Cursors are compared as plain strings because post IDs are ULIDs, which sort
// lexicographically by creation time. No timestamps travel with the IDs and
// none are needed.
func MergeNewestFirst(streams [][]string, n int) []string {
	if n <= 0 {
		return nil
	}

	h := make(streamHeap, 0, len(streams))
	for _, stream := range streams {
		if len(stream) > 0 {
			h = append(h, cursor{ids: stream})
		}
	}
	if len(h) == 0 {
		return nil
	}
	heap.Init(&h)

	out := make([]string, 0, n)
	// A post can appear in more than one stream — a viewer can follow an
	// account that was pushed to them before it grew heavy, so the same ID sits
	// in both their timeline and the author's feed. Showing it twice is a
	// visible bug, and it is the specific bug the hybrid design introduces.
	seen := make(map[string]struct{}, n)

	for len(h) > 0 && len(out) < n {
		top := h[0]
		id := top.ids[top.at]

		if _, duplicate := seen[id]; !duplicate {
			seen[id] = struct{}{}
			out = append(out, id)
		}

		if top.at+1 < len(top.ids) {
			h[0].at++
			heap.Fix(&h, 0)
		} else {
			heap.Pop(&h)
		}
	}

	return out
}

// cursor is one stream and how far into it the merge has read.
type cursor struct {
	ids []string
	at  int
}

func (c cursor) head() string { return c.ids[c.at] }

type streamHeap []cursor

func (h streamHeap) Len() int { return len(h) }

// Greater-than, not less-than: the heap yields the newest ID first, and ULIDs
// sort ascending by time.
func (h streamHeap) Less(i, j int) bool { return h[i].head() > h[j].head() }

func (h streamHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *streamHeap) Push(x any) { *h = append(*h, x.(cursor)) }

func (h *streamHeap) Pop() any {
	old := *h
	last := len(old) - 1
	item := old[last]
	*h = old[:last]
	return item
}
