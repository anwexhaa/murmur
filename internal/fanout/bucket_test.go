package fanout

import "testing"

// Fanout latency is labelled by order of magnitude rather than recorded as one
// histogram. A hundred-follower fanout and a hundred-thousand-follower fanout
// are different operations that share code; averaging them describes neither,
// and phase 4's whole argument depends on being able to see them apart.
func TestFollowerBucketBoundaries(t *testing.T) {
	tests := []struct {
		followers int
		want      string
	}{
		{0, "lt_100"},
		{99, "lt_100"},
		{100, "100_1k"},
		{999, "100_1k"},
		{1_000, "1k_10k"},
		{9_999, "1k_10k"},
		{10_000, "10k_100k"},
		{99_999, "10k_100k"},
		{100_000, "gte_100k"},
		{500_000, "gte_100k"},
	}

	for _, tt := range tests {
		if got := followerBucket(tt.followers); got != tt.want {
			t.Errorf("followerBucket(%d) = %q, want %q", tt.followers, got, tt.want)
		}
	}
}
