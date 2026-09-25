package gateway

import (
	"github.com/anwexhaa/murmur/internal/gateway/gqlgen"
)

// Field costs.
//
// The numbers are relative to each other and nothing else. What matters is
// that a field which fans out costs more than a field that reads a value
// already in hand, and that the multiplier for a list is the page size the
// caller asked for rather than a constant.
const (
	// costLookup is a field that reaches a backend, even batched. Batching
	// makes fifty of these one call; it does not make them free, and the
	// budget is about the query a caller may write rather than the plan the
	// server happens to execute.
	costLookup = 2
	// costWrite is a mutation.
	costWrite = 10
	// costAuth is a mutation that runs argon2 over 19MB. It is priced far
	// above the others because it costs far more, and a budget that says
	// otherwise is a budget that permits the cheapest denial of service in
	// the schema.
	costAuth = 200
)

// Complexity prices the schema for the query budget.
//
// The interesting entries are the connections, where the cost is multiplied by
// the page size. A depth limit would wave through
//
//	{ timeline(first: 100) { edges { node { author { followerCount } } } } }
//
// because it is four levels deep, while it actually asks for a hundred posts
// and a hundred authors. Depth is a proxy for cost that stops being one the
// moment a list argument exists, and every interesting schema has list
// arguments.
func Complexity() gqlgen.ComplexityRoot {
	var root gqlgen.ComplexityRoot

	// The timeline costs its page size, and each post below it costs its own
	// child complexity. One query asking for 100 posts is a hundred times the
	// cost of one asking for 1, which is the fact a limit has to know.
	root.Query.Timeline = func(childComplexity int, first *int, _ *string) int {
		return pageSize(first, defaultPostPage, maxPostPage) * (childComplexity + costLookup)
	}
	root.User.Posts = func(childComplexity int, first *int, _ *string) int {
		return pageSize(first, defaultPostPage, maxPostPage) * (childComplexity + costLookup)
	}

	// The three fields phase 5 batched. Batched is not free, and pricing them
	// at zero because a DataLoader exists would let a caller ask for a hundred
	// authors' follower counts inside a budget set for none.
	root.User.FollowerCount = func(childComplexity int) int { return costLookup }
	root.User.ViewerFollows = func(childComplexity int) int { return costLookup }
	root.Post.Author = func(childComplexity int) int { return costLookup + childComplexity }

	root.Query.User = func(childComplexity int, _ string) int { return costLookup + childComplexity }
	root.Query.Post = func(childComplexity int, _ string) int { return costLookup + childComplexity }
	root.Query.Me = func(childComplexity int) int { return costLookup + childComplexity }

	root.Mutation.CreatePost = func(childComplexity int, _ string) int { return costWrite + childComplexity }
	root.Mutation.DeletePost = func(childComplexity int, _ string) int { return costWrite }
	root.Mutation.Follow = func(childComplexity int, _ string) int { return costWrite + childComplexity }
	root.Mutation.Unfollow = func(childComplexity int, _ string) int { return costWrite + childComplexity }

	root.Mutation.Register = func(childComplexity int, _, _, _ string) int { return costAuth + childComplexity }
	root.Mutation.Login = func(childComplexity int, _, _ string) int { return costAuth + childComplexity }
	root.Mutation.ChangePassword = func(childComplexity int, _, _ string) int { return costAuth }
	root.Mutation.Refresh = func(childComplexity int, _ string) int { return costWrite + childComplexity }
	root.Mutation.Logout = func(childComplexity int, _ string) int { return costWrite }

	// A subscription is priced like a timeline page, because that is what its
	// replay does on connect.
	root.Subscription.TimelineUpdates = func(childComplexity int, _ *string) int {
		return replayLimit * (childComplexity + costLookup)
	}

	return root
}

// The page size is resolved with the resolvers' own pageSize helper, so the
// price a query is charged matches the work it will actually cause. Two
// different clamps would mean a query charged for fifty posts and served a
// hundred.
