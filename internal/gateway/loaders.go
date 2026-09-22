package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/vikstrous/dataloadgen"

	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
	"github.com/anwexhaa/murmur/internal/gateway/gqlmodel"
)

type loadersKey struct{}

// Loaders batch the lookups a single GraphQL request makes.
//
// GraphQL resolves a list field by calling the same resolver once per element,
// and there is no way to express "fetch all fifty authors" in the resolver
// signature. A loader closes that gap: each call registers a key and waits, the
// keys accumulate for a moment, and one batched fetch answers all of them.
//
// Two properties make this correct rather than merely fast.
//
// First, loaders are built per request and die with it. A process-lifetime
// loader would be a cache shared by every user, and `viewerFollows` depends on
// who is asking — one viewer's answer would be served to another. That is not
// a performance bug, it is a data-leak bug, and it is why these are
// constructed in middleware rather than in the resolver root.
//
// Second, a loader deduplicates within the request. Fifty posts by the same
// author produce fifty Load calls and one key, so the batch has one entry, not
// fifty.
type Loaders struct {
	Users          *dataloadgen.Loader[string, *gqlmodel.User]
	FollowerCounts *dataloadgen.Loader[string, int]
	ViewerFollows  *dataloadgen.Loader[string, bool]
}

// loaderWait is how long a loader gathers keys before dispatching.
//
// Short enough to be invisible against a round trip, long enough for every
// sibling resolver in a page to land in the same batch. gqlgen resolves list
// elements concurrently, so in practice the batch fills in microseconds and
// this is a ceiling rather than a delay anyone pays.
const loaderWait = 500 * time.Microsecond

// NewLoaders builds the per-request loaders. viewer may be empty, in which
// case viewerFollows resolves to nothing rather than calling downstream.
func NewLoaders(clients *Clients, viewer string) *Loaders {
	return &Loaders{
		Users:          dataloadgen.NewLoader(batchUsers(clients), dataloadgen.WithWait(loaderWait)),
		FollowerCounts: dataloadgen.NewLoader(batchFollowerCounts(clients), dataloadgen.WithWait(loaderWait)),
		ViewerFollows:  dataloadgen.NewLoader(batchViewerFollows(clients, viewer), dataloadgen.WithWait(loaderWait)),
	}
}

// WithLoaders attaches loaders to a request context.
func WithLoaders(ctx context.Context, loaders *Loaders) context.Context {
	return context.WithValue(ctx, loadersKey{}, loaders)
}

// LoadersFrom returns this request's loaders, or nil outside a request.
func LoadersFrom(ctx context.Context) *Loaders {
	loaders, _ := ctx.Value(loadersKey{}).(*Loaders)
	return loaders
}

// batchUsers fetches many users in one call.
//
// A key that has no user resolves to nil with no error. A deleted author on
// one post must not fail the other forty-nine, and the batch RPC was designed
// in phase 1 to make exactly this distinction: missing IDs are absent from the
// response rather than an error.
func batchUsers(clients *Clients) func(context.Context, []string) ([]*gqlmodel.User, []error) {
	return func(ctx context.Context, ids []string) ([]*gqlmodel.User, []error) {
		out := make([]*gqlmodel.User, len(ids))
		errs := make([]error, len(ids))

		resp, err := clients.Social.BatchGetUsers(ctx, &socialv1.BatchGetUsersRequest{Ids: ids})
		if err != nil {
			for i := range errs {
				errs[i] = err
			}
			return out, errs
		}

		byID := resp.GetUsers()
		for i, id := range ids {
			if user, ok := byID[id]; ok {
				out[i] = userFromProto(user)
			}
		}
		return out, errs
	}
}

// batchFollowerCounts fetches many follower counts in one call.
//
// Unlike the entity batches, every requested ID comes back: a count of zero is
// an answer, not a miss. An absent key here would mean the service failed to
// answer, which is worth distinguishing.
func batchFollowerCounts(clients *Clients) func(context.Context, []string) ([]int, []error) {
	return func(ctx context.Context, ids []string) ([]int, []error) {
		out := make([]int, len(ids))
		errs := make([]error, len(ids))

		resp, err := clients.Social.BatchGetFollowerCounts(ctx, &socialv1.BatchGetFollowerCountsRequest{UserIds: ids})
		if err != nil {
			for i := range errs {
				errs[i] = err
			}
			return out, errs
		}

		counts := resp.GetFollowers()
		for i, id := range ids {
			count, ok := counts[id]
			if !ok {
				errs[i] = fmt.Errorf("no follower count returned for %s", id)
				continue
			}
			out[i] = int(count)
		}
		return out, errs
	}
}

// batchViewerFollows answers "does the viewer follow each of these" in one call.
//
// This is the batch form of IsFollowing, and it is FilterFollowing from phase 4
// rather than a new RPC: "which of these candidates does X follow" is exactly
// the question, and it was already needed by the read path's pull side. One
// query answers fifty resolvers.
//
// The viewer is captured when the loader is built, not passed per key, because
// it is fixed for the request — which is also why these loaders must not
// outlive it.
func batchViewerFollows(clients *Clients, viewer string) func(context.Context, []string) ([]bool, []error) {
	return func(ctx context.Context, ids []string) ([]bool, []error) {
		out := make([]bool, len(ids))
		errs := make([]error, len(ids))

		if viewer == "" {
			// Nobody is signed in. The resolver returns null before reaching
			// here, but a loader that called downstream with an empty follower
			// would be a bug waiting for a caller who forgot the check.
			return out, errs
		}

		// The viewer never follows themselves, and asking would waste a slot in
		// the candidate list.
		candidates := make([]string, 0, len(ids))
		for _, id := range ids {
			if id != viewer {
				candidates = append(candidates, id)
			}
		}
		if len(candidates) == 0 {
			return out, errs
		}

		resp, err := clients.Social.FilterFollowing(ctx, &socialv1.FilterFollowingRequest{
			FollowerId:   viewer,
			CandidateIds: candidates,
		})
		if err != nil {
			for i := range errs {
				errs[i] = err
			}
			return out, errs
		}

		followed := make(map[string]struct{}, len(resp.GetFolloweeIds()))
		for _, id := range resp.GetFolloweeIds() {
			followed[id] = struct{}{}
		}
		for i, id := range ids {
			_, out[i] = followed[id]
		}
		return out, errs
	}
}
