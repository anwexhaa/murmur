package gateway

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/vektah/gqlparser/v2/gqlerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
	"github.com/anwexhaa/murmur/internal/gateway/gqlmodel"
	"github.com/anwexhaa/murmur/internal/platform/grpcx"
	"github.com/anwexhaa/murmur/internal/platform/ratelimit"
)

// Resolver carries what every resolver needs.
//
// It holds clients, not stores: the gateway owns no database, and the only
// way it can learn anything is a round trip. That constraint is what makes
// the call count in this phase honest.
type Resolver struct {
	Clients *Clients
	Log     *slog.Logger
	// Hub delivers live updates. Nil disables subscriptions, which is what a
	// gateway built without a NATS connection gets.
	Hub *Hub

	// Hydrator turns the post IDs the hub delivers into posts, sharing one
	// fetch across every socket that was notified. Nil falls back to an
	// unshared lookup per update, which is correct and does not scale.
	Hydrator *Hydrator

	// Sessions mints the token pairs the auth mutations return. Nil disables
	// them, which is what a gateway built without a signing key gets.
	Sessions *Sessions

	// Limiter throttles the expensive mutations per identity. Nil disables
	// throttling, which is a development setting and logged as such at
	// startup.
	Limiter *ratelimit.Limiter

	// TimelineFanout caps how many followed accounts the naive timeline will
	// query. Without it, a request from an account following ten thousand
	// people would make ten thousand calls and time out — which is a true
	// fact about this design, but an unmeasurable one.
	TimelineFanout int
}

// DefaultTimelineFanout is the cap used when none is configured.
const DefaultTimelineFanout = 25

// Page-size policy. These live here rather than in schema.resolvers.go
// because gqlgen owns that file and strips anything that is not a resolver.
const (
	defaultPostPage = 20
	maxPostPage     = 100
)

func (r *Resolver) fanout() int {
	if r.TimelineFanout <= 0 {
		return DefaultTimelineFanout
	}
	return r.TimelineFanout
}

// ---------------------------------------------------------- conversions

func userFromProto(u *socialv1.User) *gqlmodel.User {
	if u == nil {
		return nil
	}
	return &gqlmodel.User{
		ID:          u.GetId(),
		Handle:      u.GetHandle(),
		DisplayName: u.GetDisplayName(),
		CreatedAt:   timeOrZero(u.GetCreatedAt()),
	}
}

func postFromProto(p *socialv1.Post) *gqlmodel.Post {
	if p == nil {
		return nil
	}
	return &gqlmodel.Post{
		ID:        p.GetId(),
		Body:      p.GetBody(),
		CreatedAt: timeOrZero(p.GetCreatedAt()),
		AuthorID:  p.GetAuthorId(),
	}
}

func timeOrZero(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

// connection builds a Relay page from posts already in newest-first order.
//
// A next-page cursor is only handed out when the page came back full. A short
// page means the end was reached, and offering a cursor there costs every
// client one extra round trip to discover there is nothing more.
func connection(posts []*gqlmodel.Post, limit int) *gqlmodel.PostConnection {
	edges := make([]*gqlmodel.PostEdge, len(posts))
	for i, post := range posts {
		edges[i] = &gqlmodel.PostEdge{Node: post, Cursor: post.ID}
	}

	info := &gqlmodel.PageInfo{HasNextPage: len(posts) == limit && limit > 0}
	if len(posts) > 0 {
		cursor := posts[len(posts)-1].ID
		info.EndCursor = &cursor
	}
	return &gqlmodel.PostConnection{Edges: edges, PageInfo: info}
}

// pageSize clamps a client-supplied page size. Out-of-range is clamped rather
// than rejected: an unbounded page is a denial-of-service vector, and a
// friendly cap is better manners than an error.
func pageSize(first *int, fallback, max int) int {
	if first == nil || *first <= 0 {
		return fallback
	}
	if *first > max {
		return max
	}
	return *first
}

// translate turns a gRPC error into one safe to return to a client. Codes the
// client can act on keep their message; anything else becomes a generic
// internal error, with the detail going to the log instead.
func (r *Resolver) translate(ctx context.Context, op string, err error) error {
	if err == nil {
		return nil
	}

	code := status.Code(err)
	switch code {
	case codes.NotFound, codes.InvalidArgument, codes.AlreadyExists,
		codes.PermissionDenied, codes.Unauthenticated, codes.FailedPrecondition:
		return clientError(status.Convert(err).Message(), code)
	}

	// Anything unmapped is, by definition, an error nobody decided was safe to
	// show a client. The detail goes to the log; the client gets a code.
	r.Log.Error("downstream call failed",
		"operation", op,
		"code", code.String(),
		"request_id", grpcx.RequestID(ctx),
		"error", err,
	)
	return clientError("internal error", codes.Internal)
}

// clientError carries a machine-readable code in the GraphQL error
// extensions, so a client can branch on NOT_FOUND without parsing an English
// sentence that may be reworded later.
func clientError(message string, code codes.Code) error {
	return &gqlerror.Error{
		Message:    message,
		Extensions: map[string]any{"code": code.String()},
	}
}

// mergePostsNewestFirst merges already-sorted streams and keeps the top n.
//
// ULIDs sort by creation time as plain strings, so "newest first" is a
// descending string comparison and merging needs no timestamps at all.
func mergePostsNewestFirst(streams [][]*gqlmodel.Post, n int) []*gqlmodel.Post {
	merged := make([]*gqlmodel.Post, 0, n)
	for _, stream := range streams {
		merged = append(merged, stream...)
	}

	sort.Slice(merged, func(i, j int) bool { return merged[i].ID > merged[j].ID })

	seen := make(map[string]struct{}, len(merged))
	out := merged[:0]
	for _, post := range merged {
		if _, duplicate := seen[post.ID]; duplicate {
			continue
		}
		seen[post.ID] = struct{}{}
		out = append(out, post)
		if len(out) == n {
			break
		}
	}
	return out
}

func requireViewer(ctx context.Context) (string, error) {
	viewer := Viewer(ctx)
	if viewer == "" {
		// Returned as a GraphQL error rather than the raw status, because a
		// status returned straight from a resolver reaches the client with
		// "rpc error: code = Unauthenticated desc = " glued to the front of
		// it -- gRPC framing leaking out of a GraphQL API, and a small
		// disclosure of how the edge is built.
		return "", clientError(
			"not signed in: send an "+AuthorizationHeader+": Bearer <access token> header",
			codes.Unauthenticated)
	}
	return viewer, nil
}

// errNoLoaders means a resolver ran outside a request that built loaders,
// which can only happen if the middleware was not wired. It is an internal
// error rather than a fallback to unbatched calls: silently falling back would
// restore the N+1 and hide it, and the whole project is about not doing that.
var errNoLoaders = status.Error(codes.Internal, "no per-request loaders in context")
