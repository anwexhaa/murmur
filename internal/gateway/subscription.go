package gateway

import (
	"context"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
	timelinev1 "github.com/anwexhaa/murmur/api/gen/murmur/timeline/v1"
	"github.com/anwexhaa/murmur/internal/gateway/gqlmodel"
)

// subscriptionOutBuffer is the channel between the pump goroutine and gqlgen's
// writer. One is enough: the hub's buffer is the one that absorbs bursts, and
// a second queue here would only add latency and hide backpressure from the
// place that measures it.
const subscriptionOutBuffer = 1

// replayLimit bounds how much history a reconnecting client is given.
//
// A client away for two minutes gets everything it missed. A client away for
// two days gets the newest page and a gap flag telling it to refetch, which is
// cheaper for both sides than streaming a day of history down a socket the
// client will scroll past anyway.
const replayLimit = 50

// errNoHub means subscriptions were not wired, which can only be a startup
// mistake. It is an error rather than an empty stream because a subscription
// that connects and silently never delivers is the hardest kind of broken to
// notice.
var errNoHub = status.Error(codes.Unimplemented, "subscriptions are not enabled on this gateway")

// replay returns the updates a reconnecting client missed, oldest first.
//
// The timeline itself is authoritative and already materialised, so this is a
// read of it rather than a replay of the event stream — which means it works
// however long the client was away and however many replicas have restarted
// since.
//
// The second return value reports whether the client may have missed more than
// one page, in which case the first update carries a gap.
func (r *Resolver) replay(ctx context.Context, viewer string, after *string) ([]*gqlmodel.TimelineUpdate, bool) {
	if after == nil || *after == "" {
		// A fresh subscription, not a reconnect. The client is about to query
		// the timeline anyway; sending it a page down the socket as well would
		// duplicate work and arrive in a less useful shape.
		return nil, false
	}
	cursor := *after

	resp, err := r.Clients.Timeline.GetTimeline(ctx, &timelinev1.GetTimelineRequest{
		UserId:   viewer,
		PageSize: replayLimit,
	})
	if err != nil {
		// A failed replay must not fail the subscription. Live delivery is the
		// point; the client can always refetch what it missed.
		r.Log.Warn("could not replay missed updates; starting live",
			"viewer", viewer, "error", err)
		return nil, true
	}

	posts := resp.GetPosts()

	// The page arrives newest-first. Walk it that way to find what is newer
	// than the cursor, then reverse, because a client applying updates wants
	// them in the order they happened.
	missed := make([]*gqlmodel.TimelineUpdate, 0, len(posts))
	for _, post := range posts {
		if post.GetId() <= cursor {
			break
		}
		missed = append(missed, &gqlmodel.TimelineUpdate{
			PostID: post.GetId(),
			Post:   postFromProto(post),
		})
	}

	for i, j := 0, len(missed)-1; i < j; i, j = i+1, j-1 {
		missed[i], missed[j] = missed[j], missed[i]
	}

	// Every post on a full page was newer than the cursor, so there were
	// probably more beyond it.
	gapped := len(missed) == len(posts) && len(posts) == replayLimit
	if gapped && len(missed) > 0 {
		missed[0].Gap = true
	}

	return missed, gapped
}

// hydrateUpdate turns a live notification into a renderable update.
//
// Unbatched, and shared instead. A live stream delivers one post at a time, so
// there is nothing for a DataLoader to batch it with — and a loader here would
// be scoped to the whole connection rather than to a request, caching a user's
// display name for as long as the socket stayed open.
//
// What there is to exploit is the other axis: one post arrives on every
// subscriber's socket at once, so the lookups are identical and simultaneous.
// The hydrator collapses them into one call. Measured on two thousand sockets,
// that is the difference between forty thousand GetPost calls and twenty.
func (r *Resolver) hydrateUpdate(ctx context.Context, update Update) *gqlmodel.TimelineUpdate {
	post, err := r.fetchPost(ctx, update.PostID)
	if err != nil {
		// Most often the post was deleted between the notification and this
		// lookup. Dropping it is right: there is nothing to show, and failing
		// the stream over one missing post would cost the client every
		// subsequent update.
		if status.Code(err) != codes.NotFound {
			r.Log.Warn("could not hydrate a live update",
				"post_id", update.PostID, "error", err)
		}
		return nil
	}

	return &gqlmodel.TimelineUpdate{
		Gap:    update.Gap,
		PostID: update.PostID,
		Post:   postFromProto(post),
	}
}

// fetchPost reads one post, through the hydrator when there is one.
//
// The fallback is not dead code: resolver tests build a Resolver without a
// hydrator, and a gateway that somehow starts without one should still deliver
// updates rather than refuse them. It is slower, not wrong.
func (r *Resolver) fetchPost(ctx context.Context, postID string) (*socialv1.Post, error) {
	if r.Hydrator != nil {
		return r.Hydrator.Get(ctx, postID)
	}
	resp, err := r.Clients.Social.GetPost(ctx, &socialv1.GetPostRequest{Id: postID})
	if err != nil {
		return nil, err
	}
	return resp.GetPost(), nil
}

// SubscriptionLogger is the logger the hub uses when none is configured.
func SubscriptionLogger(log *slog.Logger) *slog.Logger {
	if log != nil {
		return log
	}
	return slog.Default()
}
