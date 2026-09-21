package timeline

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
	timelinev1 "github.com/anwexhaa/murmur/api/gen/murmur/timeline/v1"
	"github.com/anwexhaa/murmur/internal/domain"
)

// Page size bounds, clamped rather than rejected for the same reason as
// everywhere else: an unbounded page is a denial-of-service vector, and a cap
// is better manners than an error.
const (
	DefaultPageSize = 50
	MaxPageSize     = 200
)

// Service implements the TimelineService contract.
//
// Two steps: read IDs from the materialised timeline, then hydrate them
// through the social service. The read is one Redis range regardless of how
// many accounts the viewer follows — which is the entire difference between
// this and the phase 2 gateway, where the same query cost one call per
// followed account.
type Service struct {
	timelinev1.UnimplementedTimelineServiceServer

	store  *Store
	social socialv1.SocialServiceClient
	log    *slog.Logger
}

// NewService wires a service.
func NewService(store *Store, social socialv1.SocialServiceClient, log *slog.Logger) *Service {
	return &Service{store: store, social: social, log: log}
}

func (s *Service) GetTimeline(ctx context.Context, req *timelinev1.GetTimelineRequest) (*timelinev1.GetTimelineResponse, error) {
	userID, err := domain.ParseUserID("user_id", req.GetUserId())
	if err != nil {
		return nil, toStatus(err)
	}
	if token := req.GetPageToken(); token != "" {
		if err := domain.ValidatePostID("page_token", token); err != nil {
			return nil, toStatus(err)
		}
	}

	limit := clampPageSize(req.GetPageSize())

	ids, err := s.store.Range(ctx, userID.String(), req.GetPageToken(), limit)
	if err != nil {
		s.log.Error("reading timeline", "user_id", userID, "error", err)
		return nil, status.Error(codes.Internal, "could not read the timeline")
	}
	if len(ids) == 0 {
		return &timelinev1.GetTimelineResponse{}, nil
	}

	// One batch call for the whole page, not one per post. The timeline
	// service was built after the gateway taught us what the alternative
	// costs.
	hydrated, err := s.social.BatchGetPosts(ctx, &socialv1.BatchGetPostsRequest{Ids: ids})
	if err != nil {
		return nil, toStatus(err)
	}

	// Rebuild the page in timeline order. The batch response is a map, and
	// the whole point of a feed is that its order is meaningful.
	byID := hydrated.GetPosts()
	posts := make([]*socialv1.Post, 0, len(ids))
	for _, id := range ids {
		if post, ok := byID[id]; ok {
			posts = append(posts, post)
		}
		// A missing ID means the post was deleted after being fanned out. The
		// timeline is a derived view and is allowed to be briefly stale, so
		// the entry is skipped rather than rendered as a gap or an error.
	}

	// The cursor is the last ID *read*, not the last post returned. Using the
	// last surviving post would make deleted entries re-read forever: the next
	// page would start before them and skip them again, one page at a time.
	token := ""
	if len(ids) == limit {
		token = ids[len(ids)-1]
	}

	return &timelinev1.GetTimelineResponse{
		Posts:         posts,
		NextPageToken: token,
		IdsRead:       int32(len(ids)),
	}, nil
}

func clampPageSize(requested int32) int {
	switch {
	case requested <= 0:
		return DefaultPageSize
	case requested > MaxPageSize:
		return MaxPageSize
	default:
		return int(requested)
	}
}

func toStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, domain.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request cancelled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "request deadline exceeded")
	default:
		// A status from a downstream call already carries a code the caller
		// can act on; passing it through is more useful than flattening
		// everything to Internal.
		if s, ok := status.FromError(err); ok && s.Code() != codes.Unknown {
			return err
		}
		return status.Error(codes.Internal, "internal error")
	}
}
