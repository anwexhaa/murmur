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
	heavy  *HeavySet
	cache  *PostCache
	log    *slog.Logger
}

// NewService wires a service. A nil heavy set disables the pull side; a nil
// cache hydrates straight from the social service, which is the phase 4
// behaviour and the baseline the cache is measured against.
func NewService(store *Store, social socialv1.SocialServiceClient, heavy *HeavySet, cache *PostCache, log *slog.Logger) *Service {
	return &Service{store: store, social: social, heavy: heavy, cache: cache, log: log}
}

// hydrate turns post IDs into posts, through the cache when there is one.
//
// This is where the viral-post problem lives. Every reader whose timeline
// contains a popular post asks for that same ID, so without a cache the
// number of identical downstream fetches is the number of concurrent readers.
func (s *Service) hydrate(ctx context.Context, ids []string) (map[string]*socialv1.Post, error) {
	if s.cache != nil {
		return s.cache.Get(ctx, ids)
	}

	resp, err := s.social.BatchGetPosts(ctx, &socialv1.BatchGetPostsRequest{Ids: ids})
	if err != nil {
		return nil, err
	}
	return resp.GetPosts(), nil
}

// assemble reads the viewer's timeline and merges in the heavy accounts they
// follow.
//
// The pushed timeline is one stream; each heavy account the viewer follows is
// another. Merged, they are indistinguishable from a timeline that had been
// fully materialised — which is the requirement, because the push/pull split
// is a cost decision and a reader must not be able to tell.
//
// Cost: one Redis range, one gRPC call to find which heavy accounts this
// viewer follows, and one pipelined Redis read covering all of them. It does
// not grow with how many accounts the viewer follows, only with how many
// *heavy* accounts they follow — which is bounded by the size of the heavy set
// and, in a power law, is a handful.
func (s *Service) assemble(ctx context.Context, userID, pageToken string, limit int) ([]string, error) {
	pushed, err := s.store.Range(ctx, userID, pageToken, limit)
	if err != nil {
		return nil, err
	}

	heavyIDs := s.followedHeavyAuthors(ctx, userID)
	if len(heavyIDs) == 0 {
		return pushed, nil
	}

	// Over-fetch from each author feed. A single heavy account could supply
	// the whole page, so each has to offer enough to fill it.
	streams, err := s.store.RangeAuthors(ctx, heavyIDs, limit)
	if err != nil {
		// The pull side failing must not fail the read. A timeline missing a
		// heavy account's posts is worse than complete and far better than an
		// error page, and it repairs itself on the next request.
		s.log.Warn("could not read author feeds; serving the pushed timeline only",
			"user_id", userID, "heavy_authors", len(heavyIDs), "error", err)
		return pushed, nil
	}

	merged := MergeNewestFirst(append([][]string{pushed}, streams...), limit)

	// A cursor page must not re-show what the caller already has. The pushed
	// stream was already cut at the cursor by Range; the author feeds were not,
	// because they are read by rank rather than by cursor.
	if pageToken != "" {
		filtered := merged[:0]
		for _, id := range merged {
			if id < pageToken {
				filtered = append(filtered, id)
			}
		}
		merged = filtered
	}

	return merged, nil
}

// followedHeavyAuthors returns the heavy accounts this viewer follows.
//
// Returns nothing rather than failing on error: the pull side is an
// optimisation to the read path, and a timeline that is briefly missing one
// account's posts is a far better outcome than a timeline that is missing
// entirely.
func (s *Service) followedHeavyAuthors(ctx context.Context, userID string) []string {
	if s.heavy == nil || !s.heavy.Enabled() {
		return nil
	}

	candidates := s.heavy.IDs()
	if len(candidates) == 0 {
		return nil
	}

	resp, err := s.social.FilterFollowing(ctx, &socialv1.FilterFollowingRequest{
		FollowerId:   userID,
		CandidateIds: candidates,
	})
	if err != nil {
		s.log.Warn("could not resolve followed heavy authors; serving the pushed timeline only",
			"user_id", userID, "error", err)
		return nil
	}
	return resp.GetFolloweeIds()
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

	ids, err := s.assemble(ctx, userID.String(), req.GetPageToken(), limit)
	if err != nil {
		s.log.Error("reading timeline", "user_id", userID, "error", err)
		return nil, status.Error(codes.Internal, "could not read the timeline")
	}
	if len(ids) == 0 {
		return &timelinev1.GetTimelineResponse{}, nil
	}

	// One batch for the whole page, not one call per post. The timeline
	// service was built after the gateway taught us what the alternative
	// costs.
	byID, err := s.hydrate(ctx, ids)
	if err != nil {
		return nil, toStatus(err)
	}
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
