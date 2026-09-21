package social

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/anwexhaa/murmur/api/gen/murmur/events/v1"
	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
	"github.com/anwexhaa/murmur/internal/domain"
)

// Page size bounds. A caller asking for everything gets DefaultPageSize; a
// caller asking for a million gets MaxPageSize. Neither is an error, because
// an unbounded page is a denial-of-service vector rather than a feature, and
// clamping is friendlier than rejecting.
const (
	DefaultPageSize = 50
	MaxPageSize     = 1000

	// FanoutPageSize is the page the fanout worker uses from phase 3. It is
	// larger than a human-facing page for a reason: it is walking follower
	// edges by the hundred thousand, and every page is a round trip.
	FanoutPageSize = 1000
)

// Service implements the SocialService gRPC contract over the store.
type Service struct {
	socialv1.UnimplementedSocialServiceServer

	store *Store
	ids   *domain.IDGenerator
	now   func() time.Time
}

// NewService wires a service. now is injectable so tests can pin time.
func NewService(store *Store, ids *domain.IDGenerator, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{store: store, ids: ids, now: now}
}

// ---------------------------------------------------------------- users

func (s *Service) CreateUser(ctx context.Context, req *socialv1.CreateUserRequest) (*socialv1.CreateUserResponse, error) {
	user, err := domain.NewUser(req.GetHandle(), req.GetDisplayName(), s.now())
	if err != nil {
		return nil, toStatus(err)
	}
	if err := s.store.CreateUser(ctx, user); err != nil {
		return nil, toStatus(err)
	}
	return &socialv1.CreateUserResponse{User: userToProto(user)}, nil
}

func (s *Service) GetUser(ctx context.Context, req *socialv1.GetUserRequest) (*socialv1.GetUserResponse, error) {
	var (
		user domain.User
		err  error
	)

	switch key := req.GetKey().(type) {
	case *socialv1.GetUserRequest_Id:
		id, parseErr := domain.ParseUserID("id", key.Id)
		if parseErr != nil {
			return nil, toStatus(parseErr)
		}
		user, err = s.store.GetUser(ctx, id)
	case *socialv1.GetUserRequest_Handle:
		if validateErr := domain.ValidateHandle(key.Handle); validateErr != nil {
			return nil, toStatus(validateErr)
		}
		user, err = s.store.GetUserByHandle(ctx, key.Handle)
	default:
		return nil, status.Error(codes.InvalidArgument, "one of id or handle is required")
	}

	if err != nil {
		return nil, toStatus(err)
	}
	return &socialv1.GetUserResponse{User: userToProto(user)}, nil
}

func (s *Service) BatchGetUsers(ctx context.Context, req *socialv1.BatchGetUsersRequest) (*socialv1.BatchGetUsersResponse, error) {
	if len(req.GetIds()) > MaxPageSize {
		return nil, status.Errorf(codes.InvalidArgument, "at most %d ids per batch", MaxPageSize)
	}

	ids := make([]uuid.UUID, 0, len(req.GetIds()))
	for _, raw := range req.GetIds() {
		id, err := domain.ParseUserID("ids", raw)
		if err != nil {
			return nil, toStatus(err)
		}
		ids = append(ids, id)
	}

	users, err := s.store.BatchGetUsers(ctx, ids)
	if err != nil {
		return nil, toStatus(err)
	}

	out := make(map[string]*socialv1.User, len(users))
	for _, user := range users {
		out[user.ID.String()] = userToProto(user)
	}
	return &socialv1.BatchGetUsersResponse{Users: out}, nil
}

// --------------------------------------------------------- follow graph

func (s *Service) Follow(ctx context.Context, req *socialv1.FollowRequest) (*socialv1.FollowResponse, error) {
	follower, followee, err := s.edge(req.GetFollowerId(), req.GetFolloweeId())
	if err != nil {
		return nil, toStatus(err)
	}

	created, err := s.store.Follow(ctx, follower, followee)
	if err != nil {
		return nil, toStatus(err)
	}
	return &socialv1.FollowResponse{Created: created}, nil
}

func (s *Service) Unfollow(ctx context.Context, req *socialv1.UnfollowRequest) (*socialv1.UnfollowResponse, error) {
	follower, followee, err := s.edge(req.GetFollowerId(), req.GetFolloweeId())
	if err != nil {
		return nil, toStatus(err)
	}

	removed, err := s.store.Unfollow(ctx, follower, followee)
	if err != nil {
		return nil, toStatus(err)
	}
	return &socialv1.UnfollowResponse{Removed: removed}, nil
}

func (s *Service) ListFollowers(ctx context.Context, req *socialv1.ListFollowersRequest) (*socialv1.ListFollowersResponse, error) {
	id, err := domain.ParseUserID("user_id", req.GetUserId())
	if err != nil {
		return nil, toStatus(err)
	}

	limit := clampPageSize(req.GetPageSize())
	ids, err := s.store.ListFollowers(ctx, id, req.GetPageToken(), limit)
	if err != nil {
		return nil, toStatus(err)
	}

	return &socialv1.ListFollowersResponse{
		FollowerIds:   uuidStrings(ids),
		NextPageToken: nextToken(ids, limit),
	}, nil
}

func (s *Service) ListFollowing(ctx context.Context, req *socialv1.ListFollowingRequest) (*socialv1.ListFollowingResponse, error) {
	id, err := domain.ParseUserID("user_id", req.GetUserId())
	if err != nil {
		return nil, toStatus(err)
	}

	limit := clampPageSize(req.GetPageSize())
	ids, err := s.store.ListFollowing(ctx, id, req.GetPageToken(), limit)
	if err != nil {
		return nil, toStatus(err)
	}

	return &socialv1.ListFollowingResponse{
		FolloweeIds:   uuidStrings(ids),
		NextPageToken: nextToken(ids, limit),
	}, nil
}

func (s *Service) GetFollowerCount(ctx context.Context, req *socialv1.GetFollowerCountRequest) (*socialv1.GetFollowerCountResponse, error) {
	id, err := domain.ParseUserID("user_id", req.GetUserId())
	if err != nil {
		return nil, toStatus(err)
	}

	count, err := s.store.CountFollowers(ctx, id)
	if err != nil {
		return nil, toStatus(err)
	}
	return &socialv1.GetFollowerCountResponse{Followers: count}, nil
}

func (s *Service) BatchGetFollowerCounts(ctx context.Context, req *socialv1.BatchGetFollowerCountsRequest) (*socialv1.BatchGetFollowerCountsResponse, error) {
	if len(req.GetUserIds()) > MaxPageSize {
		return nil, status.Errorf(codes.InvalidArgument, "at most %d ids per batch", MaxPageSize)
	}

	ids := make([]uuid.UUID, 0, len(req.GetUserIds()))
	for _, raw := range req.GetUserIds() {
		id, err := domain.ParseUserID("user_ids", raw)
		if err != nil {
			return nil, toStatus(err)
		}
		ids = append(ids, id)
	}

	counts, err := s.store.BatchCountFollowers(ctx, ids)
	if err != nil {
		return nil, toStatus(err)
	}

	out := make(map[string]int64, len(counts))
	for id, count := range counts {
		out[id.String()] = count
	}
	return &socialv1.BatchGetFollowerCountsResponse{Followers: out}, nil
}

func (s *Service) IsFollowing(ctx context.Context, req *socialv1.IsFollowingRequest) (*socialv1.IsFollowingResponse, error) {
	follower, err := domain.ParseUserID("follower_id", req.GetFollowerId())
	if err != nil {
		return nil, toStatus(err)
	}
	followee, err := domain.ParseUserID("followee_id", req.GetFolloweeId())
	if err != nil {
		return nil, toStatus(err)
	}

	following, err := s.store.IsFollowing(ctx, follower, followee)
	if err != nil {
		return nil, toStatus(err)
	}
	return &socialv1.IsFollowingResponse{Following: following}, nil
}

// ---------------------------------------------------------------- posts

func (s *Service) CreatePost(ctx context.Context, req *socialv1.CreatePostRequest) (*socialv1.CreatePostResponse, error) {
	author, err := domain.ParseUserID("author_id", req.GetAuthorId())
	if err != nil {
		return nil, toStatus(err)
	}
	if err := domain.ValidateBody(req.GetBody()); err != nil {
		return nil, toStatus(err)
	}

	now := s.now()
	id, err := s.ids.New(now)
	if err != nil {
		return nil, status.Error(codes.Internal, "could not allocate a post id")
	}

	post := domain.Post{
		ID:        id,
		AuthorID:  author,
		Body:      strings.TrimSpace(req.GetBody()),
		CreatedAt: now.UTC(),
	}

	// The post and its event commit together. A post that exists but whose
	// event was lost would never reach a single timeline, and nothing
	// anywhere would record that the fanout was owed.
	payload, err := proto.Marshal(&eventsv1.PostCreated{
		PostId:    post.ID,
		AuthorId:  post.AuthorID.String(),
		CreatedAt: timestamppb.New(post.CreatedAt),
	})
	if err != nil {
		return nil, status.Error(codes.Internal, "could not encode the post event")
	}

	if err := s.store.CreatePostWithEvent(ctx, post, payload); err != nil {
		return nil, toStatus(err)
	}
	return &socialv1.CreatePostResponse{Post: postToProto(post)}, nil
}

func (s *Service) GetPost(ctx context.Context, req *socialv1.GetPostRequest) (*socialv1.GetPostResponse, error) {
	if err := domain.ValidatePostID("id", req.GetId()); err != nil {
		return nil, toStatus(err)
	}

	post, err := s.store.GetPost(ctx, req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &socialv1.GetPostResponse{Post: postToProto(post)}, nil
}

func (s *Service) BatchGetPosts(ctx context.Context, req *socialv1.BatchGetPostsRequest) (*socialv1.BatchGetPostsResponse, error) {
	if len(req.GetIds()) > MaxPageSize {
		return nil, status.Errorf(codes.InvalidArgument, "at most %d ids per batch", MaxPageSize)
	}
	for _, id := range req.GetIds() {
		if err := domain.ValidatePostID("ids", id); err != nil {
			return nil, toStatus(err)
		}
	}

	posts, err := s.store.BatchGetPosts(ctx, req.GetIds())
	if err != nil {
		return nil, toStatus(err)
	}

	out := make(map[string]*socialv1.Post, len(posts))
	for _, post := range posts {
		out[post.ID] = postToProto(post)
	}
	return &socialv1.BatchGetPostsResponse{Posts: out}, nil
}

func (s *Service) ListAuthorPosts(ctx context.Context, req *socialv1.ListAuthorPostsRequest) (*socialv1.ListAuthorPostsResponse, error) {
	author, err := domain.ParseUserID("author_id", req.GetAuthorId())
	if err != nil {
		return nil, toStatus(err)
	}
	if token := req.GetPageToken(); token != "" {
		if err := domain.ValidatePostID("page_token", token); err != nil {
			return nil, toStatus(err)
		}
	}

	limit := clampPageSize(req.GetPageSize())
	posts, err := s.store.ListAuthorPosts(ctx, author, req.GetPageToken(), limit)
	if err != nil {
		return nil, toStatus(err)
	}

	out := make([]*socialv1.Post, len(posts))
	for i, post := range posts {
		out[i] = postToProto(post)
	}

	token := ""
	if len(posts) == limit {
		token = posts[len(posts)-1].ID
	}
	return &socialv1.ListAuthorPostsResponse{Posts: out, NextPageToken: token}, nil
}

// --------------------------------------------------------------- shared

func (s *Service) edge(followerRaw, followeeRaw string) (uuid.UUID, uuid.UUID, error) {
	follower, err := domain.ParseUserID("follower_id", followerRaw)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	followee, err := domain.ParseUserID("followee_id", followeeRaw)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	if follower == followee {
		return uuid.Nil, uuid.Nil, fmt.Errorf("%w: a user cannot follow themselves", domain.ErrInvalid)
	}
	return follower, followee, nil
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

// nextToken returns a continuation token only when the page came back full.
// A short page means the end was reached, and handing out a token there would
// cost every caller one extra empty round trip.
func nextToken(ids []uuid.UUID, limit int) string {
	if len(ids) < limit {
		return ""
	}
	return ids[len(ids)-1].String()
}

func userToProto(user domain.User) *socialv1.User {
	return &socialv1.User{
		Id:          user.ID.String(),
		Handle:      user.Handle,
		DisplayName: user.DisplayName,
		CreatedAt:   timestamppb.New(user.CreatedAt),
	}
}

func postToProto(post domain.Post) *socialv1.Post {
	return &socialv1.Post{
		Id:        post.ID,
		AuthorId:  post.AuthorID.String(),
		Body:      post.Body,
		CreatedAt: timestamppb.New(post.CreatedAt),
	}
}

// toStatus maps domain errors onto gRPC codes. Anything unrecognised becomes
// Internal and keeps its detail out of the response, because an unmapped error
// is by definition one nobody decided was safe to show a client.
func toStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, domain.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request cancelled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "request deadline exceeded")
	default:
		return status.Error(codes.Internal, "internal error")
	}
}
