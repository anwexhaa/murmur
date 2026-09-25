package gateway

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	authv1 "github.com/anwexhaa/murmur/api/gen/murmur/auth/v1"
	socialv1 "github.com/anwexhaa/murmur/api/gen/murmur/social/v1"
	"github.com/anwexhaa/murmur/internal/auth"
	"github.com/anwexhaa/murmur/internal/gateway/gqlmodel"
)

// AccessTokenTTL is how long an access token is accepted.
//
// Fifteen minutes, and the number is a consequence of the design rather than a
// preference. An access token is not revocable -- that is the whole point of a
// signed token, and it is why every request does not cost a database lookup --
// so its lifetime *is* the worst-case window between a compromise and the
// attacker losing access. Fifteen minutes is short enough for that window to
// be uncomfortable rather than catastrophic, and long enough that the refresh
// traffic is a trickle rather than a second request path.
const AccessTokenTTL = 15 * time.Minute

// Sessions turns an authenticated identity into the pair of tokens a client
// carries.
//
// The split of responsibility is deliberate. The refresh token is minted and
// stored by the auth service, because it is revocable state and revocable
// state belongs in Postgres. The access token is signed here and never stored
// anywhere, because a token the gateway can verify from its own key is the
// only kind that does not put a round trip in front of every request.
type Sessions struct {
	auth   authv1.AuthServiceClient
	social socialv1.SocialServiceClient
	signer *auth.Signer
	ttl    time.Duration
	now    func() time.Time
}

// NewSessions builds the session minter.
func NewSessions(
	authClient authv1.AuthServiceClient,
	socialClient socialv1.SocialServiceClient,
	signer *auth.Signer,
	ttl time.Duration,
	now func() time.Time,
) *Sessions {
	if ttl <= 0 {
		ttl = AccessTokenTTL
	}
	if now == nil {
		now = time.Now
	}
	return &Sessions{auth: authClient, social: socialClient, signer: signer, ttl: ttl, now: now}
}

// issue signs an access token and packages it with a refresh token.
func (s *Sessions) issue(ctx context.Context, userID, handle, refreshToken string, expiresAt time.Time) (*gqlmodel.AuthPayload, error) {
	access, err := s.signer.Sign(userID, handle, auth.AudienceClient, s.ttl)
	if err != nil {
		return nil, status.Error(codes.Internal, "could not issue an access token")
	}

	// The call is made as the new user rather than anonymously, which matters
	// for the fields on User that depend on who is asking.
	user, err := s.social.GetUser(WithViewer(ctx, userID),
		&socialv1.GetUserRequest{Key: &socialv1.GetUserRequest_Id{Id: userID}})
	if err != nil {
		return nil, err
	}

	_ = expiresAt // the refresh token's expiry; the payload reports the access token's.

	return &gqlmodel.AuthPayload{
		AccessToken:  access,
		RefreshToken: refreshToken,
		ExpiresAt:    s.now().Add(s.ttl),
		User:         userFromProto(user.GetUser()),
	}, nil
}

// start issues a refresh token for a freshly authenticated user and returns
// the full session.
func (s *Sessions) start(ctx context.Context, userID, handle string) (*gqlmodel.AuthPayload, error) {
	issued, err := s.auth.IssueRefreshToken(ctx, &authv1.IssueRefreshTokenRequest{UserId: userID})
	if err != nil {
		return nil, err
	}
	return s.issue(ctx, userID, handle, issued.GetRefreshToken(), asTime(issued.GetExpiresAt()))
}

func asTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}
