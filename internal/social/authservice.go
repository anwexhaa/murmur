package social

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	authv1 "github.com/anwexhaa/murmur/api/gen/murmur/auth/v1"
	"github.com/anwexhaa/murmur/internal/auth"
	"github.com/anwexhaa/murmur/internal/domain"
)

// Password length bounds.
//
// The minimum is NIST's, not the folklore one: length is the only requirement
// that reliably adds entropy, and composition rules ("one symbol, one digit")
// mostly teach people to write Password1! . The maximum exists because argon2
// hashes whatever it is given and a 10MB password is a free way to spend the
// server's memory.
const (
	MinPasswordLength = 12
	MaxPasswordLength = 256
)

// RefreshTokenTTL is how long a refresh token remains exchangeable.
//
// Thirty days, against an access token's fifteen minutes. The asymmetry is the
// point of having two: the short one is presented on every request and is the
// one that leaks, so it expires before a leak is useful; the long one is
// presented rarely, is revocable, and is what stops a user having to type a
// password every quarter of an hour.
const RefreshTokenTTL = 30 * 24 * time.Hour

// AuthService implements the AuthService gRPC contract.
type AuthService struct {
	authv1.UnimplementedAuthServiceServer

	store  *Store
	hasher *auth.Hasher
	ids    *domain.IDGenerator
	log    *slog.Logger
	now    func() time.Time
	ttl    time.Duration

	// decoy is a valid hash of a password nobody knows. See Authenticate.
	decoy string
}

// NewAuthService builds the service.
func NewAuthService(store *Store, hasher *auth.Hasher, ids *domain.IDGenerator, log *slog.Logger, now func() time.Time) (*AuthService, error) {
	if now == nil {
		now = time.Now
	}
	if hasher == nil {
		hasher = auth.NewHasher(auth.HashParams{})
	}

	// Hashed once at startup with the real parameters, so verifying against it
	// costs exactly what verifying a real credential costs.
	decoy, err := hasher.Hash("a password no account has")
	if err != nil {
		return nil, fmt.Errorf("prepare decoy hash: %w", err)
	}

	return &AuthService{
		store:  store,
		hasher: hasher,
		ids:    ids,
		log:    log,
		now:    now,
		ttl:    RefreshTokenTTL,
		decoy:  decoy,
	}, nil
}

// Register creates an account.
func (a *AuthService) Register(ctx context.Context, req *authv1.RegisterRequest) (*authv1.RegisterResponse, error) {
	if err := validatePassword(req.GetPassword()); err != nil {
		return nil, toStatus(err)
	}

	user, err := domain.NewUser(req.GetHandle(), req.GetDisplayName(), a.now())
	if err != nil {
		return nil, toStatus(err)
	}

	hash, err := a.hasher.Hash(req.GetPassword())
	if err != nil {
		a.log.Error("hashing a password failed", "error", err)
		return nil, status.Error(codes.Internal, "internal error")
	}

	if err := a.store.CreateUserWithCredential(ctx, user, hash); err != nil {
		return nil, toStatus(err)
	}

	return &authv1.RegisterResponse{
		UserId:      user.ID.String(),
		Handle:      user.Handle,
		DisplayName: user.DisplayName,
		CreatedAt:   timestamppb.New(user.CreatedAt),
	}, nil
}

// Authenticate checks a password.
//
// Two things here are about what the *failure* reveals rather than what the
// success does.
//
// First, an unknown handle still runs a full argon2 verification, against a
// decoy hash. Without it, "no such user" returns in microseconds and "wrong
// password" returns in fifty milliseconds, and the difference is a fast,
// silent oracle for deciding which handles exist -- which is the first step of
// every credential-stuffing run.
//
// Second, both failures return the same Unauthenticated status with the same
// message. Saying "no such account" is the same disclosure made explicit.
func (a *AuthService) Authenticate(ctx context.Context, req *authv1.AuthenticateRequest) (*authv1.AuthenticateResponse, error) {
	handle := req.GetHandle()
	password := req.GetPassword()

	userID, actualHandle, stored, err := a.store.CredentialByHandle(ctx, handle)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			// Deliberately ignoring the result: the work is the point.
			_ = auth.Verify(password, a.decoy)
			return nil, errInvalidCredentials
		}
		return nil, toStatus(err)
	}

	if err := auth.Verify(password, stored); err != nil {
		return nil, errInvalidCredentials
	}

	// The one moment the plaintext exists is the one moment the stored hash
	// can be upgraded. A failure here is not a failed login -- the user gave
	// the right password and should be let in regardless.
	if a.hasher.NeedsRehash(stored) {
		if rehashed, err := a.hasher.Hash(password); err == nil {
			if err := a.store.UpdateCredential(ctx, userID, rehashed, a.now()); err != nil {
				a.log.Warn("could not upgrade a password hash", "user_id", userID, "error", err)
			}
		}
	}

	return &authv1.AuthenticateResponse{UserId: userID.String(), Handle: actualHandle}, nil
}

// ChangePassword rotates a credential and ends every session.
func (a *AuthService) ChangePassword(ctx context.Context, req *authv1.ChangePasswordRequest) (*authv1.ChangePasswordResponse, error) {
	userID, err := uuid.Parse(req.GetUserId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "user id must be a uuid")
	}
	if err := validatePassword(req.GetNewPassword()); err != nil {
		return nil, toStatus(err)
	}

	stored, err := a.store.CredentialByUser(ctx, userID)
	if err != nil {
		return nil, toStatus(err)
	}
	// The current password is required even though the caller is already
	// authenticated: it is what stops a stolen access token from becoming a
	// permanent account takeover.
	if err := auth.Verify(req.GetCurrentPassword(), stored); err != nil {
		return nil, errInvalidCredentials
	}

	hash, err := a.hasher.Hash(req.GetNewPassword())
	if err != nil {
		a.log.Error("hashing a password failed", "error", err)
		return nil, status.Error(codes.Internal, "internal error")
	}

	now := a.now()
	if err := a.store.UpdateCredential(ctx, userID, hash, now); err != nil {
		return nil, toStatus(err)
	}

	// Every existing session dies with the old password. Someone changing
	// their password because they think it was stolen expects exactly this,
	// and the alternative -- an attacker's session surviving the change -- is
	// the whole reason they did it.
	revoked, err := a.store.RevokeAllSessions(ctx, userID, now)
	if err != nil {
		return nil, toStatus(err)
	}

	return &authv1.ChangePasswordResponse{SessionsRevoked: int32(revoked)}, nil
}

// IssueRefreshToken starts a session.
func (a *AuthService) IssueRefreshToken(ctx context.Context, req *authv1.IssueRefreshTokenRequest) (*authv1.IssueRefreshTokenResponse, error) {
	userID, err := uuid.Parse(req.GetUserId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "user id must be a uuid")
	}

	token, err := auth.NewRefreshToken()
	if err != nil {
		a.log.Error("minting a refresh token failed", "error", err)
		return nil, status.Error(codes.Internal, "internal error")
	}

	record, err := a.store.IssueRefreshToken(ctx, userID, token.Hash, a.now(), a.ttl)
	if err != nil {
		return nil, toStatus(err)
	}

	return &authv1.IssueRefreshTokenResponse{
		RefreshToken: token.Plaintext,
		FamilyId:     record.FamilyID.String(),
		ExpiresAt:    timestamppb.New(record.ExpiresAt),
	}, nil
}

// RotateRefreshToken exchanges a token for its successor.
func (a *AuthService) RotateRefreshToken(ctx context.Context, req *authv1.RotateRefreshTokenRequest) (*authv1.RotateRefreshTokenResponse, error) {
	presented := req.GetRefreshToken()
	if presented == "" {
		return nil, status.Error(codes.InvalidArgument, "a refresh token is required")
	}

	successor, err := auth.NewRefreshToken()
	if err != nil {
		a.log.Error("minting a refresh token failed", "error", err)
		return nil, status.Error(codes.Internal, "internal error")
	}

	record, err := a.store.RotateRefreshToken(ctx,
		auth.HashRefreshToken(presented), successor.Hash, a.now(), a.ttl)
	if err != nil {
		if errors.Is(err, ErrTokenReused) {
			// Worth a log line at warning level: this is the one event in the
			// auth path that means something may actually be wrong.
			a.log.Warn("refresh token reuse detected; session family revoked", "error", err)
			return nil, status.Error(codes.PermissionDenied, "session revoked; sign in again")
		}
		if errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrForbidden) {
			// One answer for expired, revoked and unknown. A client can do
			// nothing different with the distinction, and an attacker probing
			// tokens can.
			return nil, status.Error(codes.PermissionDenied, "session revoked; sign in again")
		}
		return nil, toStatus(err)
	}

	return &authv1.RotateRefreshTokenResponse{
		UserId:       record.UserID.String(),
		RefreshToken: successor.Plaintext,
		FamilyId:     record.FamilyID.String(),
		ExpiresAt:    timestamppb.New(record.ExpiresAt),
	}, nil
}

// RevokeSession ends one session.
func (a *AuthService) RevokeSession(ctx context.Context, req *authv1.RevokeSessionRequest) (*authv1.RevokeSessionResponse, error) {
	now := a.now()

	switch key := req.GetKey().(type) {
	case *authv1.RevokeSessionRequest_RefreshToken:
		revoked, err := a.store.RevokeFamilyByToken(ctx, auth.HashRefreshToken(key.RefreshToken), now)
		if err != nil {
			return nil, toStatus(err)
		}
		return &authv1.RevokeSessionResponse{SessionsRevoked: int32(revoked)}, nil

	case *authv1.RevokeSessionRequest_FamilyId:
		familyID, err := uuid.Parse(key.FamilyId)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "family id must be a uuid")
		}
		revoked, err := a.store.RevokeFamily(ctx, familyID, now)
		if err != nil {
			return nil, toStatus(err)
		}
		return &authv1.RevokeSessionResponse{SessionsRevoked: int32(revoked)}, nil

	default:
		return nil, status.Error(codes.InvalidArgument, "a refresh token or family id is required")
	}
}

// RevokeAllSessions logs a user out everywhere.
func (a *AuthService) RevokeAllSessions(ctx context.Context, req *authv1.RevokeAllSessionsRequest) (*authv1.RevokeAllSessionsResponse, error) {
	userID, err := uuid.Parse(req.GetUserId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "user id must be a uuid")
	}

	revoked, err := a.store.RevokeAllSessions(ctx, userID, a.now())
	if err != nil {
		return nil, toStatus(err)
	}
	return &authv1.RevokeAllSessionsResponse{SessionsRevoked: int32(revoked)}, nil
}

// errInvalidCredentials is the single answer to every kind of failed sign-in.
var errInvalidCredentials = status.Error(codes.Unauthenticated, "invalid handle or password")

func validatePassword(password string) error {
	switch {
	case len(password) < MinPasswordLength:
		return fmt.Errorf("%w: password must be at least %d characters", domain.ErrInvalid, MinPasswordLength)
	case len(password) > MaxPasswordLength:
		return fmt.Errorf("%w: password must be at most %d characters", domain.ErrInvalid, MaxPasswordLength)
	default:
		return nil
	}
}
