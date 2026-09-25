// Package authx loads the deployment's keys from configuration.
//
// It exists so internal/auth can stay a pure package with no idea that
// environment variables exist, while the four binaries share one answer to
// "what happens when the key is missing" instead of four slightly different
// ones.
package authx

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"log/slog"

	"github.com/anwexhaa/murmur/internal/auth"
	"github.com/anwexhaa/murmur/internal/platform/config"
)

// LoadSigner returns the signer this process uses for outbound assertions.
//
// A nil signer with a nil error means no key is configured and the environment
// tolerates it: outbound calls carry no assertion and the backends, which are
// presumably equally unconfigured, will not ask for one. That is a development
// stack. In production a missing key is an error, because the alternative is a
// deployment that silently runs with its trust boundary switched off.
func LoadSigner(cfg config.Config, service string, log *slog.Logger) (*auth.Signer, error) {
	if cfg.AuthSigningKey == "" {
		if cfg.IsProduction() {
			return nil, errors.New("AUTH_SIGNING_KEY is required in production")
		}
		log.Warn("no AUTH_SIGNING_KEY set; outbound calls will carry no caller assertion",
			"service", service)
		return nil, nil
	}

	private, err := auth.DecodeSeed(cfg.AuthSigningKey)
	if err != nil {
		return nil, fmt.Errorf("AUTH_SIGNING_KEY: %w", err)
	}
	return auth.NewSigner(private, service)
}

// LoadVerifier returns the verifier this process uses for one audience.
//
// The verifying key is taken from AUTH_VERIFYING_KEY when it is set, and
// derived from AUTH_SIGNING_KEY otherwise. The second case is the local stack,
// where one key in one variable is what makes four containers agree. The first
// is how a real deployment is arranged: only the processes that sign hold the
// private half, and the ones that merely verify are given the public half and
// nothing else.
//
// A nil verifier with a nil error means the boundary is off, which the caller
// has already been warned about.
func LoadVerifier(cfg config.Config, audience string, log *slog.Logger) (*auth.Verifier, error) {
	var public ed25519.PublicKey

	switch {
	case cfg.AuthVerifyingKey != "":
		var err error
		if public, err = auth.DecodePublicKey(cfg.AuthVerifyingKey); err != nil {
			return nil, fmt.Errorf("AUTH_VERIFYING_KEY: %w", err)
		}

	case cfg.AuthSigningKey != "":
		private, err := auth.DecodeSeed(cfg.AuthSigningKey)
		if err != nil {
			return nil, fmt.Errorf("AUTH_SIGNING_KEY: %w", err)
		}
		public = private.Public().(ed25519.PublicKey)

	default:
		if cfg.IsProduction() {
			return nil, errors.New("AUTH_VERIFYING_KEY or AUTH_SIGNING_KEY is required in production")
		}
		log.Warn("no verifying key set; this service will accept any caller that can reach its port")
		return nil, nil
	}

	return auth.NewVerifier(public, audience)
}
