package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load("gateway", ":8080")
	if err != nil {
		t.Fatalf("Load() returned error with a clean environment: %v", err)
	}

	if cfg.Service != "gateway" {
		t.Errorf("Service = %q, want %q", cfg.Service, "gateway")
	}
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want the supplied default %q", cfg.HTTPAddr, ":8080")
	}
	if cfg.ShutdownTimeout != 20*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 20s", cfg.ShutdownTimeout)
	}
	if cfg.IsProduction() {
		t.Error("IsProduction() = true, want false by default")
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv("MURMUR_ENV", "production")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("HTTP_ADDR", ":9999")
	t.Setenv("SHUTDOWN_TIMEOUT", "45s")
	t.Setenv("POSTGRES_MAX_CONNS", "64")
	t.Setenv("REDIS_DB", "3")

	cfg, err := Load("social-svc", ":8081")
	if err != nil {
		t.Fatalf("Load() = %v, want no error", err)
	}

	if !cfg.IsProduction() {
		t.Error("IsProduction() = false, want true when MURMUR_ENV=production")
	}
	if cfg.LogLevel != slog.LevelDebug {
		t.Errorf("LogLevel = %v, want debug", cfg.LogLevel)
	}
	if cfg.HTTPAddr != ":9999" {
		t.Errorf("HTTPAddr = %q, want the override %q", cfg.HTTPAddr, ":9999")
	}
	if cfg.ShutdownTimeout != 45*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 45s", cfg.ShutdownTimeout)
	}
	if cfg.Postgres.MaxConns != 64 {
		t.Errorf("Postgres.MaxConns = %d, want 64", cfg.Postgres.MaxConns)
	}
	if cfg.Redis.DB != 3 {
		t.Errorf("Redis.DB = %d, want 3", cfg.Redis.DB)
	}
}

// An empty variable is treated as unset. Container orchestrators frequently
// inject empty strings for absent values, and falling back to the default is
// friendlier than failing to start.
func TestEmptyValueFallsBackToDefault(t *testing.T) {
	t.Setenv("HTTP_ADDR", "")

	cfg, err := Load("gateway", ":8080")
	if err != nil {
		t.Fatalf("Load() = %v, want no error", err)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want the default %q", cfg.HTTPAddr, ":8080")
	}
}

// All bad values should appear in one error, so an operator fixes the whole
// deployment in a single pass instead of one restart per typo.
func TestLoadReportsEveryBadValueAtOnce(t *testing.T) {
	t.Setenv("LOG_LEVEL", "chatty")
	t.Setenv("SHUTDOWN_TIMEOUT", "soon")
	t.Setenv("REDIS_DB", "many")

	_, err := Load("gateway", ":8080")
	if err == nil {
		t.Fatal("Load() = nil, want an error for three invalid values")
	}

	for _, key := range []string{"LOG_LEVEL", "SHUTDOWN_TIMEOUT", "REDIS_DB"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error does not mention %s:\n%v", key, err)
		}
	}
}

// A bad value must not silently become a zero value — the default stands so
// the reported error is the only consequence.
func TestBadValueKeepsDefault(t *testing.T) {
	t.Setenv("SHUTDOWN_TIMEOUT", "soon")

	cfg, _ := Load("gateway", ":8080")
	if cfg.ShutdownTimeout != 20*time.Second {
		t.Errorf("ShutdownTimeout = %v, want the default 20s to survive a parse failure", cfg.ShutdownTimeout)
	}
}
