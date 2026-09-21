package domain

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestValidateHandle(t *testing.T) {
	tests := []struct {
		name    string
		handle  string
		wantErr bool
	}{
		{"typical", "anwexhaa", false},
		{"with digits and underscore", "an_wesha_24", false},
		{"minimum length", "ab", false},
		{"maximum length", strings.Repeat("a", HandleMaxLen), false},
		{"empty", "", true},
		{"one character", "a", true},
		{"too long", strings.Repeat("a", HandleMaxLen+1), true},
		{"space", "an wesha", true},
		{"hyphen", "an-wesha", true},
		{"at sign", "@anwesha", true},
		{"emoji", "anwesha🎉", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateHandle(tt.handle)
			if tt.wantErr && err == nil {
				t.Fatalf("ValidateHandle(%q) = nil, want an error", tt.handle)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("ValidateHandle(%q) = %v, want nil", tt.handle, err)
			}
			if err != nil && !errors.Is(err, ErrInvalid) {
				t.Errorf("error does not wrap ErrInvalid: %v", err)
			}
		})
	}
}

// The database bound is char_length, which counts characters. Counting bytes
// here would reject a post that Postgres would happily accept.
func TestBodyLengthIsCountedInRunesNotBytes(t *testing.T) {
	// Devanagari: three bytes per character in UTF-8.
	body := strings.Repeat("अ", BodyMaxLen)

	if len(body) <= BodyMaxLen {
		t.Fatalf("test is not exercising the multi-byte case: %d bytes", len(body))
	}
	if err := ValidateBody(body); err != nil {
		t.Errorf("ValidateBody() rejected a %d-character body: %v", BodyMaxLen, err)
	}

	if err := ValidateBody(body + "अ"); err == nil {
		t.Error("ValidateBody() accepted a body one character over the limit")
	}
}

func TestValidateBodyRejectsWhitespaceOnly(t *testing.T) {
	for _, body := range []string{"", "   ", "\n\t  \n"} {
		if err := ValidateBody(body); err == nil {
			t.Errorf("ValidateBody(%q) = nil, want an error", body)
		}
	}
}

func TestNewUserTrimsDisplayNameAndAssignsIdentity(t *testing.T) {
	now := time.Now()

	user, err := NewUser("anwexhaa", "  Anwesha Das  ", now)
	if err != nil {
		t.Fatalf("NewUser() = %v, want nil", err)
	}
	if user.DisplayName != "Anwesha Das" {
		t.Errorf("DisplayName = %q, want it trimmed", user.DisplayName)
	}
	if user.ID.String() == "" || user.ID.Variant().String() == "Invalid" {
		t.Errorf("ID = %v, want a valid UUID", user.ID)
	}
	if user.CreatedAt.Location() != time.UTC {
		t.Errorf("CreatedAt is in %v, want UTC", user.CreatedAt.Location())
	}
}

func TestParseUserIDRejectsGarbage(t *testing.T) {
	for _, raw := range []string{"", "not-a-uuid", "12345"} {
		if _, err := ParseUserID("follower_id", raw); !errors.Is(err, ErrInvalid) {
			t.Errorf("ParseUserID(%q) = %v, want ErrInvalid", raw, err)
		}
	}
}

// Two IDs made in the same millisecond must still order consistently, or a
// timeline can show the same two posts in a different order on each read.
func TestULIDsAreMonotonicWithinAMillisecond(t *testing.T) {
	gen := NewIDGenerator()
	instant := time.Now()

	const n = 1000
	ids := make([]string, n)
	for i := range ids {
		id, err := gen.New(instant)
		if err != nil {
			t.Fatalf("New() = %v, want nil", err)
		}
		ids[i] = id
	}

	for i := 1; i < n; i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("ids are not increasing at index %d: %q then %q", i, ids[i-1], ids[i])
		}
	}
}

func TestULIDsIncreaseWithTime(t *testing.T) {
	gen := NewIDGenerator()

	earlier, err := gen.New(time.Now())
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	later, err := gen.New(time.Now().Add(time.Second))
	if err != nil {
		t.Fatalf("New() = %v", err)
	}

	if later <= earlier {
		t.Errorf("a later ULID (%q) does not sort after an earlier one (%q)", later, earlier)
	}
}

// The monotonic entropy source mutates shared state. This test exists to fail
// under -race if the generator's lock is ever removed.
func TestIDGeneratorIsSafeForConcurrentUse(t *testing.T) {
	gen := NewIDGenerator()
	instant := time.Now()

	const goroutines, perGoroutine = 16, 200

	var (
		mu   sync.Mutex
		seen = make(map[string]struct{}, goroutines*perGoroutine)
		wg   sync.WaitGroup
	)

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]string, 0, perGoroutine)
			for range perGoroutine {
				id, err := gen.New(instant)
				if err != nil {
					t.Errorf("New() = %v", err)
					return
				}
				local = append(local, id)
			}
			mu.Lock()
			defer mu.Unlock()
			for _, id := range local {
				seen[id] = struct{}{}
			}
		}()
	}
	wg.Wait()

	if len(seen) != goroutines*perGoroutine {
		t.Errorf("got %d distinct ids from %d calls; the generator handed out duplicates",
			len(seen), goroutines*perGoroutine)
	}
}

func TestValidatePostID(t *testing.T) {
	gen := NewIDGenerator()
	valid, err := gen.New(time.Now())
	if err != nil {
		t.Fatalf("New() = %v", err)
	}

	if err := ValidatePostID("id", valid); err != nil {
		t.Errorf("ValidatePostID(%q) = %v, want nil", valid, err)
	}

	for _, raw := range []string{"", "nope", strings.Repeat("Z", 26), valid[:25]} {
		if err := ValidatePostID("id", raw); err == nil {
			t.Errorf("ValidatePostID(%q) = nil, want an error", raw)
		}
	}
}
