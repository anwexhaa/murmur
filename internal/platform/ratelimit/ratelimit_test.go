package ratelimit_test

import (
	"context"
	"flag"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/anwexhaa/murmur/internal/platform/ratelimit"
)

var testClient *redis.Client

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}

	ctx := context.Background()
	addr := os.Getenv("MURMUR_TEST_REDIS_ADDR")

	var container *tcredis.RedisContainer
	if addr == "" {
		var err error
		container, err = tcredis.Run(ctx, "redis:7-alpine")
		if err != nil {
			log.Fatalf("start redis container: %v", err)
		}
		endpoint, err := container.ConnectionString(ctx)
		if err != nil {
			log.Fatalf("redis connection string: %v", err)
		}
		opts, err := redis.ParseURL(endpoint)
		if err != nil {
			log.Fatalf("parse redis url: %v", err)
		}
		addr = opts.Addr
	}

	testClient = redis.NewClient(&redis.Options{Addr: addr, DB: testRedisDB()})
	code := m.Run()

	_ = testClient.Close()
	if container != nil {
		if err := testcontainers.TerminateContainer(container); err != nil {
			log.Printf("terminate redis container: %v", err)
		}
	}
	os.Exit(code)
}

func testRedisDB() int {
	if raw := os.Getenv("MURMUR_TEST_REDIS_DB"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			return n
		}
	}
	return 1
}

// clock is a hand-wound clock, so refill can be tested without sleeping.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// newLimiter returns a limiter and a scope unique to this test.
//
// Deliberately no FlushDB. `go test ./...` runs packages in parallel and other
// packages flush this database, so a test that depends on having it to itself
// is a test that depends on the schedule. Isolating by key costs one string
// concatenation and cannot race with anybody.
func newLimiter(t *testing.T) (*ratelimit.Limiter, *clock, string) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test: -short")
	}
	if testClient == nil {
		t.Fatal("no redis: TestMain did not start one")
	}

	c := &clock{now: time.Unix(1_700_000_000, 0)}
	limiter := ratelimit.New(testClient, nil, c.Now)
	if err := limiter.Prepare(t.Context()); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	return limiter, c, t.Name()
}

func TestABurstIsAllowedUpToCapacity(t *testing.T) {
	limiter, _, scope := newLimiter(t)
	rule := ratelimit.Rule{Capacity: 5, Refill: 1, Cost: 1}

	for i := 0; i < 5; i++ {
		decision, err := limiter.Allow(t.Context(), scope, "user-1", rule)
		if err != nil {
			t.Fatalf("allow %d: %v", i, err)
		}
		if !decision.Allowed {
			t.Fatalf("request %d of a 5-token burst was refused", i+1)
		}
	}

	decision, err := limiter.Allow(t.Context(), scope, "user-1", rule)
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	if decision.Allowed {
		t.Fatal("the sixth request against a capacity of five was allowed")
	}
	if decision.RetryAfter <= 0 {
		t.Fatal("a refused request came back with no retry hint")
	}
}

// TestTheBucketRefillsContinuously is the property a fixed window does not
// have. Half a second at one token per second is half a token, not zero and
// not a whole window's worth.
func TestTheBucketRefillsContinuously(t *testing.T) {
	limiter, clk, scope := newLimiter(t)
	rule := ratelimit.Rule{Capacity: 2, Refill: 2, Cost: 1}

	for i := 0; i < 2; i++ {
		if decision, _ := limiter.Allow(t.Context(), scope, "user-1", rule); !decision.Allowed {
			t.Fatalf("request %d of the initial burst was refused", i+1)
		}
	}
	if decision, _ := limiter.Allow(t.Context(), scope, "user-1", rule); decision.Allowed {
		t.Fatal("the bucket was not empty after its capacity was spent")
	}

	// Half a second at two per second is exactly one token.
	clk.Advance(500 * time.Millisecond)
	decision, err := limiter.Allow(t.Context(), scope, "user-1", rule)
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	if !decision.Allowed {
		t.Fatal("the bucket did not refill over half a second")
	}

	if decision, _ := limiter.Allow(t.Context(), scope, "user-1", rule); decision.Allowed {
		t.Fatal("the bucket refilled more than the elapsed time earns")
	}
}

func TestRefillStopsAtCapacity(t *testing.T) {
	limiter, clk, scope := newLimiter(t)
	rule := ratelimit.Rule{Capacity: 3, Refill: 10, Cost: 1}

	if decision, _ := limiter.Allow(t.Context(), scope, "user-1", rule); !decision.Allowed {
		t.Fatal("the first request was refused")
	}

	// An hour of refill at ten per second is 36,000 tokens, and the bucket
	// holds three. Without the clamp, an idle client would accumulate an
	// unbounded burst -- which is exactly the spike a limiter is for.
	clk.Advance(time.Hour)

	for i := 0; i < 3; i++ {
		if decision, _ := limiter.Allow(t.Context(), scope, "user-1", rule); !decision.Allowed {
			t.Fatalf("request %d after a long idle period was refused", i+1)
		}
	}
	if decision, _ := limiter.Allow(t.Context(), scope, "user-1", rule); decision.Allowed {
		t.Fatal("an idle bucket accumulated more than its capacity")
	}
}

func TestBucketsAreIndependentPerIdentity(t *testing.T) {
	limiter, _, scope := newLimiter(t)
	rule := ratelimit.Rule{Capacity: 1, Refill: 1, Cost: 1}

	if decision, _ := limiter.Allow(t.Context(), scope, "user-1", rule); !decision.Allowed {
		t.Fatal("user-1 was refused its first request")
	}
	if decision, _ := limiter.Allow(t.Context(), scope, "user-1", rule); decision.Allowed {
		t.Fatal("user-1 got two requests from a one-token bucket")
	}
	if decision, _ := limiter.Allow(t.Context(), scope, "user-2", rule); !decision.Allowed {
		t.Fatal("user-2 was refused because user-1 had spent its own bucket")
	}
}

// TestScopesAreIndependent is why the key carries one. A user hammering login
// must not spend the allowance they need to read their timeline -- otherwise
// the cheapest denial of service against an account is to fail its password a
// few times.
func TestScopesAreIndependent(t *testing.T) {
	limiter, _, scope := newLimiter(t)
	rule := ratelimit.Rule{Capacity: 1, Refill: 1, Cost: 1}

	login, read := scope+"/login", scope+"/read"

	if decision, _ := limiter.Allow(t.Context(), login, "user-1", rule); !decision.Allowed {
		t.Fatal("the first login was refused")
	}
	if decision, _ := limiter.Allow(t.Context(), login, "user-1", rule); decision.Allowed {
		t.Fatal("a second login came out of an exhausted bucket")
	}
	if decision, _ := limiter.Allow(t.Context(), read, "user-1", rule); !decision.Allowed {
		t.Fatal("a read was refused because the login bucket was empty")
	}
}

// TestCostIsCharged covers the expensive operations. A login runs argon2 over
// 19MB; it is not worth the same as a timeline read and the bucket should not
// price it that way.
func TestCostIsCharged(t *testing.T) {
	limiter, _, scope := newLimiter(t)
	rule := ratelimit.Rule{Capacity: 10, Refill: 1, Cost: 4}

	for i := 0; i < 2; i++ {
		if decision, _ := limiter.Allow(t.Context(), scope, "user-1", rule); !decision.Allowed {
			t.Fatalf("request %d at cost 4 against capacity 10 was refused", i+1)
		}
	}
	if decision, _ := limiter.Allow(t.Context(), scope, "user-1", rule); decision.Allowed {
		t.Fatal("three requests at cost 4 fitted in a bucket of 10")
	}
}

// TestConcurrentCallersCannotExceedCapacity is the reason this is a Lua
// script. Read-modify-write in Go would let every concurrent caller read the
// same token count and every one of them decide there was room.
func TestConcurrentCallersCannotExceedCapacity(t *testing.T) {
	limiter, _, scope := newLimiter(t)
	rule := ratelimit.Rule{Capacity: 20, Refill: 0.0001, Cost: 1}

	const callers = 200

	var allowed atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			decision, err := limiter.Allow(context.Background(), scope, "user-1", rule)
			if err != nil {
				t.Errorf("allow: %v", err)
				return
			}
			if decision.Allowed {
				allowed.Add(1)
			}
		}()
	}

	close(start)
	wg.Wait()

	// The refill rate is low enough that nothing meaningful is added over the
	// test's lifetime, so the answer is exactly the capacity.
	if got := allowed.Load(); got != 20 {
		t.Fatalf("%d of %d concurrent requests were allowed against a capacity of 20", got, callers)
	}
}

// TestTheKeyExpires keeps an idle caller from costing memory forever. One key
// per user that never expires is a leak with a slow fuse.
func TestTheKeyExpires(t *testing.T) {
	limiter, _, scope := newLimiter(t)
	rule := ratelimit.Rule{Capacity: 10, Refill: 5, Cost: 1}

	if _, err := limiter.Allow(t.Context(), scope, "user-1", rule); err != nil {
		t.Fatalf("allow: %v", err)
	}

	ttl, err := testClient.PTTL(t.Context(), ratelimit.Key(scope, "user-1")).Result()
	if err != nil {
		t.Fatalf("pttl: %v", err)
	}
	if ttl <= 0 {
		t.Fatalf("the bucket key has no expiry (ttl = %v)", ttl)
	}
	// Capacity 10 at 5 per second refills in two seconds; the key should
	// outlive that and not much more.
	if ttl > 10*time.Second {
		t.Fatalf("ttl = %v, which is far longer than the bucket takes to refill", ttl)
	}
}

// TestAClockGoingBackwardsCreatesNoTokens covers the one arithmetic hazard in
// the script. NTP stepping a replica backwards must not mint an allowance.
func TestAClockGoingBackwardsCreatesNoTokens(t *testing.T) {
	limiter, clk, scope := newLimiter(t)
	rule := ratelimit.Rule{Capacity: 2, Refill: 1, Cost: 1}

	for i := 0; i < 2; i++ {
		if decision, _ := limiter.Allow(t.Context(), scope, "user-1", rule); !decision.Allowed {
			t.Fatalf("request %d was refused", i+1)
		}
	}

	clk.Advance(-time.Hour)

	if decision, _ := limiter.Allow(t.Context(), scope, "user-1", rule); decision.Allowed {
		t.Fatal("moving the clock backwards refilled the bucket")
	}
}

func TestAnUnenforceableRuleIsReportedAndFailsOpen(t *testing.T) {
	limiter, _, scope := newLimiter(t)

	cases := map[string]ratelimit.Rule{
		"no capacity":         {Capacity: 0, Refill: 1, Cost: 1},
		"no refill":           {Capacity: 1, Refill: 0, Cost: 1},
		"no cost":             {Capacity: 1, Refill: 1, Cost: 0},
		"cost above capacity": {Capacity: 1, Refill: 1, Cost: 2},
	}

	for name, rule := range cases {
		t.Run(name, func(t *testing.T) {
			decision, err := limiter.Allow(t.Context(), scope, "user-1", rule)
			if err == nil {
				t.Fatal("an unenforceable rule was accepted silently")
			}
			if !decision.Allowed {
				t.Fatal("a misconfigured rule refused the request instead of failing open")
			}
		})
	}
}

// TestTheLimiterFailsOpen is the trade written down as a test. With Redis
// unreachable the limit is gone, and that is on purpose: the alternative turns
// a cache outage into a total outage.
func TestTheLimiterFailsOpen(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test: -short")
	}

	// A client pointed at a port with nothing behind it.
	dead := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1",
		DialTimeout: 200 * time.Millisecond,
		MaxRetries:  -1,
	})
	defer dead.Close()

	limiter := ratelimit.New(dead, nil, time.Now)
	decision, err := limiter.Allow(t.Context(), t.Name(), "user-1",
		ratelimit.Rule{Capacity: 1, Refill: 1, Cost: 1})

	if err == nil {
		t.Fatal("an unreachable Redis produced no error, so the failure would be invisible")
	}
	if !decision.Allowed {
		t.Fatal("the limiter failed closed, turning a Redis outage into a total outage")
	}
}

// TestItSurvivesRedisForgettingTheScript covers the NOSCRIPT path, which is
// what happens after a Redis restart and is where phase 3's pipeline bug came
// from.
func TestItSurvivesRedisForgettingTheScript(t *testing.T) {
	limiter, _, scope := newLimiter(t)
	rule := ratelimit.Rule{Capacity: 5, Refill: 1, Cost: 1}

	if decision, err := limiter.Allow(t.Context(), scope, "user-1", rule); err != nil || !decision.Allowed {
		t.Fatalf("first call: decision=%+v err=%v", decision, err)
	}

	if err := testClient.ScriptFlush(t.Context()).Err(); err != nil {
		t.Fatalf("script flush: %v", err)
	}

	decision, err := limiter.Allow(t.Context(), scope, "user-1", rule)
	if err != nil {
		t.Fatalf("after a script flush: %v", err)
	}
	if !decision.Allowed {
		t.Fatal("the call after a script flush was refused")
	}
}
