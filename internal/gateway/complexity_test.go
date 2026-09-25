package gateway_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/transport"

	"github.com/anwexhaa/murmur/internal/gateway"
	"github.com/anwexhaa/murmur/internal/gateway/gqlgen"
)

// The complexity limiter is tested through the real executable schema, because
// what is being asserted is a property of the schema's prices rather than of
// the arithmetic. A unit test of the cost functions would happily agree with
// itself while the limit let a hostile query through.

func newLimitedServer(t *testing.T, limit int) http.Handler {
	t.Helper()

	// A nil resolver is fine: every query here is rejected before a resolver
	// runs, and a query that is not rejected is a failure whatever it returns.
	srv := handler.New(gqlgen.NewExecutableSchema(gqlgen.Config{
		Resolvers:  &gateway.Resolver{},
		Complexity: gateway.Complexity(),
	}))
	srv.AddTransport(transport.POST{})
	srv.Use(extension.FixedComplexityLimit(limit))
	return srv
}

func query(t *testing.T, srv http.Handler, body string) (int, string) {
	t.Helper()

	payload, err := json.Marshal(map[string]string{"query": body})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(string(payload)))
	req.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()
	srv.ServeHTTP(recorder, req)
	return recorder.Code, recorder.Body.String()
}

// TestADeliberatelyExpensiveQueryIsRejected is the phase's "done when".
//
// The query is not deep. It is four levels, which every depth limiter in every
// tutorial would wave straight through -- and it asks for a hundred posts,
// each with an author, each author's follower count and each author's own
// hundred posts with *their* authors. That is the shape a depth cap cannot
// see, which is the argument for pricing by complexity instead.
func TestADeliberatelyExpensiveQueryIsRejected(t *testing.T) {
	srv := newLimitedServer(t, 2000)

	hostile := `query Hostile {
	  timeline(first: 100) {
	    edges {
	      node {
	        author {
	          followerCount
	          posts(first: 100) {
	            edges { node { author { followerCount handle } } }
	          }
	        }
	      }
	    }
	  }
	}`

	code, body := query(t, srv, hostile)
	if code != http.StatusUnprocessableEntity && !strings.Contains(body, "complexity") {
		t.Fatalf("a query asking for 10,000 authors was accepted: status %d, body %s", code, body)
	}
	if !strings.Contains(body, "complexity") {
		t.Fatalf("the rejection does not mention complexity, so the client cannot tell why: %s", body)
	}
}

// TestAnOrdinaryQueryIsAccepted is the other half. A limit that rejects
// everything is not a limit, it is an outage, and the only way to know which
// one has been built is to check both directions.
func TestAnOrdinaryQueryIsAccepted(t *testing.T) {
	srv := newLimitedServer(t, 2000)

	ordinary := `query HomeTimeline {
	  timeline(first: 20) {
	    edges { node { id body createdAt author { handle displayName followerCount } } }
	    pageInfo { hasNextPage endCursor }
	  }
	}`

	_, body := query(t, srv, ordinary)
	if strings.Contains(body, "complexity") {
		t.Fatalf("a twenty-post timeline was rejected as too complex: %s", body)
	}
}

// TestThePriceFollowsThePageSize is the property a depth limit lacks. The same
// query text, one argument different, must cost proportionally more.
func TestThePriceFollowsThePageSize(t *testing.T) {
	// A limit sized so that twenty posts fit and a hundred do not. If the page
	// size were not in the price, both would land on the same side of it.
	srv := newLimitedServer(t, 400)

	small := `query Small { timeline(first: 20) { edges { node { id author { handle } } } } }`
	large := `query Large { timeline(first: 100) { edges { node { id author { handle } } } } }`

	if _, body := query(t, srv, small); strings.Contains(body, "complexity") {
		t.Fatalf("twenty posts exceeded a budget that should hold them: %s", body)
	}
	if _, body := query(t, srv, large); !strings.Contains(body, "complexity") {
		t.Fatalf("a hundred posts cost the same as twenty, so the page size is not priced: %s", body)
	}
}

// TestAuthMutationsArePricedAboveTheRest records the reasoning in a test
// rather than only in a comment. Login runs argon2 over 19MB; it is not worth
// the same as a write that appends a row.
func TestAuthMutationsArePricedAboveTheRest(t *testing.T) {
	// A budget that admits a post but not a login.
	srv := newLimitedServer(t, 100)

	write := `mutation Write { createPost(body: "hello") { id } }`
	login := `mutation In { login(handle: "alice", password: "hunter2hunter2") { accessToken } }`

	if _, body := query(t, srv, write); strings.Contains(body, "complexity") {
		t.Fatalf("an ordinary write was rejected: %s", body)
	}
	if _, body := query(t, srv, login); !strings.Contains(body, "complexity") {
		t.Fatalf("login is priced no higher than an ordinary write: %s", body)
	}
}

// TestIntrospectionIsOffUnlessEnabled: the executable schema still knows its
// types, but without the extension registered the query is refused. Free
// schema disclosure is a choice, and this asserts the default.
func TestIntrospectionIsOffUnlessEnabled(t *testing.T) {
	srv := newLimitedServer(t, 100000)

	_, body := query(t, srv, `query Schema { __schema { types { name } } }`)
	if !strings.Contains(body, "introspection") {
		t.Fatalf("introspection answered without being enabled: %s", body)
	}
}
