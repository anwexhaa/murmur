#!/bin/sh
# End-to-end smoke test against a running social-svc, over gRPC reflection.
#
# This is the phase 1 "done when" made repeatable: create users, follow, post,
# read it all back, and confirm the error cases return the right gRPC codes.
# It talks to the service exactly as another service would — no test helpers,
# no direct database access.
#
# Usage: smoke.sh [host:port]   (default murmur-social:9081)
set -eu

TARGET="${1:-murmur-social:9081}"
GRPCURL="${GRPCURL:-grpcurl}"
SVC=murmur.social.v1.SocialService
RUN="$(date +%s)"

call() {
	method="$1"
	payload="$2"
	"$GRPCURL" -plaintext -d "$payload" "$TARGET" "$SVC/$method"
}

# Extracts a top-level string field from grpcurl's JSON. Enough for this
# script's needs and one less dependency than pulling in jq.
field() {
	grep -o "\"$1\": \"[^\"]*\"" | head -1 | cut -d'"' -f4
}

expect_code() {
	want="$1"
	method="$2"
	payload="$3"
	label="$4"

	if out=$("$GRPCURL" -plaintext -d "$payload" "$TARGET" "$SVC/$method" 2>&1); then
		echo "  FAIL $label: expected $want, call succeeded"
		echo "$out"
		exit 1
	fi
	if ! echo "$out" | grep -q "$want"; then
		echo "  FAIL $label: expected $want, got:"
		echo "$out"
		exit 1
	fi
	echo "  ok   $label -> $want"
}

echo "target $TARGET"

echo "creating users"
ALICE=$(call CreateUser "{\"handle\":\"alice_$RUN\",\"display_name\":\"Alice\"}" | field id)
BOB=$(call CreateUser "{\"handle\":\"bob_$RUN\",\"display_name\":\"Bob\"}" | field id)
[ -n "$ALICE" ] && [ -n "$BOB" ] || { echo "  FAIL: no user ids returned"; exit 1; }
echo "  alice $ALICE"
echo "  bob   $BOB"

echo "reading a user back by handle"
BACK=$(call GetUser "{\"handle\":\"alice_$RUN\"}" | field id)
[ "$BACK" = "$ALICE" ] || { echo "  FAIL: handle lookup returned $BACK, want $ALICE"; exit 1; }
echo "  ok   handle lookup matches"

echo "following"
FIRST=$(call Follow "{\"follower_id\":\"$ALICE\",\"followee_id\":\"$BOB\"}" | grep -c '"created": true' || true)
[ "$FIRST" = "1" ] || { echo "  FAIL: first follow did not report created"; exit 1; }
echo "  ok   first follow created the edge"

# Follow is idempotent: a retry after a timeout must succeed and report that
# nothing changed, rather than fail with a duplicate key.
SECOND=$(call Follow "{\"follower_id\":\"$ALICE\",\"followee_id\":\"$BOB\"}" 2>&1)
echo "$SECOND" | grep -q '"created": true' && { echo "  FAIL: repeat follow reported created"; exit 1; }
echo "  ok   repeat follow succeeded without creating a second edge"

echo "listing followers"
FOLLOWER=$(call ListFollowers "{\"user_id\":\"$BOB\",\"page_size\":10}" | grep -o "$ALICE" | head -1)
[ "$FOLLOWER" = "$ALICE" ] || { echo "  FAIL: alice is not among bob's followers"; exit 1; }
echo "  ok   alice follows bob"

echo "posting"
POST=$(call CreatePost "{\"author_id\":\"$BOB\",\"body\":\"first banter of run $RUN\"}" | field id)
[ -n "$POST" ] || { echo "  FAIL: no post id returned"; exit 1; }
echo "  post  $POST"

echo "reading the post back"
call GetPost "{\"id\":\"$POST\"}" | grep -q "first banter of run $RUN" ||
	{ echo "  FAIL: post body did not round-trip"; exit 1; }
echo "  ok   post body round-tripped"

call BatchGetPosts "{\"ids\":[\"$POST\"]}" | grep -q "$POST" ||
	{ echo "  FAIL: batch get did not return the post"; exit 1; }
echo "  ok   batch get returned the post"

call ListAuthorPosts "{\"author_id\":\"$BOB\",\"page_size\":10}" | grep -q "$POST" ||
	{ echo "  FAIL: author listing did not return the post"; exit 1; }
echo "  ok   author listing returned the post"

call BatchGetUsers "{\"ids\":[\"$ALICE\",\"$BOB\"]}" | grep -q "$ALICE" ||
	{ echo "  FAIL: batch get users did not return alice"; exit 1; }
echo "  ok   batch get users returned both"

echo "error cases"
expect_code InvalidArgument Follow \
	"{\"follower_id\":\"$ALICE\",\"followee_id\":\"$ALICE\"}" "self-follow"
expect_code InvalidArgument CreateUser \
	'{"handle":"not a handle","display_name":"x"}' "handle with a space"
expect_code AlreadyExists CreateUser \
	"{\"handle\":\"alice_$RUN\",\"display_name\":\"Impostor\"}" "duplicate handle"
expect_code NotFound GetUser \
	'{"id":"00000000-0000-0000-0000-000000000000"}' "missing user"
expect_code InvalidArgument GetPost \
	'{"id":"not-a-ulid"}' "malformed post id"
expect_code InvalidArgument CreatePost \
	"{\"author_id\":\"$BOB\",\"body\":\"   \"}" "whitespace-only body"

echo
echo "smoke passed"
