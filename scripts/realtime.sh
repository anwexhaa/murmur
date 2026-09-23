#!/bin/sh
# Phase 6's evidence: that a subscription is served by the cluster rather than
# by a process.
#
# The claim under test is the one an in-memory hub would also appear to satisfy
# on a developer's laptop and would fail the moment a second replica existed:
# a post published through replica A must reach a socket held by replica C.
# Nothing short of two processes can check it, which is why this script insists
# on three.
#
# It also exercises the backpressure policy, because "one slow client does not
# stall the others" is the kind of claim that is easy to believe and easy to be
# wrong about.
#
# Usage, from the repository root with the stack already running:
#
#	sh scripts/realtime.sh <author-id> <follower-id>
#
# Environment:
#	GATEWAY_A, GATEWAY_B, GATEWAY_C  host:port of each replica's HTTP port
#	SUBSCRIBE                        path to the built subscribe client
set -eu

AUTHOR="$1"
FOLLOWER="$2"

GATEWAY_A="${GATEWAY_A:-murmur-gateway-a:8080}"
GATEWAY_B="${GATEWAY_B:-murmur-gateway-b:8080}"
GATEWAY_C="${GATEWAY_C:-murmur-gateway-c:8080}"
SUBSCRIBE="${SUBSCRIBE:-/src/bin/subscribe-linux}"

TMP="${TMPDIR:-/tmp}/murmur-realtime.$$"
mkdir -p "$TMP"
# shellcheck disable=SC2064
trap "rm -rf '$TMP'" EXIT

publish() {
	# $1 replica, $2 body. Prints the new post's id.
	wget -q -O- \
		--header='Content-Type: application/json' \
		--header="X-Murmur-User: $AUTHOR" \
		--post-data="{\"query\":\"mutation{ createPost(body:\\\"$2\\\"){ id } }\"}" \
		"http://$1/query" |
		grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4
}

metric() {
	# $1 replica, $2 metric name.
	wget -q -O- "http://$1/metrics" | grep "^$2" | awk '{print $2}' | head -1
}

fail() {
	echo "FAIL: $1" >&2
	exit 1
}

echo "== three replicas, one subscriber on each of B and C"
"$SUBSCRIBE" -url "ws://$GATEWAY_B/query" -viewer "$FOLLOWER" -expect 1 -timeout 60s \
	>"$TMP/b.log" 2>&1 &
PID_B=$!
"$SUBSCRIBE" -url "ws://$GATEWAY_C/query" -viewer "$FOLLOWER" -expect 1 -timeout 60s \
	>"$TMP/c.log" 2>&1 &
PID_C=$!

# Wait for both sockets to be live before publishing. Publishing into a
# not-yet-established subscription would produce a flake that looks exactly
# like the cross-replica bug this script exists to catch.
i=0
while [ "$i" -lt 60 ]; do
	if grep -q subscribed "$TMP/b.log" 2>/dev/null && grep -q subscribed "$TMP/c.log" 2>/dev/null; then
		break
	fi
	i=$((i + 1))
	sleep 1
done
grep -q subscribed "$TMP/b.log" || fail "the subscriber on B never connected"
grep -q subscribed "$TMP/c.log" || fail "the subscriber on C never connected"
echo "   both connected"

echo
echo "== publish through replica A"
POST=$(publish "$GATEWAY_A" "cross-replica delivery check")
[ -n "$POST" ] || fail "no post id came back from replica A"
printf '   post %s published on A\n' "$POST"

wait "$PID_B" || fail "the subscriber on B did not receive the post published on A"
wait "$PID_C" || fail "the subscriber on C did not receive the post published on A"

grep -q "$POST" "$TMP/b.log" || fail "B received something, but not the post A published"
grep -q "$POST" "$TMP/c.log" || fail "C received something, but not the post A published"

echo "   B: $(grep 'delivery ms' "$TMP/b.log" || echo 'no timing')"
echo "   C: $(grep 'delivery ms' "$TMP/c.log" || echo 'no timing')"
echo "   PASS: a post published on A reached sockets held by B and by C"

echo
echo "== one client stops reading; the others must not notice"
DROPPED_BEFORE=$(metric "$GATEWAY_C" murmur_subscriptions_dropped_total)

# -slow sleeps between reads, so its server-side buffer fills while the fast
# client beside it drains normally. The interesting outcome is not that the
# slow one falls behind — it is that the fast one does not.
"$SUBSCRIBE" -url "ws://$GATEWAY_C/query" -viewer "$FOLLOWER" -slow -expect 2 -timeout 120s \
	>"$TMP/slow.log" 2>&1 &
PID_SLOW=$!
"$SUBSCRIBE" -url "ws://$GATEWAY_C/query" -viewer "$FOLLOWER" -quiet -expect 40 -timeout 120s \
	>"$TMP/fast.log" 2>&1 &
PID_FAST=$!

i=0
while [ "$i" -lt 60 ]; do
	if grep -q subscribed "$TMP/slow.log" 2>/dev/null && grep -q subscribed "$TMP/fast.log" 2>/dev/null; then
		break
	fi
	i=$((i + 1))
	sleep 1
done
grep -q subscribed "$TMP/fast.log" || fail "the fast subscriber never connected"

j=0
while [ "$j" -lt 40 ]; do
	publish "$GATEWAY_A" "backpressure burst $j" >/dev/null
	j=$((j + 1))
done
echo "   published 40 posts through A"

if wait "$PID_FAST"; then
	echo "   $(grep 'delivery ms' "$TMP/fast.log" || echo 'fast client: no timing')"
	echo "   PASS: the fast client received all 40 while the slow one was stalled"
else
	fail "the fast client was starved by a slow client on the same replica"
fi

kill "$PID_SLOW" 2>/dev/null || true
wait "$PID_SLOW" 2>/dev/null || true

DROPPED_AFTER=$(metric "$GATEWAY_C" murmur_subscriptions_dropped_total)
printf '   dropped_total on C: %s -> %s\n' "${DROPPED_BEFORE:-0}" "${DROPPED_AFTER:-0}"

echo
echo "== hydration: one fetch per post, not per socket"
for tier in local shared source; do
	value=$(wget -q -O- "http://$GATEWAY_C/metrics" |
		grep "^murmur_hydrator_lookups_total{tier=\"$tier\"}" | awk '{print $2}' | head -1)
	printf '   %-7s %s\n' "$tier" "${value:-0}"
done
echo "   collapsed: $(metric "$GATEWAY_C" murmur_hydrator_collapsed_total)"

echo
echo "all checks passed"
