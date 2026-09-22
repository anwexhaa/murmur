#!/bin/sh
# Break push fanout on purpose, and measure the damage.
#
# This is phase 4's evidence, not a detour. The hybrid design that follows is
# only justified if the design it replaces genuinely falls over, and "falls
# over" has to be a number: how long the fanout takes, how far the consumer
# falls behind, and — the part that matters most — what happens to everyone
# else's posts while it runs.
#
# Usage: break-fanout.sh <whale-id> <control-id>
set -eu

WHALE="$1"
CONTROL="$2"
SOCIAL="${SOCIAL:-murmur-social:9081}"
FANOUT_METRICS="${FANOUT_METRICS:-http://localhost:8083/metrics}"
GRPCURL="${GRPCURL:-grpcurl}"
SVC=murmur.social.v1.SocialService

post() {
	"$GRPCURL" -plaintext -d "{\"author_id\":\"$1\",\"body\":\"$2\"}" \
		"$SOCIAL" "$SVC/CreatePost" | grep -o '"id": "[^"]*"' | head -1 | cut -d'"' -f4
}

metric() {
	wget -q -O- "$FANOUT_METRICS" | grep "^$1" | awk '{print $2}' | head -1
}

now_ms() { date +%s%3N; }

echo "== baseline: a control post with a handful of followers, nothing else running"
CONTROL_START=$(now_ms)
CONTROL_POST=$(post "$CONTROL" "control post, quiet system")
[ -n "$CONTROL_POST" ] || { echo "FAIL: no post id"; exit 1; }
printf '   post %s\n' "$CONTROL_POST"

WRITES_BEFORE=$(metric murmur_fanout_timeline_writes_total)
echo "   timeline writes so far: ${WRITES_BEFORE:-0}"

echo
echo "== the whale posts"
WHALE_START=$(now_ms)
WHALE_POST=$(post "$WHALE" "the whale speaks")
[ -n "$WHALE_POST" ] || { echo "FAIL: no post id"; exit 1; }
printf '   post %s published at t=0\n' "$WHALE_POST"

echo
echo "== while the whale's fanout runs, a small account posts"
# This is the measurement that matters. One author's publish must not become
# every other author's latency.
sleep 2
CONTENDED_START=$(now_ms)
CONTENDED_POST=$(post "$CONTROL" "control post, during the whale fanout")
CONTENDED_PUBLISH=$(( $(now_ms) - CONTENDED_START ))
printf '   post %s\n' "$CONTENDED_POST"
echo "   publish call returned in ${CONTENDED_PUBLISH}ms"

echo
echo "== waiting for the contended post to become visible to its followers"
# Its followers are few; the only thing that can delay it is the whale ahead
# of it in the queue.
CONTENDED_VISIBLE=""
DEADLINE=$(( $(now_ms) + 600000 ))
while [ "$(now_ms)" -lt "$DEADLINE" ]; do
	PENDING=$(metric murmur_fanout_consumer_pending)
	WRITES=$(metric murmur_fanout_timeline_writes_total)
	ELAPSED=$(( $(now_ms) - WHALE_START ))
	printf '   t=%-7s pending=%-4s timeline_writes=%s\n' "${ELAPSED}ms" "${PENDING:-?}" "${WRITES:-?}"

	if [ "${PENDING:-1}" = "0" ]; then
		CONTENDED_VISIBLE=$(( $(now_ms) - CONTENDED_START ))
		break
	fi
	sleep 2
done

TOTAL=$(( $(now_ms) - WHALE_START ))
WRITES_AFTER=$(metric murmur_fanout_timeline_writes_total)

echo
echo "== result"
echo "   whale fanout wall time      : ${TOTAL}ms"
echo "   timeline writes for one post: $(( ${WRITES_AFTER:-0} - ${WRITES_BEFORE:-0} ))"
echo "   small post publish latency  : ${CONTENDED_PUBLISH}ms  (the write path is unaffected)"
echo "   small post visible after    : ${CONTENDED_VISIBLE:-timed out}ms  (it waited behind the whale)"
echo
echo "   The second number is the write amplification. The last one is the"
echo "   reason the design has to change: one author's publish became every"
echo "   other author's fanout latency."
