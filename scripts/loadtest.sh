#!/bin/sh
# Run a k6 scenario against the running stack.
#
# Pulls viewer IDs out of the seeded graph so the load hits real accounts with
# real timelines, mints an access token for each, and prints the post cache's
# tier counters around the run.
set -eu

SCENARIO="${1:-baseline}"
NETWORK="${NETWORK:-murmur_default}"
GATEWAY="${GATEWAY:-http://murmur-gateway:8080}"
TIMELINE_METRICS="${TIMELINE_METRICS:-http://localhost:8082/metrics}"
VIEWER_FILE="bin/loadtest-viewers.txt"

psql() {
	MSYS_NO_PATHCONV=1 docker exec murmur-postgres-1 psql -U murmur -d murmur -tAc "$1"
}

cache_source_tier() {
	curl -s "$TIMELINE_METRICS" 2>/dev/null |
		grep 'postcache_lookups_total{tier="source"}' | awk '{print $2}'
}

echo "collecting viewers from the seeded graph"
VIEWERS=$(psql "SELECT string_agg(id::text, ',') FROM (
    SELECT u.id FROM users u
    JOIN follows f ON f.follower_id = u.id
    GROUP BY u.id ORDER BY count(*) DESC LIMIT 25
) t")

# Phase 7 made every request carry a signed token, and the seeded accounts
# have no passwords to log in with -- cmd/seed writes six hundred thousand
# users with COPY, and giving each one a credential would mean six hundred
# thousand argon2 hashes. So the load test mints its own tokens from the
# deployment's signing key. That is an operator capability and nothing in the
# deployed system does it; see the comment at the top of scripts/mint.go.
: "${AUTH_SIGNING_KEY:?set AUTH_SIGNING_KEY to the signing key the stack is running with}"

echo "minting access tokens"
mkdir -p bin
echo "$VIEWERS" | tr ',' '\n' >"$VIEWER_FILE"

TOKENS=$(MSYS_NO_PATHCONV=1 docker run --rm -i \
	-v "$(pwd -W 2>/dev/null || pwd):/src" \
	-v murmur-gomodcache:/go/pkg/mod \
	-w /src \
	-e AUTH_SIGNING_KEY="$AUTH_SIGNING_KEY" \
	golang:1.27 go run scripts/mint.go -users "$VIEWER_FILE" |
	tr ',' '\n' | sed 's/.*": *"//; s/"[},].*//; s/"$//' | paste -sd, -)

HOT_TOKEN=$(echo "$TOKENS" | cut -d, -f1)
[ -n "$HOT_TOKEN" ] || { echo "FAIL: no tokens were minted" >&2; exit 1; }

BEFORE=$(cache_source_tier)
echo "post cache source-tier lookups before: ${BEFORE:-0}"
echo

MSYS_NO_PATHCONV=1 docker run --rm -i \
	--network "$NETWORK" \
	-v "$(pwd -W 2>/dev/null || pwd):/src" \
	-w /src \
	-e GATEWAY="$GATEWAY" \
	-e TOKENS="$TOKENS" \
	-e TOKEN="$HOT_TOKEN" \
	-e PEAK="${PEAK:-400}" \
	grafana/k6:latest run "loadtest/$SCENARIO.js"

AFTER=$(cache_source_tier)
echo
echo "post cache source-tier lookups after : ${AFTER:-0}"
echo "fetches that reached the source during the run: $(( ${AFTER:-0} - ${BEFORE:-0} ))"
