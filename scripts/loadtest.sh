#!/bin/sh
# Run a k6 scenario against the running stack.
#
# Pulls viewer IDs out of the seeded graph so the load hits real accounts with
# real timelines, and prints the post cache's tier counters around the run.
set -eu

SCENARIO="${1:-baseline}"
NETWORK="${NETWORK:-murmur_default}"
GATEWAY="${GATEWAY:-http://murmur-gateway:8080}"
TIMELINE_METRICS="${TIMELINE_METRICS:-http://localhost:8082/metrics}"

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
HOT_VIEWER=$(echo "$VIEWERS" | cut -d, -f1)

BEFORE=$(cache_source_tier)
echo "post cache source-tier lookups before: ${BEFORE:-0}"
echo

MSYS_NO_PATHCONV=1 docker run --rm -i \
	--network "$NETWORK" \
	-v "$(pwd -W 2>/dev/null || pwd):/src" \
	-w /src \
	-e GATEWAY="$GATEWAY" \
	-e VIEWERS="$VIEWERS" \
	-e VIEWER="$HOT_VIEWER" \
	-e PEAK="${PEAK:-400}" \
	grafana/k6:latest run "loadtest/$SCENARIO.js"

AFTER=$(cache_source_tier)
echo
echo "post cache source-tier lookups after : ${AFTER:-0}"
echo "fetches that reached the source during the run: $(( ${AFTER:-0} - ${BEFORE:-0} ))"
