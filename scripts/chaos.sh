#!/bin/sh
# Phase 8's evidence: what the system does when its dependencies go away.
#
# Three experiments, each with a claim that can fail:
#
#   pod    -- kill a gateway pod under load; no request may fail
#   redis  -- delete Redis entirely; reads must degrade, not break
#   nats   -- partition NATS; writes must be accepted and drain on recovery
#
# Each one is a hypothesis with a number attached, not a smoke test. A chaos
# experiment that cannot fail is a demo.
#
# Usage, against a cluster with the stack deployed:
#
#	sh scripts/chaos.sh [pod|redis|nats|all]
#
# Environment:
#	KUBE      how to run kubectl (default: docker exec into the local k3s)
#	GATEWAY   the gateway's URL from this machine
#	HANDLE    an account to drive the experiments with
set -eu

EXPERIMENT="${1:-all}"
GATEWAY="${GATEWAY:-http://localhost:30080}"
NS="${NS:-murmur}"
HANDLE="${HANDLE:-clusterreader}"
PASSWORD="${PASSWORD:-a perfectly adequate password}"

kube() {
	if [ -n "${KUBE:-}" ]; then
		# shellcheck disable=SC2086
		$KUBE "$@"
	else
		MSYS_NO_PATHCONV=1 docker exec -i murmur-k3s kubectl "$@"
	fi
}

gql() {
	# $1 query, $2 optional bearer token
	if [ -n "${2:-}" ]; then
		curl -s -m 20 -X POST "$GATEWAY/query" \
			-H 'Content-Type: application/json' \
			-H "Authorization: Bearer $2" \
			-d "$1"
	else
		curl -s -m 20 -X POST "$GATEWAY/query" -H 'Content-Type: application/json' -d "$1"
	fi
}

login() {
	gql "{\"query\":\"mutation{ login(handle:\\\"$HANDLE\\\", password:\\\"$PASSWORD\\\"){ accessToken } }\"}" |
		sed 's/.*"accessToken":"\([^"]*\)".*/\1/'
}

psql() {
	kube exec -n "$NS" postgres-0 -- psql -U murmur -d murmur -At -c "$1" | tr -d '\r'
}

fail() {
	echo "FAIL: $1" >&2
	exit 1
}

# read_timeline prints "content", "empty" or "error" for one read.
read_timeline() {
	body=$(gql '{"query":"query{ timeline(first:10){ edges{ node{ body } } } }"}' "$1")
	case "$body" in
	"") echo error ;;
	*'"errors"'*) echo error ;;
	*'"body"'*) echo content ;;
	*) echo empty ;;
	esac
}

# ------------------------------------------------------------------ pod kill

experiment_pod() {
	echo "== killing a gateway pod under load"
	TOKEN=$(login)
	[ -n "$TOKEN" ] || fail "could not sign in"

	tmp="${TMPDIR:-/tmp}/murmur-chaos-pod.$$"
	mkdir -p "$tmp"
	# shellcheck disable=SC2064
	trap "rm -rf '$tmp'" EXIT

	# Four concurrent readers for forty seconds. The pod dies at t=12s, which
	# is long enough for the load to be steady and early enough that the
	# replacement has to come up while traffic is still arriving.
	worker() {
		ok=0
		bad=0
		end=$(($(date +%s) + 40))
		while [ "$(date +%s)" -lt "$end" ]; do
			code=$(curl -s -o /dev/null -w "%{http_code}" -m 10 -X POST "$GATEWAY/query" \
				-H 'Content-Type: application/json' -H "Authorization: Bearer $TOKEN" \
				-d '{"query":"query HomeTimeline{ timeline(first:20){ edges{ node{ id body author{ handle followerCount } } } } }"}')
			if [ "$code" = "200" ]; then ok=$((ok + 1)); else bad=$((bad + 1)); fi
		done
		echo "$ok $bad" >"$tmp/worker-$1"
	}

	i=1
	while [ "$i" -le 4 ]; do
		worker "$i" &
		i=$((i + 1))
	done

	sleep 12
	victim=$(kube get pods -n "$NS" -l app=gateway --no-headers | head -1 | awk '{print $1}')
	echo "   killing $victim at t=12s"
	kube delete pod -n "$NS" "$victim" --wait=false >/dev/null
	wait

	total_ok=0
	total_fail=0
	for f in "$tmp"/worker-*; do
		set -- $(cat "$f")
		total_ok=$((total_ok + $1))
		total_fail=$((total_fail + $2))
	done

	echo "   $total_ok succeeded, $total_fail failed"
	[ "$total_fail" -eq 0 ] || fail "$total_fail requests failed during a pod kill"
	echo "   PASS: a gateway pod was deleted mid-flight and nothing failed"
}

# ------------------------------------------------------------------- redis

experiment_redis() {
	echo "== deleting Redis entirely"
	TOKEN=$(login)
	[ -n "$TOKEN" ] || fail "could not sign in"

	before=$(read_timeline "$TOKEN")
	[ "$before" = "content" ] || fail "the timeline was not serving content before the experiment ($before)"

	kube scale statefulset/redis -n "$NS" --replicas=0 >/dev/null
	i=0
	while [ "$i" -lt 24 ]; do
		[ "$(kube get pods -n "$NS" -l app=redis --no-headers 2>/dev/null | wc -l)" -eq 0 ] && break
		i=$((i + 1))
		sleep 2
	done
	echo "   redis is gone"

	# Signing in has to work too: the rate limiter lives in Redis and fails
	# open, and a login path that needs Redis would make the outage total.
	TOKEN=$(login)
	[ -n "$TOKEN" ] || fail "could not sign in with Redis down"
	echo "   signed in with Redis down"

	content=0
	empty=0
	errors=0
	i=0
	start=$(date +%s%3N 2>/dev/null || date +%s000)
	while [ "$i" -lt 60 ]; do
		case "$(read_timeline "$TOKEN")" in
		content) content=$((content + 1)) ;;
		empty) empty=$((empty + 1)) ;;
		*) errors=$((errors + 1)) ;;
		esac
		i=$((i + 1))
	done
	end=$(date +%s%3N 2>/dev/null || date +%s000)

	echo "   $content served with content, $empty empty, $errors failed (mean $(((end - start) / 60))ms)"
	kube scale statefulset/redis -n "$NS" --replicas=1 >/dev/null

	[ "$errors" -eq 0 ] || fail "$errors reads failed with Redis down; they should have degraded to Postgres"
	[ "$content" -eq 60 ] || fail "only $content of 60 reads returned content"
	echo "   PASS: every read was served from the source of truth"
	echo "   (Redis is coming back; timelines resume from its append-only file)"
}

# -------------------------------------------------------------------- nats

experiment_nats() {
	echo "== partitioning NATS"
	TOKEN=$(login)
	[ -n "$TOKEN" ] || fail "could not sign in"

	kube scale statefulset/nats -n "$NS" --replicas=0 >/dev/null
	i=0
	while [ "$i" -lt 24 ]; do
		[ "$(kube get pods -n "$NS" -l app=nats --no-headers 2>/dev/null | wc -l)" -eq 0 ] && break
		i=$((i + 1))
		sleep 2
	done
	echo "   nats is gone"

	# The outbox's whole purpose: a post commits with its event in one
	# transaction, so the write path does not depend on the bus being there.
	written=0
	i=0
	while [ "$i" -lt 5 ]; do
		id=$(gql "{\"query\":\"mutation{ createPost(body:\\\"written during the partition $i\\\"){ id } }\"}" "$TOKEN" |
			grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
		[ -n "$id" ] && written=$((written + 1))
		i=$((i + 1))
	done
	echo "   $written of 5 writes accepted with no bus"
	[ "$written" -eq 5 ] || fail "only $written writes were accepted during the partition"

	pending=$(psql "SELECT count(*) FROM outbox WHERE published_at IS NULL;")
	echo "   $pending events waiting in the outbox"
	[ "$pending" -ge 5 ] || fail "expected at least 5 unpublished events, found $pending"

	kube scale statefulset/nats -n "$NS" --replicas=1 >/dev/null
	i=0
	drained=""
	while [ "$i" -lt 30 ]; do
		if [ "$(psql "SELECT count(*) FROM outbox WHERE published_at IS NULL;")" = "0" ]; then
			drained=$((i * 2))
			break
		fi
		i=$((i + 1))
		sleep 2
	done
	[ -n "$drained" ] || fail "the outbox did not drain within 60s of NATS returning"
	echo "   PASS: the outbox drained in ${drained}s with nothing lost"
}

case "$EXPERIMENT" in
pod) experiment_pod ;;
redis) experiment_redis ;;
nats) experiment_nats ;;
all)
	experiment_pod
	echo
	experiment_nats
	echo
	experiment_redis
	;;
*) fail "unknown experiment: $EXPERIMENT (try pod, redis, nats or all)" ;;
esac

echo
echo "all experiments passed"
