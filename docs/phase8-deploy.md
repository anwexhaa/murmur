# Phase 8: deploy, observe, prove

Measured 25 September 2026 on a single-node k3s cluster running in Docker:
3 gateway replicas, 2 social-svc, 2 timeline-svc, 1 fanout worker, plus
Postgres, Redis and NATS, all from distroless images built by
`deploy/Dockerfile`.

Seven phases produced a system. This one asks what it does when the things it
depends on stop being there, and the answer was not the one the tests implied.

## The headline

**A gateway pod was deleted mid-flight under load. 949 requests, 0 failed.**

```
== killing a gateway pod under load
   killing gateway-79cf9fc644-clcnz at t=12s
   949 succeeded, 0 failed
```

Four concurrent readers for forty seconds; the pod dies at twelve. Two things
make the zero rather than "almost zero":

- `maxUnavailable: 0` on the rollout, so a replacement is serving before
  anything is taken away.
- A five-second `preStop` sleep. Endpoint removal and SIGTERM race: the kubelet
  signals the container while kube-proxy is still rewriting rules, so a
  connection can arrive at a pod that has already stopped accepting. Sleeping
  first lets the removal propagate, and *then* the process is asked to stop.

## What the chaos experiments actually found

The fallback for a Redis outage was written first, unit tested, and green. Then
Redis was scaled to zero on a real cluster and **every read failed with
connection refused** — not a 500, not a degraded page, nothing listening at all.

Three separate bugs, each one invisible to a test that did not involve a
cluster.

### 1. The readiness probe took every replica out of rotation

The gateway's `/readyz` checked Redis. With Redis gone, all three gateways went
NotReady, the Service lost every endpoint, and the NodePort refused
connections. The fallback that had been built for exactly this outage never
ran, because nothing could reach the pod that would have used it.

Readiness answers one question: *should this pod receive traffic?* It is only
useful when a **different replica would do better**. For a stateless edge whose
dependencies are shared by every replica, failing readiness on a dependency
takes the entire Service out of rotation at once — it converts a degradation
into an outage, which is precisely backwards.

What each service checks now, and why the answers differ:

| Service | Readiness checks | Reasoning |
|---|---|---|
| gateway | nothing | Rate limiting fails open; subscriptions need NATS but queries do not. No dependency makes one replica worse than its siblings. |
| timeline-svc | social-svc | It can serve from Postgres without Redis, so Redis is not a reason to refuse traffic. |
| social-svc | Postgres | It is the source of truth. A replica whose pool has broken while its siblings' are healthy is exactly the case readiness exists for. |
| fanout-worker | Redis + NATS | It exists to read one and write the other. Nothing routes traffic to it, so this is a rollout signal rather than a load-balancer instruction. |

### 2. The services could not start without Redis

With readiness fixed, the new pods went straight to `CrashLoopBackOff`.
`kv.Open` pinged Redis and returned an error, so the process exited.

That is the worst possible property during a Redis incident: no rollout, no
rescheduling, no scaling, and every pod that restarts for any unrelated reason
stays down until the incident ends. go-redis dials lazily and reconnects on its
own, so a client built against an unreachable server becomes useful the moment
the server returns.

A failed ping is now a warning that names the consequence.

Then the same bug again, one layer up: the rate limiter's Lua script preload
was also fatal. `Allow` already falls back to `EVAL` on `NOSCRIPT` — that path
exists because Redis forgets scripts on restart, and an unreachable Redis is
the same situation with a longer outage.

**Three places assumed a dependency must be present at startup, and all three
were written by someone who had already decided the system should tolerate its
absence at runtime.** Startup is where optimism about dependencies survives
longest, because nothing exercises it until something is actually down.

### 3. Discovering the outage cost more than the caller would wait

With the pods finally up, reads degraded — 47 of 60. The other 13 timed out.

The source path was taking **0.5 to 2.5 seconds** against a downstream budget of
three, because every single request paid the full cost of rediscovering that
Redis was gone. Whether a request fit inside the budget was luck, which is why
the same outage produced a 78% success rate rather than a clean anything.

Two fixes, in order of how much they mattered:

**No retries.** go-redis retries three times with backoff by default, which is
right for a blip on a server that is there and exactly wrong for one that is
not. One timeline replica fell back and served the read while its identical
sibling was still retrying when the deadline expired. The retry for this system
*is* the fallback, and the fallback cannot run until the first attempt gives up.

**A circuit breaker.** Three consecutive failures open it for five seconds, and
while it is open Redis is not called at all. The cost of an outage becomes one
discovery per window instead of one per request.

| | reads served | mean |
|---|---|---|
| Before | 47 / 60 | 1,376 ms |
| No retries + breaker | **60 / 60** | **92 ms** |
| Healthy, for comparison | 60 / 60 | ~10 ms |

A half-open probe is let through each window, so recovery is noticed in seconds
without a thundering herd aimed at a server that has just come back.

### The final result

```
== deleting Redis entirely
   redis is gone
   signed in with Redis down
   60 served with content, 0 empty, 0 failed (mean 265ms)
   PASS: every read was served from the source of truth
```

Signing in matters as much as reading: the rate limiter lives in Redis, and a
login path that needed it would have made the outage total regardless of what
the read path did.

The degraded path is the phase 2 design, kept deliberately — except computed
properly. Phase 2 fanned out one call per followed account; `ListFollowedPosts`
is one SQL query that joins `follows` to `posts` and walks them in ULID order.
The same answer, without the N+1 that made it worth replacing.

## The gap this left behind

Redis came back **empty**, and every read then succeeded and returned nothing.

An instance that is up and cold is indistinguishable from a user whose timeline
is genuinely empty. The breaker closed, the materialised path reported healthy,
and the feed was blank — while the posts sat in Postgres the whole time.

The immediate fix is persistence, which the compose stack already had and the
first version of these manifests dropped: Redis is now a StatefulSet with a
volume and `appendonly yes`, so a restart resumes instead of starting over.

**That is not a complete answer**, and it is worth saying so. A Redis that loses
its volume, or a cluster replaced wholesale, still comes back cold. The real
answer is a backfill, and the primitive for it now exists — `ListFollowedPosts`
is exactly the query that would rebuild a timeline. It is not built.

## The outbox, under a real partition

```
== partitioning NATS
   nats is gone
   5 of 5 writes accepted with no bus
   5 events waiting in the outbox
   PASS: the outbox drained in 4s with nothing lost
```

Writes never blocked, because the post and its event commit in one transaction
and publishing happens afterwards. The reader saw 1 post during the partition
and 6 after recovery — nothing lost, nothing duplicated, which is the property
`ZADD` gives for free and phase 3 tested across ten kill-restart cycles.

## Images

| | |
|---|---|
| Base | `gcr.io/distroless/static-debian12:nonroot` |
| Binary, stripped | **28.2 MB** |
| Binary, unstripped | 38.9 MB |
| gateway image | 43.9 MB |
| social-svc | 37.5 MB |
| timeline-svc | 40.3 MB |
| fanout-worker | 39.8 MB |
| migrate | 22.9 MB |
| Build image, for comparison | 1.31 GB |

`-trimpath -ldflags="-s -w"` saves 27% of the binary. The build spec guessed
"well under 30MB" before the dependency set existed; with gRPC, OpenTelemetry,
pgx, go-redis, NATS and gqlgen linked in, 28 MB is the binary and there is no
honest way to make the image smaller than the thing inside it. The number that
matters is not 30: it is that the runtime image has no shell, no package
manager and no OS packages, so a remote code execution lands somewhere with
nothing to execute.

The migrator carries its schema. `//go:embed *.sql` in `migrations/`, because a
distroless image has one binary and no filesystem — a migrator that read SQL off
disk would start, find an empty directory, and cheerfully report the database
up to date.

## Autoscaling

The gateway scales on CPU: it is request-driven, its work is proportional to
requests, and CPU is what runs out.

The fanout worker does not, and that is the interesting half. CPU tells you how
hard the running workers are being pushed; it says nothing about how far behind
they are, and those come apart exactly when it matters. One worker at 40% CPU
with 200,000 events queued — because each fanout is bounded by Redis round trips
rather than by the CPU — needs help that a CPU-based autoscaler will never give
it. KEDA reads JetStream consumer lag directly.

Both are live on the cluster:

```
NAME                     REFERENCE                  TARGETS      MINPODS MAXPODS REPLICAS
gateway                  Deployment/gateway         cpu: 0%/65%  3       12      3
keda-hpa-fanout-worker   Deployment/fanout-worker   0/50 (avg)   1       10      1
```

**Scale-up on lag is not demonstrated.** A two-account graph drains faster than
the scaler polls: a burst of 200 posts was consumed in under ten seconds, and
replicas never left 1. The scaler is installed, `Ready: True`, and reading the
real consumer's lag — the wiring is verified and the behaviour is not.

Trying to force it produced something more useful than a green tick. With the
worker paused at zero replicas the scaler reported `<unknown>`, because **the
durable consumer is created by the worker itself**. A lag-based autoscaler
cannot scale a consumer from zero if the consumer does not exist until the
consumer runs. `minReplicaCount: 1` is therefore not a nicety in this design; it
is what makes the metric exist at all. The alternative is declaring the consumer
out of band, in the migration job.

I also misread `kubectl get hpa` while checking this — `TARGETS` renders as two
whitespace-separated fields, so the column I was reading as REPLICAS was
MAXPODS, and I briefly believed it had scaled to 10. It had not. Recorded
because the wrong number was in front of me for several minutes and looked
exactly like success.

## Disruption budgets

`minAvailable: 2` on the gateway, absolute rather than a percentage: under an
HPA the replica count moves, and a percentage silently allows more eviction the
busier the service gets — loosest exactly when it should be tightest.

The fanout worker has none, deliberately. It is a queue consumer; evicting all
of it stops progress and loses nothing, because JetStream holds the backlog and
the timeline writes are idempotent. A budget there would protect something that
does not need protecting and block a node drain to do it.

## The dashboard

Provisioned from `deploy/observability/grafana/dashboards/murmur.json`, so it is
in the git history rather than in somebody's browser. `make up` brings up
Prometheus and Grafana alongside the stack; the dashboard is at
<http://localhost:3001/d/murmur-overview/murmur>.

Verified live against three minutes of traffic (428 write cycles, ~15 reads/s).
The panel worth looking at first is **downstream gRPC calls per GraphQL
request**, which reads `3.98` — the number phases 2 through 5 exist to move,
from 163 to 4, on a live graph rather than in a log line.

Panels are chosen for the questions this system actually raises: read p99 split
by path so the cost of degrading is visible rather than averaged away; cache hit
ratio by tier, because "cached" is not a measurement; consumer backlog as
outstanding rather than undelivered, because reporting only undelivered made a
twenty-four-second fanout read as zero in phase 4.

## CI

Three jobs. `check` builds, vets, staticchecks and runs the full suite under the
race detector. `images` builds all five images from the same Dockerfile on every
pull request — so a broken Dockerfile is caught by the change that broke it —
and pushes to GHCR only from main. `loadtest` stands the stack up, seeds two
thousand users and runs the k6 baseline as a gate, where k6's own thresholds
decide pass or fail.

## Reproducing

```bash
make up                      # infra, Prometheus, Grafana
make cluster-up              # k3s, images, manifests, KEDA
sh scripts/chaos.sh all      # the three experiments
```

Each experiment asserts and exits non-zero on failure. A chaos experiment that
cannot fail is a demo.
