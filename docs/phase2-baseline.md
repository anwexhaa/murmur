# Phase 2 baseline: what the obvious design costs

Measured 21 September 2026 against the local compose stack.

This is the "before" half of phase 5's headline number. The gateway here is
written the obvious way on purpose — a resolver per field, each doing its own
lookup, and a timeline assembled by asking every account you follow for their
recent posts. Nothing below is a bug. It is what this design costs, measured
rather than asserted.

## Conditions

| | |
|---|---|
| Query | `HomeTimeline`, 50 posts, each with `author { id handle displayName followerCount viewerFollows }` |
| Viewer | an account following 12 others |
| Dataset | 2,000 users · 18,555 follow edges · 12,000 posts |
| Stack | Postgres 16, one `social-svc`, one `gateway`, all in containers on one laptop |
| Samples | 200 requests, sequential, after warmup |

## The number

**163 downstream gRPC calls to serve one 50-post timeline.**

```
 50  GetUser            one per post, for Post.author
 50  GetFollowerCount   one per post, for User.followerCount
 50  IsFollowing        one per post, for User.viewerFollows
 12  ListAuthorPosts    one per followed account, to assemble the timeline
  1  ListFollowing      to find out who the viewer follows
```

Confirmed by the Prometheus histogram rather than by the log alone:
`murmur_gateway_downstream_calls_per_request_sum{operation="HomeTimeline"}`
divided by `_count` is 32600/200 — exactly 163, on every one of 200 requests.
All 200 observations fall in the 128–256 bucket.

## Latency

| | |
|---|---|
| p50 | 24.24 ms |
| p90 | 32.02 ms |
| p95 | 34.72 ms |
| p99 | 45.67 ms |
| max | 135.63 ms |

These are honest-but-flattering numbers, and the reason matters: the dataset
is small enough that Postgres serves everything from cache, the gateway and
the service are on the same machine, so a "network" round trip is a loopback
copy, and the resolver fan-out runs concurrently rather than in sequence. On
real hardware with real network latency, 163 calls would not cost 24 ms.

The call count is the durable finding. The latency is a floor.

## The trace

`traces/phase2-naive-timeline.json` is one real request, exported from Jaeger.

**723 spans. Two services. Depth 5.**

```
 gateway      1  POST /query                  the root span, 157.68 ms
 gateway    163  <one span per gRPC call>
 social-svc 163  <the server side of each>
 social-svc 163  pool.acquire
 social-svc 220  SELECT
 social-svc  13  connect
```

To see the waterfall, open <http://localhost:16686>, choose **Upload** in the
Jaeger UI, and load that file. A JSON export is committed rather than a
screenshot because it can be re-opened, inspected span by span, and diffed
against the phase 5 trace — none of which a PNG allows.

Two things the trace shows that the call count alone does not:

- **220 SELECTs for 163 calls.** The follow-graph lookups issue more than one
  query each. A count of gRPC calls understates the load on Postgres.
- **163 `pool.acquire` spans.** Every call takes a connection from the pool
  and gives it back. Under concurrency this is the contention point, and it
  is invisible from the gateway side.

## What phase 3 and phase 5 each fix

The 163 splits into two independent problems with two different fixes, and
they should be measured separately:

- **63 calls are timeline assembly** (`ListFollowing` + `ListAuthorPosts`).
  Phase 3 replaces this with a timeline materialised at write time, so the
  read becomes one range query against Redis.
- **150 calls are resolver fan-out** (`GetUser`, `GetFollowerCount`,
  `IsFollowing`). Phase 5 batches these per request, and each should collapse
  to one call regardless of page size.

They overlap because a post's author is also a followed account, which is why
the parts sum to more than the whole. The target after both is a small
constant — single digits — that does not grow with the page size.
