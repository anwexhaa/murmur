# Phase 5: caching and the read path

Measured 22 September 2026 against the local compose stack: 2,000 users,
18,523 follow edges, 12,000 posts — the same dataset shape as the phase 2
baseline, so the numbers are comparable.

## The headline

**163 → 4 downstream calls for one 50-post timeline.**

```
phase 2   50 GetUser + 50 GetFollowerCount + 50 IsFollowing
          + 12 ListAuthorPosts + 1 ListFollowing            = 163

phase 3   50 GetUser + 50 GetFollowerCount + 50 IsFollowing
          + 1 GetTimeline                                   = 151

phase 5   1 BatchGetUsers + 1 BatchGetFollowerCounts
          + 1 FilterFollowing + 1 GetTimeline               =   4
```

Confirmed by the Prometheus histogram rather than the log alone:
`murmur_gateway_downstream_calls_per_request_sum{operation="HomeTimeline"}`
divided by `_count` is 831/201 — four per request in steady state, with the
extra on the first request before the gRPC connections were warm.

Crucially, **4 does not grow with the page size.** Each of the three loaders
issues exactly one call whether the page holds ten posts or a hundred, which is
the property the original 163 lacked.

| | Phase 2 | Phase 3 | Phase 5 |
|---|---|---|---|
| Calls per request | 163 | 151 | **4** |
| p50 | 24.24 ms | 24.95 ms | **9.25 ms** |
| p90 | 32.02 ms | 32.71 ms | **11.94 ms** |
| p95 | 34.72 ms | 35.53 ms | **13.22 ms** |
| **p99** | **45.67 ms** | **43.06 ms** | **16.29 ms** |

200 samples each, same machine, same dataset shape. **p99 improved 2.8×.**

## The viral scenario

1,000 → 300 concurrent readers all reading the same timeline, so every request
hydrates the same fifty post IDs. Cache cold at the start.

| | |
|---|---|
| Requests | 11,937 at 298/s |
| Post hydrations | **596,850** |
| Reached the source | **50** |
| Errors | **0** |
| p95 | 1.71 s |

```
murmur_postcache_lookups_total{tier="local"}   596,098   99.87%
murmur_postcache_lookups_total{tier="redis"}       702    0.12%
murmur_postcache_lookups_total{tier="source"}       50    0.008%
```

**50 is exactly the number of distinct posts on the page.** It did not move as
virtual users climbed from 50 to 300. That is the claim: downstream fetches are
a function of how many distinct posts exist, not of how many people are reading
them.

### What the first run found instead

At 1,000 concurrent readers the same test returned **30.8% GraphQL errors** and
a p95 of 12.65 s. Every one of the 91,836 errors was `DeadlineExceeded`, and
every one was on the same field: `user.followerCount`.

That is not a cache failure — the cache held perfectly at 1,000 readers too
(312,364 lookups, 50 to the source). It is the next bottleneck becoming
visible. Follower counts go to Postgres through `BatchGetFollowerCounts` with
no cache in front of them, so once post hydration stopped being the constraint,
they became it.

This is worth recording rather than tuning away. Fixing one bottleneck does not
make a system fast; it makes the next bottleneck the answer. The honest capacity
of this stack on one laptop is around 300 concurrent readers on this query, and
the thing to cache next is follower counts.

## Baseline

Steady mixed load: 50 readers ramping, 5 writes/second, 55 seconds.

| | |
|---|---|
| Requests | 9,440 at 171/s |
| p95 | **89.52 ms** |
| Errors | **0** |

The writes matter: they keep the fanout worker and the invalidation path
active while the reads are measured, so the numbers are not from an artificially
quiet system.

## What was built

### Per-request loaders

Three loaders — users, follower counts, viewer-follows — created in middleware
and destroyed with the request.

They must not be shared, and the reason is not performance. `viewerFollows`
depends on who is asking, so a loader outliving its request would serve one
viewer's answer to another. That is a data-leak bug, and building them in
middleware is what prevents it. `TestLoadersAreScopedToTheirRequest` asserts
two viewers produce two calls rather than one cached answer.

`viewerFollows` batches through `FilterFollowing` — the RPC phase 4 already
needed for the pull side. "Which of these candidates does X follow" turned out
to be exactly the batch form of `IsFollowing`, so no new RPC was required.

The resolvers still read as though they fetch one thing each, because from
their own point of view they do. The batching lives entirely in the loaders,
which is why this rewrite could not disturb the measurement that judges it.

### Three-tier post cache

```
in-process LRU  →  Redis  →  BatchGetPosts over gRPC
```

Each tier answers a different question. The LRU removes the network for the
handful of posts everyone is reading right now. Redis shares that work across
replicas, so a post fetched by one process is free for the others
(`TestRedisTierServesASecondProcess`). The gRPC call is the only tier that can
produce a post nobody has read, and it is the one the other two exist to avoid.

`singleflight` sits in front of the misses, keyed per post ID rather than per
batch so two readers wanting overlapping sets still share the posts they have
in common. `TestSingleflightCollapsesAStampede` releases 500 goroutines at one
instant against a deliberately slow source: **500 concurrent readers, 2
downstream fetches.**

### Invalidation, with a bound

Deleting a post publishes `post.deleted`, which clears Redis for everyone and
each replica's memory via an **ephemeral** consumer — not a durable one. A
durable queue-group consumer would deliver each event to exactly one replica,
leaving the others serving a post that no longer exists. Here the requirement
is the opposite of work-sharing: every replica needs the message.

The local TTL is the backstop for when that event is lost.
`TestLocalTierExpiresWithinItsTTL` asserts both halves: the post *is* still
served inside the TTL, and it *is not* afterwards. A staleness window that is
only claimed is not a bound.

Deletes are soft. The post ID is already in hundreds of thousands of Redis
timelines, and the read path skips IDs it cannot hydrate — removing the row
would leave those as permanent silent gaps.

## The instrumentation bug, again

The source-tier counter read **zero** while the service was demonstrably
fetching from the source.

`singleflight.Do` reports `shared=true` to *every* participant, including the
leader that actually performed the fetch. Branching on `shared` alone therefore
counted the leader as a collapsed request and never recorded its fetch — so the
moment any sharing occurred, the counter that proves the cache works stopped
counting the thing it measures.

The fix sets a flag inside the closure, which runs exactly once per real fetch,
and counts a collapse only for participants that did not lead.

This is the third measurement bug in three phases — after the consumer-lag
gauge counting only undelivered messages, and the operation label reading a
path every request shares. The pattern is worth naming: **an instrument that
reports a flattering number is indistinguishable from success until something
forces it to be wrong.** Here, a single controlled cold read — one request, no
concurrency — made the counter produce exactly 50, and that is what exposed the
zero.
