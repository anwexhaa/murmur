# Phase 4: the hybrid cutover

Measured 22 September 2026 against the local compose stack: 600,000 users,
2,213,656 follow edges, one account with 500,000 followers.

The decision and its reasoning are in
[adr-001-fanout-threshold.md](adr-001-fanout-threshold.md). This is the record
of what was run.

## Breaking it first

A 500,000-follower account posts once, under phase 3's push-everything design.

| | |
|---|---|
| Fanout wall time | **24,522 ms** |
| Timeline writes, one post | **500,000** |
| Publish call | 33 ms — the write path never noticed |

Then the same thing at the default 60-second acknowledgement deadline, with
concurrency 8 and other posts in flight. The fanout did not finish in time.
JetStream redelivered. The worker did all 500,000 writes again, three more
times:

```
events_total{outcome="ok"}   6            3 posts, one counted 4 times
timeline_writes_total        2,000,010  = 4 × 500,000  +  2 × 5
```

**One post, two million timeline writes.** No corruption — `ZADD` is
idempotent, so every follower still held exactly one copy — but four times the
cost, and four times the head-of-line blocking for everyone else.

## After

Same account, same post, routing on at a threshold of 10,000.

| | Before | After |
|---|---|---|
| Fanout wall time | 24,522 ms | **2,594 ms** |
| Timeline writes | 500,000 | **0** |
| Routed as | push | `routed_total{mode="pull",bucket="gte_100k"}` |
| Visible to followers | yes | **yes — 5 of 5 sampled** |

The remaining 2.6 seconds is the outbox relay's 250 ms poll plus container
start-up, not fanout work. The fanout itself is a single `ZADD` into the
author's own feed.

Followers were sampled at random from the 500,000 and queried through the real
`TimelineService`, not by reading Redis directly — so what was verified is that
the merge works, not merely that the data is somewhere.

## The numbers behind the threshold

Write side, end to end through the worker: **~50 µs per follower**, including
the follower paging rather than just the Redis write.

Read side, `BenchmarkPullRead` against real Redis:

| Heavy accounts merged | Time | Premium |
|---|---|---|
| 0 | 145 µs | — |
| 1 | 346 µs | +201 µs |
| 5 | 425 µs | +280 µs |
| 10 | 565 µs | +420 µs |
| 20 | 699 µs | +554 µs |

The first merged author costs ~200 µs because it adds a round trip. Each one
after that costs ~19 µs, because they share it.

Heavy-set size at each candidate threshold, from the real graph:

| Threshold | Heavy accounts | Worst single push |
|---|---|---|
| 1,000 | 132 | 0.05 s |
| 5,000 | 35 | 0.25 s |
| **10,000** | **19** | **0.5 s** |
| 25,000 | 9 | 1.25 s |
| 100,000 | 3 | 5 s |

## The finding that changed the design

The naive framing — find where pushing becomes more expensive than merging —
gives the wrong answer, and the measurement says so plainly.

Making an account heavy saves `50 µs × F` per post and costs `19 µs × F × R`,
where R is reads per follower per post. Those are equal at **R ≈ 2.6**. A feed
reader reads far more often than any single account posts, so R is realistically
10 to 100, and **pushing is cheaper at every follower count tested, including
500,000**.

So the hybrid is not justified by cost. It is justified by the shape of the
work: a 500,000-follower fanout is one indivisible 24-second unit that outruns
its own acknowledgement deadline, blocks the accounts behind it, and has no
upper bound. The threshold is a budget on a single unit of work.

At 50 µs per follower, 10,000 caps the largest push a worker can be handed at
half a second — 120× under the acknowledgement deadline, and still 15× under it
at concurrency 8. The redelivery loop cannot start.

## What was built

- `author_stats`, maintained in the same transaction as the follow edge, with
  `seed -stats-only` to rebuild it in one pass and report drift. Drift measured
  after a 2.2M-edge load and a rebuild: **0**.
- A `Router` in the fanout worker, caching follower counts for 30 s. A stale
  count routes one post down the less efficient path, never the wrong one.
- `author:{id}:recent` written for **every** post regardless of mode — one
  extra `ZADD` that buys both threshold transitions for free.
- A `HeavySet` in the read path, refreshed every 15 s, which keeps the last
  good copy on a failed refresh. An empty heavy set would silently make every
  large account invisible.
- `MergeNewestFirst`, a k-way heap merge over the pushed timeline and each
  heavy author's feed.

## The merge, and why it is property-tested

It is the one piece a reader can catch being wrong: a duplicated post or a
post out of order is visible on screen.

`TestMergePropertiesOverGeneratedStreams` generates 500 shapes — varying stream
counts, lengths, page sizes, and a deliberately small ID space so collisions
are common — and asserts the output is strictly descending, free of duplicates,
and exactly the top-k of the union.

The duplicate case is not hypothetical. A viewer who followed an account
*before* it grew heavy has its older posts in their pushed timeline and its
newer posts in the author feed, so the same post genuinely arrives from two
streams.

## Two instrumentation bugs found

**The consumer lag gauge measured the wrong thing.** It reported JetStream's
`NumPending`, which counts only messages not yet *delivered*. A 24-second
fanout is invisible to it: the message was handed out, so the gauge reads zero
while the work is very much not done. The first break-it run declared success
in 2 seconds because of this. It now reports `NumPending + NumAckPending`, and
`NumRedelivered` beside it — which is the signal that a fanout is outrunning
its deadline.

**The seeder refused a stats-only run.** The guard `whaleFollowers >= users`
is true when both are zero, so rebuilding counters without seeding was
impossible. Now guarded on `whaleFollowers > 0`.
