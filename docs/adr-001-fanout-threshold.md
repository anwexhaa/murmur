# ADR 001: where push fanout stops

**Status**: accepted, 22 September 2026
**Decision**: stop pushing at **10,000 followers**. Above that, a post is
written only to its author's own feed and merged into readers' timelines at
read time.

Every number below was measured on this system, on one laptop, against the
same Redis and the same 600,000-user graph. Nothing here is taken from a
conference talk.

## The measurement that started it

A 500,000-follower account posts once, with phase 3's push-everything design.

| | |
|---|---|
| Fanout wall time | **24,522 ms** |
| Timeline writes for one post | **500,000** |
| Publish call latency | 33 ms — the write path is unaffected |

The publish returns immediately; it is the fanout behind it that takes
twenty-five seconds. That alone would be tolerable. What is not tolerable is
what happens next.

### The feedback loop

At the default 60-second acknowledgement deadline, running at concurrency 8
alongside other traffic, that fanout did not finish in time. JetStream
concluded the worker had died and redelivered the message. The worker started
the same 500,000 writes again. That happened three times.

```
events_total{outcome="ok"}   6      3 posts, one of them counted 4 times
timeline_writes_total        2,000,010
                             = 4 × 500,000  +  2 × 5
```

One post cost **two million timeline writes**. The arithmetic is exact and
leaves no room for interpretation: the whale's fanout completed four times.

Nothing was corrupted — `ZADD` of an existing member is a no-op, so every
follower still held exactly one copy. The damage was entirely to cost and to
everything queued behind it. *Slow* became *slow enough to be retried*, which
became *four times slower*.

## What the naive framing gets wrong

The obvious way to choose a threshold is to find where pushing becomes more
expensive than merging. So that was measured first, and the answer was
surprising.

Read side, measured with `BenchmarkPullRead` against real Redis:

| Heavy accounts merged into one read | Time | Premium |
|---|---|---|
| 0 (fully pushed) | 145 µs | — |
| 1 | 346 µs | +201 µs |
| 2 | 410 µs | +265 µs |
| 5 | 425 µs | +280 µs |
| 10 | 565 µs | +420 µs |
| 20 | 699 µs | +554 µs |

The first merged author costs ~200 µs, because it adds a round trip that a
pure-push read does not make. Each additional author after that costs about
**19 µs**, because they are pipelined into that same trip.

Write side, end to end through the real worker: about **50 µs per follower**,
including the follower paging, not just the Redis write.

Now the comparison. Making one account heavy *saves* `50 µs × F` per post it
publishes, and *costs* `19 µs × F × R`, where R is how many times each
follower reads while that post is still in their window. Setting them equal:

```
50 µs × F  =  19 µs × F × R
          R ≈ 2.6
```

**If a follower reads their timeline more than about three times per post
from that author, pushing is cheaper.** In a feed, readers read far more often
than any single account posts — R is realistically 10 to 100. On raw
aggregate cost, pushing essentially always wins, at every follower count
tested, including 500,000.

So the hybrid design is **not** justified by total cost. That was the
assumption going in and the measurement contradicted it.

## What actually decides it

Cost is the wrong axis. The problem with a 500,000-follower fanout is not the
total work — it is that the work arrives as **one indivisible 24-second unit**.
That has four consequences no aggregate cost model captures:

1. **It outruns its own acknowledgement deadline**, and the redelivery
   multiplies it by four. Measured above.
2. **It blocks the accounts behind it.** The consumer has finite concurrency;
   a slot held for 24 seconds is a slot not fanning out anyone else's post.
3. **It does not amortise.** Ten large accounts posting in the same minute is
   five million queued writes, and the backlog grows faster than it drains.
4. **It is unbounded by anything the system controls.** Follower counts have
   no ceiling, so the worst case is whatever the largest account happens to be.

The threshold is therefore a **budget on a single unit of work**, not a
cost crossover:

```
threshold = acceptable single-fanout wall time ÷ per-follower push cost
```

## The decision

At 50 µs per follower, choosing 10,000 means the largest push a worker can
ever be handed is:

```
10,000 × 50 µs ≈ 0.5 s
```

That is **120× under the 60-second acknowledgement deadline**, and still 15×
under it at concurrency 8 where each slot effectively gets 7.5 seconds. The
redelivery loop cannot start.

What it costs, from the real 600,000-user graph:

| Threshold | Heavy accounts | Worst single push | Read premium |
|---|---|---|---|
| 1,000 | 132 | 0.05 s | up to ~2.7 ms |
| 5,000 | 35 | 0.25 s | up to ~0.85 ms |
| **10,000** | **19** | **0.5 s** | **up to ~0.55 ms** |
| 25,000 | 9 | 1.25 s | up to ~0.35 ms |
| 100,000 | 3 | 5 s | up to ~0.24 ms |

10,000 keeps the heavy set to 19 accounts. A reader who followed *all
nineteen* would pay about 0.55 ms — and in this graph the median account
follows three accounts in total, so the realistic premium is the ~200 µs of
the first merge.

Going lower buys little: the worst push is already half a second. Going higher
costs the safety margin that the whole ADR exists to protect, for a read
saving measured in hundreds of microseconds.

**The threshold is set by the latency budget for one fanout, and the read-side
premium is cheap enough not to constrain it.**

## Consequences

### Verified

With routing on, the same 500,000-follower post:

| | Before | After |
|---|---|---|
| Fanout wall time | 24,522 ms | **2,594 ms** |
| Timeline writes | 500,000 | **0** |
| Visible to followers | yes | yes — 5 of 5 sampled |

The remaining 2.6 seconds is the outbox relay's poll interval plus process
startup, not fanout work. The fanout itself is one `ZADD`.

### The author feed is written for every post

Not only for heavy accounts. It costs one `ZADD` per post regardless of
follower count, and it buys both transitions for free:

- **Crossing upward** needs no backfill. The account's recent posts are
  already in its feed, so readers begin merging them the moment the heavy set
  refreshes.
- **Crossing downward** is safe for the same reason. Posts published while the
  account was heavy are recoverable from its feed rather than stranded.

This is the one place the design pays a small permanent cost to avoid a large
occasional one, and it is worth stating plainly because it looks like waste
until the transitions are considered.

### Staleness is bounded and one-directional

The worker caches follower counts for 30 seconds; the read path refreshes the
heavy set every 15. An account can therefore be routed on a slightly old
decision. Both routes are correct — a post reaches its followers either way —
so staleness costs efficiency, never visibility. That is precisely what makes
caching acceptable here at all.

### A reader cannot tell

The merge in `internal/timeline/merge.go` is checked by
`TestMergePropertiesOverGeneratedStreams` over 500 generated shapes: the
output is strictly descending, free of duplicates, and exactly the top-k of
the union. The duplicate case is not hypothetical — a viewer who followed an
account *before* it grew heavy has its older posts in their pushed timeline
and its newer posts in its author feed, so the same post can arrive from two
streams.

### What would change this decision

- **Per-follower push cost falling.** At 10 µs the same half-second budget
  would justify 50,000.
- **The read premium rising**, if the heavy set grew large enough that
  `FilterFollowing` stopped being a cheap indexed lookup.
- **A shorter acceptable publish-to-visible latency.** The budget here is
  loose because 0.5 s of fanout is invisible next to the relay's 250 ms poll.

The threshold is `FANOUT_THRESHOLD`, and it is configuration rather than
schema precisely so that re-measuring can change it without a migration.
