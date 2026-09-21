# Phase 3: fanout on write

Measured 21 September 2026 against the local compose stack.

Posting is now an event. A post commits with its event in one transaction, a
relay moves that event to JetStream, and a worker writes the post ID into
every follower's timeline. Reading a feed became a Redis range behind one RPC.

## The read path

| | Phase 2 | Phase 3 |
|---|---|---|
| Downstream calls per 50-post timeline | 163 | **151** |
| Timeline assembly | 1 `ListFollowing` + 12 `ListAuthorPosts` | 1 `GetTimeline` |
| Cost scales with | accounts the viewer follows | nothing |

```
 50  GetUser            ← phase 5
 50  GetFollowerCount   ← phase 5
 50  IsFollowing        ← phase 5
  1  GetTimeline
```

The 13 calls that grew with the size of the viewer's following list are gone,
and what remains is pure resolver fan-out — a fixed 3 calls per post, which is
exactly what phase 5 batches away.

Latency barely moved: p50 24.95 ms, p90 32.71 ms, p95 35.53 ms, **p99 43.06 ms**
(200 samples), against 45.67 ms in phase 2. That is expected and not a
disappointment. The 150 remaining calls dominate the wall time, and they run
concurrently, so removing 12 of them changes the shape of the work rather than
its duration. The number that moved is the one that would have kept growing.

## The write path

12,000 seeded posts, 2,000 users, 18,468 follow edges.

| | |
|---|---|
| Outbox drained | **41 s** for 12,000 events |
| Events fanned out | **12,000**, outcome `ok`, zero failures |
| Timeline writes | **110,808** |
| Publish-to-visible | **min 463 ms · p50 482 ms · max 588 ms** (8 samples) |

Publish-to-visible is dominated by the relay's 250 ms poll interval, not by
any of the work. It is a tunable, not a floor: a relay woken by a notification
rather than a ticker would cut most of it, and that is the obvious thing to do
if the number ever matters. It does not yet.

## Fanout cost by follower count

This is the measurement phase 4 is built on.

| Followers | Events | Mean fanout |
|---|---|---|
| under 100 | 11,832 | **3.9 ms** |
| 100 – 1k | 150 | **30.4 ms** |
| 1k – 10k | 18 | **169 ms** |

Each order of magnitude of followers costs roughly eight times as much work.
Extrapolated, a 100k-follower account is seconds and a 500k-follower account
is tens of seconds — during which the consumer falls behind and every other
author's post waits. Phase 4 measures where that stops being worth paying,
rather than guessing.

The numbers above are means, because the per-bucket sample counts are too
small for a meaningful p99 at the top end — 18 events is not a distribution.
Phase 4 seeds specifically for this and will produce percentiles.

## Crash recovery

The claim this phase exists to support, as a test rather than a demonstration:
`TestKillingTheWorkerLosesNothingAndDuplicatesNothing`.

Ten kill-restart cycles, a worker killed every 120 ms mid-fanout, 400
followers, 10 posts. Afterwards every follower holds exactly 10 posts.

**0 entries lost. 0 entries duplicated.**

Both halves come from one property: `ZADD` of a member already present changes
nothing. A sorted set cannot hold the same member twice, so "exactly 10" proves
absence of loss and absence of duplication in a single assertion — and
at-least-once delivery needs no deduplication table, no processed-message set,
and no coordination between workers to be safe.

The other guarantees, also tested:

- A post and its event commit together, so a post that exists always has a
  fanout owed to it (`TestPostAndEventCommitTogether`).
- A failed post writes no orphan event (`TestAFailedPostWritesNoEvent`).
- Two relays claiming at once never take the same event, via
  `FOR UPDATE SKIP LOCKED` (`TestConcurrentClaimsDoNotOverlap`).
- A malformed event is dead-lettered rather than retried forever, and the
  valid event behind it still gets through
  (`TestMalformedEventIsDeadLetteredNotRetriedForever`).
- A transient downstream failure is retried and then succeeds, rather than
  being parked (`TestTransientFailureIsRetriedAndThenSucceeds`).
- A timeline cap holds exactly, even under eight concurrent writers
  (`TestCapHoldsUnderConcurrentWriters`).

## Three things that went wrong

Worth recording, because each one was invisible until something specific
caught it.

**The outbox payload column was the wrong type.** It was `jsonb`, and the
payload is encoded protobuf, which is not valid UTF-8. Postgres rejected it at
COPY time with `invalid byte sequence for encoding "UTF8"`. Now `bytea`, which
is also the better design: the outbox stores exactly the bytes it will
publish, so there is no transformation between what was committed and what was
delivered.

**Lua scripts do not work in a pipeline the way they do outside one.**
go-redis runs a script by trying `EVALSHA` and falling back to `EVAL` when
Redis answers `NOSCRIPT`. Inside a pipeline it cannot: the commands are queued
and the error only surfaces at `Exec`, long after the chance to substitute.
The script is now loaded explicitly once and invoked by hash, with a reload on
`NOSCRIPT` in case Redis restarts.

**`TruncateAll` did not include the new table**, so outbox events leaked
between integration tests — and two tests leaked open transactions, whose row
locks then blocked the next test's `TRUNCATE`. The social package took **990
seconds** and failed five tests. Both fixed; it now runs in 2.2 seconds.
