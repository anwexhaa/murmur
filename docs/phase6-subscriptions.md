# Phase 6: real-time subscriptions

Measured 22 September 2026 against the local compose stack: 500 users, 3,880
follow edges, and one author (`seed_00000001`) with **466 followers**. Three
gateway replicas — a, b and c — on ports 8080, 8085 and 8090, all three
identical processes sharing nothing but NATS, Redis and Postgres.

## The headline

**A post published through replica A arrives on a socket held by replica C.**

```
subscribed on C (ws://murmur-gateway-c:8080/query)
publish   on A (http://localhost:8080/query)

update 1: {"data":{"timelineUpdates":{"gap":false,"post":{
  "author":{"handle":"seed_00000001"},
  "body":"cross-pod check two: A publishes, C delivers",
  "id":"01M34Q4PXZ7J9JMTFWFSGG63M4"}}}}
```

Repeated at volume: subscribers on **B and C**, 100 posts published through
**A**, every one delivered to both.

| | Replica B | Replica C |
|---|---|---|
| Updates received | **100 / 100** | **100 / 100** |
| Gaps | 0 | 0 |
| p50 | 157.7 ms | 157.5 ms |
| p95 | 279.6 ms | 279.5 ms |
| p99 | **309.4 ms** | **292.7 ms** |

This is the only claim in the phase that a single replica could not have faked,
and it is the reason the hub is built the way it is.

## Why the obvious design is wrong

The natural first implementation of a subscription hub is a map from user ID to
a list of channels, guarded by a mutex. It is simple, it is fast, and every test
that runs in one process passes.

It also delivers nothing the moment there are two replicas. The client's socket
is held by whichever gateway its load balancer picked; the post is created on
whichever gateway the author's load balancer picked. Those are the same process
only by coincidence, and with three replicas the coincidence happens a third of
the time — which is worse than never, because it looks like flakiness rather
than a design error.

So the routing lives in NATS instead:

```
fanout worker  --publish-->  timeline.<user-id>  --deliver-->  every replica
                                                               holding a socket
                                                               for that user
```

Each gateway subscribes only to the subjects for the users currently connected
to *it*. The publisher does not know or care where anyone is connected. There is
no shared registry to keep consistent, no sticky sessions, and no reason for a
publish on one replica to know about a socket on another.

`TestUpdatesCrossReplicas` runs two hubs over two separate NATS connections and
asserts an update published through one arrives on the other. It is the unit
test equivalent of the run above, and it is the test that caught the bug below.

## The bug a single connection cannot show you

`Hub.Subscribe` returned before NATS had registered the subscription.

`nats.Subscribe` only buffers the SUB protocol message; the subscription is not
live until the connection flushes. A caller that subscribed and then read its
timeline could miss anything published in between — the exact gap the
replay-then-live handoff exists to close, reopened one layer down.

On a single connection this is invisible, because a publish on the same
connection flushes the pending SUB along with it. It shows up the moment the
publisher is a different process, which is to say in production. The fix is a
`FlushTimeout` before returning, and the reason it was found at all is that
`TestUpdatesCrossReplicas` uses two connections rather than one.

## The bug only a real upgrade could show you

The first live subscription attempt failed with **HTTP 501**:

```
unable to upgrade *gateway.statusRecorder to websocket
  failed to accept WebSocket connection:
  http.ResponseWriter does not implement http.Hijacker
```

The gateway's instrumentation middleware wraps every `http.ResponseWriter` in a
`statusRecorder` so the status code can be logged. Wrapping a ResponseWriter
silently removes every optional interface the original implemented — `Flusher`,
`Hijacker`, `ReaderFrom` — because the wrapper satisfies only
`http.ResponseWriter` and Go has no way to forward the rest.

Every query kept working. Only the upgrade broke, and the 501 blamed the
transport rather than the wrapper that had removed its ability to function.
`Flush` had already been forwarded, months of queries ago, for exactly this
reason; `Hijack` had not, because nothing had needed it yet.

`TestInstrumentedHandlerCanStillHijack` performs a real upgrade through the real
middleware rather than asserting on a type, because nothing short of an actual
upgrade attempt surfaces the problem.

## Delivery latency, and what actually sets it

End-to-end from the post's own `createdAt` to the byte arriving on the client —
so the number covers the outbox relay, JetStream, the fanout worker, NATS and
the socket, not just the part inside the gateway.

| `OUTBOX_INTERVAL` | p50 | p95 | p99 | max |
|---|---|---|---|---|
| 250 ms (default) | 157.7 ms | 279.6 ms | 309.4 ms | 374.7 ms |
| **25 ms** | **31.4 ms** | **55.0 ms** | **125.5 ms** | 222.9 ms |

100 posts each, one subscriber, same machine, same graph.

**The socket path is not the cost.** Changing one configuration value — how
often the transactional outbox relay polls — moved p50 by 5×. Of the remaining
31 ms, the fanout itself accounts for **13.2 ms** mean
(`murmur_fanout_duration_seconds`, 1.3248 s across 100 posts at 466 followers
each), leaving roughly 18 ms for the outbox wait, JetStream, NATS and the
WebSocket write combined.

This is worth stating plainly because it is the opposite of the intuition that
made WebSockets feel like the interesting part of the phase. The interesting
part was a polling interval in a component written three phases ago.

The 250 ms default stays. It is a throughput/latency trade the relay makes on
behalf of the write path, and a subscription that is a sixth of a second behind
is not a subscription anybody notices being behind. The number is here so the
trade is visible rather than accidental.

## Capacity: 2,000 sockets on one replica

2,000 connections ramped at 400/s against replica C, held for 60 seconds, then
20 posts published through replica A — so every connection receives every post.

| | |
|---|---|
| Connections opened | **2,000**, 0 failed |
| Ramp | 5.01 s |
| Memory, idle at 2,000 connections | **139.6 MiB** (≈ 66 KiB/connection) |
| Memory, during delivery | 274.5 MiB |
| Updates delivered | **40,000** |
| Dropped | **0** |
| Evicted | **0** |

Baseline for the memory figure: the same process holds **7.96 MiB** with no
connections.

### What that run actually found

Delivery latency collapsed under it:

| | 1 socket | 2,000 sockets |
|---|---|---|
| p50 | 31.4 ms | **702.1 ms** |
| p95 | 55.0 ms | 907.6 ms |
| p99 | 125.5 ms | **986.7 ms** |

Nothing was dropped and nothing was evicted, so the backpressure policy was not
the problem. The arithmetic was: `hydrateUpdate` made **one `GetPost` call per
update**, and 2,000 sockets × 20 posts is **40,000 gRPC calls for 20 distinct
rows**.

This is the same N+1 the read path spent phase 5 removing, reappearing on the
live path in a shape the request-scoped DataLoaders could not reach. A loader
batches *within* one request; here every delivery is its own request on its own
goroutine, all arriving within microseconds of each other. There is nothing to
batch along that axis and everything to share along the other one — a broadcast
means every subscriber wants the *same* post at the *same* instant.

### The fix, and the same run again

A per-replica hydrator: a TTL'd LRU keyed by post ID with `singleflight` in
front of it.

| | Before | After |
|---|---|---|
| Hydrations | 40,000 | 40,000 |
| **`GetPost` calls** | **40,000** | **21** |
| p50 | 702.1 ms | **121.2 ms** |
| p95 | 907.6 ms | 433.6 ms |
| p99 | 986.7 ms | **677.4 ms** |
| Peak memory | 274.5 MiB | 230.5 MiB |

```
murmur_hydrator_lookups_total{tier="local"}    13,540
murmur_hydrator_lookups_total{tier="shared"}   26,439   <- singleflight
murmur_hydrator_lookups_total{tier="source"}       21
murmur_hydrator_collapsed_total                26,439
murmur_hydrator_entries                            20
```

**21 fetches for 20 posts.** The `shared` tier is larger than the `local` tier,
which is the signature of a broadcast: most subscribers arrive while the fetch
is still in flight, not after it has completed. A plain cache without
singleflight would have served those 26,439 lookups as 26,439 downstream calls,
because none of them had anything to hit yet.

The hydrator deliberately is *not* the timeline service's three-tier cache. The
posts it holds are seconds old and every subscriber wants the same handful at
the same instant, so the tier that matters is the one with no network in it; a
Redis hop would add latency to the exact path this exists to make fast.

### What is left

p99 is still 677 ms at 2,000 sockets against 125 ms at one. The remaining cost
is the gateway's own per-socket work — 40,000 GraphQL field resolutions and
40,000 WebSocket writes from one process in a burst — and it is no longer
downstream calls, because there are 21 of those.

The answer to that is more replicas, which is precisely what the NATS-routed hub
makes possible: the subscription state that would make a gateway hard to scale
horizontally does not exist. A three-way split of the same 2,000 connections is
the run that would prove it, and it is the one measurement this phase is still
missing.

## Backpressure

Per connection: a **64-slot** buffer, and eviction after **128** consecutive
drops.

The buffer is small on purpose. It exists to absorb a brief stall, not to store
a backlog — a client persistently slower than its timeline is moving cannot be
helped by a bigger buffer, only delayed, and memory spent on it is memory
multiplied by every connection. At 2,000 connections, a 64-slot buffer is part
of what makes 66 KiB per socket possible.

The NATS delivery callback must never block. Blocking there stalls delivery for
every other subject on the same connection, so one slow client could stop
updates for every other client on the replica. The send is therefore
non-blocking and a full buffer drops rather than waits:

```go
select {
case s.ch <- update:
        s.dropped.Store(0)
case <-s.closeCh:
        return
default:
        s.gapped.Store(true)
        if s.dropped.Add(1) >= s.hub.maxDrops {
                s.Close()
        }
}
```

A drop is not data loss, because the timeline in Redis is authoritative. The
dropped update sets a **gap flag** that travels with the next update that does
get through, and a client seeing `gap: true` refetches. The flag is claimed with
a compare-and-swap and put back if the send that would have carried it fails, so
a gap cannot be lost by being dropped.

Past 128 consecutive drops the connection is closed. A client that never drains
would otherwise hold a buffer and a NATS subscription forever while receiving
nothing; a disconnect is a signal it can act on, an indefinitely stalled stream
is not.

Proven by unit test rather than by assertion:

- `TestSlowClientIsGappedNotBlocked` — a subscriber that stops reading gets a
  gap flag, and the hub keeps running.
- `TestOneSlowClientDoesNotStallAnother` — two subscribers, one stalled; the
  other receives everything.
- `TestHopelesslySlowClientIsEvicted` — the drop counter reaches the threshold
  and the subscription closes itself.
- `TestCloseDuringDeliveryDoesNotPanic` — the reason `Close` does not close the
  update channel.

## Lifecycle

`Close` deliberately does **not** close the update channel. A NATS delivery
goroutine may be inside `handle` at that moment, and closing a channel another
goroutine is selecting a send on is a panic, not a race the detector would even
have to find. The sender never closes; the consumer watches `Done()` instead,
which is the only arrangement that is safe without a lock on the hot path.

`goleak` runs at package scope over the hub tests: no goroutine survives a
closed connection.

The WebSocket also had to be exempted from the request timeout. `Instrument`
gives every request a 15-second deadline so a request the client has abandoned
stops costing the backends money — and applying that to an upgrade would have
cancelled every subscription's context on a fixed clock, fifteen seconds in. A
subscription's lifetime is bounded by its socket and by the keepalive ping that
notices when the peer is gone, not by a deadline meant for a query.

## Replay

A reconnecting client passes `after: <last post id it saw>`. The gateway
subscribes *first*, then replays, then switches to live — subscribing after the
replay would leave a window exactly as wide as the replay takes.

The replay reads the **timeline**, not the event stream. The timeline is already
materialised and authoritative, so it works however long the client was away and
however many replicas have restarted since. It is bounded to one page of 50: a
client away for two minutes gets everything it missed, and a client away for two
days gets the newest page plus a gap flag telling it to refetch, which is cheaper
for both sides than streaming a day of history down a socket.

One loose end closed while writing this: when the replay itself *failed*, it
returned "you have a gap" and nothing to attach the flag to, and the flag was
dropped. It now survives into live delivery and lands on the first update that
gets through. A client believing it has an unbroken stream while a hole sits
behind its cursor is the one failure a gap flag exists to prevent.

## Instruments

```
murmur_subscriptions_connections        gauge
murmur_subscriptions_delivered_total    counter
murmur_subscriptions_dropped_total      counter
murmur_subscriptions_evicted_total      counter
murmur_subscriptions_buffer_depth       histogram
murmur_subscriptions_delivery_seconds   histogram
murmur_hydrator_lookups_total{tier}     counter
murmur_hydrator_collapsed_total         counter
murmur_hydrator_entries                 gauge
```

`dropped_total` being non-zero is not a failure — it is the backpressure policy
working. A rising *rate* means clients are slower than the timeline is moving.

The hydrator's source counter uses a flag set inside the singleflight closure
rather than the `shared` return value, for the reason phase 5 learned the hard
way: `singleflight.Do` reports `shared=true` to the leader as well as the
followers, so branching on it makes the counter that proves the cache works read
zero exactly when sharing starts. Four phases, four instrumentation bugs — this
one was avoided rather than discovered, which is the first time that has
happened.

## Reproducing

```
make run-social run-timeline run-fanout     # each in its own shell
make run-gateway-replicas                   # a, b, c on 8080, 8085, 8090
make realtime AUTHOR=<id> FOLLOWER=<id>
```

`scripts/subscribe.go` drives one subscription and prints delivery percentiles.
`scripts/wsbench.go` opens thousands and reports the same numbers under load.
Both are `//go:build ignore` programs rather than commands, because neither is
part of the deployed system.
