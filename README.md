# Murmur

A real-time feed and fanout service in Go.

Posts land in Postgres, travel an event bus, and are pushed into per-user
timelines in Redis — except for creators too large to push to, whose posts are
merged in at read time. The project exists to produce one honest number: the
follower count at which pushing stops being worth it.

The full build plan lives in [`docs/build-spec.html`](docs/build-spec.html).

## Status

| Phase | | Status |
|---|---|---|
| 00 | Foundations | **done** |
| 01 | Domain over gRPC | **done** |
| 02 | GraphQL gateway | **done** |
| 03 | Fanout on write | **done** |
| 04 | Hybrid cutover | not started |
| 05 | Caching and the read path | not started |
| 06 | Real-time subscriptions | not started |
| 07 | Auth and hardening | not started |
| 08 | Deploy, observe, prove | not started |

## Quickstart

Requires Go 1.27, Docker and GNU Make.

```bash
make up                # start Postgres, Redis, NATS and Jaeger, then migrate
make test              # unit tests, race detector on
make test-integration  # adds the tests that need a real Postgres
make lint              # go vet and staticcheck
make help              # every target
```

To drive the API by hand:

```bash
make seed ARGS="-reset -users 2000 -avg-following 12 -posts-per-user 6"
make run-social                          # terminal 1: write path + outbox relay
make run-fanout                          # terminal 2: materialises timelines
make run-timeline                        # terminal 3: read path
make run-gateway                         # terminal 4: GraphQL
```

Then open the playground at <http://localhost:8080/>, or the Jaeger UI at
<http://localhost:16686>. Sign in by sending an `X-Murmur-User` header with a
user ID — real auth arrives in phase 7.

```bash
make smoke                               # end-to-end gRPC flow
make grpcurl ARGS="murmur-social:9081 list"
```

`make up` waits for every container to report healthy before applying
migrations, so it either works or fails loudly — it never leaves you with a
half-started stack.

## Services

Four processes, split along the axes they actually scale on.

| Binary | Speaks | Owns | Scales with |
|---|---|---|---|
| `gateway` | GraphQL over HTTP + WS | auth, batching, rate limits, subscriptions | connected clients |
| `social-svc` | gRPC | users, follow edges, posts, the outbox | write volume |
| `timeline-svc` | gRPC | timeline assembly, merge, pagination | read volume |
| `fanout-worker` | JetStream consumer | push/pull routing, timeline writes | follower-edge volume |

Each serves `/healthz` and `/readyz` on its own port (8080–8083). The two
probes are deliberately different: liveness touches nothing, because a
liveness probe that pings Postgres turns one slow database into a rolling
restart of every pod that talks to it. Readiness is where dependency checks
belong.

## Layout

```
api/proto/    the .proto source of truth
api/graphql/  the GraphQL schema
api/gen/      generated protobuf Go, committed so CI needs no plugins
cmd/          one directory per binary, plus seed and migrate
internal/
  domain/     entities and rules, no infrastructure imports
  social/     store (pgx), the SocialService, and the outbox relay
  timeline/   materialised feeds in Redis, and the TimelineService
  fanout/     the post.created consumer that writes timelines
  gateway/    resolvers, gRPC clients, middleware, call counting
  platform/   config, logging, lifecycle, health, db, kv, bus, grpcx,
              metrics, otelx
migrations/   goose SQL migrations
scripts/      generate.sh, smoke.sh
deploy/       docker-compose, kubernetes           (phase 8)
loadtest/     k6 scenarios                         (phase 5)
docs/         the build spec, measurements, traces
```

`make generate` regenerates both layers — buf for protobuf, gqlgen for
GraphQL — inside a container, from the tool versions pinned in `go.mod`. The
output is committed, so CI never runs it and nothing needs installing.

Note that gqlgen owns `internal/gateway/schema.resolvers.go` and rewrites it
on every run, moving anything that is not a resolver out of the file. Helpers
and constants belong in `resolver.go`, which it leaves alone.

## Measurements

Each phase produces a number. They live in `docs/`, with the conditions
attached, because a number without its conditions is not evidence.

| Measurement | Value | Phase | Detail |
|---|---|---|---|
| Downstream calls per 50-post timeline | 163 → **151** | 02 → 03 | [phase2](docs/phase2-baseline.md), [phase3](docs/phase3-fanout.md) |
| Timeline p99 | 45.67 → 43.06 ms | 02 → 03 | 200 samples; the 150 remaining calls dominate |
| **Timeline entries lost per worker kill** | **0** | 03 | 10 kill-restart cycles, asserted in a test |
| **Timeline entries duplicated** | **0** | 03 | same test; `ZADD` is idempotent by construction |
| Publish-to-visible | 482 ms p50 | 03 | dominated by the relay's 250 ms poll |
| Fanout cost, under 100 followers | 3.9 ms | 03 | 11,832 events |
| Fanout cost, 1k–10k followers | 169 ms | 03 | ~8× per order of magnitude |
| Spans in one timeline trace | 723 | 02 | [traces/phase2-naive-timeline.json](docs/traces/phase2-naive-timeline.json) |
| Seed: 500k-follower account | 76 s | 01 | 600k users, 2.21M edges |
| Fanout threshold (crossover) | — | 04 | |

## Decisions already made

So they don't get relitigated:

- **ULIDs for post IDs.** They sort lexicographically by creation time, so the
  Redis timeline score falls out of the ID, cursor pagination needs no second
  column, and merging two post streams is a string comparison.
- **The follow index runs backwards.** The primary key answers "who does X
  follow"; fanout asks "who follows X", so `follows_by_followee` exists and is
  covering.
- **`lifecycle.Run` drains on *any* component exit,** not just on a signal. A
  process with a dead consumer and a live HTTP server still passes its
  liveness probe, which is worse than being dead.
- **Batch RPCs are first-class**, not an optimisation to add later. `BatchGetUsers`
  and `BatchGetPosts` exist now so phase 5's DataLoader is a small change
  rather than a rewrite of every call site.
- **Follower listings return bare IDs**, never `User` messages. Fanout walks
  500,000 edges for one post and needs nothing but identifiers.
- **Keyset pagination everywhere**, never OFFSET, which re-scans every row it
  skips. Follow edges page on the second column of `follows_by_followee`;
  posts page on the ULID itself.
- **Follow is idempotent.** `ON CONFLICT DO NOTHING` plus a `created` flag, so
  a client retrying after a timeout gets success and the truth, not a
  duplicate-key error it would have to interpret.
- **The gateway's field resolvers are deliberately naive, and instrumented.**
  Each still does its own lookup, and phase 5 batches them. Counting happens in
  a gRPC client interceptor rather than in the resolvers, so that rewrite
  cannot touch the measurement — which is what makes the before/after a fair
  comparison rather than two numbers from two different rulers.
- **Tracing degrades, never fails.** A missing collector logs a warning and the
  service serves. Observability is not a dependency.
- **The outbox stores wire bytes, not JSON.** `payload` is `bytea` holding the
  encoded protobuf exactly as it will be published, so nothing transforms
  between what was committed and what was delivered.
- **Timeline writes are idempotent by construction.** `ZADD` of an existing
  member changes nothing, which is what makes at-least-once delivery safe with
  no dedup table, no processed-message set and no coordination between workers.
  Every other safety property in the fanout follows from that one.
- **Dead-lettering acknowledges the failure.** It feels wrong and is right: a
  message redelivered forever stops every message behind it, so parking one
  bad event costs one fanout instead of all of them.

## Local toolchain caveats

This repo was bootstrapped on a Windows machine with Application Control
enabled. That policy blocks freshly built executables from running — by
content and unpredictably, and including the test binaries `go test` writes to
the system temp directory. It blocked `gofmt`, `protoc-gen-go`, and roughly
half the binaries built here, while allowing the rest.

Rather than fight it, anything that has to *execute* what it just built runs in
the `golang:1.27` container: tests, lint, formatting, codegen, and the services
themselves. This costs a few seconds per invocation and buys three things —
the policy stays untouched, local runs match CI exactly, and the services get
their dependencies by hostname on a Docker network, which is how they will
find each other from phase 8 onward.

Compiling on the host still works, so `make build` stays native. Migrations
were native too until a rebuild changed `./bin/migrate` enough for the policy
to start blocking it — the decision is per binary and re-made on every build,
so "it ran yesterday" is not a property worth depending on.

One consequence worth knowing: formatting is asserted by
`TestEveryGoFileIsFormatted`, which uses `go/format` in-process rather than
shelling out to a `gofmt` binary. That check therefore holds even where the
binary cannot run.

**On Windows, run `make` from Git Bash, not PowerShell.** GNU Make picks its
shell from the environment: Git Bash gives it `sh.exe` and everything works,
while PowerShell gives it `cmd.exe`, which has no `grep`, no `mkdir -p`, and
different quoting. Recipes are therefore POSIX `sh` and contain no bashisms,
since a bashism would break on Windows and nowhere else.
