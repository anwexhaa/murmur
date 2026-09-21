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
| 01 | Domain over gRPC | not started |
| 02 | GraphQL gateway | not started |
| 03 | Fanout on write | not started |
| 04 | Hybrid cutover | not started |
| 05 | Caching and the read path | not started |
| 06 | Real-time subscriptions | not started |
| 07 | Auth and hardening | not started |
| 08 | Deploy, observe, prove | not started |

## Quickstart

Requires Go 1.27, Docker and GNU Make.

```bash
make up      # start Postgres, Redis and NATS, then migrate
make test    # run the suite
make check   # what CI runs: formatting, lint, race-enabled tests
make help    # every target
```

`make up` waits for all three containers to report healthy before applying
migrations, so it either works or fails loudly — it never leaves you with a
half-started stack.

## Services

Four processes, split along the axes they actually scale on.

| Binary | Speaks | Owns | Scales with |
|---|---|---|---|
| `gateway` | GraphQL over HTTP + WS | auth, batching, rate limits, subscriptions | connected clients |
| `social-svc` | gRPC | users, follow edges, posts, the outbox | write volume |
| `timeline-svc` | gRPC | timeline assembly, merge, pagination | read volume |
| `fanout-worker` | NATS consumer | push/pull routing, timeline writes | follower-edge volume |

Each serves `/healthz` and `/readyz` on its own port (8080–8083). The two
probes are deliberately different: liveness touches nothing, because a
liveness probe that pings Postgres turns one slow database into a rolling
restart of every pod that talks to it. Readiness is where dependency checks
belong.

## Layout

```
api/          proto and GraphQL schema            (phase 1, 2)
cmd/          one directory per binary
internal/
  domain/     entities and rules, no infra imports (phase 1)
  platform/   config, logging, lifecycle, health, db, kv, bus
migrations/   goose SQL migrations
deploy/       docker-compose, kubernetes           (phase 8)
loadtest/     k6 scenarios                         (phase 5)
docs/         the build spec, ADRs, screenshots
```

## Notes for the next phase

Three decisions already made, so they don't get relitigated:

- **ULIDs for post IDs.** They sort lexicographically by creation time, so the
  Redis timeline score falls out of the ID, cursor pagination needs no second
  column, and merging two post streams is a string comparison.
- **The follow index runs backwards.** The primary key answers "who does X
  follow"; fanout asks "who follows X", so `follows_by_followee` exists and is
  covering.
- **`lifecycle.Run` drains on *any* component exit,** not just on a signal. A
  process with a dead consumer and a live HTTP server still passes its
  liveness probe, which is worse than being dead.

## Local toolchain caveats

This repo was bootstrapped on a Windows machine with Application Control
enabled, which shaped three choices. None of them require weakening the
policy, and none of them cost anything on Linux or macOS.

- **No `go run`.** It executes from the system temp directory, which the
  policy blocks. Every make target builds to `./bin` and runs from there,
  which is also faster on repeat invocations.
- **No `gofmt` binary.** The policy blocks it by content, so rebuilding or
  renaming it does not help. Formatting is asserted by
  `TestEveryGoFileIsFormatted` instead, which runs inside the test process and
  therefore works anywhere `go test` does. `make fmt` still shells out to
  `go fmt` for the machines where that works.
- **No native race detector.** It requires cgo, and therefore a C toolchain on
  Windows. `make test-race` runs the suite in the same `golang:1.27` image CI
  uses, which is also the platform the services deploy to. `make test` is the
  fast native run without it.

Makefile recipes are POSIX `sh`, not bash: GNU Make on Windows resolves
`SHELL` to Git's `sh.exe` whatever the makefile asks for, so a bashism breaks
there and nowhere else.
