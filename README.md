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
make up                # start Postgres, Redis and NATS, then migrate
make test              # unit tests, race detector on
make test-integration  # adds the tests that need a real Postgres
make lint              # go vet and staticcheck
make help              # every target
```

To drive the write path by hand:

```bash
make run-social                          # in one terminal
make smoke                               # in another: full end-to-end flow
make grpcurl ARGS="murmur-social:9081 list"
make seed ARGS="-reset -users 600000 -whale-followers 500000 -avg-following 3"
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
api/proto/    the .proto source of truth
api/gen/      generated Go, committed so CI needs no plugins
cmd/          one directory per binary, plus seed and migrate
internal/
  domain/     entities and rules, no infrastructure imports
  social/     store (pgx) and the SocialService implementation
  platform/   config, logging, lifecycle, health, db, kv, bus, grpcx
migrations/   goose SQL migrations
scripts/      generate.sh, smoke.sh
deploy/       docker-compose, kubernetes           (phase 8)
loadtest/     k6 scenarios                         (phase 5)
docs/         the build spec, ADRs, screenshots
```

Regenerating protobuf code is `make generate`. It runs buf and the plugins
inside a container, built from the versions pinned in `go.mod`, so nothing
needs installing and the output is reproducible. The generated code is
committed, so this is rare.

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

Compiling on the host still works, so `make build` and `make migrate` are
native and fast.

One consequence worth knowing: formatting is asserted by
`TestEveryGoFileIsFormatted`, which uses `go/format` in-process rather than
shelling out to a `gofmt` binary. That check therefore holds even where the
binary cannot run.

**On Windows, run `make` from Git Bash, not PowerShell.** GNU Make picks its
shell from the environment: Git Bash gives it `sh.exe` and everything works,
while PowerShell gives it `cmd.exe`, which has no `grep`, no `mkdir -p`, and
different quoting. Recipes are therefore POSIX `sh` and contain no bashisms,
since a bashism would break on Windows and nowhere else.
