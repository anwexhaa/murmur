# Phase 7: auth and hardening

Measured 25 September 2026 against the local compose stack.

Six phases of this project ran on a header. `X-Murmur-User: <uuid>` named the
account making the request, every service believed it, and anyone who could
reach a port could be anybody. It was honest scaffolding — it let the read path
be built and measured before auth existed, and it was confined to one function
so that replacing it touched nothing else. This is the phase that replaces it.

## The headline

**Replaying a spent refresh token revokes the whole session.**

```
mutation refresh(refreshToken: R1)   -> R2 issued, R1 spent
mutation refresh(refreshToken: R1)   -> "session revoked; sign in again"
mutation refresh(refreshToken: R2)   -> "session revoked; sign in again"
```

The third line is the interesting one. R2 was the *legitimate* client's live
token and it died too. That is the intended outcome rather than collateral
damage: from the server's position, "the real client is retrying after a failed
response" and "an attacker is using a token they stole" are the same event, and
the safe reading of two parties holding a secret meant for one is that the
session is compromised.

## The trust boundary

Every internal call now carries a signed assertion, minted per call, naming the
user it is made for. A backend with no assertion, a forged one, or a *client's
own* token refuses it:

```
grpcurl murmur-social:9081 SocialService/GetUser
  -> Unauthenticated: no caller assertion

grpcurl -H "x-murmur-assertion: <signed by another key>"
  -> Unauthenticated: invalid caller assertion

grpcurl -H "x-murmur-assertion: <a real user's real access token>"
  -> Unauthenticated: invalid caller assertion
```

The third case is the one worth dwelling on. That token is signed by the same
key the backend verifies with, so its signature is perfectly valid. The only
thing separating it from an assertion is the `aud` claim — `murmur-client`
versus `murmur-internal`. Without that check, any signed-in user could take the
token their browser was handed, present it straight to social-svc, and be
believed.

### Ed25519, not a shared secret

With an HMAC secret, every service able to *verify* an assertion is able to
*mint* one, so compromising the least important service yields the ability to
impersonate any user to every other service.

Here the signing key is held by the gateway, the fanout worker and
timeline-svc — the three processes that make outbound calls. **social-svc holds
only the public half.** It owns the user table, the credentials and the follow
graph, and it can check that the gateway said something while being unable to
say anything itself.

That is narrower than "only the gateway can sign", and it is worth being
accurate about: three of four processes hold the private key. What it buys is
that the largest store of data in the system is not one of them. The production
answer to the rest is mTLS or SPIFFE, where identity comes from the transport
rather than from a shared file.

### Algorithm confusion

```go
jwt.WithValidMethods([]string{jwt.SigningMethodEdDSA.Alg()})
```

One line, and it looks like nothing. The verifying key is public — every
backend has it, and anything public is available to an attacker. A parser that
trusts the token's own `alg` header will, handed an HS256 token, use that key
material as an HMAC secret. The attacker therefore holds the "secret", mints
whatever claims they like, and the signature verifies.

`TestAlgorithmConfusionIsRejected` forges exactly that token using only the
public key and asserts it is refused — then signs a genuine one and asserts it
is accepted, because a check that rejects everything is not a check.
`TestAlgNoneIsRejected` covers the sibling attack.

## Passwords

argon2id at OWASP's parameters: **19 MiB, two passes, one lane**, with a fresh
16-byte salt per hash and the whole thing stored in PHC form:

```
$argon2id$v=19$m=19456,t=2,p=1$<salt>$<hash>
```

The parameters live *in the stored string*, not in the binary. That is what
makes them changeable: raising the cost does not invalidate a single existing
hash, because each one still carries the settings it was made with. Login is
the only moment the plaintext exists, so it is the only moment an upgrade is
possible, and `NeedsRehash` rewrites weak hashes one user at a time as people
sign in.

19 MiB rather than RFC 9106's 64 MiB profile because **the memory is per
concurrent login, not per process**. At 64 MiB a hundred simultaneous logins
ask for 6.4 GB, and the login path becomes the cheapest way to take the service
down. The rate limiter in front of it is part of that calculation rather than a
separate feature.

### The timing oracle

An unknown handle still runs a full argon2 verification, against a decoy hash
computed once at startup with the real parameters.

| | median |
|---|---|
| Existing account, wrong password | **148 ms** |
| Account that does not exist | **154 ms** |

Five samples each. The distributions overlap, which is the point — without the
decoy, an unknown handle returns in about two milliseconds on a database miss
and a wrong password takes fifty-odd milliseconds of argon2. That difference is
a fast, silent oracle for deciding which handles exist, and it is the first step
of every credential-stuffing run. Both failures also return the same message.

## Two hashes, on purpose

The password column uses argon2id. The refresh token column uses **SHA-256**,
and the contrast is deliberate rather than an oversight.

Argon2 is slow so that guessing a *human-chosen* secret is expensive. A refresh
token is 256 bits of CSPRNG output: there is nothing to guess, and an attacker
who can brute-force it can brute-force the signing key too. What hashing buys
here is the same thing it buys for passwords — a database dump does not hand
over live credentials — and a fast hash buys all of it. Spending 19 MB and two
passes on every refresh would be a cost with no corresponding benefit, and the
kind of thing that looks rigorous until somebody asks what it is for.

## Why the refresh token is opaque

It could have been a JWT, and that would have been worse. A refresh token has
to be revocable — reuse detection *is* the feature — and a self-contained
signed token is valid until it expires whether the server likes it or not.
Revoking one means keeping a list of revoked tokens, at which point the database
lookup a JWT exists to avoid is happening anyway, and the token is carrying
claims nobody reads.

So: a random string, and the truth lives in Postgres.

### The transaction is the design

```sql
SELECT ... FROM refresh_tokens WHERE token_hash = $1 FOR UPDATE
```

Two tabs refreshing at the same instant must not both succeed. That would
produce two live successors in one family, and the next rotation of either
would look like reuse of the other — logging out a perfectly innocent client at
random. The row lock makes the second caller wait, see `used_at` already set,
and take the reuse path, which is the correct answer because a token really was
presented twice.

`TestConcurrentRotationOfOneTokenProducesOneSuccessor` releases eight
goroutines at one instant against a real Postgres: **1 succeeds, 7 report
reuse.**

A spent row is kept rather than deleted, because a deleted token and a
never-issued token look identical on replay and only one of them means a
breach. The sweep runs on expiry instead.

## Complexity, not depth

The hostile query, against the running gateway:

```
query Hostile {
  timeline(first: 100) {
    edges { node { author { followerCount
      posts(first: 100) { edges { node { author { followerCount handle } } } } } } }
  }
}
```

```
operation has complexity 90800, which exceeds the limit of 2000
```

**Forty-five times the budget — and four levels deep.** Every depth limiter in
every tutorial waves it straight through, because depth is a proxy for cost
that stops being one the moment a list argument exists, and every interesting
schema has list arguments. Complexity multiplies a list's cost by the page size
it asked for, so the thing that scales is the thing that is counted.

The same limit lets an ordinary twenty-post timeline through untouched, which
`TestAnOrdinaryQueryIsAccepted` pins so the limit cannot quietly become an
outage.

Auth mutations are priced at **200** against a write's 10, because login runs
argon2 over 19 MB and a budget that prices it the same permits the cheapest
denial of service in the schema.

Introspection is off unless `GRAPHQL_INTROSPECTION` says otherwise, and the
default is computed from the environment so development keeps its playground.
Automatic persisted queries stay off: they are a bandwidth optimisation that is
only a security control when paired with an allowlist of approved hashes, and
without one they are a way to have the server cache arbitrary queries on an
attacker's behalf.

## Rate limiting

A token bucket in Redis, as a Lua script.

Redis rather than process memory, because the gateway runs as several replicas
and a per-process limiter divides the real limit by however many pods happen to
be running — a limit that changes when the autoscaler acts, and is loosest
exactly when the system is busiest.

A bucket rather than a fixed window, because a fixed window lets a caller spend
its whole allowance in the last millisecond of one window and again in the
first of the next: twice the intended rate, at the worst possible instant.

Live, against `login`:

```
attempt 1-5: invalid handle or password
attempt 6:   too many requests; slow down   (retryAfter: 10)
```

and a different account is unaffected, because the buckets are per handle.

| Scope | Burst | Sustained | Why |
|---|---|---|---|
| `login` (per handle) | 5 | 1 per 12s | Guessing at this rate takes centuries; a person mistyping does not notice |
| `login-src` (per address) | 30 | 0.5/s | One address is an office or a carrier's NAT — sized to stop stuffing, not people |
| `register` | 3 | 1 per 60s | Rarer than login and more expensive to undo |
| `refresh` | 30 | 1/s | Cheap, and a flapping client retries |
| `request` | 120 | 20/s | Well above a person browsing, well below what one client should take from a replica |

Two buckets on login rather than one, because they stop different attacks:
per-handle stops one account being guessed from many addresses, per-address
stops many accounts being tried from one. Either alone leaves the other open.

The script is atomic for the same reason the rotation is: read-modify-write in
Go would let every concurrent caller read the same token count and every one of
them decide there was room. `TestConcurrentCallersCannotExceedCapacity`
releases 200 goroutines against a capacity of 20 and gets **exactly 20**
allowed.

### It fails open, and that is a trade

A limiter that cannot reach Redis chooses between refusing every request and
allowing every request. Refusing turns a cache outage into a total outage — the
protective measure becomes the thing that takes the service down. Allowing
degrades to the behaviour of the previous six phases, which was no limiter at
all.

This is not free: while Redis is down there is no limit.
`murmur_ratelimit_failures_total` counts it so the gap is visible rather than
silent, and that counter is where the alert belongs.

## What the header cost to remove

`X-Murmur-User` reached further than it looked. Removing it touched the
middleware, the WebSocket handshake, both k6 scenarios, the subscription
client, the connection benchmark and the phase 6 verification script — because
every one of them was authenticating by assertion rather than by proof, and
none of them had a password to use instead.

The seeded graph has no credentials: `cmd/seed` writes six hundred thousand
users with `COPY`, and giving each one a password would mean six hundred
thousand argon2 hashes — argon2 working exactly as designed, in the one place
it is not wanted. So `scripts/mint.go` signs tokens from the deployment's key
for the tooling. It can impersonate anybody, which is precisely as dangerous as
it sounds; it reads the development key, and nothing in the deployed system
does this.

## One small leak, found live

The first live request with no token came back as:

```
rpc error: code = Unauthenticated desc = not signed in: send an Authorization...
```

`requireViewer` returned a gRPC status straight out of a resolver, and gqlgen
rendered it verbatim — transport framing from one protocol leaking into
another, and a small disclosure of how the edge is assembled. It now returns a
GraphQL error with the code in `extensions`, like every other failure the
gateway reports.

Worth recording because it is the kind of thing only a live request finds. Every
unit test asserted on the error's *code*, which was correct the whole time.

## The bug that was not in this phase's code

The suite went red on `internal/fanout`: the kill-restart test timed out after
ninety seconds on work that takes twenty-two. It passed in isolation and failed
in the full run, which is the shape of a resource shared between packages, and
it took two wrong answers to get to the right one.

**First guess.** This phase added `internal/platform/ratelimit`, whose tests
call `FlushDB` on the same Redis database `internal/timeline` and
`internal/fanout` use. A new flusher appeared and the suite broke in the same
week. The new tests were reworked to isolate by key and flush nothing — which
is the right way to write them regardless — and the failure continued.

**Second guess.** Running only `./internal/fanout ./internal/timeline`
reproduced it, and those two have shared a flushed database since phase 3. They
were given separate databases. The pair went green; the full suite failed once
more and then passed, so nothing was actually settled.

**The cause.** The consumer in that test is configured `MaxDeliver: 3`, and the
test deliberately kills its worker **ten times**. Every kill leaves a message
unacknowledged, and JetStream counts each redelivery against the same budget a
poisoned event spends. Whether a message survives to be finished by the last
worker is therefore a race between the restarts and the delivery counter, and
machine load decides which side it lands on. When a message exhausts the budget
it is parked forever, the last follower never reaches ten posts, and the wait
runs to its full timeout — which is why more time never helped.

The fix is a delivery budget sized for the test's own kills. The test is about
not losing data across restarts; where the dead-letter threshold sits is
`TestMalformedEventIsDeadLetteredNotRetriedForever`, which sets its own.

Three things worth keeping from this. The failure was in phase 3's code and had
been passing on luck for four phases. Both wrong guesses had good evidence and
neither was tested against the thing it claimed to explain — the database split
was declared a fix on one green run of a pair that had failed once. And the
answer only appeared from reading what the test configures rather than from
reasoning about what else was running: `MaxDeliver: 3` against `cycles = 10` is
visible in six lines of the test file, and no amount of thinking about Redis was
going to produce it.

## Instruments

```
murmur_ratelimit_decisions_total{scope,outcome}   counter
murmur_ratelimit_failures_total                   counter
```

The ratio of the first is what tells an operator whether a limit is protecting
the service or breaking a legitimate client, which a raw count of rejections
cannot.

## Reproducing

```
make up
make run-social run-timeline run-fanout     # each in its own shell
make run-gateway-replicas
```

The local stack shares one signing key, committed in the Makefile as
`DEV_AUTH_SIGNING_KEY`. It is the bytes 0..31, it is in a public repository,
and it exists because four containers have to agree on a key or none of their
assertions verify. Production sets `AUTH_SIGNING_KEY` from a secret store and
gives the verify-only services `AUTH_VERIFYING_KEY` instead — the gateway
refuses to start in production without one.
