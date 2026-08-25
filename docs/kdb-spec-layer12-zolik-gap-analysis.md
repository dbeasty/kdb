# Layer 12 — Zolik Integration: Gap Analysis (Revision 2)

## Status: PROPOSED (not started)

Companion to [`kdb-spec.md`](./kdb-spec.md). **Revision 2** — supersedes the original Layer 11
draft of this document after a second, deeper audit against the codebase as it stands today.
Almost everything changed since Revision 1: a large parallel Go engine port now exists, RBAC went
from a static config file to a real dynamic store, and — most importantly for Zolik specifically —
a serious correctness hole was found in exactly the mechanism Zolik's own architecture doc had
planned to lean on. **Renumbered to Layer 12** because this repo's own commit history has since
made "Layer 11, Component 32" a real, shipped thing (the stored-procedure engine) — a genuine
collision with Revision 1's proposed numbering, not a hypothetical one. See §7 for the full
renumbering table.

This document evaluates KDB against one concrete external consumer — **Zolik**, a Go/React-Native
multiplayer card game (`/Users/davidj/devel/zolik`) — that wants to (1) run its cloud server on KDB
instead of MongoDB, as cheaply as possible, and (2) eventually embed KDB inside the mobile app
itself alongside a `gomobile`-compiled Go rules engine, so any player's phone can host a match. See
[`zolik/docs/distributed-architecture.md`](../../zolik/docs/distributed-architecture.md) for the
consumer-side plan this feeds into.

**Method, unchanged from Revision 1:** every claim below is checked against actual source, not
doc comments or spec-only claims — this repo's own status labels ("first Kotlin cut",
"IMPLEMENTED") have repeatedly turned out to mean "compiles" or "JVM-only," not "works end to end."
Revision 2 found several more instances of exactly this pattern, this time in the client-server
sync path the project's README names as its core purpose.

```
Layer 12 — Zolik Integration      [PROPOSED, Revision 2]
  [ ] 38. Go-Native Server (cost-critical)         — spec: kdb-spec-layer12-component38-go-native-server.md
  [ ] 39. Peer-Sync Conflict Detection (correctness-critical) — spec: kdb-spec-layer12-component39-peersync-conflict-detection.md
  [ ] 40. Go Client SDK (revised: + upsert)        — spec: kdb-spec-layer12-component40-go-client-sdk.md
  [ ] 41. Auth Session/Token Issuance (revised: narrower now) — spec: kdb-spec-layer12-component41-auth-tokens.md
  [ ] 42. Native TCP Transport (revised: deprioritized) — spec: kdb-spec-layer12-component42-native-transport.md
  [ ] 43. Embed Durable + Mobile Storage (revised: maybe unnecessary) — spec: kdb-spec-layer12-component43-embed-durable-storage.md
  [ ] 44. Cross-Write Commit Notification Bridge (minor, spec'd inline §5)
  [ ] 45. Session/Lock Disconnect Cleanup (minor, spec'd inline §5)
  [ ] 46. Stream Write-Back Mode Fix (minor, spec'd inline §5)
```

---

## 1. What changed since Revision 1 — the headline findings

Two things dominate everything else this revision found, and they point in opposite directions —
one is good news for cost, one is a real correctness gap that has to be fixed before a piece of
Zolik's plan can ship as designed.

### 1.1 Good news: a Go engine port exists, and it changes the cost math

A full parallel Go implementation of KDB lives at `/Users/davidj/devel/kdb/go` (`module
github.com/limidus/kdb/go`, ~180 files) — storage engine, transactions, schema, SQL, document
model, an embed runtime, all mirroring the Kotlin `dev.kdb.*` packages. **This directly answers a
question Zolik asked about hosting cost**: running the cloud server as JVM `kdb-server` needs a
Lightsail tier sized for a JVM (realistically 2GB RAM, ~$12/mo) purely to cover JVM heap/metaspace
overhead a Go process wouldn't have. See Component 38.

**But the server side of the Go port is explicitly unfinished** — `go/kdb/server/server_runtime.go`'s
`Commit` returns `errNotImplemented("commit")`, and `go/cmd/kdb-service/main.go` prints
`sql=disabled (wire listeners not ported)` on startup. What *is* complete is the embedded,
in-process side (`go/kdb/embed`, `go/kdb/driver`) — usable today as a local library, not as
something a second process can connect to over a socket. **Component 38 (new, top priority)**
specs finishing exactly the missing piece: wire listeners + commit path + RBAC enforcement ported
to `go/kdb/server`, which is what turns "a Go engine exists" into "the cloud deployment needs no
JVM at all."

A second, related implication worth flagging even though it's Zolik-side, not KDB-side: if the Go
embed engine can be linked into a `gomobile`-built binary the same way the Go rules engine can (both
being ordinary Go), the mobile app may need only **one** native toolchain, not two — Zolik's plan
currently assumes `gomobile` (Go) *and* Kotlin Multiplatform/Kotlin-Native (KDB) as two independent
native builds. That assumption should be revisited once Component 38 (and a look at whether
`go/kdb/embed` actually cross-compiles under `gomobile`) lands. Flagged here, not resolved — it
belongs in Zolik's own architecture doc, not this one.

### 1.2 Bad news: Mode 3 peer-sync has no conflict detection at all

Revision 1 recommended using KDB's peer-sync (source-control "diverge and merge") to get a
completed LAN match's results from a host to the cloud with no bespoke retry/queue code — on the
strength of the spec's own claim that "conflicts surface to the application; KDB never silently
resolves them." **That claim is false for peer-sync as currently implemented.** Tracing
`PeerSyncFrameHandler.handleCommitPush` precisely: a pushed commit is accepted if its *parent*
exists in the local DAG (`putCommit(requireParents = true)`), then the branch head is
unconditionally moved to it (`dag.setHead("main", msg.commits.last().hash)`) — no check that the
incoming commit is a descendant of the current head. If peer A and peer B both wrote locally while
disconnected from each other and then sync, **whichever one pushes last simply overwrites the
other's branch pointer**; the loser's commits remain physically present in the DAG's object store
but become unreachable from `main` — a silent, undetected loss of a locally-committed write, not
a `ConflictReport`. `grep`ing the whole `kdb-peer-sync` module for `ConflictReport`/`merge` returns
nothing — there is no merge logic in this module at all, at any conflict policy.

This isn't cosmetic for Zolik: it's exactly the mechanism Revision 1 proposed for "host syncs
match_results to the cloud, KDB handles the merge, no custom code needed." **Component 39 (new,
critical)** specs the fix. Until it lands, Zolik's plan needs an interim mitigation — see Component
39 §9 for the recommended one (branch-per-host rather than a shared `main`).

### 1.3 Everything else that changed, briefly

| Area | Revision 1 said | Now |
|---|---|---|
| RBAC/auth | `kdb-auth-static`: plaintext config file only | **Real**: dynamic `_system/users`/`_system/roles` KDB documents, PBKDF2-HMAC-SHA256 password hashing (120k iterations), per-document write authorization checked at commit time (not just handshake), `CREATE USER`/`GRANT`/`REVOKE` SQL admin surface. **Still missing**: session/bearer-token issuance — a "token" is still just `user:password` sent per connection. Component 41's scope narrows accordingly (see below). |
| Write-phase rollback / document lock manager | N/A, didn't exist | Real, but **additive atomicity, not a CAS replacement** — rolls back a transaction whose physical write phase fails after conflict checks already passed; a separate, in-process, per-document pessimistic lock held only for the duration of one commit call. Doesn't change how a Go client should structure a CAS write. One real downstream effect found this revision, though: **the lock is never released if a client disconnects mid-transaction** — see Component "45" in §5. |
| Storage sharding | N/A | 64-way hash-sharded in-process lock, purely for write concurrency *within* one namespace on one node. Not distributed sharding, doesn't change single-document-CAS semantics at all. |
| Native TCP transport (old Component 32) | Stub, throws | **Still a stub**, unchanged. But now lower priority — see §1.1, the Go embed path may make it unnecessary for Zolik's mobile goal specifically. Renumbered to Component 42 (see §7). |
| iOS/Android KMP targets | Missing | **Still missing**, unchanged. Same reprioritization applies. |
| Cross-write commit notifications | Not investigated in Revision 1 | **Major gap, new finding**: SQL writes (the normal client write path) never notify Mode 1 stream subscribers — only writes arriving via peer-sync do. The two write paths don't share a notification bus. See §5, Component 44. |
| Disconnect handling | Not investigated in Revision 1 | **Major gap, new finding**: `SessionManager.end()` is dead code, never called by the connection lifecycle. A client that drops mid-transaction leaks its document locks and session state forever — no timeout, no heartbeat. See §5, Component 45. |
| Mode 2 write-back stream | Assumed working per spec | **Non-functional end to end**: the client library returns a hardcoded rejection without waiting for a response; the server-side stream handler doesn't even parse the message type it would need to. See §5, Component 46. |
| `kdb-server` test coverage | Not checked | **Zero unit tests.** Existing "multi-client" integration tests drive a single in-process host directly (`host.handleFrame(...)`) — no real concurrent sockets, no disconnect simulation anywhere in the suite. |

---

## 2. Upserts — Zolik's requirement, and what it needs from KDB

Zolik needs two genuinely different write shapes, not one:

1. **Version-checked CAS** (match/game documents): load at version X, mutate, commit iff nothing
   else committed since — this is `match.Repository.UpdateWithVersion` today, and it's what
   Component 39/40's `Commit(tx)` (anchored on `BaseVersion`, `STRICT` policy) already covers
   correctly. No change needed here.
2. **Unconditional upsert** (player stats, sessions): `stats.Repository.UpsertPlayerStats` today is
   a Mongo `ReplaceOne(filter, doc, upsert=true)` — create the document if it doesn't exist, replace
   it unconditionally if it does, **with no version check at all**. `auth.SessionRepository.CreateSession`/
   `CreateGuestSession` are the create-only relative (no prior document expected — plain
   `InsertOne` in Mongo, not even upsert). Both are fundamentally "I don't care what was there
   before" writes, unlike the match-document CAS path.

**What KDB needs to expose for this, precisely:**

- `ConflictPolicy.LAST_WRITE` ("incoming write wins; no conflict surfaced") is structurally the
  right policy for an upsert-shaped namespace like `player_stats` — this is not a new engine
  feature, it's an existing conflict policy that needs to actually be exercised by a write path
  Zolik's Go client can call without also supplying a `BaseVersion` it has no reason to have
  fetched first.
- **Needs verifying, not assumed**: does a `Write` op targeting a document ID that doesn't exist
  yet in a `LAST_WRITE` namespace succeed (create-on-first-write), or does the engine require a
  prior `Insert`/existence check? `kdb-spec.md` §7.1's `Op.Write{docId, patch: JsonPatch}` uses the
  word "patch," which in some document-store designs implies the target must already exist (you
  can't incrementally patch nothing). `kdb-embed`'s `EmbedOperations.putJson` reads more like a
  full-document set (parse and store, not apply-a-delta-to-existing), which would naturally support
  create-or-replace — but this needs a one-line confirmation against the actual `Write`-op handling
  in `kdb-transaction`/`kdb-storage-engine` before Zolik's Go client can rely on it, rather than
  being assumed either way.
- **Component 40 (Go Client SDK) adds an explicit `Upsert` method**, separate from the
  version-anchored `Commit`, so the two write shapes aren't conflated at the API Zolik's repository
  implementations actually call — see Component 40 §3 for the exact signature.

---

## 3. What Zolik needs, mapped to what KDB has (updated from Revision 1)

| Zolik need | KDB mechanism | Status |
|---|---|---|
| Arbitrary nested-JSON match document | Document model, `_doc` | **Have it.** Unchanged from Revision 1. |
| Version-checked CAS on match/game documents | `Commit` anchored on `BaseVersion` + `STRICT` | **Have it.** Unchanged. |
| **Unconditional upsert on player_stats/sessions** | `ConflictPolicy.LAST_WRITE` + a create-or-replace write path | **Needs the explicit `Upsert` client method (Component 40) + the create-on-first-write verification above.** New this revision, per Zolik's explicit ask. |
| Append-only per-match action log | `mode = APPEND_ONLY` namespace | **Have it.** Unchanged — still a pre-existing worked example in the spec. |
| Unique lookup by join code / gameId / username | Hash index + `unique = true` | **Have it.** Unchanged. |
| A completed LAN match's results reaching the cloud once connectivity returns | Peer-sync Mode 3 | **Blocked** until Component 39 lands (§1.2) — this is the one place Revision 1's optimism didn't survive contact with the actual merge code. |
| Cloud server runs with no JVM (cost) | A finished Go-native `kdb-service` | **Blocked** until Component 38 lands — new, highest-leverage item this revision. |
| bcrypt-equivalent password hashing, dynamic user store | RBAC (`RegistryAuthStore`, `PasswordHasher`) | **Have it now** — Revision 1's biggest single gap is mostly closed. Caveat: registry currently wired to an in-memory `CommitDag` at server startup, so it doesn't survive a restart yet — worth a one-line fix, not a new component. |
| Session/bearer tokens (not `user:password` per connection) | — | **Still a gap**, narrower than Revision 1 thought. Component 41, revised scope. |

---

## 4. Priority order (revised — cost and correctness first)

1. **Component 38 — Go-Native Server.** Highest leverage: directly determines whether the cloud
   deployment needs a JVM at all, which is a ~40% cut to the fixed Lightsail cost floor on its own
   (see the cost conversation this revision grew out of — $12/mo JVM+Go tier vs. a plausible
   $7/mo all-Go tier).
2. **Component 39 — Peer-Sync Conflict Detection.** Highest risk: ships broken/silently-lossy
   behavior if skipped, in the one place Zolik's plan explicitly depends on KDB doing something
   Mongo-plus-bespoke-code doesn't.
3. **Component 40 — Go Client SDK**, now including `Upsert`. Depends on 38 for the "talk to a
   Go-native server" path to make sense as the primary target rather than a JVM fallback; can start
   against today's JVM `kdb-server` in parallel, per Component 40's own dual-path note.
4. **Component 41 — Auth Session/Token Issuance.** Needed regardless of 38/39, narrower than before.
5. **Components 42/43 — Native transport, embed durable/mobile storage.** Deprioritized behind
   §1.1's open question about whether the Go embed path removes the need for Kotlin/Native mobile
   targets entirely. Don't invest here until that's answered.
6. **Components 44–46 and the minor items in §5/§6** — real, but none block Zolik's near-term
   milestones (cloud-on-KDB, then LAN-hosted-binary). Worth fixing before this becomes a
   general-purpose "client-server sync" product, less urgent for Zolik specifically.

---

## 5. Minor/medium gaps, spec'd inline (new this revision)

### 5.1 Component 44 — Cross-write commit notification bridge

**Purpose.** `StreamBroadcastHub.publish()` is only ever called from one place in the whole repo —
`PeerHostConfig.materializeCommit` in `kdb-service`'s peer-sync path. A normal SQL write via
`SqlWireHost`/`KdbServerRuntime.commit()` → `EmbedWrites.commitViaEngine` has no notification hook
at all. **Effect for Zolik**: if a design ever wants "push the updated match state to any
subscribed viewer automatically" via KDB's own Mode 1 stream instead of the application's existing
WebSocket broadcast, it silently wouldn't work for the normal write path — only for writes that
happen to arrive via peer-sync. Not currently load-bearing for Zolik (the app already does its own
broadcast fan-out over its existing WS hub, independent of KDB), but worth knowing before ever
designing around KDB's stream mode for anything.
**Fix shape.** Add a commit-listener hook to `KdbServerRuntime.commit()`/`EmbedWrites`, invoked
after a successful commit, that calls the same `StreamBroadcastHub.publish()` peer-sync already
uses. Est. 150–300 NBNC — small, the hard part is making sure it fires from every commit path
(SQL, peer-sync, and any future one) rather than being bolted onto just one again.

### 5.2 Component 45 — Session/lock disconnect cleanup

**Purpose.** `SessionManager.end()` correctly releases held document locks, but nothing in the
connection lifecycle (`SqlWireListen.pipelinedPerConnection`, `JvmNetworkWebSocketServer`'s
per-connection handler) ever calls it — the `finally` block that does run only decrements a
connection counter. A client that holds a document lock (via `LockingTransactionBuilder`) and then
drops the connection mid-transaction leaks that lock **forever**, until process restart; the
session map itself leaks the same way. **Effect for Zolik**: a mobile client on a flaky connection
— exactly the profile of a phone on wifi at a LAN party — dropping mid-write could permanently
block every future write to that document for the life of the server process.
**Fix shape.** Wire a `finally`/`invokeOnCompletion` at the transport layer (both
`JvmNetworkWebSocketServer` and the TCP equivalent) that calls `sessions.end(sessionId)` on
connection close, plus a defensive idle-session timeout as a second line of defense against a
half-open TCP connection that never signals close cleanly. Est. 200–350 NBNC.

### 5.3 Component 46 — Stream write-back mode fix

**Purpose.** `DefaultStreamSubscriber.submitTransaction()` sends a `TransactionReplay` frame and
immediately returns `ReplayResult.Rejected("async replay not awaited in v1")` without waiting for
any response; `StreamBroadcastHub.handleFrame()` doesn't handle `TransactionReplay` at all (falls
through, silently dropped). Mode 2 ("write-back stream" — the mode the master spec names as fitting
"standard browser app users, mobile apps with simple write needs," i.e. closest to Zolik's thin
non-hosting clients) is non-functional end to end today. **Not currently load-bearing for Zolik**
(the plan's clients talk to KDB via the SQL wire / Component 40's client, not the stream protocol),
but worth fixing before anyone designs a feature around Mode 2 specifically, and worth knowing the
client library actively lies about the outcome rather than erroring loudly.
**Fix shape.** Route `TransactionReplay` frames arriving at the stream host into the same
`TransactionEngine`/commit path the SQL host uses, and make `submitTransaction` actually await the
correlated response before returning a result. Est. 250–400 NBNC.

---

## 6. Other minor items carried over from Revision 1, still accurate

- **Array/multikey index predicate** (Zolik's `subjectKeys` membership filter) — still not a
  blocker; full-scan fallback is fine at Zolik's scale. Unchanged from Revision 1 §5.
- **Peer-sync offline outbox** (`PeerSyncClient.request()`'s 20s poll-with-timeout, no durable
  retry queue) — still an app-level concern to solve on Zolik's side, cheaper than building it into
  KDB. Unchanged from Revision 1 §6.
- **New this revision, related**: peer-sync also bypasses the fine-grained per-document
  `WriteAuthorizer` that SQL writes get — `PeerSyncFrameHandler` writes straight to `CommitDag`
  with no `TransactionEngine` in the path at all, so RBAC granularity is inconsistent between the
  two write protocols. Worth fixing alongside Component 39, since both are peer-sync-path issues.
- **New this revision**: `kdb-server` has zero unit tests of its own; its behavior is only proven
  via `kdb-integration` tests that drive a single in-process host directly, never real concurrent
  sockets, never a disconnect. This is exactly the class of gap that let Component 45's hole go
  unnoticed. Not a blocker for Zolik's near-term milestones, but worth naming as a reason to be
  skeptical of "integration-tested" claims elsewhere in this codebase without reading the actual
  test, not just its name.

---

## 7. Renumbering table (why, and what moved)

Revision 1 used Components 32–37 under "Layer 11" for proposed work. Since then, this repo's own
commit history made **Component 32 real** — `docs/kdb-spec-layer11-component32-stored-procedures.md`,
shipped (`b927ddf`, "Phases 1–4 implemented and tested"). That's a genuine collision, not a
hypothetical one, so this revision moves the whole proposed batch to **Layer 12**, numbered fresh
from 38 (Components 33–37 were otherwise unclaimed, but moving everything together avoids a
half-renumbered mess):

| Revision 1 (Layer 11) | Revision 2 (Layer 12) | Change |
|---|---|---|
| — | Component 38 — Go-Native Server | **New** |
| — | Component 39 — Peer-Sync Conflict Detection | **New** |
| Component 34 — Go Client SDK | Component 40 — Go Client SDK | Renumbered + revised (upsert, dual-path) |
| Component 35 — Auth Tokens | Component 41 — Auth Session/Token Issuance | Renumbered + revised (narrower scope) |
| Component 32 — Native Transport | Component 42 — Native TCP Transport | Renumbered (collision fix) + deprioritized |
| Component 33 — Embed Durable Storage | Component 43 — Embed Durable + Mobile Storage | Renumbered + deprioritized pending §1.1 |
| Component 36 — Multikey index | (kept inline, §6) | Unclaimed, no collision, left as a minor note rather than promoted to a numbered component |
| Component 37 — Peer-sync offline outbox | (kept inline, §6) | Same |
