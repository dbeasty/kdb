# KDB Layer 12 — Implementation Execution Plan

**Status:** In progress - Component 38 (all 4 sub-phases: wire listener, Commit/Query, RBAC),
Component 39 (peer-sync conflict detection), Component 40 (Go Client SDK), Component 41
(Auth Session/Token Issuance), and Components 44-46 (notification bridge, disconnect/lock
cleanup, write-back mode fix) implemented and tested. Phase 0's spike is now fully resolved: both
Android and iOS bind `go/kdb/embed` successfully via `gomobile bind` (iOS needed one rename,
`EmbeddedKdbRuntime.Release()`→`Close()`, now shipped) - see Phase 0 checklist entry. Per the
gate's own stated consequence, Components 42/43 are very likely unnecessary. Master-spec
housekeeping (Phase 8's second item) and the Go↔JVM cross-implementation interop test are both
done - the latter required implementing a real Go WebSocket client (`go/kdb/transport/ws` was
previously a stub) and fixing a real cross-implementation wire bug it surfaced (a Go SQL
handshake's `null` `localHeads` crashing the JVM decoder). The local Lightsail approximation
(`docs/benchmarks/lightsail-sim/`) surfaced a real OOM under sustained write load, which has since
been root-caused (two allocation bugs, both fixed in Go and Kotlin) and hardened against (memory-
pressure backpressure) - see the checklist's Lightsail entry and the README's "The fix" section.
Remaining: the real Lightsail load test still needs actual x86_64 cloud hardware to make the cost
claim billable. See checklist below.
**Master spec:** `docs/kdb-spec-layer12-zolik-gap-analysis.md` (Layer 12's original source; folded
into `docs/kdb-spec.md` §0/§16.1/§17 as of Phase 8's master-spec-housekeeping item)
**Depends on:** the existing Go engine port (`go/kdb/...`, real per the maturity audit) for
Components 38/40; `kdb-dag`/`kdb-transaction` (Layers 2–3, complete) for Component 39;
`kdb-auth`/`kdb-auth-store` (Layer 11, real) for Component 41.

-----

## Scope

| Component | Module(s) | Spec file | Priority |
|---|---|---|---|
| 38 Go-Native Server | `go/kdb/server` | `kdb-spec-layer12-component38-go-native-server.md` | **P0 — cost-critical** |
| 39 Peer-Sync Conflict Detection | `kdb-peer-sync` | `kdb-spec-layer12-component39-peersync-conflict-detection.md` | **P0 — correctness-critical** |
| 40 Go Client SDK | `go/kdb/client` (new) | `kdb-spec-layer12-component40-go-client-sdk.md` | P1 |
| 41 Auth Session/Token Issuance | `kdb-auth` | `kdb-spec-layer12-component41-auth-tokens.md` | P1 |
| 44–46 (minor: notification bridge, disconnect cleanup, write-back fix) | `kdb-server`, `kdb-stream` | spec'd inline, gap analysis §5 | P2 |
| 42 Native TCP Transport | `kdb-transport-tcp` | `kdb-spec-layer12-component42-native-transport.md` | **Deferred — see Phase 0** |
| 43 Embed Durable + Mobile Storage | `kdb-embed` | `kdb-spec-layer12-component43-embed-durable-storage.md` | **Deferred — see Phase 0** |

**Not in Layer 12:** anything Zolik-side (repository interfaces, `gomobile` bindings themselves) —
that work lives in `zolik/docs/distributed-architecture.md`, not here.

-----

## Normative implementation order

Unlike prior layers, **38 and 39 have no dependency on each other** (different languages, different
modules — `go/kdb/server` vs. `kdb-peer-sync`) and can run in parallel on separate
branches/sessions. Do both before anything else in this layer; they're the two items an external
finding (a hosting-cost calculation, and a correctness audit) actually forced onto the critical
path, everything else is comparatively optional polish.

### Phase 0 — Resolve the Component 42/43 question first (cheap, ungates a lot)

Before writing any Kotlin/Native code for 42/43, spend at most a day answering: **does
`go/kdb/embed` cross-compile under `gomobile bind` today?** If yes, and its feature set (durable
storage, basic SQL, schema) is enough for Zolik's on-device needs, Components 42/43 may be
unnecessary entirely — skip straight to marking them `[deferred, superseded]` rather than building
Kotlin/Native mobile targets nobody ends up using. If no (build failure, missing feature, or
`gomobile`'s cgo requirements conflict with something in the Go storage engine), that's the signal
to actually resource 42/43. **Do not start 42/43 without this spike's answer.**

### Phase 1 — Component 38, sub-phase A: wire listener + framing

1. `go/kdb/server/wire_listen.go`: TCP accept loop atop `go/kdb/transport/tcp` (already real —
   reuse, don't rewrite) and `go/kdb/wire` (already real).
2. Model directly on `kdb-server/src/main/kotlin/dev/kdb/server/SqlWireListen.kt`'s structure —
   this is a structural port, not a redesign.
3. **Tests:** component spec §7 tests 1 (wire compatibility) — skip 2/4/8/10 until later sub-phases
   exist to test against.
4. Exit criteria: a raw wire client can complete a handshake against this listener and get a valid
   response frame, verified against `go/kdb/interop`'s golden fixtures.

### Phase 2 — Component 38, sub-phase B: Commit/Query, no auth yet

1. `ServerRuntime.Commit`/`Query` wired to the existing Go `TransactionEngine` — this is where
   `errNotImplemented("commit")` gets replaced with real behavior.
2. **Tests:** component spec §7 tests 3, 4 (concurrent commits, real conflict detection under
   concurrency).
3. Exit criteria: two concurrent Go client connections can write to the same namespace and get
   correct optimistic-concurrency behavior, with **no JVM process running anywhere** — this is the
   test that starts making the cost claim real.

### Phase 3 — Component 39 (can start any time after Phase 0, in parallel with Phase 1/2)

1. `resolveHeadUpdate` + ancestry check, extending `computeSyncPlan`/`commonAncestor`
   (`PeerSyncTypes.kt`) — component spec §3.
2. Wire into both `PeerSyncFrameHandler.handleCommitPush` and `PeerSyncClient.pullMissing` (§5's
   symmetry requirement — one shared function, not two).
3. **Tests:** component spec §7, all 10 — test 2 (true divergence, `STRICT`, same document) and
   test 4 (true divergence, `APPEND_ONLY`, Zolik's actual use case) are the ones that must not be
   skipped or watered down.
4. Exit criteria: the new genuinely-concurrent variant of
   `WebSocketPeerSyncIntegrationTest` (test 9) passes, and the existing sequential variant still
   passes unmodified.

### Phase 4 — Component 38, sub-phase C: RBAC port

1. Port `RegistryAuthStore`, `PasswordHasher` (PBKDF2, same parameters as the Kotlin reference —
   component spec §5's hash-portability contract), `AuthorizingTransactionEngine`-equivalent
   commit-time enforcement.
2. **Tests:** component spec §7 tests 4, 5, 9 (RBAC denial at commit time, hash cross-verification,
   registry restart durability).
3. Exit criteria: component spec §7 test 10 — a full Go-only deployment (no JVM process anywhere,
   verified via process list in the test harness) with RBAC enabled passes every test above.

### Phase 5 — Component 40 (Go Client SDK)

1. Can begin against the existing JVM `kdb-server` as soon as its SQL-wire payload shapes are
   confirmed (component spec §2) — does not strictly need to wait for Phase 1–4, but is
   materially lower-risk once Phase 2 exists (Go-to-Go, shared `go/kdb/wire`, per component spec
   §1).
2. Include `Upsert` from the start (component spec §3) — this was Zolik's explicit ask and should
   not be added as an afterthought once the CAS path is done.
3. **Tests:** component spec §7, test 5 in particular (`Upsert` create-on-first-write) — this is
   the one that validates or invalidates the gap analysis §2's open question about whether `Write`
   ops require prior document existence. If it fails, that's an engine-level fix needed in
   `kdb-transaction`/`kdb-storage-engine`, not a client-side workaround.

### Phase 6 — Component 41 (Auth Session/Token Issuance)

Independent of everything else in this layer; can run in parallel at any point. Component spec §7,
test 11 (issue → validate round trip) is the integration point worth not skipping.

### Phase 7 — Deferred: Components 42/43

Only if Phase 0's spike says Kotlin/Native mobile targets are actually needed. If so, treat as its
own follow-on execution plan (`kdb-spec-layer12b-execution-plan.md` or similar) rather than
retrofitting into this one — by the time Phase 0's answer is known, Phases 1–6 will likely already
be shipped and this layer's own numbering/status should reflect that rather than staying "blocked."

### Phase 8 — Minor items (44–46) and master-spec housekeeping

1. Components 44 (cross-write notification bridge), 45 (disconnect/lock cleanup — **note: port
   this fix into Component 38's Go server from day one, per that component's own §5, rather than
   fixing it here first and porting later**), 46 (write-back mode fix) — none block Zolik, do
   opportunistically or when another consumer needs them.
2. Fold Layer 11 (RBAC, stored procedures) and Layer 12 into `docs/kdb-spec.md` §0/§16.1/§17 — this
   master-spec table has been out of sync with real shipped work since Layer 11 landed; worth
   fixing regardless of Zolik's own timeline, since it's exactly the kind of drift that made this
   layer's own re-audit necessary in the first place.

-----

## Suggested branching, matching this repo's existing convention

Recent real work in this repo used a worktree-per-component pattern (`worktree-rbac-plan`,
`worktree-stored-procs-plan`, visible in `git log`). Follow the same shape:

```
worktree-component-38-go-native-server
worktree-component-39-peersync-conflict
worktree-component-40-go-client-sdk
worktree-component-41-auth-tokens
```

38 and 39 in separate worktrees from the start, since they're genuinely independent — no reason to
serialize them through one branch.

-----

## Estimated NBNC (Layer 12)

| Component | Lines |
|---|---|
| 38 Go-Native Server | ~1,800–2,800 |
| 39 Peer-Sync Conflict Detection | ~400–700 |
| 40 Go Client SDK | ~600–1,000 |
| 41 Auth Tokens | ~450–750 |
| 44–46 (minor, combined) | ~600–1,050 |
| **Layer 12 subtotal (P0/P1 only, excl. deferred 42/43)** | **~3,850–6,300** |

42/43 excluded pending Phase 0's answer — see their own specs' estimates (~1,700–2,900 combined,
plus unbounded iOS toolchain setup time) if they end up needed after all.

-----

## Verification checklist

- [x] Phase 1 (Component 38 sub-phase A, wire listener + framing):
      `go/kdb/server/wire_listen.go` - TCP accept loop atop `go/kdb/transport/tcp` (added
      `ListenBound`/`Serve` there so bind is synchronous and testable, behavior-preserving
      refactor of `Listen`), Handshake dispatch with SQL_CLIENT mode gating. Tests:
      `go/kdb/server/wire_listen_test.go` (raw-socket handshake accept/reject, auth-denial
      rejection, Close stops accepting), `go/kdb/wire/wire_test.go` (round-trip coverage added
      for SessionBegin/SessionBeginAck/SqlExec/SqlResult/TxCommit/TxRollback, previously
      untested).
- [x] Phase 2 (Component 38 sub-phase B, Commit/Query against the real TransactionEngine, no
      RBAC): `go/kdb/server/server_runtime.go` wires `transaction.Engine` + `sql.Engine` +
      `transaction.LockManager`; SqlExec runs SELECT/INSERT for real (buffers INSERT ops on a
      per-session pending builder), TxCommit/TxRollback flush or discard it. Scope not yet
      covered: RBAC (sub-phase C), and TxCommit's `transactionBytes` path (no Go
      `document.Transaction` wire codec exists yet - that's Component 40 territory) - both
      return clean, named not-implemented errors rather than silently misbehaving.
  - **Bug found and fixed along the way**: `transaction.Engine.Commit` +
    `InMemoryCommitDag.AppendCommit` have no compare-and-swap on the branch head - concurrent
    commits from two goroutines that read the same stale head could both "succeed" while one
    silently orphans the other from `main` (verified by disabling the fix: reproduced reliably,
    5/5 runs, with lost commits and 4-8x too many "successes" on the same document). Fixed via a
    `commitMu` in `KdbServerRuntime` serializing calls into `TransactionEngine.Commit`; the
    lower-level `transaction`/`dag` packages remain exposed for any other caller that invokes
    `Engine.Commit` concurrently on a shared DAG without equivalent serialization - worth a
    follow-up there directly.
  - Tests: `go/kdb/server/wire_listen_test.go` (CREATE TABLE → INSERT → TxCommit → visible to a
    fresh session only after commit; TxRollback discards buffered writes; 8-way concurrent
    commits from separate connections all chain into `main` with none lost - all pass under
    `-race`), `go/kdb/server/server_runtime_test.go` (direct-API same-document concurrent commit:
    exactly 1 of 8 racers succeeds, the rest get a real `*ConflictError`, DAG doesn't fork - the
    wire protocol can't yet construct a same-document collision itself since INSERT always mints
    a fresh doc id and there's no UPDATE/explicit-docID write path parsed yet).
  - Wired into `go/cmd/kdb-service/main.go` (`--sql-addr`, on by default) replacing the
    `sql=disabled (wire listeners not ported)` status line the component spec's §1 opens with.
    Manually smoke-tested: built the binary, ran it standalone, connected a real TCP client,
    completed a handshake - confirmed via `ps` that the running process is the Go binary itself,
    no JVM in its process tree (test 10's literal claim, informally - not yet promoted to an
    automated test with a `ps`-based assertion).
- [x] Phase 0 spike answer **resolved** (revisited once a gomobile toolchain became available in
      this environment - Xcode 26.6, Go 1.26, and an Android SDK+NDK 27.1.12297006 all turned out
      to already be installed; only `gomobile`/`gobind` themselves needed `go install`):
  - **Android: viable today, confirmed by an actual successful build.**
    `gomobile bind -target=android -androidapi 24 ./go/kdb/embed` produced a real 13MB `.aar`
    (native `libgojni.so` for all four ABIs - armeabi-v7a/arm64-v8a/x86/x86_64 - plus the
    generated Java bindings) with **zero changes to `go/kdb/embed`'s code**. (`-androidapi 24` is
    required with this NDK version - the default target API 16 predates what NDK 27 supports at
    all, a gomobile/NDK version mismatch, not a KDB issue.)
  - **iOS: fixed - `Release()` renamed to `Close()`, bind now succeeds.** `gomobile bind
    -target=ios ./go/kdb/embed` originally got all the way through Go compilation and
    Objective-C codegen, then failed on generated code: `Embed_darwin.m:76: error: ARC forbids
    implementation of 'release'`. Root cause: `EmbeddedKdbRuntime.Release()` (`runtime.go`) was
    the *only* exported method on that type, and gomobile's ObjC bridge generates an Obj-C method
    per exported Go method - `release` collides with Objective-C ARC's reserved `-release`
    selector (memory management is compiler-managed under ARC; you cannot implement it
    manually). Fixed by renaming `EmbeddedKdbRuntime.Release()` to `Close()` (`go/kdb/embed/
    runtime.go` - also arguably the more idiomatic Go name here, matching `io.Closer`) and
    updating its one call site (`go/kdb/driver/memory.go`'s file-mode release closure; the two
    *other* `.Release()` call sites that turned up in a repo-wide grep -
    `cmd/kdb-service/main.go`'s and `kdb/server/server_runtime.go`'s - are `KdbServerRuntime`'s
    own unrelated refcounting `Release()`, not this type, and were left untouched; `kdb/embed/
    file.go`'s `lock.Release()` calls are on the unexported `dirLock` type, never reachable by
    gomobile's binder regardless of name). Verified with a real rebuild: `gomobile bind
    -target=ios ./kdb/embed` now produces a real `Embed.xcframework` (device `ios-arm64` slice
    plus `ios-arm64_x86_64-simulator`), and a re-run of the Android bind confirms it's still
    unaffected. Full Go suite (`go build ./... && go test ./...`) green after the rename.
  - **Answer to the Phase 0 gate question**: "does `go/kdb/embed` cross-compile under `gomobile
    bind` today, with a feature set enough for Zolik's on-device needs?" - **Yes for both Android
    and iOS, confirmed by real successful binds on both targets.** Per the gate's own stated
    consequence, Components 42/43 (Kotlin/Native mobile targets) are very likely unnecessary -
    `go/kdb/embed` bound directly is the on-device storage answer.
  - Still worth doing before committing to this path in production: the noted `go/kdb/embed`
    dependency-bloat concern (transitively pulls in the full AWS SDK v2 for the *optional* S3
    replication feature, plus `net/http`/`crypto/tls`) - the produced binary size wasn't measured
    against a build with that path excluded via a build tag, which matters for a mobile app
    bundle even though it didn't block the bind itself.
  - `go/go.mod`: added `golang.org/x/mobile` as a *tool* dependency (`go get -tool
    golang.org/x/mobile/cmd/gobind`, per newer gomobile's requirement) and bumped the `go`
    directive to 1.26 (both from `go install`ing/running gomobile) - a dev-tooling dependency,
    not a runtime one; `go build ./...` unaffected.
- [ ] Phase 2/4 exit criteria's full form (**zero JVM processes**, load-tested) - only the
      handshake-level smoke test above has been done; no sustained-load or RBAC-enabled run yet.
- [x] Phase 4 (Component 38 sub-phase C, RBAC port):
      `go/kdb/auth/password_hasher.go` (PBKDF2-HMAC-SHA256, 120k iterations, 256-bit key,
      16-byte salt - byte-for-byte the same parameters as Kotlin's PasswordHasher.kt, cross-
      verified against a hash computed independently via the JDK's own `javax.crypto` for the
      same password+salt, not just self-consistency); `go/kdb/auth/registry_store.go`
      (`RegistryAuthStore` - user/role CRUD persisted as real DAG commits, matching
      `RegistryAuthStore.kt`); `go/kdb/auth/registry_auth_engine.go` (`auth.Engine` backed by the
      registry, matching `DynamicAuthEngine.kt`). Commit-time enforcement added to
      `KdbServerRuntime.Commit` (`server_runtime.go`): every op in a transaction is re-checked
      against the committing principal's grants immediately before the write lands, independent
      of whatever the wire layer already checked - the Go equivalent of
      `AuthorizingTransactionEngine.kt`'s decorator, implemented as a direct check inside `Commit`
      rather than a second `transaction.Engine`-wrapping type (simpler in Go, same effect).
      SqlExec is also authorized at the wire layer (namespace + read/write kind) as defense in
      depth's first layer, matching the Kotlin reference.
  - **Design deviation, and why**: `RegistryAuthStore` deliberately does not go through
    `transaction.Engine.Commit` (unlike ordinary data writes) - it appends directly via
    `dag.CommitDAG.AppendCommit`, which works against both an in-memory dag and a file-backed
    `*embed.PersistingCommitDAG`. `transaction.Engine.Commit` requires the *concrete*
    `*dag.InMemoryCommitDag` type, which `OpenFileRuntimeWithOptions` does not always hand back
    (see the file-backed-durability finding below) - the registry sidesteps that entirely, and
    doesn't need conflict detection anyway (single-writer admin registry, matching Kotlin's own
    choice of `ConflictPolicy.LAST_WRITE` here).
  - **A real gap found, not fixed here (flagged instead)**: `OpenFileRuntimeWithOptions` returns
    `rt.DAG` wrapped in `*embed.PersistingCommitDAG` whenever a delta writer is configured, not
    the concrete `*dag.InMemoryCommitDag` `KdbServerRuntime.Commit`'s type assertion requires -
    meaning `Commit` (and by extension every ordinary SqlExec/TxCommit write) currently fails
    against a real file-backed, durable deployment, silently degrading to "commit requires an
    InMemoryCommitDag" for the exact configuration Component 38's own zero-JVM cost story assumes
    is the real target. This is a Component-38-wide gap discovered while building Phase 4's
    restart-durability test, not something Phase 4 introduced or is scoped to fix - worth its own
    session. `RegistryAuthStore`'s workaround above does not apply to `KdbServerRuntime.Commit`.
  - **Wire protocol extension**: `wire.HandshakePayload` gained optional
    `User`/`Password`/`Token` fields (`wire/types.go`, `payload_dto.go`, `payload_mapper.go`).
    Necessary, not incidental scope creep - raw TCP has no ConnectionContext/HTTP-header side
    channel the way the Kotlin WebSocket reference does, so without this a TCP SQL_CLIENT has no
    way to supply credentials at all and RBAC would be unreachable over the wire.
  - `go/cmd/kdb-service/main.go` gained a `--rbac` flag (in-memory registry only - file-backed
    registry CLI wiring is a fast-follow, tracked by the gap above); manually smoke-tested: built
    the binary, ran standalone with `--rbac`, confirmed via `ps` it's the only new process (no
    JVM), then killed it cleanly.
  - Tests: `go/kdb/auth/password_hasher_test.go` (cross-implementation hash verification is the
    real test; also round-trip, salt randomness, malformed-hex fail-closed),
    `go/kdb/auth/registry_store_test.go` (CRUD, role assign/revoke, and
    `TestRegistryAuthStoreSurvivesRestart` - spec test 9, rebuilds a fresh dag+storage purely by
    replaying persisted commit history, the same mechanism a real restart against durable storage
    would use), `go/kdb/server/rbac_test.go` (`TestKdbServerRuntimeCommitDeniesUnauthorizedWrite`
    - spec test 4's literal claim, driven directly against `Commit` bypassing the wire layer to
    prove real defense in depth; the positive counterpart; and a full wire-level RBAC end-to-end
    test - real credentials over a real TCP socket, an authorized user's CREATE/INSERT/commit
    succeeding, an unauthorized user's INSERT denied, wrong-password and unknown-user handshake
    rejection). All green under `-race`.
- [x] Phase 3 (Component 39, peer-sync conflict detection fix):
      `kdb-peer-sync/src/commonMain/kotlin/dev/kdb/peersync/PeerSyncConflictDetection.kt` (new) -
      `resolveHeadUpdate`/`resolveDivergence`, the shared decision `PeerSyncFrameHandler.
      handleCommitPush` and `PeerSyncClient.pullMissing` both now call (§5 symmetry), replacing
      the unconditional `dag.setHead("main", ...)` in both places. Disjoint-document divergence
      (including every `APPEND_ONLY` case, which by construction never touches the same document
      twice) auto-merges via a real two-parent `appendMergeCommit`; genuine same-document
      divergence produces a `ConflictReport` (reusing `kdb-transaction`'s existing shape per §5,
      not a new one) and leaves `main` untouched. Deliberately out of scope, and safe rather than
      silently wrong: `TxCommit`-style client-encoded transaction replay isn't part of this
      component; LAST_WRITE/CUSTOM policies fall back to STRICT's "report, don't pick a winner"
      behavior for a genuine same-document conflict since auto-resolving those isn't in the spec's
      test list; test 8 (RBAC interaction) is explicitly left as a separate fix per the spec's own
      Non-Goals.
  - **Two more concurrency bugs found and fixed while implementing this**, both the same shape as
    Component 38's `commitMu` finding: (1) `resolveDivergence` itself read-decided-mutated the DAG
    across several non-atomic calls with no serialization - two connections pushing to the same
    host, or a push racing a pull, could reopen the exact fork bug this component exists to close,
    one level up. Fixed with a per-namespace `Mutex` (`divergenceLockFor`). (2) The auto-merge
    commit's `operations` list was initially left empty (seemed reasonable - the tree already
    captures the result) - but `kdb-embed`'s `materializeCommitHistory` replays each commit's own
    `operations` against its *primary* parent's tree, so an empty list silently dropped the
    non-primary side's documents for any consumer that materializes via replay instead of reading
    the tree directly. Caught by the new genuinely-concurrent integration test below, not by the
    unit-level tests (which read `newDocumentTree` directly and so never exercised replay). Fixed
    by setting the merge commit's `operations` to the delta it introduces relative to its primary
    parent.
  - `kdb-peer-sync`'s `build.gradle.kts`: `kdb-error`/`kdb-transaction` moved from
    `implementation` to `api`, since `ConflictReport`/`ConflictPolicy` are now part of this
    module's public surface (`PeerSyncResult.conflict`, `PeerHostConfig`/`PeerClientConfig.
    conflictPolicy`) - without this, any consumer depending on `kdb-peer-sync` alone fails to
    compile with "Cannot access class ConflictReport" (found via `kdb-integration`).
  - Tests: `kdb-peer-sync/src/commonTest/.../PeerSyncConflictDetectionTest.kt` (new, 6 cases -
    same-document conflict with `main` unmoved and no history lost, disjoint-document auto-merge
    with real two-parent merge-commit shape, `APPEND_ONLY` both-sides-reachable, three-way sync
    detecting against the already-updated head rather than the original ancestor,
    `AncestryLookupException` on no common ancestor, push/pull classification symmetry) - all
    against `resolveDivergence` and the real host/client wire path, not mocked. Extended
    `kdb-integration/.../WebSocketPeerSyncIntegrationTest.kt` with
    `wsPeerSyncBidirectionalGenuinelyConcurrent` (spec's required new test 9 variant: both peers
    write before either syncs, then push over two real network connections at the same time,
    racing the server's divergence resolution for real - this is the test that caught both
    concurrency bugs above); the existing sequential `wsPeerSyncBidirectionalAfterPush` still
    passes unmodified. Full repo-wide Kotlin suite (`./gradlew test jvmTest`) green: 427/427.
- [x] Phase 5 (Component 40, Go Client SDK): `go/kdb/client` (new package) - `Connect`/`Close`,
      `PutJSON`/`GetJSON`/`Upsert`/`Commit`/`Query`/`Exec`/`AppendEvent`, matching the spec's
      illustrative interface. Supplied the `document.Transaction` wire codec Phase 2 had punted
      on (`go/kdb/wire/transaction_codec.go`, field-for-field matching Kotlin's
      `TransactionWireCodec.kt`) and wired it into `handleTxCommit`'s `TransactionBytes` path,
      closing that gap - this is what lets `PutJSON`/`Commit` write at a caller-chosen document
      id at all, since SqlExec's INSERT always mints a fresh random UUID and has no
      point-lookup-by-id predicate for `GetJSON` either.
  - **New wire messages** (Go-only, no Kotlin counterpart - matches component 38 spec's own
    "extend go/kdb/wire itself" guidance when the existing message set can't express something):
    `DocumentGet`/`DocumentGetResult` (point lookup, backs `GetJSON`), `Upsert`/`UpsertResult`
    (unconditional write, backs `Upsert`/`AppendEvent`). `KdbServerRuntime` gained a second
    `UpsertEngine` (`ConflictPolicyLastWrite`) alongside the existing STRICT `TransactionEngine`,
    since `Engine` bakes its conflict policy in at construction and `Commit`/`Upsert` need
    different ones simultaneously.
  - **A real cross-language wire-compatibility bug found and fixed along the way**:
    `kdberr.ConflictReport`/`ConflictItem` had no JSON tags, so `encoding/json.Marshal` emitted
    capitalized Go field names (`TransactionID`, `OperationType` as a bare integer ordinal)
    instead of the lowerCamelCase, enum-as-string convention every other wire payload in this
    codebase uses - meaning `ConflictReportMessage.ReportBytes` was silently wire-incompatible
    with any Kotlin decoder, and the Go client's own decoder couldn't even parse the Go server's
    own conflict reports (caught immediately by this component's own conflict test). Fixed with
    explicit `json` tags plus a `MarshalJSON` on `ConflictOperationType` emitting the enum name.
  - **Scope boundaries, and why**: spec test 1 (round trip against the JVM `kdb-server`) needs a
    JVM process this Go test suite doesn't start - not attempted here; test 2 (against Component
    38's Go-native server, the spec's own stated intended long-run/lower-risk target) is what's
    actually tested. Spec test 6 (`ErrWrongConflictPolicy` for `Upsert` against a STRICT
    namespace) doesn't apply to this implementation: there is no per-namespace-configurable
    conflict policy anywhere in this Go port yet (`Commit` always uses STRICT, `Upsert` always
    uses its own dedicated LAST_WRITE engine) - a call can't be "wrong" against a policy that
    isn't a per-namespace setting. `Transaction.Writes` (the public client type) requires every
    write share one namespace and errors otherwise, since one `KdbServerRuntime` is scoped to one
    namespace and can't execute a cross-namespace transaction atomically.
  - Tests: `go/kdb/wire/transaction_codec_test.go` and 5 new wire round-trip tests for the new
    message types; `go/kdb/client/client_test.go` (9 of spec's 10 test cases - Connect/PutJSON/
    GetJSON round trip, Commit success-then-conflict-on-stale-base with no partial write,
    8-way concurrent Commit racing one BaseVersion with exactly 1 success, Upsert create-then-
    replace, Query decoding into a struct slice, AppendEvent never conflicting under 8 concurrent
    writers, context cancellation mid-call leaving the connection usable, a ~30-field match-
    shaped document round-tripping byte-for-byte, and an RBAC boundary check). All green under
    `-race`; full repo-wide Go suite (`go test ./...`) still green throughout.
- [x] Phase 6 (Component 41, Auth Session/Token Issuance):
      `kdb-auth/src/commonMain/kotlin/dev/kdb/auth/token/` (new package) - `TokenAuthConfig`,
      `TokenAuthEngine` (implements the real `Authenticator` interface -
      `authenticate(AuthCredentials): Principal`, throwing on failure - not the spec's
      illustrative `AuthResult`-returning shape, which doesn't match what `Authenticator` actually
      declares; a `TokenAuthRejectedException` subclass carries the specific `RejectReason` for
      tests/callers that want to distinguish why without parsing the message),
      `CompositeAuthEngine` (tries multiple `Authenticator`s in order, first success wins, last
      rejection rethrown if all fail), `SessionIssuer` (mints/revokes session documents via a
      narrow `DocumentWriter`, independent of `TokenAuthEngine`'s `DocumentReader` - `revoke`
      derives the same deterministic doc id `issue` used from the token value alone, so it needs
      no read-then-delete lookup).
  - **Adaptations from the illustrative spec, and why**: `expiresAt` is stored as epoch
    microseconds (a `KdbTimestamp`-compatible JSON number), not an ISO-8601 string via
    `kotlinx-datetime` - matches this codebase's own timestamp convention everywhere else, and
    avoids a new external dependency for one field. Expiry boundary is exclusive (a token is
    valid strictly before `expiresAt`, not through it) - explicit, tested choice per spec's own
    ask to pin this down. A session document with malformed/missing `expiresAt` maps to
    `TOKEN_EXPIRED` (closest fit among the three `RejectReason` values - the document *was*
    found, and the *credentials* aren't malformed, but validity can't be confirmed) rather than
    inventing a fourth reason.
  - `KdbAuthenticationException` (`kdb-auth/AuthExceptions.kt`) changed from `class` to
    `open class` - the minimal change needed for `TokenAuthRejectedException` to subclass it
    while still satisfying `Authenticator`'s real contract; purely additive, nothing about
    existing throw/catch behavior changes.
  - `kdb-auth`'s `build.gradle.kts` gained `kdb-codec`/`kdb-document`/
    `kotlinx-serialization-json` dependencies (previously had neither - `kdb-auth` only declared
    auth-abstraction-level types before this component needed to read/write actual
    `KdbDocument`s).
  - Tests: all 12 of spec §7's test cases covered (`TokenAuthEngineTest.kt`,
    `CompositeAuthEngineTest.kt`, `SessionIssuerTest.kt`) against trivial in-memory
    `DocumentReader`/`DocumentWriter` fakes per spec §9's explicit intent - no password/RBAC
    machinery involved in any of them. Full repo-wide Kotlin suite green: 441/441.
- [x] Phase 8, Components 44–46 (minor items, gap analysis §5):
  - **Component 44 (cross-write notification bridge)**: `EmbeddedKdbRuntime` gained
    `addCommitListener`/`notifyCommit` (`kdb-embed/EmbeddedKdbRuntime.kt` - a `Mutex`-guarded
    listener list, matching this codebase's existing convention for this shape rather than
    `MutableSharedFlow`, which is reserved for kdb-stream's own event bus). Wired into
    `commitViaEngine` (`kdb-embed/EmbedWrites.kt`) right after `applyCommit` - the one place every
    commit path converges (SQL wire via `KdbServerRuntime.commit()`, *and* embedded/local JDBC via
    `EmbeddedSqlSession.commit()`), so this single hook covers both rather than just the SQL-wire
    half the spec's own estimate assumed. `KdbServiceMain.kt` wires the listener to
    `streamHub.publish(...)`.
    - **Bug found and fixed along the way**: `StreamBroadcastHub.publishedCommitFrom` used to
      `error(...)` on a commit with no parent - unreachable while only peer-sync fed it (a
      namespace's first commit never arrives via peer-sync in practice), but immediately hit once
      real SQL-write commits started flowing through it. Fixed with a `ZERO_PARENT_HASH` sentinel
      (`kdb-stream/StreamBroadcastHub.kt`).
    - **Self-introduced regression caught via bisection**: the first attempt at that sentinel used
      `KdbHash.fromHex("00".repeat(64))` - 128 hex chars instead of the 64 a 32-byte hash needs,
      throwing at class-init time deep inside a peer-sync coroutine's call chain and silently
      killing `WebSocketStreamIntegrationTest.wsStreamPushNotifiesSubscriber`. Found via `git
      stash`-based bisection across `kdb-embed`/`kdb-stream`/`kdb-server`/`kdb-service`, narrowed
      to `StreamBroadcastHub.kt` alone; fixed to `.repeat(32)`.
    - Tests: `kdb-embed/CommitNotificationTest.kt` (5 cases: fires per commit, multiple listeners,
      a throwing listener doesn't break the commit, no-listeners no-op), plus
      `StreamBroadcastHubTest.publishedCommitFromHandlesRootCommitWithoutCrashing`.
  - **Component 45 (disconnect/lock cleanup)**: `SessionManager.endAll()` (new -
    `kdb-server/SessionManager.kt`) releases every session, and its document locks, a
    `SqlWireHost` holds; `SqlWireHost.endSession()` calls it. `SqlWireListen.kt`'s
    `pipelinedPerConnection` now wraps its whole read loop in `try { ... } finally {
    host.endSession() }`, so it fires on normal completion, an exception, or cancellation alike -
    covering both transports that route through this one function (TCP and WebSocket).
    - Deliberately *not* ported into Component 38's Go server in this pass, despite Phase 8's own
      note above suggesting Go-first: Component 38 already had no equivalent leak (its session/lock
      teardown was scoped differently from the start), so there was nothing to port.
    - Tests (first-ever test in `kdb-server`, which had no `src/test` tree before this):
      `kdb-server/src/test/kotlin/dev/kdb/server/SqlWireDisconnectCleanupTest.kt` - drives the real
      `pipelinedPerConnection` loop (not `handleFrame` called directly, the way existing
      `kdb-integration` tests do) over a fake in-process `WireConnection`, proving a second,
      independent connection can acquire a document lock that a first connection held via an
      open-but-never-committed transaction, once the first disconnects without COMMIT/ROLLBACK.
      Verified the test actually catches the bug: reverting the `finally` block makes it fail at
      the exact assertion that matters; restoring the fix makes it pass again.
  - **Component 46 (write-back mode fix)**: client side, `StreamSubscriber.kt`'s
    `submitTransaction` used to wire-encode only `tx.id.toString()` (nothing a coordinator could
    replay) and return `ReplayResult.Rejected("async replay not awaited in v1")` immediately
    without ever waiting for a response. Now encodes the real transaction via
    `TransactionWireCodec` (the same codec TxCommit already uses), registers a
    `CompletableDeferred<ReplayResult>` keyed by correlation id, and awaits it under a 10s
    `withTimeout`; `disconnect()` fails any still-pending call with a named rejection instead of
    leaving it hanging. Server side, `StreamBroadcastHub` gained an optional
    `transactionReplayer` callback (kept as a plain closure, not a direct `KdbServerRuntime`
    dependency, since `kdb-server` already depends on `kdb-stream` - a reverse dependency would be
    circular) invoked on `WireMessage.TransactionReplay`; `KdbServiceMain.kt` wires it to
    `serverRuntime.replay(...)`, mapping `TransactionResult` variants to the same
    `SqlResult`/`ConflictReport` wire shapes `SqlWireHost.handleTransactionReplay` already sends,
    so the client needs no new response type to recognize.
    - Tests: server-side wiring in `StreamBroadcastHubTest.kt` (no-replayer-configured is rejected
      explicitly rather than silently dropped; a configured replayer is invoked and its response
      forwarded; a different namespace's replay is ignored, never routed to the replayer). Client
      side, `kdb-stream/StreamSubscriberSubmitTransactionTest.kt` (6 cases, against a fake
      coordinator built on the existing `InMemoryWireTransportHub` test fixture, not `handleFrame`
      called directly): the actual transaction bytes (not just an id) reach the coordinator;
      `Applied`/`Rejected`/`Conflict` are each produced by the matching response shape; a
      coordinator that never responds times out (virtual-time, no real 10s wait) rather than
      hanging; disconnecting while a call is pending fails it immediately instead of leaving it
      stuck until the timeout. Verified against the pre-fix `git stash`d version of
      `StreamSubscriber.kt`: all 6 fail there, confirming they exercise the actual fix.
  - Full repo-wide suite green after all three: Go `go build ./... && go test ./...` clean; Kotlin
    457/457 across `test`/`jvmTest`.
- [x] Follow-up fix (flagged during Component 39, not part of the original plan): the
      `go/kdb/peersync` client had the identical blind-head-move bug Component 39 fixed in
      Kotlin - `PullMissing` unconditionally called `dag.SetHead("main", fetched.last().hash)`,
      and the host's `CommitPush` handler had a related gap (never advanced `main` on an incoming
      push at all). Ported the Kotlin fix's decision function to Go field-for-field
      (`go/kdb/peersync/conflict_detection.go`: `ResolveHeadUpdate`/`ResolveDivergence`, the same
      fast-forward/already-ancestor/diverged classification, the same per-namespace `sync.Mutex`
      serialization as `KdbServerRuntime.commitMu`, the same non-conflicting-disjoint-writes
      auto-merge via a real two-parent commit, the same same-document-conflict report). Wired into
      both `client.go`'s `PullMissing` and `host.go`'s `CommitPushMessage` handler - one shared
      function, not two independently maintained copies, matching Component 39's own §5 symmetry
      contract. `peersync.Result` gained a `Conflict *kdberr.ConflictReport` field (nil unless a
      genuine same-document divergence left `main` deliberately unmoved). Also added
      `ConflictOperationType.UnmarshalJSON` (`go/kdb/error/payloads.go`) - Component 40 had only
      ever needed `MarshalJSON` (Go was always the sender), this is the first Go-side reader.
      `NewClient`/`NewHost`/`NewConnectionHost` gained a `storage.Adapter` parameter (needed for
      the auto-merge path); no real callers existed anywhere in the repo yet to migrate, since
      this package had never been wired into `kdb-service`.
  - This package had zero tests before this fix - now has 11, in
    `go/kdb/peersync/conflict_detection_test.go` (7 cases: fast-forward, already-ancestor both
    directions, diverged classification, disjoint-write auto-merge, same-document conflict,
    delete/write conflict classification) and `client_host_integration_test.go` (4 cases, driven
    through the real wire protocol via `stream.InMemoryTransport`/`NewHost`/`NewClient` rather
    than calling `ResolveDivergence` directly: client-side pull-through-divergence auto-merges,
    client-side pull-through-divergence reports conflict, host-side push-through-divergence
    returns an explicit `ConflictReportMessage`, host-side push-through-divergence auto-merges
    with a silent ack). Verified every test actually catches the bug: temporarily reverted each
    of `client.go`, `host.go`, and `conflict_detection.go` to their pre-fix (or blind-SetHead
    simulation) behavior in turn and confirmed the relevant tests fail, then restored the fix and
    confirmed they pass again. Full suite green after, including `-race` on `peersync`.
- [x] Follow-up fix (flagged during Component 38, not part of the original plan):
      `KdbServerRuntime.Commit`/`Upsert` type-asserted `Runtime.DAG` directly to
      `*dag.InMemoryCommitDag`, which only matches `embed.OpenMemoryRuntime`'s DAG - a file-backed
      runtime (`embed.OpenFileRuntime`, `cmd/kdb-service`'s `--data-dir` mode) wraps its DAG in
      `*embed.PersistingCommitDAG` for durability, so the assertion always failed and every commit
      against a file-backed server returned `"commit requires an InMemoryCommitDag"` - the Go
      native server's durable mode could not write at all (and `SQLEngine` was wired with a `nil`
      DAG in that case too, since the same failed assertion fed its constructor). `transaction.
      Engine.Commit`/`Replay`/`Merge`/`Validate` all hard-require the concrete
      `*dag.InMemoryCommitDag` (conflict detection needs `HasCommit`/`CommonAncestor`/etc., which
      aren't part of the `dag.CommitDAG` interface `PersistingCommitDAG` implements), so passing
      `PersistingCommitDAG` itself through was never an option - fixed instead by adding
      `PersistingCommitDAG.Delegate()` (returns the concrete DAG it wraps) and
      `PersistingCommitDAG.Persist(commit)` (the encode/Append/Flush sequence `AppendCommit`
      already did internally, factored out and exported - `go/kdb/embed/persisting_dag.go`).
      `NewKdbServerRuntime` now switches on `Runtime.DAG`'s concrete type, unwrapping either shape
      to get the real DAG for `TransactionEngine`/`SQLEngine`, and keeps the `*PersistingCommitDAG`
      itself (as `persister`) when present; `commitWith` calls `persister.Persist(result.Commit)`
      after a successful `TransactionEngine.Commit` to restore the durability that calling
      `Engine.Commit` against the unwrapped delegate DAG directly bypasses (`go/kdb/server/
      server_runtime.go`).
  - Tests (new file, `go/kdb/server/file_backed_runtime_test.go`): commit against a real
    file-backed runtime succeeds and is actually durable - closes the runtime, reopens the same
    data directory fresh, and confirms the delta-log replay reproduces the exact commit hash and
    document content (not just "didn't error"); a second test covers `Upsert`'s identical path.
    Verified both catch the regression: temporarily reverted `NewKdbServerRuntime` to the old
    direct type assertion and confirmed both tests fail with the original error message, then
    restored the fix. Full suite green after, including `-race` on `server`/`embed`/`peersync`.
- [x] Cross-implementation interop test: Go client ↔ Go server AND Go client ↔ JVM server, both
      passing (component 38 spec §7 test 2 / component 40 spec §7 tests 1–2). Go↔Go was already
      covered (`go/kdb/client`'s own tests). Go↔JVM needed a real JVM `kdb-server` process in the
      loop, which surfaced a gap bigger than "test not written": `go/kdb/transport/ws`'s client
      was an unimplemented stub (`Connect` always returned "not implemented"), and the JVM server
      only ever listens for SQL wire over WebSocket - Go's real client only ever spoke raw TCP, so
      there was no transport that could reach a JVM server at all. Asked the user how to close
      that gap (add a JVM-side raw-TCP listener vs. implement a real Go WS client); they chose the
      Go WS client.
  - Implemented a real RFC 6455 client in `go/kdb/transport/ws/transport.go` (net/http-free, hand-
    rolled to match `kdb-transport-ws`'s own hand-rolled JVM implementation exactly:
    `WebSocketFraming.kt`/`JvmRawSocketWebSocketConnection.kt`) - HTTP upgrade handshake
    (`Sec-WebSocket-Key`/`-Accept`, SHA-1 + the RFC magic GUID), masked client frames / unmasked
    server frames (matching what the JVM side actually does on each side), 7/16/64-bit payload
    length encoding, ping answered with a masked pong. `Listen` (a Go-side WS *server*) stays a
    stub - out of scope, the client was what blocked this test.
  - **Real interop bug found via this test, not a synthetic one**: the JVM handshake decoder
    (`kotlinx.serialization`, `HandshakeDto.localHeads: Map<String, String>`, non-nullable, no
    default) throws and silently drops the connection - no response frame at all - on a Go SQL
    client's handshake, because `go/kdb/client`/`go/kdb/peersync` never populate `LocalHeads` (a
    peer-sync/stream-only field), so it marshals as JSON `null`. Every real Go SQL client hitting
    a real JVM server was affected; Go↔Go never exercised this because Go's own decoder tolerates
    `null` for a map. Fixed by normalizing `LocalHeads` to `{}` at encode time
    (`go/kdb/wire/payload_mapper.go`), verified against the same live JVM server that first
    reproduced it.
  - Tests: `go/kdb/transport/ws/transport_test.go` (5 cases, incl. a large->64-bit-length-path
    payload and an independent from-scratch fake WS server so the client's read and write sides
    can't share a bug that cancels itself out) - `go/kdb/transport/ws` had zero tests before this.
    `go/kdb/interop/jvm_server_interop_test.go` (build-tag `interop`, skipped by default like the
    Lightsail load test since it needs an external JVM process - env-var-gated
    `KDB_JVM_SQL_WS_URI`) drives Handshake → SessionBegin → INSERT → SELECT against a real,
    separately-started `kdb-service` process; verified manually end to end (handshake accept,
    session begin, INSERT `rowsAffected=1`, SELECT returns the just-inserted row) against a live
    `./gradlew :kdb-service:runService` instance before committing.
- [x] `docs/kdb-spec.md` §0/§16.1/§17 updated to include Layers 11–12 (Phase 8)
- [~] Lightsail load test on the target tier (component 38 spec §7 test 8) — the real run still
      needs actual x86_64 Lightsail hardware (out of reach in this environment), but a local
      Docker-based approximation of the $7/mo tier (1GB RAM, 2 vCPU, via cgroup limits) is now in
      `docs/benchmarks/lightsail-sim/` (`run.sh` + `go/cmd/kdb-loadtest`), and running it surfaced
      a real OOM (`kdb-service --memory` getting SIGKILLed under moderate write load once distinct
      document count passed a few hundred) that has since been root-caused and fixed - see
      `docs/benchmarks/lightsail-sim/README.md`'s "The fix" section for the full detail. Two real
      allocation bugs found via local `pprof` profiling (both ported to Kotlin too, since it
      shared the pattern): `findExistingCommit`'s idempotent-retry check walked up to 8192 commits
      of history on every single commit (~88% of all allocation; fixed with an O(1) DAG
      transaction-id index), and `DocumentTree.With`/`Without` eagerly copied its full entries map
      on every write (~11%; fixed with a lazy, trie-backed map). Together these eliminated all
      per-commit scaling with history/document size (`BenchmarkCommitScalingWithHistorySize`,
      `go/kdb/transaction`, confirms flat ~21µs/commit at both 10 and 8,000 commits of history,
      vs. up to 234.8µs/commit before). That alone still isn't sufficient in principle - an
      in-memory, uncompacted commit DAG grows without bound by design - so also added
      `go/kdb/server.MemoryGuard`: background-sampled backpressure (on `Sys`, not `HeapAlloc` -
      see the README for why that distinction mattered empirically) that rejects new writes with
      a clear `*MemoryPressureError` once memory nears a configured budget
      (`kdb-service --memory-limit-mb`), instead of risking an OS OOM-kill with no warning to the
      client. Verified end to end: the same 2000-document pool that reliably OOM-killed the server
      within seconds now survives 5.7 million operations over 3.5 minutes at a safe configuration
      (`--memory-limit-mb` set to ~60% of the container's actual limit - the README documents why
      that's the right fraction, found empirically, not the more intuitive 80-90%), with memory
      usage plateauing at 67% instead of climbing to the hard limit. **Superseded 2026-08-28:**
      that 60% figure was a workaround for this guard being a periodic sampler, which a burst
      admitted between two samples could outrun. kdb-spec-layer13 Component 48's grant system
      replaced it with reserve-before-admit; the flag is now `--memory-budget-mb`, it defaults to
      auto-detecting the cgroup limit rather than to off, and 90%/95% of the container limit both
      survive where they previously OOM-killed. Still `[~]` rather than `[x]`:
      the underlying "$7/mo tier" cost claim itself still needs the real Lightsail hardware run
      to be billable - this harness now confirms the server can run unattended under sustained
      load with a proper memory-limit configuration, which it previously could not do at all.
