# KDB open issues and cleanup plan

Written 2026-09-24 against `main` at `e125a10` (v0.6.2 + #104). Every claim below was checked
against that commit, not carried over from older plans. Several items that older docs and notes
still list as open turned out to be done; they are recorded in [Already resolved](#already-resolved)
so nobody re-does them.

The plan is a sequence of **units**. Each unit is one branch and one PR, small enough to review on
its own, and leaves `main` green. Execute them in order unless a unit says it is independent.

## How to execute a unit

1. **Check for other sessions first.** Other Claude sessions work in `/Users/davidj/devel/kdb`.
   Run `ListAgents` (or look at `git worktree list`) before branching, pushing or tagging, and work
   in a worktree, not the shared checkout.
2. Branch from fresh `main`: `git fetch && git switch -c <branch> origin/main`.
3. **Reproduce before fixing.** Every defect unit starts with a test that fails on `main`. Revert the
   fix once, locally, to confirm the test catches it — the repo's convention, and what
   `docs/kdb-finish-up-plan.md`'s progress log records per item.
4. Gates, all local before pushing:
   - `cd go && go test -race ./...`
   - `./gradlew build --no-daemon` once U1 has landed (before that, `./gradlew test allTests`)
   - `make test-physical-golden` for anything touching an on-disk format
   - Check `uptime` first; don't run the Go suite alongside another heavy job.
5. Anything that changes a wire or on-disk format lands in **Go and Kotlin in the same PR**
   (the "must land together" table in `docs/kdb-finish-up-plan.md`).
6. Update the **Progress log** at the bottom of this file in the same PR: what landed, the commit,
   and anything found along the way.
7. When CI goes red, read every failing job. Don't write a failure off as "flaky" until the same
   SHA has passed on a re-run.

---

## Phase A — Unblock and tidy (small, low risk, do first)

### U1. Make `./gradlew build` green and gate CI on it — S

**Status: done** (#106).

**Problem.** `:kdb-storage-io:compileCommonMainKotlinMetadata` fails on clean `main`:
`Class 'JvmFileBackedPlatformIoShim' is not abstract and does not implement abstract member
'appendToSegment'` (and the Native/Browser equivalents), at
`kdb-storage-io/src/commonMain/kotlin/dev/kdb/storage/io/FileBackedPlatformIoShimFactory.kt:9,11,13`.

**Root cause.** Those three are `expect class X(config) : PlatformIoShim` with no members. Every
`actual` extends `FileBackedPlatformIoShimBase` (commonMain, which implements `appendToSegment` at
`FileBackedPlatformIoShimBase.kt:34`), but the common-metadata compilation checks the `expect`
declaration alone and sees a concrete class implementing nothing. Per-platform compilations see the
actuals and pass. CI runs only `./gradlew test allTests` (`.github/workflows/ci.yml:47-53`), which
never compiles common metadata, which is how the break went unnoticed.

**Fix.**
- Declare the supertype on all three `expect` lines as `: FileBackedPlatformIoShimBase, PlatformIoShim`.
  Verified in a scratch worktree: the full `./gradlew build --continue --no-daemon` is then
  **BUILD SUCCESSFUL** (10m49s), with no other failing task.
- In the same PR, change the Kotlin CI job to `./gradlew build --no-daemon`. `build` includes
  `check`, and so everything `test allTests` ran. Land both together, or CI goes red.

**Done when.** CI's Kotlin job runs `build` and is green. Update the memory note
`kdb-known-failing-baseline` (it will be wrong once this lands).

### U2. Ship `kdb/server` in the embeddable Go bundle — S/M

**Status: done** (#108; #26 superseded).

**Problem.** The only known issue in `docs/releases/v0.6.2.md:95-98`: a downstream project (Zolik)
that imports `kdb/server` gets a bundle without it. PR #26 has the change but conflicts, and its
numbers are stale.

**Do not rebase #26.** Open a fresh branch from `main` and close #26 in favour of it:
1. Add `kdb/server` to `go/embedbundle/entrypoints.txt`.
2. Carry over #26's text changes to `go/embedbundle/main.go` (the EMBEDDING.md template, ~L452-456)
   and `README.md` (~L105). Both merge cleanly.
3. Rewrite `docs/kdb-release-plan.md` by hand from `main`'s version, not #26's:
   - §2.1 (L81-84): new scope.
   - §2.2: add the entry point. The closure grows **27 → 40 packages** (the added 13 are listed in the
     scratch `srv.pkgs`; regenerate them); remove `server`, `wire`, `transport`, `peersync`, `stream`
     and `version` from the excluded list (L113-117); the skipped test files become three
     (`database_backup_test.go`, `kdb/server/backup_txn_test.go`,
     `kdb/server/panic_recovery_test.go`), and `transaction/procedure_resolver_test.go` stops
     being skipped.
   - §2.3/§2.5 (L157, L204, L208): counts and size, **426 → 665 files, ~907 KB → ~1.64 MB**. Keep
     `main`'s testdata wording.
   - §7.1 (L403-408).
4. Update the entry-point comment at `Makefile:122-125`.
5. Move the v0.6.2 known issue into the next release's notes as fixed.

**Already verified:** no cgo anywhere in the closure; `CGO_ENABLED=0 go build ./kdb/server` passes
for linux/amd64, linux/arm64, darwin/arm64, windows/amd64 and js/wasm; `scripts/verify-bundle.sh` on
the 40-package bundle passes (the same gate as `release.yml:79-80`). New third-party requirements:
`github.com/dop251/goja` and `golang.org/x/sync` (pure Go). Call that out in the PR, since it's a
dependency change for downstream users.

**Worth adding while here:** `go/embedbundle` has no test and the counts live only in prose. Add a
small test that asserts the closure contains every entry point and no cgo package, so a future
entry-point change can't break the bundle unnoticed. Leave the counts out of the test, since they
change with every package added.

### U3. Branch and worktree cleanup — S (needs your go-ahead; deletes are not reversible on origin)

**Status: local half done 2026-10-11** (97 merged local branches deleted, stale worktrees pruned; `kdb-deleted-local-branches-2026-10-11.bundle` keeps the two non-ancestor ones). **Origin half pending:** 44 merged or superseded branches on GitHub, plus closing #26, are left for the owner to run (automated deletion was refused); every tip is saved in `kdb-deleted-branches-2026-10-11.bundle` beside the checkout.

Verified with `git cherry`. None of these hold content that isn't on `main`:

| Branch | Why it's safe |
|---|---|
| `feat/segment-preallocation` (local + origin) | landed as #58 + #61 |
| `perf/single-location-deltas` (local) | landed as #36; the extra commit is an old version bump |
| `local/final-check` (local) | a merge of two commits already on `main` |
| `release/embeddable-go-bundle-and-release-scripts` (local + origin) | same tree as #26's branch |
| `fix/embedbundle-include-kdb-server` (local + origin) | superseded by U2 (delete after U2 merges) |

Plus about 80 merged local branches, and `git worktree prune` for the 6 prunable worktrees.

**Do not remove** `.claude/worktrees/keen-edison-cdead3`. It holds **uncommitted** edits from
2026-09-21 in 14 files (`go/kdb/peersync/client.go`, `go/kdb/wire/payload_dto.go`,
`PeerSyncClient.kt`, `docs/kdb-user-guide.md`, …), the Go↔Kotlin peer-sync interop work. Diff them
against `main` first. Whatever isn't on `main` becomes its own PR or is discarded deliberately.

### U4. Correct stale docs and notes — S

**Status: done** (this unit's PR): finish-up-plan entries annotated, memory notes corrected, unit statuses added here.

- `docs/kdb-finish-up-plan.md:378,386,388,596,597` still list the explicit compression flag and the
  cross-language SSTable fixture test as open. Both landed (see [Already resolved](#already-resolved)).
- The memory notes on flaky Kotlin tests, the Gradle baseline and preallocation (KDB **does**
  preallocate delta segments, behind `KDB_PREALLOCATE_SEGMENTS`).

Can ride along with U1 or U3.

---

## Phase B — Correctness

### U5. JS/Native write zstd-tagged blocks that are not zstd — S/M, format correctness

**Status: done** (#109). Real zstd on those targets is R1, done in #116.

**Problem.** On JS and Native, `ZstdCompression.compress` is a 4-byte length prefix plus the raw bytes
(`kdb-compression/src/jsMain/.../ZstdCompression.js.kt:3-31`,
`nativeMain/.../ZstdCompression.native.kt:4-24`), but the SSTable writer always tags the block
`BLOCK_CODEC_ZSTD` (`SsTableCodec.kt:38`, with `compress = true` hard-coded at `:177`; the default is
ZSTD at `StorageEngineConfig.kt:47`). The delta page writer has the same shape
(`DefaultDeltaSegmentWriter.kt:105-153`). A file written by JS or Native claims to be zstd, and
Go/JVM will fail to decode it. Reading JVM/Go files on JS/Native fails too, since they really are zstd.

**Fix (the cheap, correct half).** The v2 block header already has an explicit codec byte, and both
readers handle `CODEC_NONE` (`go/kdb/storage/sstable/codec.go:41,90`, `SsTableCodec.kt:71`). So:
- Expose whether the platform's zstd is real (e.g. `ZstdCompression.isAvailable`, an `expect val`).
- Have both writers (SSTable and delta page) emit `CODEC_NONE` when it isn't.
- Make `ZstdCompression.decompress` on those targets throw a typed "zstd not supported on this
  platform" error instead of misreading bytes.

**Tests.**
- `commonTest`: a block written on each target is readable on the same target.
- A `jsTest`/`nativeTest` assertion that the codec byte is NONE.
- A physical golden: a CODEC_NONE SSTable and delta segment, read by Go. Add them to
  `go/testdata/golden/physical/kotlin/` via the existing exporter, and have `golden-freshness` cover them.

**Out of scope here:** real zstd on JS/Native (see [R1](#roadmap)). After U5, JS/Native can write
readable data but still can't read JVM/Go-written compressed data. Say so in the PR.

### U6. Issue #43 — acknowledged write unreadable after `kill -9` — L, in four steps

**Status: U6a done** (#112, which also added the multi-node suite); **U6c done** (#121; see the
progress log, which also records a third silent path it found); **U6b done** (#122: the server-path reproducer, plus a nightly Linux soak); **U6d: no loss reproduced** in 620 kills across five configurations (110,950 acknowledged writes). #43 stays open until the nightly Linux soak has run (see the progress log).

The durability contract is the most important open item. It is sequenced so each step is its own PR
and each makes the next one possible. **Do not relax or retry the test** at any step.

**What is already known** (issue #43 plus a fresh code read):
- Ruled out: premature ack (`embed/commit_log.go:143-165`, `:218-233`), torn-tail prefix loss, and
  checkpoint + unseen segment (PR #45).
- Also ruled out: Linux vs macOS fsync (`kill -9` keeps the page cache, and writes are `pwrite` with
  no user-space buffer, `storage/io/os_store.go:146-169`); the checkpoint and #41 floor (the test's
  first open finds no segments, so `saveCheckpoint` is a no-op at `through=-1`, and the maintenance
  timer defaults to 5 min); the cross-namespace group gate (client commits carry no marker).
- The test can't currently tell a lost document from a read error or an empty namespace.
  `get()` returns `None` on any non-zero helper exit and discards stderr
  (`kdb-integration/e2e/server_fixtures.py:318-324`). Doc `…01` is always checked first.
  `_launch_once` reopens `service.log` with `"w"` (`:183`, `:200`), so the killed run's log is
  erased on restart.

**U6a — make the failure diagnosable (test-only PR).**
- `get()` distinguishes "not found" from "helper failed", and includes stderr in the assertion.
- Check **all** acknowledged docs before asserting, then report which are missing, the restarted
  node's head hash, and the commit count. One missing doc and all missing are different bugs.
- Keep each launch's `service.log` (`service.<n>.log`) and upload the e2e directory as a CI artifact
  on failure. Also save the data dir, and `kdb-inspect` output for it.
- Change nothing in the product. Merge, then let CI collect failures.

**U6b — a deterministic-enough server-path reproducer (test PR).** PR #45's tests go through
`embed.PutJSONDocument` in-process and single-threaded. They skip the server path where the gap is:
the write gate's early release, the transaction engine, the wire layer, and a commit in flight at the
moment of the kill.
- A Go test (under `go/kdb/integration`, behind a build tag or `-short` skip if slow) that starts
  `kdb-service` as a subprocess, writes concurrently over the wire, SIGKILLs at a random moment,
  restarts, and checks every acknowledged write.
- Loop it N times with `GOMAXPROCS=2`, the CI shape.
- Record the iteration count that reproduces. If none does locally, run it in CI via
  `workflow_dispatch` before guessing at a cause.

**U6c — close the silent paths regardless (product PR).** Two places in recovery drop data without
a word. Each is a defect whether or not it explains #43:
1. `ListSegments` skips a segment whose `scanSegmentRef` fails (`go/kdb/storage/delta/writer.go:323-325`),
   `continue`-ing past every commit in it, while `kdb-inspect verify` can still pass.
   Surface it as a typed error, the way the legacy-name case two lines above does.
2. `applyReplayedCommit` stores and does not apply a commit whose parents don't include the head
   (`go/kdb/embed/delta_replay.go:352-362`). For peer side-branches that is intended. For a
   single-writer log it should never happen, so count it and log it at open (namespace, commit,
   head). That also makes it visible in U6a's artifacts.

Each needs a test that corrupts or reorders a segment and asserts the open reports it.

**U6d — root cause and fix.** Remaining candidates, most plausible first. Confirm with U6a/U6b
evidence before fixing any of them:
1. **Tree-hash mismatch after replay.** `GetDocument` resolves through `commit.DocumentTreeHash`
   (`server/server_runtime.go:991`). A tree the replay didn't rebuild falls through to tree objects,
   which live only in the memtable and are lost on kill, so every read returns nil. A specific
   risk: `commitTreeLocked` takes *all* pending documents namespace-wide
   (`storage/engine/server_engine.go:695`), so documents staged by a failed or in-flight transaction
   could land in the next commit's tree and make the logged tree hash unreproducible on replay.
2. The two silent paths closed in U6c.
3. Discarded `DiscoverTables` errors (`server_engine.go:282`); only matters combined with 1.

The fix lands with the U6b reproducer as its regression test.

**Done when** U6b passes a large iteration count at `GOMAXPROCS=2` locally and in CI, and the e2e test
has run clean in CI for a stretch (say 30 consecutive runs). Then close #43 with the root cause in
the closing comment.

### U7. Kotlin transport shutdown leaks exceptions into later tests — M

**Status: done** (#110).

**Problem.** None of the four "flaky Kotlin tests" failed in the last 45 failed CI runs. Two were
already fixed (`4fa8665`, `52f99fa`). The other two (`IndexDurabilityIntegrationTest`,
`MultiClientSqlIntegrationTest`) fail with `UncaughtExceptionsBeforeTest`: another test in the same
JVM (`kdb-integration` runs `maxParallelForks = 1`) leaks an exception. The leaks are **product**
defects:
- `TcpLoopbackServer`'s per-connection handler has `try/finally` and no `catch`
  (`kdb-transport-tcp/src/jvmMain/.../JvmTcpWireTransport.kt:185-191`, same in `launchTcpHandler`
  `:59-72`). A client disconnect surfaces as `ConnectionClosedException` escaping a `SupervisorJob`
  with no handler. Main suspect: `FullStackIntegrationTest.kt:85-106`.
- `JvmNetworkWebSocketServer`'s accept loop (`:65-67`) has no catch, and `stop()` (`:92-97`) cancels
  and then closes the socket, so `accept()` throws `SocketException` on **every** stop. The TCP
  server already handles this (`:177-181`).
- `stop()` cancels without joining. It should `cancelAndJoin`.

**Fix.**
- Catch the expected close exceptions in both handlers and the WS accept loop.
- Make `stop()` suspend until its jobs finish.
- Move the ad-hoc `CoroutineScope(...)` / `cancel()` test helpers onto the class `testScope`:
  `WebSocketPeerSyncIntegrationTest.kt:402-419`, `WebSocketStreamIntegrationTest.kt:178-203`,
  `WebSocketBranchIntegrationTest.kt:59,124-127`, `WebSocketStreamFallbackIntegrationTest.kt:57,93-96`.
- Wrap `TcpWireTransportTest.listenAccept_twoClients` in `withTimeout`, so a lost frame fails instead
  of hanging.

**Test.** A unit test that stops each server with a connected client and asserts nothing reaches a
`CoroutineExceptionHandler`. Run `:kdb-integration:test` with `-XX:ActiveProcessorCount=2` repeatedly;
the memory note `kdb-ci-is-intermittently-red-on-main` has the init-script recipe.

### U8. Go `json` getters panic instead of returning their error — S

**Status: done** (#107).

`go/kdb/json/api.go:245-289`: `navigateGet`, behind `Get`/`GetAll`/`Contains`, panics with
`JsonPathError` on a type mismatch, though all three return `error`. There are no production callers
today, and the wire path recovers anyway (`wire_listen.go:304`), which is why this is small. But it's
a trap for the first caller. Convert the panic to a returned error, and add a table test of mismatched paths.

---

## Phase C — Cleanup and scope decisions

These need a decision from you. Each has a recommendation.

### U9. Remove dead stubs — S each

**Status: done** (#118): both stubs deleted; neither was ever released or imported.

- **Kotlin `kdb-storage-compaction`.** `plan()` returns `emptyList()` and every runner throws
  `CompactionNotImplementedException` (`Compaction.kt:31,45-76`). No source anywhere references it;
  `kdb-storage-manager/build.gradle.kts:10` declares the dependency and never uses it. (Not to be
  confused with `kdb-compaction`, which is real and used.)
  **Recommend deleting the module**, since its whole contract is "throws". The spec
  (`kdb-spec-layer4a-component10f-storage-compaction.md`) keeps the design.
- **Go `go/kdb/tier`.** 42 lines; `ArchiveCommit` fires a callback and stores nothing. No non-test
  importer, untouched since `0a98b66`. **Recommend deleting it.** Tiered storage is roadmap item
  [R6](#roadmap) and should start from the spec, not this stub.

### U10. `kdb branch checkout` does nothing — S

**Status: done** (this unit's PR): refused at parse time with an explanation; the dead command is removed.

`go/cmd/kdb/cli/commands.go:578-592` calls `SetHead(name, b.HeadHash)`, which sets a branch to its own
head. The CLI has no persisted "current branch". **Recommend** making it fail clearly ("checkout is not
supported: the CLI has no current branch; pass `--branch` per command") rather than inventing a
`.kdb-cli/HEAD` file nobody has asked for. If you do want git-style checkout, it's its own small design.

### U11. Kotlin WAL ignores the configured segment size — S

**Status: done** (branch `fix/kotlin-wal-segment-size`).

`kdb-storage-engine/.../ServerStorageEngine.kt:279` builds `DefaultWriteAheadLogFactory()` with no
arguments, so the rotation cap is always the 64 MiB default. Thread `walMaxSegmentBytes` from the
engine config, with a test that a small cap rotates. (Rotation itself is done; see below.)

### U12. In-memory stream transport drops frames — S, low priority

**Status: done** (#120): both halves fixed, not just `emit`; see the progress log.

`go/kdb/stream/transport.go:129-139` (`deliver`, `memory://` hub only) and `subscriber.go:335-340`
(`emit`, including the desync `EventError` at `:259`) drop on a full buffer. `NewSubscriber` has no
non-test caller, so no production traffic is affected. **Recommend:** make `emit` close the
subscription on overflow (a dropped desync error is the dangerous case), and leave `deliver`. Do
this only if the Go subscriber gets a real caller. Otherwise note it and move on.

---

## Roadmap

Spec work too big for a unit. Each needs its own plan doc before any code, like the history-modes
and commit-graph plans. These are listed so they're tracked, not scheduled.

| # | Item | State at `e125a10` | First step |
|---|---|---|---|
| R1 | Real zstd on Kotlin JS/Native | **Done** 2026-10-11 (#116, branch `feat/zstd-js-native`): pure-Kotlin decoder and encoder in `kdb-compression` commonMain, both directions on both targets — see the progress log | — |
| R2 | Go LSM leveled compaction | `LsmBlobStore.Compact` always merges to one table (`sstable/types.go:235-294`) | Measure write amplification at realistic sizes first |
| R3 | Commit graph on disk | Phase 1 (generation numbers) + settings landed, off by default; `KDB_GRAPH_FILE` is accepted and ignored (`config/descriptors.go:625-627`) | **Phase 0 measurement** at 100k/500k/2M commits; it gates the rest (`docs/kdb-commit-graph-on-disk.md`). Also: stop accepting the ignored flag, or warn. |
| R4 | Kotlin history-mode parity | wire messages 0x1F-0x22 are Go-only; Kotlin NONE is validator-only | Decide whether Kotlin needs NONE at all (it's the embedded/reference tree) |
| R5 | Resource governance (Layer 13) | comps 48, 52 missing; 49 partial | `docs/kdb-finish-up-plan.md` 4.A |
| R6 | Tiered storage | Go stub (U9 removes it); Kotlin `kdb-storage-tier` has real code | Spec layer 7 comp 20 |
| R7 | Encryption at rest (Layer 14) | not started | 4.B; the toggle and whole-store/per-collection scope are already decided |
| R8 | Go fulltext/vector indexes, HNSW | 4.F | — |
| R9 | Durable RBAC store in Go | `kdb-rbac-plan.md` says "not yet" | — |
| R10 | Browser persistence (IndexedDB/OPFS), WebGPU | 4.G | — |

---

## Already resolved

Listed so nobody re-opens them. All were checked at `e125a10`.

- **Cross-language SSTable fixture test:** exists. `go/kdb/interop/sstable_physical_golden_test.go` ↔
  `kdb-storage-sstable/src/jvmTest/.../SsTablePhysicalGoldenTest.kt`, enforced by the CI
  `golden-freshness` job. WAL and delta segments have the same pairing.
- **Explicit compression flag in the block header:** landed in `456c673` (v2 16-byte header, both
  languages).
- **Kotlin WAL rotation and multi-segment reopen order:** landed in `01d0654`, `adfb98b`.
- **Go history-mode maintenance timer:** `embed/maintenance_loop.go`, started per namespace in
  `service/service.go:741-758` (`--maintenance-interval`, default 5m).
- **Bounded history trees:** `treesByHash` is a byte-budgeted LRU (`storage/engine/memory_budget.go`).
- **`TcpWireTransportTest.listenAccept_twoClients`:** fixed in `4fa8665` (U7 still adds a timeout).
- **`wsPeerSyncBidirectionalGenuinelyConcurrent`:** a real race, fixed in `52f99fa`.
- **`TestKotlinReadsGoConflictResolution` failing in `go-test`:** read Gradle's wrapper-download
  output as the value; fixed in `ce9b172`, with no failure on `main` since.
- **The long red streak on `main` around 2026-09-22:** gomobile `gobind` missing, fixed by #96.

## Progress log

_(append one entry per unit: date, unit, PR, commit, and anything found along the way)_

- **2026-10-11, U8** (branch `fix/json-getters-return-errors`): `navigateGet`, `collectAll` and
  `normalizeIndex` in `go/kdb/json/api.go` now return `*JsonPathError` instead of panicking, so
  `Get`/`GetAll`/`Contains`/`Keys`/`TypeName`/`ArrayLength`/`Set`/`Delete` all report a mismatched
  path as an error. Found along the way: the panic reached further than the plan said. `collectAll`
  (wildcard `GetAll`) and `normalizeIndex` (`[-2]` and below, reached from `Set` and `Delete` too)
  panicked the same way, and `Set` with `[-1]` on an empty array indexed `list[-1]`, a runtime
  panic. `Set` is the one production caller (`sql/dml.go`, SQL `UPDATE ... SET` on a JSON path), so
  that path was reachable from a SQL statement; it now gets an error. Table test
  `go/kdb/json/getter_errors_test.go` fails with panics on `main`.
### 2026-10-11 — U2: `kdb/server` in the embeddable Go bundle

PR #108 (branch `fix/embedbundle-kdb-server`, commit `c043449`), supersedes #26 (left open for the user to close).
`kdb/server` added to `go/embedbundle/entrypoints.txt`; #26's EMBEDDING.md and README text carried
over; `docs/kdb-release-plan.md` §2.1, §2.2, §2.3, §2.5 and §7.1 rewritten from `main`'s version;
`Makefile` comment updated; new `go/embedbundle/main_test.go` asserts every entry point is in the
computed closure and no file in any closure package imports `"C"` (counts deliberately not
asserted). Probed it: a temporary `import "C"` file in `kdb/server` fails the test.

Measured on this branch (regenerated, not copied from the plan):
- Closure **27 → 40 packages**; the 13 added are `server`, `wire`, `transport/{core,tcp,ws}`,
  `peersync`, `stream`, `version`, `script`, `index/{analyzer,fulltext,stores,vector}`.
- Bundle **426 → 665 files** (bundle.json manifest), 2.45 → 4.58 MB unpacked,
  **913,545 → 1,655,615 bytes zipped** (~914 KB → ~1.66 MB). The plan's ~907 KB / ~1.64 MB were
  slightly stale; `main`'s release plan still said 351 files / ~708 KB, older still.
- Skipped test files: `kdb/embed/database_backup_test.go`, `kdb/server/backup_txn_test.go`,
  `kdb/server/panic_recovery_test.go`; `kdb/transaction/procedure_resolver_test.go` now ships
  (its `kdb/script` import is in the closure).
- New bundle requires: `dop251/goja` (from `kdb/script`) and `golang.org/x/sync` (from
  `kdb/server`), plus indirects `dlclark/regexp2/v2`, `go-sourcemap/sourcemap`, `google/pprof`,
  `x/text` — 14 → 18 indirects. All pure Go.

Gates: `scripts/verify-bundle.sh` on the 40-package bundle passes (build, vet, `go test` all 40
ok, cross-compile linux/amd64, linux/arm64, darwin/arm64); the bundle also builds for
windows/amd64 and js/wasm; `CGO_ENABLED=0 go build ./kdb/server` passes on all five platforms;
two runs into different output directories produce a byte-identical zip; `go vet ./...` and
`go test -race ./embedbundle/...` pass.

Found along the way: `main`'s §2.2 said "27 of the module's 37 top-level packages" and its
excluded list named packages that no longer match the tree; the module has 59 `kdb/...` packages,
and the excluded list now names the real 19 (incl. `replication`, `syncnode`, `storage/manager`).
No convention existed for notes between releases, so this adds `docs/releases/unreleased.md`
(renamed at the `VERSION` bump) and documents it in the release plan §4. The v0.6.2 known issue is
recorded there as fixed; `docs/releases/v0.6.2.md` is left as shipped, matching how v0.6.2's own
notes close v0.6.1's known issue.
- **2026-10-11, U5** (branch `fix/js-native-codec-none`): `ZstdCompression.isAvailable` (an
  `expect val`, true only on the JVM). On JS/Native, `compress`/`decompress` now throw
  `ZstdUnavailableException` instead of the length-prefix stub. `SsTableCodec.encodeBlock` and
  `DeltaPageCodec.frame` write `CODEC_NONE` when zstd is not available, and the sealed
  `DeltaSegmentRef` reports the codec actually used. `DefaultSsTableWriter` takes an optional
  `codec` (default ZSTD) so the JVM can export the NONE shape. The ice-bundle writer
  (`kdb-storage-tier`) stores plain JSON there; its reader already falls back to raw bytes.
  New physical goldens `sstable_segment_none.hex` and `delta_segment_none.hex` are read by Go.
  The delta one is compared byte for byte.
  `golden-freshness` and `make test-physical-golden` now also fail on *untracked* fixtures:
  `git diff` ignored them, so a new exporter file nobody committed would have passed. The
  js/native/common codec tests failed (codec byte 1, want 0) with the old stub and writers put
  back. Found along the way, existing data: JS persistence is in memory
  (`BrowserSegmentByteStore`), so none survives. **Native does persist to real files**
  (`NativeSegmentByteStore`). A Native data directory written before U5 holds ZSTD-tagged
  length-prefixed blocks, and Native now refuses those with `ZstdUnavailableException` instead
  of reading them back. No Native or JS artifact has ever been released (releases ship Go
  binaries and JVM jars only), and no fixture or test in the repo holds JS/Native-written files.
  Still open: JS/Native cannot read JVM/Go zstd data (R1).
### 2026-10-11 — U7: Kotlin transport shutdown leaks (branch `fix/kotlin-transport-shutdown`)

- **Repro first.** `TcpLoopbackServerShutdownTest` (2) and `NetworkWebSocketServerShutdownTest` (3)
  install a default `Thread.UncaughtExceptionHandler` and stop each server with a connected client.
  On `main` all five fail: TCP leaks `ConnectionClosedException` when a client disconnects, the WS
  server leaks `SocketException: Socket closed` on **every** `stop()` (idle or not), and both
  `stop()`s return before their handlers finish.
- **Fix.** `TcpLoopbackServer` and `JvmNetworkWebSocketServer`: the per-connection handler catches
  `ConnectionClosedException` (TCP also an `IOException`, but only once `stop()` has begun); the
  accept loops treat `SocketException` as the normal exit only when the server is stopping or the
  socket is closed (the WS loop now rethrows anything else). `stop()` cancels, closes the server
  socket, closes the sockets of handlers still running, and joins — inside
  `withContext(NonCancellable + Dispatchers.IO)`. `launchTcpHandler` and the WS `listen` wrapper also
  catch `ConnectionClosedException`. `TcpLoopbackServer(scope = …)` now runs under a child job, so
  `stop()` no longer cancels a caller-supplied scope. `CancellationException` is never caught.
- **Found along the way.**
  - The old `stop()` did `withContext(Dispatchers.IO) { serverSocket.close() }`. Called from a
    cancelled coroutine — which is exactly what `JvmWebSocketWireTransport.listen`'s
    `finally { server.stop() }` does after `awaitCancellation()` — that `withContext` throws
    immediately, so cancelling a WS `listen` never closed its listening socket.
  - Joining in `stop()` is only safe if handlers blocked in socket reads are unblocked: a client
    that connects and never sends its HTTP upgrade request would otherwise hang `stop()` forever
    (cancellation does not interrupt a blocking read). Hence closing in-flight sockets;
    `stopDuringHandshake_doesNotHang` fails (10 s timeout) if that sweep is removed.
  - Not fixed (out of scope): `JvmTcpWireTransport.listen` blocks in `accept()` and cannot be
    cancelled until a connection arrives, and its handlers (one fresh scope each) and the WS
    `listen` handlers (`listenScope`) are not children of the `listen` call, so cancelling `listen`
    leaves running connections behind.
- **Tests.** Ad-hoc `CoroutineScope(...)`/`cancel()` in the four WebSocket integration tests moved to
  the class `testScope` (joined in `@AfterTest`); `listenAccept_twoClients` bounded with
  `withTimeout`.
- **Evidence.** `:kdb-integration:test` with `-XX:ActiveProcessorCount=2` (init script,
  `--rerun-tasks`), load average 30–50: 9/9 runs green (40 tests) with the fix. The same loop on
  `main`'s sources was also 6/6 green, with no `Exception in thread` in captured stderr — the
  integration-level flake does not reproduce locally, so the unit tests above are the before/after
  evidence. `:kdb-transport-tcp:jvmTest`, `:kdb-transport-ws:jvmTest`, `:kdb-jdbc:test` green.
### 2026-10-11 — N1 (new, found by multi-node e2e): snapshot-bootstrapped node stuck on "missing parent" — PR #111, branch `fix/shallow-node-missing-parent`, commit `0641f37`

**Found by** the multi-node e2e `test_cluster.py::test_late_joiner_bootstraps_from_a_snapshot_while_the_cluster_writes`
(sibling worktree `e2e-multi`, not on `main` yet), 3/3: a node joining a busy three-writer mesh
with `bootstrap=snapshot` failed every replication cycle, for ever, with
`peer sync: storing commit C: missing parent P`. `consecutiveFailures` grew without bound.

**Root cause.** The joiner's snapshot root S was a merge with parents [Q, P]. Another node went on
committing on P (C, parent P) before it saw S. The sender computes a pack as "reachable from my head
minus the ancestor closure of the receiver's haves" (`peersync/refs.go` `MissingCommitsFrom`,
`have := d.AncestorSetOf(haves)`); P is an ancestor of S, so it was treated as held and never sent.
A shallow node holds nothing behind S, so `StoreCommits` (`peersync/ingest.go`) refused C. The
existing single-source test never forks behind the boundary, and `TestSnapshotReplicaFollowsMainDespiteBranchBelowRoot`
had enshrined the side-branch half of this as unfixable ("its parents can never arrive").

**Fix (no wire change, Go only - Kotlin has no v2 sync).**
- Receiver (`peersync/boundary.go`, called from `fetchRef`): a page commit whose parent is neither in
  the page nor held, on a node with shallow roots, and not itself one of the peer's shallow roots
  (that case stays Graft's), has its parent's *state* taken in by `SNAPSHOT_FETCH` at the parent and
  admitted as a shallow root through Graft's durable path (`admitRootFromState`: foreign tree, marker
  `graftRoots` before the log frame). No `allowUnrelated` needed: the parent is history S already
  descends from. One state transfer per fork point; no history is fetched.
- Host (`peersync/snapshot_state.go`): a node never builds a tree for a commit it merged rather than
  fast-forwarded onto, so `SNAPSHOT_FETCH` at such a commit failed "no tree". It now folds the commit's
  first-parent chain forward from the nearest tree it holds, checks the result against the declared
  tree hash, and serves that (cached for the snapshot's later pages). A pre-fix host still answers
  "no tree", which the joiner reports as below.
- Adoption (`peersync/ingest.go` `adoptable`): shallow roots are dropped from the commits a
  fast-forward or merge adopts - they were logged on their own, and their operations are a change
  from parents this node lacks; re-logging them, fence-checking them or announcing them to commit
  listeners was wrong (by inspection the same already happened to graft roots).
- Surfacing: when the parent's state cannot be had, the pull fails with a typed
  `peersync.ShallowBoundaryError` naming both commits and pointing at deepen; that text is what
  `/v1/peers` `lastError` and the namespace's `lastError` show. Successful extensions are in
  `NamespaceSyncResult.BoundaryExtended` and logged by the replicator. An automatic *full* deepen
  fallback was rejected: it defeats snapshot bootstrap, and FETCH_REQUEST has no depth to bound it.

**Tests.** `server/shallow_boundary_test.go` (file-backed joiner, real `PutJSON` writes, memory and
file-backed source): fast-forward across the fork point, a real merge on the joiner that traces a
value down to the fork point's tree, clean restart, crash replay onto the bootstrap checkpoint, and
deepen-after-extension with a no-checkpoint replay. `peersync/boundary_test.go`: the fork point is
logged once, not re-adopted; a failure is a `ShallowBoundaryError` and leaves nothing resident.
Revert-confirmed: without the receiver change the server tests fail with the original
"missing parent"; without the host fold they fail "no tree"; without `adoptable` the fork point is
logged twice. The e2e late-joiner test passes 3/3 with `--runxfail` (both parametrizations).

**Not fixed, found along the way.**
- The push direction (by inspection, not reproduced): a node that dials a shallow host and pushes it a branch forked behind the
  host's root still gets "missing parent" from the host, which cannot fetch from its client. It
  heals once the shallow node pulls from anyone; a shallow node that is only ever dialled stays
  stuck on that ref. The pusher has the host's advertised horizon and could send the fork point's
  state first (as `graftRootsFor` does for its own roots); not done here.
- A `history=none` (replay strategy) receiver cannot hold the fork point's tree
  (`ErrForeignTreeNeedsObjects`), so it gets the typed error and has to deepen.
- 2026-10-11 — Scan cap (silent truncation at `MaxRows` before `WHERE`): found by qtask, Go fix on
  `fix/scan-cap-before-filter`. Follow-on units live in `docs/kdb-scan-cap-plan.md`.
- **2026-10-11 — multi-node e2e harness, and U6a.** New `kdb-integration/e2e/test_cluster.py` +
  `cluster_fixtures.py`: real kdb-service clusters (mesh, chain, star; 2-5 nodes) replicating with
  `--peer`, concurrent writers on every node, `kill -9` of one node, two at once and the whole
  cluster, clean drains, partitions (a node down while others write), late snapshot joiners,
  SQL schema/UPDATE/DELETE replication, last-write and strict conflicts (queued, then resolved
  through the control plane). Every scenario ends on a strict convergence check: the same head
  commit on every node, identical documents, no open conflicts. Fixtures: ports pinned across
  restarts, the control plane on demand, one log per launch (a restart no longer erases the
  killed run's log), and evidence (data dirs, logs, `kdb-inspect verify`) preserved to
  `e2e/evidence/` and uploaded by CI on failure. U6a: `test_kill9_mid_load...` checks every
  acknowledged document before failing and reports missing / rolled back / unreadable
  separately with the head each read was served at; `kdb-e2e-helper get` now exits 0 with an
  empty body for not-found, so "absent" and "read failed" are distinguishable. A six-pass soak
  (three with `GOMAXPROCS=2`) found three defects, each with its own PR:
  **N1** snapshot joiner stuck on a branch forked behind its root (#111);
  **N2** five-node mesh merge storm, 10k+ empty merges after writes stopped, never converging
  (`fix/mesh-merge-convergence`); **N3** a drain mid-replication checkpoints a stored-but-unlogged
  peer commit, after which the node fails every sync with "no frame in the delta log", 4 of 8
  runs (`fix/checkpoint-unlogged-peer-commits`). The three tests that hit them carry non-strict
  xfail marks to remove as the fixes land.
- **2026-10-11 — new item, found by multi-node e2e testing: a drained node checkpointed peer
  commits it never logged** (branch `fix/checkpoint-unlogged-peer-commits`, PR #113, commit 70424df). SIGTERM a
  node of a 3-node mesh under concurrent writes and, in 5 of 20 runs here, the restarted node failed
  every sync forever with `commit ... has no frame in the delta log`. A push that arrived as the
  drain began was stored (`peersync.StoreCommits`, outside the write gate) and then refused by
  `Adopt` (`serverLocalNode.Exclusive` refuses while draining), so it was never logged; the clean
  close checkpointed the whole in-memory DAG, that commit included, with no operations; the
  reopened DAG held it, so every sync skipped the peer's copy and then failed to log it on
  adoption. Not drain-specific: any stored-but-unadopted commit (conflict side, unfinished
  multi-page transfer) reached a checkpoint the same way. Fixed by marking peer-stored commits
  unlogged until adoption logs them (`dag/unlogged.go`): checkpoints leave them out (and any
  non-main ref naming one), the operations budget does not evict them, and a resident commit whose
  operations the log cannot supply is refilled from a peer's copy that reproduces its hash, which
  heals data directories v0.6.2 already wrote (verified on the preserved e2e evidence). The
  peer-sync listener's `Close` now ends inbound sessions and waits for them, and the service closes
  it before draining. 0 of 20 runs broken after. **Found along the way:** the ops loader indexed the
  delta log once, on its first cold load, and never again, so any commit logged after that and then
  evicted reported the same "no frame" error with its frame on disk; it now re-indexes the newest
  and unseen segments on a miss. **Not fixed here, same family:** a failed `Deepen` and a failed
  merge-commit persist still leave unlogged commits a checkpoint will describe; the HTTP/websocket
  peer endpoint (`syncnode.Handler`) is not drained before storage closes; Kotlin peer sync was not
  checked for the same checkpoint behaviour.

### 2026-10-11 — new item, found by multi-node e2e testing: mesh merge storm (branch `fix/mesh-merge-convergence`)

**Symptom.** In a five-node `kdb-service` full mesh with concurrent writes and kill -9/restart (`test_cluster.py::test_rolling_kill9_soak`), the mesh failed to converge 2 of 2 times under load avg 20-45. For 150s+ after writes stopped, every head differed and kept changing, with every sync succeeding and the same 40 documents on every node. One node held 10,677 empty `kdb:merge/1` commits.

**Cause.** Merges were deterministic per pair but not confluent. Fixed with canonical joins of same-content heads; see "Merge convergence in a mesh" in `docs/kdb-distributed-implementation-plan.md`.

**Tests.** Failing tests first, each confirmed against the reverted fix:
- `server/mesh_convergence_test.go`: the ring, the property and the restart tests
- `syncnode/mesh_test.go`
- `peersync/join_test.go`
- `integration/join_interop_test.go` (Kotlin)

**Evidence.** The e2e soak is 1 of 2 non-converging (XFAIL after 174s) with `origin/main` binaries, and 5 of 5 converged (~25s per run, about 2-3s of it after writes stop) with this branch, both under load avg 18-28.

- **2026-10-11 — U10, U4 and the local half of U3.** U10: `kdb branch checkout` is refused while
  parsing (exit 2, nothing opened) with an explanation, instead of setting a branch to its own head
  and printing "checked out"; `TestBranchCheckoutIsRefused` fails on main. U4: the finish-up plan's
  stale "not done" entries for the explicit compression flag (`456c673`) and the cross-language
  SSTable fixture (`adfb98b`) carry dated notes, and every unit above now opens with its status.
  U3 local: 97 merged local branches deleted and finished worktrees removed. The origin half (44
  merged or superseded branches, and closing #26) was refused to automation and is left to the
  owner; every tip is saved in `kdb-deleted-branches-2026-10-11.bundle` beside the checkout.
- **2026-10-11 — U11.** `StorageEngineConfig.walMaxSegmentBytes` (kdb-storage) is new: the
  Kotlin twin of Go's `storage.StorageEngineConfig.WalMaxSegmentBytes`, same name and meaning,
  default 64 MiB. It is validated (zero or negative throws) where Go treats zero as "use the
  default", since the Kotlin field has a real default. `DefaultWriteAheadLogFactory` now resolves
  the cap as Go's `wal.DefaultFactory` does: an explicit factory override
  (`walMaxSegmentBytesOverride`), otherwise the config's. The engine's no-argument factory
  therefore picks up the config, which it previously never consulted. Not added: a knob on
  `openFileRuntime` (kdb-jdbc) or any runtime setting descriptor. Go has neither for the WAL cap
  (only `DeltaMaxSegmentBytes` is exposed through `embed`), and no Kotlin caller exposes any other
  `StorageEngineConfig` field either. Tests: `WalSegmentSizeTest` (kdb-storage-engine) opens a
  SERVER engine with a 512-byte cap, writes ~5 KiB, and asserts several WAL segments, that a
  reopened engine recovers every blob, and that the replay is in write order. It fails on `main`
  (one segment) and was confirmed failing against the reverted resolution. It also covers the
  64 MiB default and rejecting 0/-1. `WalTest` adds the factory's override-vs-config order.
- **2026-10-11, U9** (branch `chore/remove-dead-stubs`): deleted the Kotlin stub module
  `kdb-storage-compaction` (its `settings.gradle.kts` include and the unused dependency in
  `kdb-storage-manager/build.gradle.kts` were the only references) and the Go stub package
  `go/kdb/tier` (no importer, test or otherwise; it had no tests). No CI workflow, Makefile target or
  release script named either. The module never produced a release asset (checked `v0.4.0` and
  `v0.6.2`: neither lists a `kdb-storage-compaction` jar), so the 50-jar count is unchanged. Docs
  that listed them as implemented now say 10f is not implemented (`kdb-spec.md`,
  `kdb-lld-storage.md`, `kdb-spec-layer6-execution-plan.md`); the 10f spec keeps its design with a
  removal note; `kdb-release-plan.md` §2.2 drops `kdb/tier` from the excluded list and the module's
  package count goes 59 to 58. Historical entries in `kdb-finish-up-plan.md` were left as written.
- **2026-10-11 — U12: the in-memory stream transport and the Go subscriber stop dropping.**
  Done although the plan said to wait for a real caller, and `deliver` was fixed too, not left.
  Frames: `clientLink.deliver` (`memory://` hub) now blocks until the client reads or closes,
  instead of dropping once its 64-slot buffer is full. That is backpressure on `ServerSend`, which
  is how a push transport with no DAG to catch up from keeps the documented contract ("No dropped
  frames. A slow subscriber is caught up when it drains"); disconnecting a slow client would break
  the second half of it. Close-safety follows 1-G12/1-G13: `Close` sets `closed` and closes a `done`
  channel under the mutex, releases it, waits for in-flight delivers (which `done` has woken), and
  only then closes `incoming`. The mutex is never held while a deliver waits. Events: `emit` keeps
  the last slot of the 32-slot buffer for one terminal `EventDisconnected` per subscription. Any
  other event that finds the rest full closes the subscription and takes that slot as
  `EventDisconnected{Cause: *EventOverflowError}`, which wraps the event that did not fit;
  `errors.As` finds a `*DesyncError` through it. So the reader never blocks, a stalled consumer
  can't stall the hub for long, and a desync is never lost. `Connect` refuses while the buffer is
  completely full. Tests (`go/kdb/stream/backpressure_test.go`), each failing on main and again
  against the reverted fix: a 500-frame burst arrives whole and in order (main: 64 of 500); sends
  past a full buffer wait, and `Close` during them neither deadlocks nor panics under `-race`
  (main: the sends returned at once); a desync after 32 or 40 undrained notices is reported or ends
  the subscription with a terminal event (main: silently gone). **Found along the way:** a second
  `Disconnect` closed `stopIncoming` twice (panic), and `readLoop` read `s.stopIncoming` without the
  lock; both fixed. Still open, same family: `Coordinator.updateSubscriber` drops `SubscriberState`
  updates on a full 16-slot channel, and a closed `clientLink` is never removed from its hub.

- **2026-10-11 — U6c: recovery's two silent paths (branch `fix/recovery-silent-skips`).**
  1. `delta.DefaultReader.ListSegments` (`storage/delta/writer.go`) `continue`d past any segment
     `scanSegmentRef` could not scan. That happens only for an I/O error or a frame that passes its
     body CRC and still will not decode (a header field such as the codec byte at offset 5 is
     outside the CRC). A torn write never gets there: a short frame or a CRC mismatch stops the
     scan cleanly and still lists. It is now a typed `*delta.UnreadableSegmentError` (namespace,
     sequence, file, cause), newest segment or not. The one tolerated case is a segment that is no
     longer listed when re-checked, which is truncation deleting it between the listing and the
     read. Callers: replay, checkpoint matching, the ops loader, the cold loader, history, truncate
     and restore-archive all propagate it or already treated a listing error as "don't trust".
     `kdb-inspect verify` and backup read segments themselves and are unchanged.
     **Before**, codec byte damaged in the first frame of the newest non-empty segment: open
     succeeded with main 3 commits behind the last acknowledged one, with or without a checkpoint.
     In a non-newest segment it failed with the misleading "parent commits never found in the log".
     **After**, both fail with `UnreadableSegmentError` naming the segment. A torn newest segment
     still opens at its last intact commit.
  2. Replay (`embed/delta_replay.go` `applyReplayedCommit`) stores a logged commit that neither
     extends main's head nor supersedes it, and does not apply it. Which commits are applied is
     unchanged. Open now collects them and, after the whole log is read, splits them by whether
     main reaches them. Those main reaches are a peer's commits a logged merge adopted, which is
     expected, so they are counted but not logged. Those it does not reach get one `slog.Warn` each
     (`namespace`, `commit`, `head` it failed to extend, `mainHead`), capped at 20 lines plus a
     count. A single-writer log has neither. The counts are in `embed.RecoveryReport`
     (`rt.Recovery()`: `replayedInFull`, `commitsNotApplied`, `commitsOffMain`) and in
     `GET /v1/ns/{ns}/status` as `recovery`, so a crash test's service log and status both show it.
  **Tests** (each confirmed failing with its fix reverted): `storage/delta/unreadable_segment_test.go`,
  `embed/unreadable_segment_test.go` (file-backed: non-newest and newest, with and without a
  checkpoint, plus a torn newest segment), `server/replay_unapplied_test.go` (an unmerged peer commit
  counted and logged at WARN, a merged one counted and not logged, a single-writer log reporting
  zero), and `control/recovery_status_test.go`.
  **Found, not fixed (same family):** (a) replay's torn-tail rule accepts a `CorruptFrameError`
  anywhere in the newest segment, even with intact frames after it. A CRC-valid frame that will not
  decode in the middle of the newest segment therefore truncates the rest of that segment with
  only a `log.Printf`. Per the #81 rule, a torn tail can only be at the end. (b) **silent loss, worth its own
  unit:** `scanSegmentRef` ends a segment at a garbled frame header or a CRC mismatch, in any
  segment, and records only the bytes before it as the segment's size. Replay therefore never sees
  damage in a non-newest segment, so its "corrupt and not the most recent segment" refusal cannot
  fire for `DefaultReader`. The damage is caught only indirectly: by checkpoint matching, or by
  later commits whose parents are missing. A probe showed the silent case: 6 commits in one run,
  then a run that wrote nothing (an empty newer segment), the 4th frame's magic flipped, and the
  checkpoint removed. Open succeeded at commit 3 of 6 with no error and no log line. Simply refusing
  is wrong, because a torn tail from an earlier killed run legitimately ends a segment that is no
  longer the newest. The correct test is "intact frames after the stop" (the #81 rule;
  `SkimSegment` / `DamageTolerantReader` can already read past damage).

### 2026-10-11 — R1: real zstd on Kotlin/JS and Kotlin/Native (#116, branch `feat/zstd-js-native`)

- **Choice.** A pure-Kotlin codec in `kdb-compression`'s commonMain, used by both JS and Native;
  the JVM keeps zstd-jni. Rejected: `fzstd` (decoder only, and JS only), `@bokuweb/zstd-wasm` /
  `zstd-codec` (async wasm initialisation behind the synchronous `ZstdCompression` API, a
  different loader for node and the browser, 0.9MB and 2.1MB unpacked; all three are MIT), and cinterop to libzstd (a static
  library per target - linuxX64 on the ubuntu runner, macosArm64 only on macOS - for one of the two
  targets). One decoder (`ZstdDecoder`, RFC 8878: every block, literals and sequence-table mode,
  XXH64 checksum, concatenated and skippable frames; no dictionaries) serves both targets and runs
  under the JVM's differential tests against libzstd.
- **Both directions.** `ZstdEncoder` writes real frames: greedy hash matches that try the most
  recent offset first, repeat-offset codes, Huffman-coded literals (direct or FSE-compressed
  weights), and per-block RLE, predefined or custom FSE tables for the sequence codes. Frames are
  single-segment, with content size and checksum always present. It matches zstd-jni level 3 on
  small payloads (158-byte record: 143 bytes each; 700-byte JSON: 232 vs 230) and is about 1.2x
  its size on large ones (400KB JSON: 79,141 vs 65,497), and Go and the JVM read it. So `canCompress` and `canDecompress` (replacing `isAvailable`) are both true
  everywhere, and JS/Native SSTable blocks, delta pages and ice bundles are zstd again. The split
  stays so a decode-only target remains expressible; every writer checks `canCompress`.
- **Errors.** Malformed input throws `ZstdFormatException` (an `IllegalArgumentException`) on
  every target; the JVM now wraps zstd-jni's `ZstdException` the same way. `ZstdUnavailableException`
  is gone - nothing throws it.
- **Tests.** Golden fixtures are compiled into commonTest (`gradle/physical-golden-fixtures.gradle.kts`),
  so JS (node and browser) and Native read Go's and the JVM's bytes: a zstd corpus from klauspost
  (levels 1/3/7/19), zstd-jni (1/3/19 and a streamed frame without content size) and the
  pure-Kotlin encoder; both `sstable_segment.hex` fixtures; and a new `delta_segment_zstd.hex` from
  each side. JS/Native assert their encoder output equals the committed `_purekotlin` fixtures,
  which Go decodes (`TestKotlinZstdCorpusCrossDecodes`). Corruption: truncated, flipped and zeroed
  frames fail typed, never with another exception and never with wrong bytes from a checksummed
  frame. Restoring the pre-R1 stubs fails 4 kdb-compression, 2 SSTable and 2 delta tests on both
  jsNode and macosArm64.
- **Found along the way.**
  - A zero-length body is a valid "frame" (Go writes nothing for an empty payload), so a body
    truncated to zero bytes decodes to empty output. Every KDB format stores the uncompressed
    length beside the body and checks it, which is where that truncation is caught.
  - A pre-U5 Native data directory (length-prefixed blocks tagged ZSTD) now fails with
    `ZstdFormatException` (bad frame magic) instead of `ZstdUnavailableException` - still refused,
    never misread. No such directory was ever released.
  - The encoder was built up in three steps in this PR; measured on the 400KB JSON corpus
    (zstd-jni level 3: 65,497): raw literals and predefined tables 108,624; + Huffman literals
    104,986; + per-block sequence tables and repeat offsets 79,141. A 128KB block of that corpus
    has ~4KB of literals but ~10,700 sequences, so sequence coding was the gap: custom tables alone
    took it to 79,321, repeat offsets alone only to 104,483. Huffman and repeat offsets matter
    most on small payloads (700-byte JSON: 299 -> 265 -> 232).
  - The repeat-offset history persists across a frame's compressed blocks but a raw block leaves
    the decoder's untouched, so the encoder rolls its history back when it stores a block raw
    (`rawBlockDoesNotAdvanceRepeatOffsets`). A wrong update to the third history slot can survive
    round-trip tests (the greedy parser rarely reads it), so `RepeatOffsetsTest` checks every
    step against an RFC model.
  - Not done: lazy matching (the rest of the large-input gap) and repeat-mode tables; and no
    throughput numbers - the machine's load average was 50-140 for most of the work.
- **2026-10-11 — U6b and U6d (issue #43).** U6b: `go/kdb/integration/kill9_server_test.go` builds
  the real `kdb-service` and, on ONE data directory, repeatedly: six connections write their own
  documents over the wire (`PutJSON` replaces and `Upsert` merges; writer 0 reconnects for every
  write, the e2e test's shape), the process is SIGKILLed at a random moment, it restarts with
  `GOMAXPROCS=2`, and every write acknowledged in ANY iteration so far must read back at its
  acknowledged value or later. So recovery is tested on top of recovered data, kill after kill.
  It reports missing, rolled-back and unreadable documents separately, keeps the data directory
  and every launch's log, and prints a seed to replay the kill timing. `KDB_KILL9_ITERATIONS` and
  `KDB_KILL9_SERVICE_ARGS` set the soak. **Control:** with `--durability async`, which may lose
  acknowledged writes by design, it fails on the first kill with every document reported rolled
  back, so a pass means something. It runs 3 kills in the normal suite, and a new nightly job
  (`kill9-soak` in `e2e-nightly.yml`) runs 150 + 60 on Linux, where #43 was seen, uploading the
  data directory on failure.
  U6d results, macOS, every acknowledged write intact in all of them:

  | Configuration | Kills | Acknowledged writes |
  |---|---|---|
  | default (`sync`) | 300 | 36,462 |
  | checkpoints mid-load (`--maintenance-interval 200ms`) | 80 | 14,803 |
  | `history=none`, 1 s retention (old segments deleted) | 80 | 32,503 |
  | tiny document, ops and tree caches (reads from the log) | 80 | 11,543 |
  | preallocated segments + `--sync-mode fast` | 80 | 15,819 (slowest restart 9.7 s; see N4) |

  So the server's acknowledge-then-crash path holds on this machine. The original #43 failure
  reported its FIRST document (`...01`) as lost, and the old test reported any failed read as lost
  (fixed in U6a), so a read error after restart is as likely as data loss. #43 stays open until
  the nightly Linux soak has run. Found along the way, both with their own PRs:
  **(N4)** crash-recovery startup costs commits × SSTable size: `LsmBlobStore.Lookup` builds a
  fresh reader per lookup, and `DefaultReader.Index()` reads the WHOLE table in 8 KB chunks just
  to learn its size, then re-parses the footer index, on every lookup. Replay probes every tree
  object, so 14,736 commits with no checkpoint took 28 s to open (24 documents, a 1.3 MB table).
  That surfaced as "not ready after 30 s" in the preallocation run; the test now waits 2 minutes
  and reports the slowest restart instead. Fix: `perf/sstable-index-cache`.
  **(N5)** the third silent recovery path from U6c: damage in the middle of a segment that is no
  longer the newest truncates that segment at the damage, with no error.

- **2026-10-11 — found while working on #43 (U6): crash-recovery open scaled with commits ×
  SSTable size** (branch `perf/sstable-index-cache`). The #43 kill -9 reproducer left a namespace
  with 14,736 commits and no checkpoint; it took 28 s to open (~1.9 ms per replayed commit, 24
  documents). Root cause: `LsmBlobStore.Lookup` (`go/kdb/storage/sstable/types.go`) built a new
  `DefaultReader` per table per probe, and `DefaultReader.Index` (`codec.go`) learned the file size
  in `segmentSize` by reading the whole segment in 8 KB chunks (each chunk an open+stat+read+close
  in `OSByteStore.Read`), then re-read and re-parsed the footer. Replay probes once per tree object
  per commit (`engine.skipExistingObject`), so each replayed commit cost time proportional to the
  table. The ordinary write path does not probe; history reads through the object store did pay it.
  Fix, no format change: an optional `storage.SegmentSizer` (implemented by `InMemoryPlatformIO`,
  and by `FileBackedPlatformIO` forwarding to `OSByteStore.Size`, which stats or uses the open
  handle's logical size, and through `PrimaryWithReplicas` to the primary; S3 is a replica sink and
  never serves reads). Shims without it, or returning `storage.ErrSegmentSizeUnknown`, fall back
  to the old scan. `LsmBlobStore` caches each table's parsed index (LRU, budget
  `storage.ResolvedSSTableIndexCacheBytes` = 1/8 of the hot tier, re-cut by `SetMemoryBudgetBytes`;
  ~128 B per key; the most recent index is kept even over budget so one huge table never goes back
  to per-probe parsing). It is guarded by the same mutex as the table list, so `Compact` drops the
  inputs' indexes in the critical section that swaps them, and a load racing the swap is only
  cached if its table is still registered. The newest-first order is cached instead of re-sorted
  per probe. Replay's existence probe now asks `Contains` (index only) instead of `Get`, which
  read and decompressed the probed tree object. A bloom filter was not added: replay's probes are
  hits, and a warm miss is already ~20 ns. Numbers (14,000 commits / 24 docs, no checkpoint, one
  ~3.3 MB table, load 7-9 on 16 cores): open 148.9 s → 0.62 s with the cache → 0.36 s with
  `Contains`. Lookup against a ~1 MB table: file-backed hit 2.55 ms → 15 µs, miss 2.50 ms → 19 ns;
  in-memory hit 554 µs → 1.6 µs. **Also found and fixed:** `Compact` replaced the table list
  with its output outright, so a table flushed while a compaction ran vanished from the store until
  the next open (`TestCompactKeepsTablesAddedDuringTheMerge`, fails against the old swap).
  **Still open:** a lookup that took the table list just before a compaction's swap can miss a key,
  because the inputs are deleted while it reads them; predates this change. Tests:
  `go/kdb/storage/sstable/index_cache_test.go` (a first probe makes at most 4 reads where main made
  155, warm hits one read, warm misses and `Contains` none; compaction invalidation, budget,
  concurrency, the no-sizer fallback, file-backed size), and the opt-in measurement
  `TestReplayOpenTimeWithoutCheckpoint` (`KDB_REPLAY_COMMITS=14000`) in `go/kdb/embed`.
