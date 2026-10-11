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

- `docs/kdb-finish-up-plan.md:378,386,388,596,597` still list the explicit compression flag and the
  cross-language SSTable fixture test as open. Both landed (see [Already resolved](#already-resolved)).
- The memory notes on flaky Kotlin tests, the Gradle baseline and preallocation (KDB **does**
  preallocate delta segments, behind `KDB_PREALLOCATE_SEGMENTS`).

Can ride along with U1 or U3.

---

## Phase B — Correctness

### U5. JS/Native write zstd-tagged blocks that are not zstd — S/M, format correctness

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

`go/kdb/json/api.go:245-289`: `navigateGet`, behind `Get`/`GetAll`/`Contains`, panics with
`JsonPathError` on a type mismatch, though all three return `error`. There are no production callers
today, and the wire path recovers anyway (`wire_listen.go:304`), which is why this is small. But it's
a trap for the first caller. Convert the panic to a returned error, and add a table test of mismatched paths.

---

## Phase C — Cleanup and scope decisions

These need a decision from you. Each has a recommendation.

### U9. Remove dead stubs — S each

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

`go/cmd/kdb/cli/commands.go:578-592` calls `SetHead(name, b.HeadHash)`, which sets a branch to its own
head. The CLI has no persisted "current branch". **Recommend** making it fail clearly ("checkout is not
supported: the CLI has no current branch; pass `--branch` per command") rather than inventing a
`.kdb-cli/HEAD` file nobody has asked for. If you do want git-style checkout, it's its own small design.

### U11. Kotlin WAL ignores the configured segment size — S

`kdb-storage-engine/.../ServerStorageEngine.kt:279` builds `DefaultWriteAheadLogFactory()` with no
arguments, so the rotation cap is always the 64 MiB default. Thread `walMaxSegmentBytes` from the
engine config, with a test that a small cap rotates. (Rotation itself is done; see below.)

### U12. In-memory stream transport drops frames — S, low priority

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
| R1 | Real zstd on Kotlin JS/Native | stub; after U5 at least not wrong | Decoder-only is enough to read JVM/Go data: pure-JS `fzstd` via `npm()` on JS; cinterop to libzstd on Native |
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

### 2026-10-11 — new item, found by multi-node e2e testing: mesh merge storm (branch `fix/mesh-merge-convergence`)

**Symptom.** In a five-node `kdb-service` full mesh with concurrent writes and kill -9/restart (`test_cluster.py::test_rolling_kill9_soak`), the mesh failed to converge 2 of 2 times under load avg 20-45. For 150s+ after writes stopped, every head differed and kept changing, with every sync succeeding and the same 40 documents on every node. One node held 10,677 empty `kdb:merge/1` commits.

**Cause.** Merges were deterministic per pair but not confluent. Fixed with canonical joins of same-content heads; see "Merge convergence in a mesh" in `docs/kdb-distributed-implementation-plan.md`.

**Tests.** Failing tests first, each confirmed against the reverted fix:
- `server/mesh_convergence_test.go`: the ring, the property and the restart tests
- `syncnode/mesh_test.go`
- `peersync/join_test.go`
- `integration/join_interop_test.go` (Kotlin)

**Evidence.** The e2e soak is 1 of 2 non-converging (XFAIL after 174s) with `origin/main` binaries, and 5 of 5 converged (~25s per run, about 2-3s of it after writes stop) with this branch, both under load avg 18-28.
