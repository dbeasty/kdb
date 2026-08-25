# Component 43 — `kdb-embed` Durable + Mobile Storage

Layer 12 (renumbered from Revision 1's Component 33 — see the gap analysis §7).

**Deprioritized in Revision 2, and its very premise now in question.** A full Go engine port
already exists in this repo (`go/kdb/embed`) — ordinary Go, with no Kotlin/Native toolchain
required at all. If that embed engine can be linked directly into Zolik's `gomobile`-built mobile
binary, everything this component specs (Kotlin/Native iOS/Android targets, a durable file-backed
embed API) may simply be unnecessary for Zolik's specific goal — Zolik would get on-device durable
storage "for free" as part of the same Go toolchain it already needs for the rules engine, with no
second native toolchain at all. **This is an open question, not yet answered** — someone needs to
check whether `go/kdb/embed` actually cross-compiles under `gomobile bind` and whether its
feature set (durable storage, SQL, schema) is complete enough for Zolik's needs, before investing
further here. This spec is preserved as-is (still accurate about the Kotlin/Native side) as the
fallback plan if the Go-embed path doesn't pan out.

Layer 11. Depends on Layer 4a (Component 10e, Storage Engine Core), Layer 4a (Component 10g,
Platform I/O Shim), and Component 32 (native transport, for the networked half of mobile use —
this component covers the *storage* half only).

## 1. Purpose

`kdb-embed` today only wires `openMemoryRuntime` to `InMemoryStorageAdapter`, and only targets
`jvm()`/`js(IR)` — there is no durable, on-device embed path, and no Kotlin/Native target at all.
Durable storage *engine* code already exists (WAL, SSTable, delta log, `PlatformIoShim` with a
named `NativeSegmentByteStore` alongside the JVM one) but isn't reachable through the simple embed
API `kdb-embed` was built to provide. This component closes that gap: a durable
`openFileRuntime(path, ...)`-shaped entry point in `kdb-embed`, and the target wiring
(`androidTarget()`, `iosArm64()`, `iosSimulatorArm64()`, plus proving out `linuxX64`/`macosArm64`
which most other core modules already have) needed for that entry point to produce something a
mobile app can actually link.

## 2. Dependencies

- `kdb-storage-engine` (Component 10e) — `StorageEngine`, open/close/read/write over WAL +
  MemTable + SSTable + delta log.
- `kdb-storage-io` (Component 10g) — `PlatformIoShim` `expect`/`actual`; `JvmSegmentByteStore`
  exists, `NativeSegmentByteStore` exists per the audit but is unproven for iOS specifically (only
  `linuxX64`/`macosArm64` native targets currently build anywhere in this repo).
- `kdb-transaction` (Component 7), `kdb-dag` (Component 6), `kdb-schema` (Component 5) — unchanged;
  `kdb-embed`'s existing commit/query plumbing (`EmbeddedKdbRuntime`, `EmbedWrites`, `EmbedSchema`)
  is reused as-is against whichever `StorageAdapter` is wired in, in-memory or file-backed.
- Component 42 (native transport) — not a build dependency of this component, but the pairing that
  makes it useful: durable storage with no networking gets a device a working *local* cache; it
  needs Component 42 too to actually peer-sync or serve other devices.

## 3. Public Interface

```kotlin
// kdb-embed/src/commonMain/kotlin/dev/kdb/embed/EmbeddedKdbRuntime.kt — additive, alongside
// the existing openMemoryRuntime, not a replacement for it (in-memory stays useful for tests).

object EmbeddedKdbRuntime {
    // existing:
    suspend fun openMemoryRuntime(
        catalog: String,
        namespaceId: String,
        schema: NamespaceSchema? = null,
    ): EmbedRuntime

    // new:
    suspend fun openFileRuntime(
        catalog: String,
        namespaceId: String,
        dataDir: String,          // platform-native path; caller resolves app-sandbox dirs
        schema: NamespaceSchema? = null,
    ): EmbedRuntime
}

// unchanged shape — file-backed and in-memory runtimes are interchangeable at this interface,
// which is the whole point (EmbedOperations.putJson/getJson/querySql already don't care which
// StorageAdapter backs them).
interface EmbedRuntime {
    val namespaceId: String
    suspend fun close()
}
```

```kotlin
// kdb-storage-io/src/nativeMain/kotlin/dev/kdb/storage/io/NativeSegmentByteStore.kt
// Confirmed to already exist by name per the maturity audit; this component's job is to prove
// it against real iOS/Android targets, not invent it from scratch. Interface shown for
// completeness — change only if the audit's assumption that it already matches
// PlatformIoShim's expect side turns out wrong once iOS targets actually compile it.
actual class NativeSegmentByteStore actual constructor(
    private val basePath: String,
) : PlatformIoShim {
    actual override suspend fun append(segment: String, bytes: ByteArray)
    actual override suspend fun read(segment: String, offset: Long, length: Int): ByteArray
    actual override suspend fun list(): List<String>
    actual override suspend fun delete(segment: String)
    actual override suspend fun sync(segment: String)   // fsync-equivalent; see §5
}
```

## 4. Data Structures

No new wire- or document-visible data structures. `EmbedRuntime` gains no new fields; the only
change is a second factory path (`openFileRuntime`) that resolves to a different `StorageAdapter`
implementation than `openMemoryRuntime` does. Everything above `StorageAdapter` (documents, commits,
schema, SQL) is unaware of which one is in use — this is a direct consequence of Layer 3's
`StorageAdapter` boundary already existing for this exact reason.

## 5. Contracts

- `openFileRuntime(dataDir, ...)`: if `dataDir` doesn't exist, create it (platform-appropriate
  directory creation, including parent dirs); if a runtime was previously closed with data in
  `dataDir`, reopening replays the WAL/delta log to reconstruct the last committed state — mirrors
  the existing SERVER-side "file persistence... via the SERVER storage engine and delta log
  replay" behavior the README already claims for JVM; this component's job is to make the *same*
  replay path reachable from `kdb-embed`'s simpler API, and to make it work when the underlying
  `PlatformIoShim` is native/mobile rather than JVM's `java.nio`.
- Crash-safety: a runtime killed mid-write (a very normal event on mobile — the OS can terminate a
  backgrounded app without warning) must not corrupt `dataDir`; on next `openFileRuntime`, WAL
  replay recovers to the last `sync()`-confirmed write, consistent with what Component 10a (WAL)
  already promises on JVM — this component's contract is that the promise holds identically on
  Kotlin/Native, not a new promise.
- `sync()` must map to a real durability primitive per platform: `fsync(2)` on POSIX/native,
  `FileChannel.force(true)` on JVM (already presumably how the JVM adapter does it — verify, don't
  assume, since a missing/no-op `sync()` would make the crash-safety contract above false silently).
- Concurrent access: exactly one `EmbedRuntime` may hold a given `dataDir` open at a time (single
  local process, single mobile app — no multi-process file locking is required for v1, unlike
  `kdb-server`'s multi-client story, which this component does not need to solve). Attempting to
  open an already-open `dataDir` a second time within the same process is undefined by this spec
  and should be rejected with a clear error rather than silently sharing or corrupting state — pick
  one and document it during implementation.
- Target wiring: `kdb-embed/build.gradle.kts` gains `linuxX64()`, `macosArm64()`,
  `androidTarget()`, `iosArm64()`, `iosSimulatorArm64()` alongside its existing `jvm()`/`js(IR)`.
  Every module in `kdb-embed`'s dependency chain that doesn't already have these targets
  (`kdb-storage-engine`, `kdb-storage-io`, `kdb-storage-wal`, `kdb-storage-sstable`,
  `kdb-storage-memtable`, `kdb-storage-delta`, `kdb-index*`, `kdb-sql`, `kdb-transaction`,
  `kdb-document`, `kdb-schema`, `kdb-dag`, `kdb-json`, `kdb-codec`, `kdb-error`) needs the same
  targets added — most already share `gradle/kdb-kmp.gradle.kts`, so for those this is a
  convention-file change in one place, not 15 separate edits; confirm during implementation which
  modules opted out of the shared convention file and need individual attention.

## 6. Error Cases

- `KdbStorageException("cannot create data directory: <path>")` — permission denied, disk full,
  invalid path (notably: iOS app-sandbox paths are per-install and change across reinstalls — the
  caller, not this component, is responsible for resolving a stable sandbox-relative path each
  launch).
- `KdbStorageException("WAL replay failed: corrupt segment at <offset>")` — on open, if replay
  encounters a truncated/corrupt record. Per Component 10a's existing contract (unchanged here):
  a corrupt tail record (consistent with a crash mid-write) is truncated and discarded, not fatal;
  a corrupt record *before* the tail is fatal and surfaces this exception rather than silently
  losing history.
- `KdbStorageException("data directory already open")` — second `openFileRuntime` call against a
  `dataDir` already held open in-process (see §5).
- Disk-full mid-write: propagates as `KdbStorageException` from the failing `append`/`sync` call;
  the transaction that triggered it is not committed (matches Component 7's existing "commit only
  on full success" contract — this component must not introduce a partial-commit path under disk
  pressure).

## 7. Test Cases

1. **Open, write, close, reopen, read back** — the core durability round trip, on `jvm()` first
   (fastest iteration), then `linuxX64`, then `androidTarget()`/`iosArm64Simulator` once wired.
2. **Kill mid-write, reopen, verify WAL replay recovers the last synced commit** — simulate by
   closing the underlying file descriptor without calling the runtime's clean shutdown path.
3. **Empty `dataDir` on first open** — creates directory structure, no error, subsequent read
   returns "not found" rather than throwing.
4. **In-memory and file-backed runtimes produce identical query results** — same `putJson`/
   `querySql` sequence against both, assert equal `QueryResultJson` output; this is the test that
   proves the `StorageAdapter` boundary is actually being respected and nothing embed-specific
   leaked an in-memory assumption.
5. **Second `openFileRuntime` on an already-open `dataDir` in the same process** — rejected per
   §5's chosen error contract, not a silent hang or data race.
6. **Large document set survives close/reopen** — enough documents to force at least one SSTable
   flush + compaction cycle before reopening, so the test exercises more than "still in the WAL."
7. **`sync()` is actually durable** — write, `sync()`, hard-kill the process (not a clean close),
   reopen: data present. Without this test, a no-op `sync()` on a new platform adapter would pass
   every other test here and still be silently unsafe.
8. **Reinstall-equivalent: fresh `dataDir` after a prior one existed** — mobile-specific: iOS/
   Android can hand an app a fresh sandboxed path after reinstall or OS-level data clearing;
   confirm this is indistinguishable from "empty `dataDir` on first open" (test 3) rather than an
   error.
9. **iOS simulator smoke test** — round trip (test 1) actually running under `iosSimulatorArm64`
   in CI, not just compiling. Compiling for a target and correctly running the storage engine on
   it are different claims; this test is what closes that gap.
10. **Android smoke test** — same, under `androidTarget()`, ideally via Robolectric or an emulator
    in CI rather than compile-only verification.

## 8. Non-Goals

- Multi-process file locking / multiple app instances sharing one `dataDir` concurrently — not a
  mobile-app scenario (one process owns its sandbox), and `kdb-server`'s multi-client story already
  solves the "many clients, one store" case for the cases that need it.
- Automatic migration of an existing in-memory runtime's data into a file-backed one — an app that
  wants durability from the start should call `openFileRuntime` from the start; converting a live
  in-memory runtime is out of scope.
- Encryption at rest. Mobile OSes provide filesystem-level encryption already (iOS Data Protection,
  Android FBE); an additional KDB-level encryption layer is a separate concern this component does
  not address.
- Networking of any kind — this component is purely the storage side. A file-backed embed runtime
  with no transport wired up is a valid, useful configuration (a pure offline local cache) and this
  component should be testable and usable in exactly that configuration without Component 42.

## 9. Implementation Notes

- Prove the target-wiring change (`linuxX64`, `macosArm64` for `kdb-embed` specifically, since
  today only *other* core modules have these, not `kdb-embed` itself) before touching iOS/Android
  — it isolates "does the durable-storage embed API even compile against a native target" from
  "does the mobile toolchain work," which are genuinely separate risks.
- iOS packaging: Kotlin/Native produces an `.xcframework` (via `binaries.framework` /
  `XCFramework` DSL in Gradle) for consumption from Xcode/CocoaPods/SPM. This is standard KMP
  tooling, not bespoke to this component, but it's new to this repo (no module currently produces
  one) — budget toolchain setup time (Xcode + cinterop + signing for device builds, not just
  simulator) separately from the Kotlin implementation effort in §10's line estimate.
- Android packaging: `androidTarget()` in a KMP module produces a `.aar` consumable from a normal
  Android Gradle project — no Kotlin/Native involved on this side at all, since Android's Kotlin
  runtime is JVM-based. This means the Android half of this component is materially lower-risk
  than the iOS half, and should be done first if the two need to be sequenced.
- Reuse Component 10a's (WAL) existing replay logic verbatim — this component is a *reachability*
  fix (expose durable storage through `kdb-embed`, prove it on new targets), not a storage-engine
  rewrite. Resist the temptation to "improve" WAL/compaction behavior while doing this; that risks
  conflating two kinds of change in one component, which the master spec's own dependency rules
  (§0) warn against ("never mix spec generation and implementation," and by extension, never mix
  unrelated implementation changes).

## 10. Estimated Lines

1,200–2,000 NBNC for the Kotlin/Native-specific code and Gradle wiring (target declarations across
~15 modules, `openFileRuntime` + its tests, `NativeSegmentByteStore` verification/fixes,
`.xcframework`/`.aar` packaging scripts). **This estimate does not include iOS toolchain setup
time** (Xcode project/workspace integration, code signing, CocoaPods or SPM package definition) —
that is calendar time and tooling risk, not a line count, and per the gap analysis's sequencing
recommendation should be scoped and time-boxed separately before committing to a delivery date for
this component.
