# KDB Component Spec — Layer 4a
## Component 10g: Platform I/O Shim (Production)
### `dev.kdb.storage.io`

**File:** `kdb-spec-layer4a-component10g-platform-io-shim.md`
**Layer:** 4a — KDB Storage Engine
**Depends on:** Layer 3 Component 9 (`expect PlatformIoShim` in `dev.kdb.storage`)

---

## 1. Purpose

The Platform I/O Shim is the **only** Kotlin Multiplatform `expect/actual` boundary for durable storage I/O. Layer 3 defines the `expect interface PlatformIoShim` in `dev.kdb.storage`; this component supplies **production** `actual` implementations that back WAL segments, SSTable files, and delta log pages on real media (JVM filesystem, Native POSIX, Browser in-memory segments plus Web Storage snapshots).

Test-only in-memory shims remain in Layer 3 (`dev.kdb.storage.mem.InMemoryPlatformIoShim`). Implementers of WAL (10a), SSTable (10c), and Delta Segment Writer (10d) call **only** `PlatformIoShim` — never platform APIs directly.

---

## 2. Dependencies

| Module | Interface used |
|---|---|
| `dev.kdb.storage` (Layer 3, §17) | `expect interface PlatformIoShim` — contract to implement |
| `dev.kdb.error` | `KdbException`, `KdbErrorCode` |
| `dev.kdb.codec` | `KdbUuid` (segment id parsing in helpers only) |

**Not depended on:** WAL, MemTable, SSTable, StorageAdapter, Transaction Engine.

**Consumers (Layer 4a, via injected `StorageEngineConfig.ioShim`):** 10a WAL, 10c SSTable, 10d Delta Segment Writer, 10e Storage Engine Core.

---

## 3. Public Interface

```kotlin
package dev.kdb.storage.io

import dev.kdb.storage.PlatformIoShim

// ╔══════════════════════════════════════════════════════════════════╗
// ║  Segment naming (commonMain helpers)                           ║
// ╚══════════════════════════════════════════════════════════════════╝

/** Logical segment kinds; encoded into [SegmentNameBuilder] paths. */
enum class SegmentKind {
    /** Append-only delta log page (Component 10d). */
    DELTA,
    /** Write-ahead log for blob / tree durability (Component 10a). */
    WAL,
    /** Immutable SSTable file at [level] (Component 10c). */
    SSTABLE,
}

/**
 * Canonical segment path: `ns/{namespaceId}/{kind}/[{level}/]{segmentId}`.
 * [segmentId] is opaque (UUID string, sequence number, or file stem).
 * Used by WAL, SSTable, and delta writers — must match [PlatformIoShim.listSegments] prefix rules.
 */
object SegmentNameBuilder {

    fun delta(namespaceId: String, segmentId: String): String =
        path(namespaceId, SegmentKind.DELTA, segmentId)

    fun wal(namespaceId: String, walId: String): String =
        path(namespaceId, SegmentKind.WAL, walId)

    fun sstable(namespaceId: String, level: Int, fileId: String): String =
        "ns/$namespaceId/${SegmentKind.SSTABLE.name.lowercase()}/L$level/$fileId"

    fun namespacePrefix(namespaceId: String): String = "ns/$namespaceId/"

    private fun path(namespaceId: String, kind: SegmentKind, segmentId: String): String =
        "ns/$namespaceId/${kind.name.lowercase()}/$segmentId"
}

/** Browser snapshot keys (not filesystem segments). Prefix avoids collision with segment namespaces. */
object SnapshotKeyBuilder {
    fun enlistment(enlistmentId: String): String = "kdb:snap:$enlistmentId"
}

// ╔══════════════════════════════════════════════════════════════════╗
// ║  Production shim factory                                       ║
// ╚══════════════════════════════════════════════════════════════════╝

/**
 * Configuration for file-backed I/O. [rootDirectory] is JVM/Native only;
 * ignored on JS (segments live in an in-process map).
 */
data class PlatformIoConfig(
    /** Absolute or relative data root. JVM/Native: created if missing. */
    val rootDirectory: String? = null,
    /** When true, [flushSegment] and [sealSegment] call fsync (or platform equivalent). Default: true. */
    val fsyncOnFlush: Boolean = true,
    /** Max single append size (guardrail). Default: 16 MiB. */
    val maxAppendBytes: Int = 16 * 1024 * 1024,
)

/**
 * Factory for production [PlatformIoShim] instances.
 * Platform `actual` classes implement [PlatformIoShim] directly.
 */
expect object FileBackedPlatformIoShimFactory {

    /** Open or create a shim rooted at [config.rootDirectory] (JVM/Native) or in-memory map (JS). */
    fun open(config: PlatformIoConfig = PlatformIoConfig()): PlatformIoShim
}

// ╔══════════════════════════════════════════════════════════════════╗
// ║  Platform actuals (implement PlatformIoShim)                     ║
// ╚══════════════════════════════════════════════════════════════════╝

/** JVM: [java.nio.channels.FileChannel] per segment under [rootDirectory]. */
expect class JvmFileBackedPlatformIoShim(config: PlatformIoConfig) : PlatformIoShim

/** Native: POSIX open/write/fsync via kotlinx.cinterop. */
expect class NativeFileBackedPlatformIoShim(config: PlatformIoConfig) : PlatformIoShim

/**
 * Browser: segment bytes in a process-local map; [readSnapshot]/[writeSnapshot] use
 * localStorage with sessionStorage fallback. No durable segment files across reload.
 */
expect class BrowserFileBackedPlatformIoShim(config: PlatformIoConfig = PlatformIoConfig()) : PlatformIoShim

// ╔══════════════════════════════════════════════════════════════════╗
// ║  Diagnostics (optional, commonMain)                              ║
// ╚══════════════════════════════════════════════════════════════════╝

/** Thrown when segment I/O fails after retries. */
class PlatformIoException(
    message: String,
    val segmentName: String? = null,
    cause: Throwable? = null,
) : dev.kdb.error.KdbException(message, cause) {
    override val code: dev.kdb.error.KdbErrorCode
        get() = dev.kdb.error.KdbErrorCode.STORAGE_TIER_ERROR
}

/** Result of validating a segment exists and is readable (startup / recovery). */
data class SegmentHealthReport(
    val segmentName: String,
    val sizeBytes: Long,
    val readable: Boolean,
    val error: String? = null,
)
```

`FileBackedPlatformIoShimFactory.open()` returns the platform-appropriate `actual` class. All three actuals implement the Layer 3 `PlatformIoShim` methods verbatim:

```kotlin
// Contract surface (defined in dev.kdb.storage — not re-declared here)
interface PlatformIoShim {
    suspend fun appendToSegment(segmentName: String, bytes: ByteArray): Long
    suspend fun readFromSegment(segmentName: String, offset: Long, length: Int): ByteArray
    suspend fun flushSegment(segmentName: String)
    suspend fun sealSegment(segmentName: String)
    suspend fun listSegments(namespaceId: String): List<String>
    suspend fun deleteSegment(segmentName: String)
    suspend fun availableBytes(): Long
    suspend fun readSnapshot(key: String): ByteArray?
    suspend fun writeSnapshot(key: String, data: ByteArray)
    suspend fun deleteSnapshot(key: String)
}
```

---

## 4. Data Structures

### `SegmentKind`
Enumerates logical segment families so WAL, SSTable, and delta writers do not collide under the same namespace prefix. SSTable paths include a level subdirectory (`L0`, `L1`, …) for compaction (Component 10f).

### `SegmentNameBuilder`
Pure string builder — no I/O. All Layer 4a writers must use these helpers so `listSegments(namespaceId)` returns a consistent set for compaction and recovery.

### `SnapshotKeyBuilder`
Browser-only keys for realized-store snapshots (Layer 3 `EnlistmentHandle`). Distinct from segment paths; never passed to `appendToSegment`.

### `PlatformIoConfig`
`fsyncOnFlush` defaults to `true` for server durability. Tests may set `false` for speed when using JVM shim with a temp directory.

### `SegmentHealthReport`
Used by engine open/recovery (10e) to log corrupt segments without failing entire namespace open.

---

## 5. Contracts

### `appendToSegment`
**Pre:** `bytes.size <= maxAppendBytes`. Segment must not be sealed (implementation tracks sealed set per `segmentName` after `sealSegment`).
**Post:** Returns new total segment size in bytes. Append is atomic at the byte-range level: concurrent readers see either the full prior length or the new length after the append completes.
**Ordering:** Appends for a given `segmentName` are totally ordered.

### `readFromSegment`
**Pre:** `offset >= 0`, `length >= 0`, `offset + length <= currentSize`.
**Post:** Returns exactly `length` bytes, or fewer only when `offset + length > size` (then returns `size - offset` bytes, never negative length).
**Error:** Unknown `segmentName` → `PlatformIoException`.

### `flushSegment`
**Post:** All buffered writes for `segmentName` are visible to subsequent `readFromSegment` on any thread/process that shares the storage medium.
**Durability:** When `fsyncOnFlush` is true, data survives process crash after successful return (OS/page-cache guarantees on JVM/Native; JS: visible within same tab session only).

### `sealSegment`
**Post:** Segment is marked immutable; further `appendToSegment` calls throw `PlatformIoException` ("segment sealed"). Invokes the same durability path as `flushSegment` (including fsync when enabled).
**Idempotent:** Second `sealSegment` on the same name is a no-op.

### `listSegments`
**Post:** Returns all segment names under `SegmentNameBuilder.namespacePrefix(namespaceId)`, lexicographically sorted. Does not include snapshot keys.

### `deleteSegment`
**Post:** Segment removed from listing and storage; subsequent reads throw. Safe to call on missing segment (no-op).
**Compaction:** Called by 10f after SSTable merge replaces inputs.

### `availableBytes`
**JVM/Native:** Free space on filesystem hosting `rootDirectory`, or `Long.MAX_VALUE` if unknown.
**JS:** `Long.MAX_VALUE` (no quota enforcement in v1).

### `readSnapshot` / `writeSnapshot` / `deleteSnapshot`
**Browser:** `writeSnapshot` is best-effort; quota exceeded → log warning, do not throw (Layer 3 contract).
**JVM/Native:** Optional file under `{root}/snap/{key}` for integration tests; production server enlistments use 11d for snapshot policy — shim provides uniform API only.

---

## 6. Error Cases

| Exception | When |
|---|---|
| `PlatformIoException` | I/O failure, sealed segment append, append over `maxAppendBytes`, unrecoverable read shortfall. |
| `IllegalArgumentException` | Malformed `segmentName` (empty, contains `..`, or wrong prefix) — thrown at first use, not at builder time. |

Underlying `IOException` (JVM) or errno (Native) are wrapped in `PlatformIoException` with `segmentName` set.

---

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `appendRead_roundtrip` | Append 1 KiB, read from offset 0. | Bytes equal; returned size == 1024. |
| 2 | `flushSurvivesReopen` | JVM: append, flush with fsync, new shim instance same root. | Read returns appended bytes. |
| 3 | `sealBlocksAppend` | Seal segment, append again. | `PlatformIoException`. |
| 4 | `listSegments_prefix` | Create delta + wal segments for `ns1`. | Only `ns/ns1/...` names returned. |
| 5 | `deleteSegment_removes` | Delete after seal. | `listSegments` omits name; read throws. |
| 6 | `maxAppend_guard` | Append > `maxAppendBytes`. | `PlatformIoException` before write. |
| 7 | `browser_snapshot_roundtrip` | JS: `writeSnapshot` + `readSnapshot`. | Same bytes; `deleteSnapshot` returns null on read. |
| 8 | `concurrentAppend_ordered` | Two coroutines append 100 B each (mutex in shim). | Final size 200; reads see contiguous data. |
| 9 | `readPastEnd_partial` | Read offset at end, length 10. | Empty array, no throw. |
| 10 | `segmentNameBuilder_sstable` | `sstable("app", 2, "abc")`. | `ns/app/sstable/L2/abc`. |

---

## 8. Non-Goals

- **In-memory shim for tests** — stays `dev.kdb.storage.mem.InMemoryPlatformIoShim` (Layer 3).
- **Object storage / S3** — warm/cold tier archival is Layer 6 + tier manager.
- **Encryption at rest** — future security layer.
- **Cross-process file locking** — single process per data directory in v1; cluster locking is out of scope.
- **Delta / SSTable framing** — length prefixes and codecs belong to 10a–10d.
- **Replacing `expect` location** — `PlatformIoShim` remains declared in `dev.kdb.storage`; this module only supplies `actual` classes.

---

## 9. Implementation Notes

### JVM (`jvmMain`)
- Map `segmentName` → file path: `{rootDirectory}/{segmentName}` (replace `/` with platform separator or use single-level encoding).
- One `FileChannel` per open segment, guarded by a per-segment mutex; lazy open on first append.
- `flushSegment`: `channel.force(true)` when `fsyncOnFlush`.
- `sealSegment`: set sealed flag, force, close channel.
- `availableBytes`: `FileStore.getUsableSpace` on `rootDirectory`.

### Native (`nativeMain`)
- `open(O_CREAT|O_RDWR)`, `pwrite`, `fsync` on flush/seal, `unlink` on delete.
- Same path layout as JVM.

### Browser (`jsMain`)
- `MutableMap<String, ByteArray>` for segments; no cross-tab durability for segments.
- Snapshots: try `localStorage.setItem`; on `QuotaExceededError` try `sessionStorage`; failures swallowed per Layer 3.
- `FileBackedPlatformIoShimFactory.open()` ignores `rootDirectory`.

### Sealed segment tracking
Maintain `mutableSetOf<String>` of sealed names in each actual. WAL rotation and delta `seal()` call `sealSegment`; compaction calls `deleteSegment` on inputs.

### Migration from stubs
Replace empty `actual interface PlatformIoShim` in `kdb-storage` with typealiases or delegate to `JvmFileBackedPlatformIoShim` etc., or move `actual` classes into `:kdb-storage-io` module that depends on `:kdb-storage`.

### Performance
- Batch small appends in 10a/10d before calling shim (reduce fsync frequency); shim does not coalesce.
- Prefer sequential read sizes aligned to 4 KiB on JVM for OS readahead.

---

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| `SegmentNameBuilder` + `SnapshotKeyBuilder` + `PlatformIoConfig` | 80 |
| `PlatformIoException` + `SegmentHealthReport` | 40 |
| `JvmFileBackedPlatformIoShim` | 280 |
| `NativeFileBackedPlatformIoShim` | 320 |
| `BrowserFileBackedPlatformIoShim` | 220 |
| `FileBackedPlatformIoShimFactory` (expect + 3 actuals) | 60 |
| Unit + integration tests (JVM temp dir, JS snapshots) | 450 |
| **Total** | **~1,450** |

Production shim alone (excluding tests): **~380 NBNC** per spec target band when counting only 10g deliverable.
