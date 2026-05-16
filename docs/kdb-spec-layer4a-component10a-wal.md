# KDB Component Spec — Layer 4a
## Component 10a: Write-Ahead Log (WAL)
### `dev.kdb.storage.wal`

**File:** `kdb-spec-layer4a-component10a-wal.md`  
**Layer:** 4a — KDB Storage Engine (LSM substrate)  
**Status:** Implementation-ready  
**Depends on:** Layer 0 (Codec, Error Model), Layer 3 (Component 9 — `PlatformIoShim`, `StorageEngineConfig`)

-----

## 1. Purpose

The Write-Ahead Log durably records every mutation to the content-addressed blob store and realized-store block index **before** those mutations are applied to the in-memory MemTable or on-disk SSTables. On process restart, the storage engine replays the WAL to reconstruct the latest durable state, satisfying the crash-recovery contract required by `StorageAdapter.commitTree` (Layer 3). WAL segments are ordinary named segments managed through `PlatformIoShim`; this module owns record framing, checksum validation, append ordering, recovery, and truncation after a successful flush.

-----

## 2. Dependencies

| Module | Interfaces / types used |
|---|---|
| `dev.kdb.codec` (Layer 0) | `KdbHash`, `KdbUuid`, `KdbTimestamp`, `encodeToBytes`, `decodeFromBytes` — WAL record bodies use Layer 0 typed records registered in this module’s wire registry |
| `dev.kdb.error` (Layer 0) | `KdbException`, `KdbErrorCode`, `KdbResult`, `kdbRunCatching` |
| `dev.kdb.storage` (Layer 3 — Component 9) | `PlatformIoShim`, `StorageEngineConfig` — segment I/O only; no `StorageAdapter` dependency |

-----

## 3. Public Interface

```kotlin
package dev.kdb.storage.wal

import dev.kdb.codec.*
import dev.kdb.error.*
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.StorageEngineConfig

/** Builtin Layer 0 wire types for WAL records ([WalRecord], [WalBatch]). */
fun WalWireRegistry(): KdbTypeRegistry

/**
 * Append-only durable log for one logical store partition (typically one namespace
 * or one global blob partition). Backed by a single active segment file on disk.
 */
interface WriteAheadLog {

    /** Stable id for this WAL instance (included in segment names). */
    val walId: KdbUuid

    /** Logical partition key (e.g. namespace id or `"blobs"`). */
    val partitionKey: String

    /** Monotonic sequence of the last appended record, or 0 if empty. */
    val lastSequence: Long

    /** Total bytes in the active segment (including headers). */
    val activeSegmentSizeBytes: Long

    /**
     * Append one record. Durability is caller-controlled: call [sync] before
     * acknowledging a commit to the Transaction Engine.
     */
    suspend fun append(record: WalRecord): WalAppendResult

    /** Append multiple records in one atomic batch (single checksum block). */
    suspend fun appendBatch(records: List<WalRecord>): WalAppendResult

    /** Flush OS buffers via [PlatformIoShim.flushSegment]. */
    suspend fun sync()

    /**
     * Replay all valid records from the active segment and any un-truncated
     * historical segments listed by [WalSegmentCatalog]. Invoked at engine open.
     */
    suspend fun recover(handler: suspend (WalRecord) -> Unit): WalRecoverySummary

    /**
     * Discard WAL bytes that are fully reflected in flushed SSTables / MemTable.
     * [truncateThroughSequence] is inclusive; records with sequence ≤ this value
     * may be removed from the active segment or rotated to a sealed archive segment.
     */
    suspend fun truncate(truncateThroughSequence: Long)

    /** Close the WAL; no further appends. Idempotent. */
    suspend fun close()
}

/** Factory — one active WAL per [partitionKey] in a storage root. */
interface WriteAheadLogFactory {

    suspend fun openOrCreate(
        partitionKey: String,
        config: StorageEngineConfig,
        ioShim: PlatformIoShim,
    ): WriteAheadLog

    /** Segment name convention: `wal/{partitionKey}/{walId}` (active suffix `.log`). */
    fun activeSegmentName(partitionKey: String, walId: KdbUuid): String
}

/** Catalog of sealed WAL segments retained until truncation. */
interface WalSegmentCatalog {

    suspend fun listSegments(partitionKey: String): List<WalSegmentInfo>

    suspend fun deleteSegment(segmentName: String)
}

data class WalSegmentInfo(
    val segmentName: String,
    val walId: KdbUuid,
    val firstSequence: Long,
    val lastSequence: Long,
    val sizeBytes: Long,
    val isActive: Boolean,
)

/** One logical mutation in the WAL. */
data class WalRecord(
    /** Monotonic per-WAL; assigned by [WriteAheadLog.append]. */
    val sequence: Long,
    val timestamp: KdbTimestamp,
    val kind: WalRecordKind,
    /** Layer 0–encoded payload for [kind] (see wire registry). */
    val payload: ByteArray,
) {
    override fun equals(other: Any?): Boolean
    override fun hashCode(): Int
}

sealed class WalRecordKind {
    /** Insert or update a content-addressed blob. [payload] encodes [WalPutBlob]. */
    data object PutBlob : WalRecordKind()

    /** Tombstone a blob hash (compaction / explicit delete). */
    data object DeleteBlob : WalRecordKind()

    /** MemTable flush checkpoint — SSTable file metadata. */
    data object FlushCheckpoint : WalRecordKind()

    /** No-op used to pad or mark segment boundaries. */
    data object Marker : WalRecordKind()
}

data class WalPutBlob(
    val contentHash: KdbHash,
    val bytes: ByteArray,
)

data class WalFlushCheckpoint(
    val sstableFileId: KdbUuid,
    val minKey: KdbHash,
    val maxKey: KdbHash,
    val recordCount: Long,
    val fileSizeBytes: Long,
)

data class WalAppendResult(
    val sequence: Long,
    val segmentOffset: Long,
    val segmentSizeAfterBytes: Long,
)

data class WalRecoverySummary(
    val recordsReplayed: Long,
    val recordsSkippedCorrupt: Long,
    val lastSequence: Long,
    val segmentsScanned: Int,
)

class WalCorruptionException(
    message: String,
    val partitionKey: String,
    val segmentName: String,
    val offset: Long,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}

class WalClosedException(
    message: String,
    val partitionKey: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}
```

-----

## 4. Data Structures

### On-disk physical record layout

Each record is written to the active segment via `PlatformIoShim.appendToSegment` as:

| Field | Size | Description |
|---|---|---|
| `magic` | 4 | `0x4B444257` (`"KDBW"`) |
| `recordLen` | 4 | Big-endian length of bytes following this field until end of record |
| `sequence` | 8 | Big-endian `WalRecord.sequence` |
| `kindOrdinal` | 1 | `WalRecordKind` ordinal |
| `payloadCrc32` | 4 | CRC32-IEEE of `payload` |
| `headerCrc32` | 4 | CRC32-IEEE of all bytes from `magic` through `payloadCrc32` (exclusive of this field) |
| `payload` | `recordLen - 21` | Layer 0 bytes for the record body |

The reader validates `headerCrc32` first, then `payloadCrc32`, then decodes `payload`. A mismatch throws `WalCorruptionException` during `recover` (skippable only when `config.walSkipCorruptRecords` is true; default **false**).

### `WalBatch` (in-memory only)

`appendBatch` concatenates multiple logical records into one `PlatformIoShim` append with a batch header (`magic = 0x4B444242`, record count, per-record sub-headers). Batches are atomic: either the entire batch is replayed or none of it.

### Segment naming

Active segment: `wal/{partitionKey}/{walId}.log`  
Sealed segment after rotation: `wal/{partitionKey}/{walId}.{firstSeq}-{lastSeq}.log.sealed`

`WalSegmentCatalog.listSegments` returns active + sealed, sorted by `firstSequence`.

### `StorageEngineConfig` extensions (owned by 10e, consumed here)

| Field | Default | Use |
|---|---|---|
| `walMaxSegmentBytes` | 64 MiB | Rotate active segment; new segment continues sequence |
| `walSkipCorruptRecords` | false | Recovery policy on checksum failure |

-----

## 5. Contracts

### `WriteAheadLog.append`

- **Pre:** WAL is open; `record.sequence` must be 0 (assigned by implementation).
- **Post:** Returns `WalAppendResult` with assigned `sequence == lastSequence` before call + 1; `segmentOffset` is the byte offset of this record’s `magic` in the active segment.
- **Ordering:** Sequences are strictly monotonic; no gaps except after truncation.
- **Durability:** Record is visible to `recover` only after `sync()` (or platform-equivalent flush) completes.

### `WriteAheadLog.recover`

- **Pre:** May be called on empty WAL → `WalRecoverySummary(recordsReplayed=0)`.
- **Post:** Invokes `handler` in ascending `sequence` order across all non-deleted segments. Sets `lastSequence` to the highest seen.
- **Idempotency:** Safe to call multiple times only if `handler` is idempotent; production calls once at open.

### `WriteAheadLog.truncate`

- **Pre:** `truncateThroughSequence ≤ lastSequence`; caller guarantees all mutations ≤ this sequence are persisted in SSTables (10c) and MemTable (10b) is consistent.
- **Post:** Active segment may be rewritten or rotated; sealed segments wholly below the truncate point are deleted via `WalSegmentCatalog.deleteSegment` + `PlatformIoShim.deleteSegment`.
- **Safety:** Must not remove records with `sequence > truncateThroughSequence`.

### `PlatformIoShim` usage

All byte I/O goes through `appendToSegment`, `readFromSegment`, `flushSegment`, `deleteSegment`. WAL never opens files directly.

-----

## 6. Error Cases

| Exception | When |
|---|---|
| `WalCorruptionException` | Checksum or magic mismatch during `recover` when `walSkipCorruptRecords=false`; or `append` detects torn write on re-open |
| `WalClosedException` | `append`, `appendBatch`, or `truncate` after `close()` |
| `KdbException` (`STORAGE_TIER_ERROR`) | `PlatformIoShim` I/O failure; message wraps platform cause |

-----

## 7. Test Cases

### TC-01 — Append and recover round-trip

**Input:** Open WAL, `append` one `PutBlob` record, `sync`, `close`, re-open, `recover`.  
**Expected:** Handler receives one record; `contentHash` and bytes match; `WalRecoverySummary.recordsReplayed == 1`.

### TC-02 — Monotonic sequences

**Input:** Append 100 records without truncate.  
**Expected:** Sequences 1..100; `lastSequence == 100`.

### TC-03 — Batch append atomicity

**Input:** `appendBatch` of 5 `PutBlob` records, `sync`, recover.  
**Expected:** Exactly 5 records in order; no partial batch on clean replay.

### TC-04 — Truncate removes replayed prefix

**Input:** Append 50 records, flush to SSTable (mock), `truncate(50)`, recover.  
**Expected:** Handler not invoked (or segment empty); `lastSequence` preserved for new appends starting at 51.

### TC-05 — Segment rotation at size cap

**Input:** Append records until `activeSegmentSizeBytes >= walMaxSegmentBytes`.  
**Expected:** New active segment created; recover replays both segments in sequence order.

### TC-06 — Corrupt payload fails recovery (edge)

**Input:** Append valid record, `sync`, manually flip one payload byte in segment via test shim, recover with `walSkipCorruptRecords=false`.  
**Expected:** `WalCorruptionException` with correct `offset`.

### TC-07 — Skip corrupt record mode (edge)

**Input:** Same corruption as TC-06 with `walSkipCorruptRecords=true`.  
**Expected:** `WalRecoverySummary.recordsSkippedCorrupt == 1`; subsequent valid records still replay.

### TC-08 — Append after close throws (error)

**Input:** `close()` then `append`.  
**Expected:** `WalClosedException`.

### TC-09 — Crash before sync not visible (edge)

**Input:** Append without `sync`, simulate new process (new WAL instance), recover.  
**Expected:** Record absent (platform shim may still show bytes; recovery validates checksums and ignores incomplete tail).

### TC-10 — FlushCheckpoint ordering

**Input:** Append blobs 1..3, `FlushCheckpoint`, blob 4; recover.  
**Expected:** Handler order: PutBlob×3, FlushCheckpoint, PutBlob.

-----

## 8. Non-Goals

- **MemTable or SSTable format** — owned by Components 10b and 10c.
- **Delta log segments** — `DeltaSegmentWriter` (10d) is a separate append path; WAL does not store `DeltaRecord`.
- **Transaction / commit DAG logic** — WAL stores physical blob and flush metadata only.
- **Cross-node replication** — wire framing is Layer 7.
- **Browser localStorage** — JS `PlatformIoShim` may be in-memory; WAL still uses the same interface but durability is best-effort on JS (documented in 10g).

-----

## 9. Implementation Notes

### CRC32

Use a `commonMain` CRC32-IEEE implementation (same approach as other modules — no `java.util.zip` in common code). Compute over payload before append; verify before decode on recovery.

### Tail scan on recovery

If the last record’s `recordLen` extends past file size (crash mid-append), treat as end-of-log: stop scanning, do not throw (incomplete record discarded). If `magic` is valid but checksum fails, apply `walSkipCorruptRecords` policy.

### Truncation strategy

Prefer **segment rotation** over in-place rewrite for JVM/Native: write new empty active segment, delete old sealed files below truncate point. In-place rewrite is acceptable for in-memory test shims.

### Sequence assignment

Use an atomic long in memory; persist `lastSequence` in a 8-byte footer at segment seal time so re-open does not reuse sequences after truncate.

### Kotlin Multiplatform

All logic in `commonMain`. Only `PlatformIoShim` crosses the expect/actual boundary (Layer 3 / 10g).

### Integration with 10e

`ServerStorageEngine` opens one WAL per namespace for realized-store block index and one global `"blobs"` WAL for content-addressed storage. `commitTree` path: append WAL records → apply MemTable → on flush, append `FlushCheckpoint` → `truncate` through checkpoint sequence.

-----

## 10. Estimated Lines

| Section | Est. NBNC lines |
|---|---|
| On-disk codec + `WalWireRegistry` | 180 |
| `DefaultWriteAheadLog` append/recover/truncate | 420 |
| `WalSegmentCatalog` + segment rotation | 120 |
| `WriteAheadLogFactory` | 60 |
| Exceptions + result types | 80 |
| Unit tests (in-memory shim, corruption injection) | 450 |
| **Total** | **~1,310** |
