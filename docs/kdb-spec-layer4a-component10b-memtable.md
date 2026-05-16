# KDB Component Spec — Layer 4a
## Component 10b: MemTable
### `dev.kdb.storage.memtable`

**File:** `kdb-spec-layer4a-component10b-memtable.md`  
**Layer:** 4a — KDB Storage Engine (LSM substrate)  
**Status:** Implementation-ready  
**Depends on:** Layer 0 (Codec), Layer 3 (Component 9 — `KdbHash`, `StorageEngineConfig`), Component 10a (WAL — flush checkpoints), Component 10c (SSTable — merge-on-read)

-----

## 1. Purpose

The MemTable is the in-memory sorted mutable map that absorbs recent writes to the content-addressed blob store before they are flushed into immutable SSTables. Keys are `KdbHash` values (content hash for blobs; composite hashes for realized-store index entries as defined by the storage engine core). Reads consult the MemTable first, then overlay SSTable levels, with newer layers winning. When the MemTable exceeds a configurable byte or entry threshold, it is frozen, flushed to disk via `SsTableWriter` (10c), and replaced by an empty active table while the WAL records a `FlushCheckpoint` (10a).

-----

## 2. Dependencies

| Module | Interfaces / types used |
|---|---|
| `dev.kdb.codec` (Layer 0) | `KdbHash`, `KdbUuid`, `ByteArray` helpers |
| `dev.kdb.error` (Layer 0) | `KdbException`, `KdbErrorCode` |
| `dev.kdb.storage` (Layer 3) | `StorageEngineConfig` |
| `dev.kdb.storage.wal` (Layer 4a — 10a) | `WalFlushCheckpoint`, `WalRecordKind` — emitted after flush |
| `dev.kdb.storage.sstable` (Layer 4a — 10c) | `SsTableWriter`, `SsTableReader`, `SsTableHandle`, `MergedIterator` — flush target and read path |

-----

## 3. Public Interface

```kotlin
package dev.kdb.storage.memtable

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.sstable.MergedIterator
import dev.kdb.storage.sstable.SsTableHandle
import dev.kdb.storage.sstable.SsTableReader
import dev.kdb.storage.sstable.SsTableWriter

/**
 * In-memory sorted table. Thread-safety: external synchronization by the
 * storage engine (single-writer); concurrent reads require snapshot iterator.
 */
interface MemTable {

    /** Approximate uncompressed bytes held (keys + values). */
    val estimatedSizeBytes: Long

    val entryCount: Long

    /** True after [freeze]; no further [put] / [delete]. */
    val isFrozen: Boolean

    /** Insert or overwrite [key] → [value]. Tombstone via [delete]. */
    fun put(key: KdbHash, value: ByteArray)

    /** Mark [key] deleted; visible to iterators until compacted away. */
    fun delete(key: KdbHash)

    /** Point lookup in this table only (no SSTable). */
    fun get(key: KdbHash): MemTableEntry?

    /**
     * Freeze for flush. Returns an immutable snapshot view sharing structure
     * (copy-on-write or frozen tree) until [SsTableWriter.finish] completes.
     */
    fun freeze(): MemTableSnapshot

    /** Discard all entries (called on new active table after flush). */
    fun clear()
}

/** Immutable view of a frozen MemTable used during flush. */
interface MemTableSnapshot {

    val estimatedSizeBytes: Long
    val entryCount: Long

    fun iterator(): MemTableIterator
}

interface MemTableIterator {

    fun seekToFirst()
    fun seek(key: KdbHash)
    fun isValid(): Boolean
    fun key(): KdbHash
    fun value(): ByteArray
    /** True if this entry is a tombstone. */
    fun isDeleted(): Boolean
    fun next()
}

data class MemTableEntry(
    val key: KdbHash,
    val value: ByteArray?,
    val isDeleted: Boolean,
)

/**
 * Manages active + optional frozen MemTables and coordinates flush to SSTable.
 */
interface MemTableManager {

    val active: MemTable

    /** Frozen table awaiting or undergoing flush; null if none. */
    val pendingFlush: MemTableSnapshot?

    /**
     * Write path. May trigger [maybeFlush] when thresholds exceeded.
     * Returns true if a flush was started (async completion via [awaitFlush]).
     */
    suspend fun put(key: KdbHash, value: ByteArray): Boolean

    suspend fun delete(key: KdbHash): Boolean

    /**
     * Read path: active MemTable → pending flush snapshot (if any) → [sstables]
     * merged by [MergedIterator] (newest wins for same key).
     */
    suspend fun get(
        key: KdbHash,
        sstables: List<SsTableHandle>,
        sstableReader: SsTableReader,
    ): MemTableEntry?

    /** Full merge iterator over mem + SSTable layers (ascending key order). */
    fun newMergedIterator(
        sstables: List<SsTableHandle>,
        sstableReader: SsTableReader,
    ): MergedIterator

    /** If [active] exceeds config thresholds, freeze and begin flush. */
    suspend fun maybeFlush(writer: SsTableWriter): Boolean

    /** Suspend until in-flight flush completes. No-op if none. */
    suspend fun awaitFlush()

    /** Replace active table after successful flush. */
    suspend fun rotateActive()
}

data class MemTableConfig(
    /** Flush when estimated bytes ≥ this. Default: 4 MiB. */
    val flushThresholdBytes: Long = 4L * 1024 * 1024,
    /** Flush when entry count ≥ this. Default: 10_000. */
    val flushThresholdEntries: Long = 10_000,
    /** Stop accepting writes when frozen + active would exceed budget. */
    val maxTotalBytes: Long = 32L * 1024 * 1024,
)

fun MemTableConfig.fromEngine(config: StorageEngineConfig): MemTableConfig

/** Factory for [MemTable] implementations. */
fun newMemTable(config: MemTableConfig = MemTableConfig()): MemTable

class MemTableFrozenException(message: String) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}

class MemTableBudgetExceededException(
    message: String,
    val estimatedBytes: Long,
    val maxBytes: Long,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}
```

-----

## 4. Data Structures

### Internal ordering

Entries are stored in a **sorted map keyed by `KdbHash`**, using unsigned lexicographic order of `KdbHash.bytes` (32-byte SHA-256 digest). This matches SSTable key order so flush is a single sequential scan without re-sorting.

### `MemTableEntry`

| Field | Type | Notes |
|---|---|---|
| `key` | `KdbHash` | Content hash or engine-defined composite hash |
| `value` | `ByteArray?` | Null when `isDeleted == true` |
| `isDeleted` | `Boolean` | Tombstone; propagated to SSTable as deleted marker |

### Flush lifecycle states (`MemTableManager`)

```
ACTIVE_ONLY → FLUSH_PENDING → FLUSHING → ACTIVE_ONLY (new empty active)
```

- **ACTIVE_ONLY:** writes go to `active`.
- **FLUSH_PENDING:** `active` frozen; new empty `active` accepts writes; frozen snapshot flushed in background.
- **FLUSHING:** `SsTableWriter` consuming snapshot iterator.

At most one frozen snapshot exists per manager (no overlapping flushes in v1).

### Merge precedence (read path)

For key `K`:

1. `active.get(k)` if present → return (wins over all).
2. Else scan `pendingFlush` snapshot if not yet installed in SSTables.
3. Else `MergedIterator` across SSTables newest-to-oldest (level 0 = most recent flush).

Deleted entries in a newer layer mask older values.

### `MemTableConfig.fromEngine`

Maps `StorageEngineConfig.globalMemoryBudgetBytes` heuristically: `flushThresholdBytes = min(4 MiB, budget/16)`, `maxTotalBytes = min(32 MiB, budget/4)`.

-----

## 5. Contracts

### `MemTable.put` / `delete`

- **Pre:** Table not frozen.
- **Post:** `get(key)` returns the written entry; `estimatedSizeBytes` increases (tombstones count key size only).
- **Post (frozen):** Throws `MemTableFrozenException`.

### `MemTable.freeze`

- **Post:** `isFrozen == true`; subsequent `put`/`delete` throw. Iterator reflects stable snapshot.

### `MemTableManager.get`

- **Post:** Returns the newest visible entry for `key` per merge precedence; null if absent in all layers.
- **Consistency:** Does not block on flush; may read pending snapshot and SSTables that temporarily both contain the same key — newer layer wins.

### `MemTableManager.maybeFlush`

- **Pre:** `writer` not finished.
- **Trigger:** `active.estimatedSizeBytes >= flushThresholdBytes` OR `active.entryCount >= flushThresholdEntries`.
- **Post:** If triggered, `pendingFlush` is set, new active table is empty, flush writes all snapshot entries in key order via `writer.add(key, value, isDeleted)`.
- **Post (finish):** Caller (10e) calls `writer.finish()`, appends WAL `FlushCheckpoint`, `wal.truncate`, then `rotateActive()`.

### `newMergedIterator`

- **Post:** Iterator yields keys in ascending `KdbHash` order with deduplication (newest entry per key).
- **Tombstones:** Exposed to caller; storage engine may filter deleted keys for `readBlob`.

-----

## 6. Error Cases

| Exception | When |
|---|---|
| `MemTableFrozenException` | `put` / `delete` on frozen table |
| `MemTableBudgetExceededException` | Active + frozen estimated bytes > `maxTotalBytes` before flush can relieve pressure |
| `KdbException` (`STORAGE_TIER_ERROR`) | Flush I/O failure from `SsTableWriter` (propagated) |

-----

## 7. Test Cases

### TC-01 — Put and get round-trip

**Input:** `put(hash, bytes)`, `get(hash)` on active only.  
**Expected:** Entry with equal bytes; `isDeleted == false`.

### TC-02 — Delete tombstone

**Input:** `put`, `delete`, `get`.  
**Expected:** `MemTableEntry.isDeleted == true`, `value == null`.

### TC-03 — Iterator key order

**Input:** Insert keys `0x03`, `0x01`, `0x02` (distinct hash prefixes).  
**Expected:** Iterator yields `01`, `02`, `03`.

### TC-04 — Freeze blocks writes

**Input:** `freeze()`, then `put`.  
**Expected:** `MemTableFrozenException`.

### TC-05 — Flush threshold triggers maybeFlush

**Input:** `MemTableConfig(flushThresholdEntries=3)`, three puts, `maybeFlush`.  
**Expected:** Returns true; `pendingFlush != null`; new `active.entryCount == 0`.

### TC-06 — Merge read: MemTable over SSTable (edge)

**Input:** SSTable has `key=A, value=old`; MemTable has `key=A, value=new`.  
**Expected:** `MemTableManager.get` returns `new`.

### TC-07 — Merge read: tombstone hides SSTable (edge)

**Input:** SSTable has `key=A`; MemTable `delete(A)`.  
**Expected:** `get` returns deleted entry or null per engine filter; merged iterator shows deleted.

### TC-08 — Budget exceeded throws (error)

**Input:** `maxTotalBytes=100`, fill active beyond 100 without flush.  
**Expected:** `MemTableBudgetExceededException`.

### TC-09 — MergedIterator deduplicates three layers

**Input:** SSTable L0: `A=1`; L1: `A=2`; MemTable: `A=3`.  
**Expected:** Single `A` with value 3 in iteration order.

### TC-10 — Pending flush visible until SSTable installed

**Input:** Freeze + flush in progress; `get` before `rotateActive`.  
**Expected:** Reads still see frozen snapshot values for keys not yet in SSTable list.

-----

## 8. Non-Goals

- **WAL record encoding** — Component 10a.
- **SSTable block layout and cache** — Component 10c.
- **Compaction of SSTable levels** — Component 10f.
- **Document / `KdbDocument` semantics** — MemTable stores opaque `ByteArray` values keyed by hash.
- **Concurrent lock-free writes** — single-writer assumption; 10e provides mutex.

-----

## 9. Implementation Notes

### Data structure choice

Use a sorted `MutableMap` backed by `TreeMap`-style ordering via a portable sorted dictionary (e.g. `kotlinx.collections.immutable` persistent map or custom red-black tree in commonMain). Avoid `java.util.TreeMap` in common code.

### Size estimation

`estimatedSizeBytes = sum(key.bytes.size + (value?.size ?: 0) + 16)` per entry (16 = overhead fudge). Used only for thresholds, not exact accounting.

### Flush without blocking reads

After freeze, point `active` at a new empty table immediately so writes continue. Serve reads from `active` + `pendingFlush` + SSTables until flush completes and handle is added to SSTable list.

### Integration with `readBlob`

`StorageAdapter.writeBlob` → WAL `PutBlob` → `MemTableManager.put(contentHash, bytes)`. `readBlob` → `MemTableManager.get` with SSTable list for namespace/blob partition.

### Kotlin Multiplatform

Pure `commonMain`. No platform types.

-----

## 10. Estimated Lines

| Section | Est. NBNC lines |
|---|---|
| `SortedMemTable` + iterator | 280 |
| `MemTableManager` + flush orchestration | 220 |
| Merge helpers / `MergedIterator` glue | 120 |
| Config + factory | 40 |
| Exceptions | 30 |
| Tests | 380 |
| **Total** | **~1,070** |
