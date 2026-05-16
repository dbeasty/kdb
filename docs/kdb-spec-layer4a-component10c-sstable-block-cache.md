# KDB Component Spec — Layer 4a
## Component 10c: SSTable + Block Cache
### `dev.kdb.storage.sstable`

**File:** `kdb-spec-layer4a-component10c-sstable-block-cache.md`  
**Layer:** 4a — KDB Storage Engine (LSM substrate)  
**Status:** Implementation-ready  
**Depends on:** Layer 0 (Codec, `KdbHash`), Layer 3 (`PlatformIoShim`, `StorageEngineConfig`), Component 10b (MemTable flush source), Component 10a (WAL flush checkpoints reference SSTable files)

-----

## 1. Purpose

SSTables persist immutable sorted runs of `KdbHash` → `ByteArray` entries flushed from the MemTable. Files are **content-addressed**: the file name and `SsTableHandle` identity derive from SHA-256 over the file footer (or canonical header), enabling deduplication and integrity verification. `SsTableReader` serves point lookups and range scans via index blocks; `BlockCache` provides an LRU of decompressed data blocks to avoid repeated disk reads. An optional Bloom filter per table reduces disk access for negative lookups. This module does not implement compaction (10f) but produces and consumes SSTable files in the LSM level structure managed by the storage engine core (10e).

-----

## 2. Dependencies

| Module | Interfaces / types used |
|---|---|
| `dev.kdb.codec` (Layer 0) | `KdbHash`, `KdbUuid`, `encodeToBytes`, `decodeFromBytes` |
| `dev.kdb.error` (Layer 0) | `KdbException`, `KdbErrorCode` |
| `dev.kdb.storage` (Layer 3) | `PlatformIoShim`, `StorageEngineConfig` |
| `dev.kdb.storage.memtable` (4a — 10b) | `MemTableSnapshot`, `MemTableIterator` — flush input |

-----

## 3. Public Interface

```kotlin
package dev.kdb.storage.sstable

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.StorageEngineConfig

/** Handle to one sealed SSTable file on disk. */
data class SsTableHandle(
    /** Content hash of the sealed file (SHA-256 of entire file bytes). */
    val fileHash: KdbHash,
    val fileId: KdbUuid,
    val segmentName: String,
    val minKey: KdbHash,
    val maxKey: KdbHash,
    val entryCount: Long,
    val fileSizeBytes: Long,
    val level: Int,
    val createdAtMillis: Long,
)

/** Byte range of a compressed block within the SSTable file. */
data class BlockHandle(
    val offset: Long,
    val compressedSize: Int,
    val uncompressedSize: Int,
    val blockType: BlockType,
)

enum class BlockType {
    DATA,
    INDEX,
    FILTER, // optional Bloom
    FOOTER,
}

interface SsTableWriter {

    val entriesWritten: Long

    /**
     * Add one entry in **strictly increasing key order** (caller / MemTable iterator
     * must enforce). [value] ignored when [isDeleted] is true (tombstone marker stored).
     */
    suspend fun add(key: KdbHash, value: ByteArray, isDeleted: Boolean = false)

    /** Seal file, compute [SsTableHandle.fileHash], write footer. */
    suspend fun finish(): SsTableHandle
}

interface SsTableReader {

    /**
     * Point lookup. Returns null if key not in table or Bloom filter (if present)
     * excludes key. Tombstones return [SsTableEntry.isDeleted] == true.
     */
    suspend fun get(handle: SsTableHandle, key: KdbHash): SsTableEntry?

    /** Iterator over [minKey, maxKey] inclusive within one table. */
    fun newIterator(handle: SsTableHandle): SsTableIterator

    /** Open handle from on-disk segment (validates footer hash). */
    suspend fun openHandle(segmentName: String): SsTableHandle
}

data class SsTableEntry(
    val key: KdbHash,
    val value: ByteArray?,
    val isDeleted: Boolean,
)

interface SsTableIterator {

    fun seekToFirst()
    fun seek(key: KdbHash)
    fun isValid(): Boolean
    fun key(): KdbHash
    fun entry(): SsTableEntry
    fun next()
}

/**
 * Merges multiple SSTable iterators plus optional MemTable iterators (10b).
 * Newest source first in [sources] wins on duplicate keys.
 */
class MergedIterator(
    sources: List<SsTableIterator>,
) : SsTableIterator

/** LRU cache of uncompressed [BlockHandle] payloads. */
interface BlockCache {

    val capacityBytes: Long
    val usedBytes: Long

    suspend fun get(
        handle: SsTableHandle,
        block: BlockHandle,
        loader: suspend () -> ByteArray,
    ): ByteArray

    fun invalidate(handle: SsTableHandle)
    fun clear()
}

interface BlockCacheFactory {
    fun create(capacityBytes: Long): BlockCache
}

data class SsTableWriterConfig(
    val dataBlockSizeBytes: Int = 64 * 1024,
    val compression: SsTableCompression = SsTableCompression.ZSTD,
    val bloomBitsPerKey: Int = 10, // 0 = disable Bloom
    val segmentNamePrefix: String = "sst",
)

enum class SsTableCompression { NONE, ZSTD }

interface SsTableWriterFactory {
    suspend fun newWriter(
        fileId: KdbUuid,
        level: Int,
        config: SsTableWriterConfig,
        engineConfig: StorageEngineConfig,
        ioShim: PlatformIoShim,
    ): SsTableWriter
}

class SsTableCorruptionException(
    message: String,
    val fileHash: KdbHash?,
    val segmentName: String,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}

class SsTableKeyOrderException(
    message: String,
    val previousKey: KdbHash,
    val key: KdbHash,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}
```

-----

## 4. Data Structures

### File layout (version 1)

```
[ data block 0 ][ data block 1 ] ... [ index block ][ filter block? ][ footer ]
```

| Block | Content |
|---|---|
| **Data block** | Layer 0 or raw KV pairs: repeated `(keyLen, key, valLen, value, tombstoneFlag)` sorted within block |
| **Index block** | `(firstKeyInBlock, BlockHandle)` entries pointing to data blocks |
| **Filter block** | Optional Bloom filter over all keys (SipHash-style probes, `bloomBitsPerKey`) |
| **Footer** | `version`, `indexHandle`, `filterHandle?`, `entryCount`, `minKey`, `maxKey`, `fileId`, `footerCrc32` |

Each stored block on disk: `compressedSize (4 BE) | uncompressedSize (4 BE) | crc32 (4) | zstd/none payload`.

Segment name: `{segmentNamePrefix}/{level}/{fileId}.sst` written via `PlatformIoShim.appendToSegment` until `sealSegment`.

### `SsTableHandle.fileHash`

After `finish()`, SHA-256 over the complete sealed segment bytes (read back via shim). Used as `StorageAdapter.readBlob` / `writeBlob` content address when values are blob payloads.

### `BlockCache` entry key

`(fileHash, block.offset)` — invalidation on compaction deletes old handles and calls `invalidate`.

### `MergedIterator` heap

Min-heap keyed by current head key across sources; on duplicate keys, pop all equal heads and retain only the first popped (newest source listed first in `sources`).

-----

## 5. Contracts

### `SsTableWriter.add`

- **Pre:** Keys strictly increasing vs previous `add`; table not finished.
- **Post:** `entriesWritten` incremented; data block rotated when uncompressed size ≥ `dataBlockSizeBytes`.
- **Violation:** `SsTableKeyOrderException` if `key <= previousKey`.

### `SsTableWriter.finish`

- **Post:** Returns `SsTableHandle` with correct `minKey`, `maxKey`, `entryCount`, `fileSizeBytes`, `fileHash`. Segment sealed via `ioShim.sealSegment`.
- **Idempotency:** Second `finish` throws `KdbException` (illegal state).

### `SsTableReader.get`

- **Pre:** `handle` file exists.
- **Post:** If Bloom present and key definitely absent → null without reading data blocks. If key in range `[minKey, maxKey]` and present → entry; else null.
- **Tombstones:** `isDeleted == true`, `value == null`.

### `BlockCache.get`

- **Post:** Returns uncompressed block bytes; on miss calls `loader` exactly once per cache miss (per-key synchronization on `(fileHash, offset)`).
- **Eviction:** LRU by uncompressed byte size; evict until `usedBytes <= capacityBytes`.

### `MergedIterator`

- **Post:** Keys strictly increasing in output; one entry per key (newest wins).

-----

## 6. Error Cases

| Exception | When |
|---|---|
| `SsTableCorruptionException` | Footer CRC mismatch, block CRC mismatch, or `fileHash` mismatch on open |
| `SsTableKeyOrderException` | Out-of-order `add` during write |
| `KdbException` (`STORAGE_TIER_ERROR`) | `PlatformIoShim` failure during read/write |

-----

## 7. Test Cases

### TC-01 — Write and read round-trip

**Input:** Writer adds 3 keys in order, `finish`, `reader.get` each key.  
**Expected:** Values match; `fileHash` stable across reopen.

### TC-02 — Tombstone round-trip

**Input:** `add(key, value, isDeleted=true)`, finish, get.  
**Expected:** `SsTableEntry.isDeleted == true`.

### TC-03 — Out-of-order key throws (error)

**Input:** `add(k2)`, then `add(k1)` where `k1 < k2`.  
**Expected:** `SsTableKeyOrderException`.

### TC-04 — Iterator range scan

**Input:** 100 sequential keys, iterator from first to last.  
**Expected:** 100 entries, sorted order, no gaps.

### TC-05 — BlockCache hit avoids second load

**Input:** Mock loader counting invocations; two `get` same block.  
**Expected:** Loader called once; `usedBytes` reflects one block.

### TC-06 — BlockCache eviction (edge)

**Input:** `capacityBytes` = one block size; load two distinct blocks.  
**Expected:** First block evicted; second load calls loader again.

### TC-07 — Bloom negative lookup (edge)

**Input:** Table with Bloom; `get` key not inserted.  
**Expected:** null without reading data blocks (verify via instrumented shim read count).

### TC-08 — Corrupt footer (error)

**Input:** Finish table, flip footer byte, `openHandle`.  
**Expected:** `SsTableCorruptionException`.

### TC-09 — MergedIterator newest wins

**Input:** Two handles, same key different values; sources ordered newest-first.  
**Expected:** Iterator yields newest value once.

### TC-10 — Empty table

**Input:** Writer with no `add`, `finish`.  
**Expected:** `entryCount == 0`; `get` any key returns null; `minKey`/`maxKey` are sentinel empty hashes (all-zero `KdbHash`).

-----

## 8. Non-Goals

- **Compaction / level merging** — Component 10f.
- **MemTable mutation** — Component 10b.
- **Delta segments** — Component 10d (separate file format).
- **SQL or index structures** — SSTable stores opaque bytes only.
- **GPU columnar layout** — GPU engine ingests delta segments directly (Layer 3 `ingestDeltaSegment`).

-----

## 9. Implementation Notes

### Compression

Default ZSTD level tuned for speed (level 3). `StorageEngineConfig.compressionCodec` from Layer 3 applies to delta segments; SSTable uses `SsTableWriterConfig.compression` independently (both default ZSTD).

### BlockCache sizing

Default `capacityBytes = min(256 MiB, StorageEngineConfig.globalMemoryBudgetBytes / 4)` — configured in 10e when constructing engine.

### Index block two-level lookup

For files &gt; 1 data block, use root index block in footer pointing to leaf index blocks if needed (standard LSM). v1 may use single index block up to 4 MiB.

### Content-hash file naming

After seal, optionally hard-link segment name to hex(`fileHash`) for deduplication; `segmentName` in handle remains canonical path.

### Platform I/O

All reads/writes via `PlatformIoShim`. `readFromSegment(segmentName, offset, length)` for block loads.

### Kotlin Multiplatform

`commonMain` only; zstd via shared expect/actual or pure Kotlin wrapper used elsewhere in repo.

-----

## 10. Estimated Lines

| Section | Est. NBNC lines |
|---|---|
| `SsTableWriter` + block builder | 380 |
| `SsTableReader` + index/filter | 340 |
| Bloom filter (optional) | 120 |
| `BlockCache` LRU | 150 |
| `MergedIterator` | 100 |
| Footer codec + corruption checks | 140 |
| Tests | 520 |
| **Total** | **~1,750** |
