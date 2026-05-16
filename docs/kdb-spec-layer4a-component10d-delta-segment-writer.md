# KDB Component Spec — Layer 4a
## Component 10d: Delta Segment Writer
### `dev.kdb.storage.delta`

**File:** `kdb-spec-layer4a-component10d-delta-segment-writer.md`  
**Layer:** 4a — KDB Storage Engine  
**Status:** Implementation-ready  
**Depends on:** Layer 0 (Codec — `DeltaRecord` Layer 0 wire shape), Layer 1 (`KdbCommit` payload bytes via `commitPayload`), Layer 3 (Component 9 — `DeltaRecord`, `DeltaSegmentWriter`, `DeltaSegmentReader`, `DeltaSegmentRef`, `PlatformIoShim`, `StorageEngineConfig`, `CompressionCodec`)

-----

## 1. Purpose

This module provides the **concrete implementation** of Layer 3’s `DeltaSegmentWriter` and `DeltaSegmentReader` interfaces. It frames append-only delta pages on disk (or in-memory segments via `PlatformIoShim`), Layer 0–encodes each `DeltaRecord`, compresses with zstd (per `StorageEngineConfig.compressionCodec`), and seals a segment when `currentSizeBytes` reaches `pageTargetSizeBytes` (8–16 MB design v3). Sealed segments surface as `DeltaSegmentRef` for rebuild, peer sync, and GPU direct ingest. Page boundaries enable forward scan, corruption isolation, and alignment with the storage engine’s large-page delta log design.

-----

## 2. Dependencies

| Module | Interfaces / types used |
|---|---|
| `dev.kdb.codec` (Layer 0) | `KdbHash`, `KdbUuid`, `KdbTimestamp`, `encodeToBytes`, `decodeFromBytes`, `KdbTypeRegistry` |
| `dev.kdb.document` (Layer 1) | `KdbDocument`, `DocumentPatch` — carried inside `DeltaRecord`; commit bytes are opaque `commitPayload` |
| `dev.kdb.error` (Layer 0) | `KdbException`, `KdbErrorCode`, `DeltaSegmentSealedException` (Layer 3) |
| `dev.kdb.storage` (Layer 3) | `DeltaRecord`, `DeltaAuthorshipEnvelope`, `DocumentPatch`, `DeltaSegmentWriter`, `DeltaSegmentReader`, `DeltaSegmentRef`, `CompressionCodec`, `PlatformIoShim`, `StorageEngineConfig` |

-----

## 3. Public Interface

```kotlin
package dev.kdb.storage.delta

import dev.kdb.codec.*
import dev.kdb.document.DocumentPatch
import dev.kdb.error.*
import dev.kdb.storage.*

/** Layer 0 wire registry for [DeltaRecord], [DocumentPatch], [DeltaAuthorshipEnvelope]. */
fun DeltaWireRegistry(): KdbTypeRegistry

/**
 * Production [DeltaSegmentWriter] — one open segment per namespace writer instance.
 * Segment file: `delta/{namespaceId}/{segmentId}.seg` via [PlatformIoShim].
 */
class DefaultDeltaSegmentWriter(
    override val namespaceId: String,
    override val segmentId: KdbUuid,
    private val config: StorageEngineConfig,
    private val ioShim: PlatformIoShim,
) : DeltaSegmentWriter {

    override val currentSizeBytes: Long
    override val isSealed: Boolean

    override suspend fun append(record: DeltaRecord): Long
    override suspend fun flush()
    override suspend fun seal(): DeltaSegmentRef
}

/**
 * Production [DeltaSegmentReader] for sealed (and test-only open) segments.
 */
class DefaultDeltaSegmentReader(
    override val namespaceId: String,
    private val config: StorageEngineConfig,
    private val ioShim: PlatformIoShim,
) : DeltaSegmentReader {

    override suspend fun readAll(segment: DeltaSegmentRef): List<DeltaRecord>
    override suspend fun readRange(
        segment: DeltaSegmentRef,
        sinceCommit: KdbHash,
        untilCommit: KdbHash,
    ): List<DeltaRecord>
    override suspend fun listSegments(): List<DeltaSegmentRef>
}

/** Factory for namespace-scoped writers/readers. */
interface DeltaSegmentFactory {

    suspend fun openWriter(namespaceId: String): DefaultDeltaSegmentWriter

    fun openReader(namespaceId: String): DefaultDeltaSegmentReader
}

/**
 * Page builder used internally; exposed for testing page seal boundaries.
 */
interface DeltaPageBuilder {

    val pageIndex: Int
    val uncompressedBytesInPage: Long

    /** Returns false if record does not fit in current page (caller seals page and retries). */
    fun tryAppendEncodedRecord(encodedRecord: ByteArray, commitHash: KdbHash): Boolean

    /** Seal current page; returns compressed page bytes ready for segment append. */
    fun sealPage(): DeltaPageFrame

    fun reset()
}

/** One sealed page within a segment. */
data class DeltaPageFrame(
    val pageIndex: Int,
    val recordCount: Int,
    val firstCommitHash: KdbHash,
    val lastCommitHash: KdbHash,
    val uncompressedSize: Int,
    val compressedPayload: ByteArray,
    val compression: CompressionCodec,
)

/** Parsed on-disk page header (see §4). */
data class DeltaPageHeader(
    val magic: Int,
    val pageIndex: Int,
    val recordCount: Int,
    val uncompressedSize: Int,
    val compressedSize: Int,
    val firstCommitHash: KdbHash,
    val lastCommitHash: KdbHash,
    val payloadCrc32: Int,
)

class DeltaSegmentCodec(
    registry: KdbTypeRegistry = DeltaWireRegistry(),
) {
    fun encodeRecord(record: DeltaRecord): ByteArray
    fun decodeRecord(bytes: ByteArray): DeltaRecord
    fun encodePage(frames: List<DeltaPageFrame>): ByteArray // test helper
}

class DeltaPageCorruptionException(
    message: String,
    val namespaceId: String,
    val segmentId: KdbUuid,
    val pageIndex: Int,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}

class DeltaRecordTooLargeException(
    message: String,
    val recordBytes: Int,
    val maxPageBytes: Long,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}
```

-----

## 4. Data Structures

### Segment lifecycle

| State | `isSealed` | `append` | On-disk |
|---|---|---|---|
| Open | false | allowed | active `.seg` via shim |
| Sealed | true | throws `DeltaSegmentSealedException` | sealed; metadata in segment catalog |

`currentSizeBytes` = sum of all appended page frame sizes (compressed on disk).

### Per-record logical layout (before page packing)

Per Layer 3 implementation notes:

1. **Plaintext prefix** (for scan without full decode): `commitHash` (32 bytes), `recordLen` (4 BE).
2. **Body:** Layer 0 `DeltaRecord` bytes (`commitPayload`, `documentPatches`, `authorship`, `namespaceId`).
3. Records are packed into pages until adding the next record would exceed `pageTargetSizeBytes` uncompressed; then `DeltaPageBuilder.sealPage()` runs.

### On-disk page frame

| Field | Size | Description |
|---|---|---|
| `magic` | 4 | `0x4B444250` (`"KDBP"`) |
| `pageIndex` | 4 | BE, 0-based within segment |
| `recordCount` | 4 | BE |
| `uncompressedSize` | 4 | BE |
| `compressedSize` | 4 | BE |
| `firstCommitHash` | 32 | First record in page |
| `lastCommitHash` | 32 | Last record in page |
| `payloadCrc32` | 4 | CRC32 of compressed payload |
| `compressedPayload` | `compressedSize` | zstd or none per `config.compressionCodec` |

Inside **compressed payload** (uncompressed view):

Repeated: `[ commitHash 32 ][ recordLen 4 BE ][ layer0Bytes recordLen ]`

### `DeltaSegmentRef` (on `seal()`)

| Field | Source |
|---|---|
| `segmentId` | writer’s `segmentId` |
| `namespaceId` | writer’s `namespaceId` |
| `firstCommitHash` | first record appended, or zero hash if empty |
| `lastCommitHash` | last record appended |
| `sizeBytes` | `currentSizeBytes` (compressed total) |
| `compressionCodec` | `config.compressionCodec` |

### Seal triggers

- **Explicit:** `seal()` called by storage engine / compaction.
- **Implicit roll:** `currentSizeBytes >= config.pageTargetSizeBytes` after page seal — writer returns new segment id from factory (10e policy); current writer must `seal()` before roll.
- **Hard cap:** Must not exceed `config.pageMaxSizeBytes`; force `sealPage()` + `seal()` if next record would exceed.

### `readRange`

Linear scan pages in order; within each page, skip records until `commitHash >= sinceCommit`, emit until `commitHash > untilCommit`. Commit hash order follows append order (not necessarily total order of hash bytes).

-----

## 5. Contracts

### `DefaultDeltaSegmentWriter.append`

- **Pre:** `!isSealed`; `record.namespaceId == namespaceId`.
- **Post:** Returns byte offset of record’s start in segment (offset of page frame start + intra-page offset). Updates `currentSizeBytes`.
- **Encoding:** Uses `DeltaSegmentCodec.encodeRecord`; does not mutate caller’s `DeltaRecord` byte arrays.
- **Post (sealed):** `DeltaSegmentSealedException` (Layer 3).

### `DefaultDeltaSegmentWriter.flush`

- **Post:** All buffered page data appended via `ioShim.appendToSegment`; `ioShim.flushSegment` called.

### `DefaultDeltaSegmentWriter.seal`

- **Post:** `isSealed == true`; `ioShim.sealSegment`; returns accurate `DeltaSegmentRef`. Further `append` throws.
- **Empty segment:** Allowed; `firstCommitHash` / `lastCommitHash` are zero `KdbHash`; `sizeBytes == 0`.

### `DefaultDeltaSegmentReader.readAll`

- **Post:** Records in append order across all pages; full decode after decompress + CRC verify.
- **Corruption:** `DeltaPageCorruptionException` unless test flag allows skip (not in v1 production).

### `DefaultDeltaSegmentReader.readRange`

- **Pre:** `sinceCommit` and `untilCommit` refer to commits present in segment (caller's responsibility).
- **Post:** Inclusive range on commit hash equality per record’s `commitHash` field.

### `listSegments`

- **Post:** All sealed segments for `namespaceId`, oldest first (by `firstCommitHash` append order / segment creation time).

### Layer 3 interface fidelity

`DefaultDeltaSegmentWriter` and `DefaultDeltaSegmentReader` **must** be usable wherever Layer 3 specifies `DeltaSegmentWriter` / `DeltaSegmentReader` with no widening of the public API.

-----

## 6. Error Cases

| Exception | When |
|---|---|
| `DeltaSegmentSealedException` (Layer 3) | `append` after `seal()` |
| `DeltaPageCorruptionException` | Page magic, size, or CRC mismatch on read |
| `DeltaRecordTooLargeException` | Single encoded record &gt; `pageMaxSizeBytes` (cannot fit any page) |
| `KdbException` (`KDB_DECODE_ERROR`) | Layer 0 decode failure on record body |
| `KdbException` (`STORAGE_TIER_ERROR`) | `PlatformIoShim` I/O error |

-----

## 7. Test Cases

### TC-01 — Append, seal, readAll round-trip

**Input:** 3 distinct `DeltaRecord` instances with patches, append, seal, `readAll`.  
**Expected:** List equals inputs (commit hash, patches, authorship); order preserved.

### TC-02 — Page seal at target size (edge)

**Input:** `pageTargetSizeBytes = 4KiB` (test config), append records until page rolls.  
**Expected:** Multiple pages in segment; `readAll` still returns all records in order.

### TC-03 — append after seal throws (error)

**Input:** `seal()` then `append`.  
**Expected:** `DeltaSegmentSealedException`.

### TC-04 — readRange inclusive bounds

**Input:** 5 records with known commit hashes C1..C5; `readRange(C2, C4)`.  
**Expected:** Exactly C2, C3, C4.

### TC-05 — zstd round-trip

**Input:** `compressionCodec = ZSTD`, large `commitPayload`.  
**Expected:** `DeltaSegmentRef.compressionCodec == ZSTD`; decoded payload bit-identical.

### TC-06 — Corrupt page CRC (edge)

**Input:** Seal segment, flip CRC byte, `readAll`.  
**Expected:** `DeltaPageCorruptionException` with `pageIndex`.

### TC-07 — Record larger than max page

**Input:** Single record encoded size &gt; `pageMaxSizeBytes`.  
**Expected:** `DeltaRecordTooLargeException` on append (no partial write).

### TC-08 — Empty segment seal

**Input:** Open writer, immediate `seal`, `readAll`.  
**Expected:** Empty list; valid `DeltaSegmentRef` with `sizeBytes == 0`.

### TC-09 — listSegments ordering

**Input:** Seal two segments A then B for same namespace.  
**Expected:** `listSegments()` returns `[A, B]` oldest-first.

### TC-10 — Plaintext commit hash scan

**Input:** Append record with known hash; read raw page without full Layer 0 decode of body.  
**Expected:** Prefix scan finds `commitHash` at expected intra-page offset (unit test on codec layout).

### TC-11 — flush durability

**Input:** Append, `flush` without seal, new reader instance `readAll` on same open segment (test shim persists).  
**Expected:** Records visible.

-----

## 8. Non-Goals

- **WAL / MemTable / SSTable** — separate LSM path for blobs (10a–c).
- **Rights validation** on `DeltaAuthorshipEnvelope` — Layer 3: stored verbatim only.
- **Compaction / segment GC** — Component 10f and Layer 6.
- **Wire protocol framing** for peer sync — Layer 7; this module reads/writes local segments only.
- **GPU decompression kernels** — GPU engine consumes `DeltaSegmentRef` via this reader in CPU commonMain.

-----

## 9. Implementation Notes

### Layer 0 encoding

Register `DeltaRecord`, `DocumentPatch`, and `DeltaAuthorshipEnvelope` in `DeltaWireRegistry()` with stable field ordinals per `kdb-spec-layer0-codec.md`. Never use JSON or BSON on the wire.

### Compression

When `CompressionCodec.NONE`, store uncompressed payload with `compressedSize == uncompressedSize`. Default from `StorageEngineConfig` is ZSTD.

### `pageTargetSizeBytes` vs `pageMaxSizeBytes`

- **Target:** soft page seal — start new page when next record would exceed target uncompressed bytes in current page.
- **Max:** hard segment cap — if entire segment compressed size approaches `pageMaxSizeBytes`, call `seal()` and let 10e open a new `segmentId`.

### Browser / in-memory shim

JS `PlatformIoShim` keeps segments in memory; format identical so tests and server share codec. Browser does not persist delta log durably (`persistsDeltaLog = false`); implementation still honors interface for tests.

### `append` return offset

Offset is opaque to callers but must be stable for debugging; defined as absolute byte offset in segment file of the record’s `commitHash` prefix.

### Integration

- **Transaction Engine** produces `DeltaRecord` per commit; storage engine calls `append`.
- **Rebuild** (`EvictableStorageAdapter.rebuildDocuments`) uses `DeltaSegmentReader.readAll` / `readRange`.
- **GPU ingest** uses `readAll` on `DeltaSegmentRef` passed to `ingestDeltaSegment`.

### Kotlin Multiplatform

All codec and framing in `commonMain`. zstd via shared compression helper (same as snapshots in Layer 3 notes).

-----

## 10. Estimated Lines

| Section | Est. NBNC lines |
|---|---|
| `DeltaWireRegistry` + `DeltaSegmentCodec` | 220 |
| `DeltaPageBuilder` + page frame codec | 200 |
| `DefaultDeltaSegmentWriter` | 280 |
| `DefaultDeltaSegmentReader` + scan | 240 |
| `DeltaSegmentFactory` | 60 |
| Exceptions | 50 |
| Tests (round-trip, corruption, page sizing) | 480 |
| **Total** | **~1,530** |
