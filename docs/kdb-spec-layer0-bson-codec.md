# KDB Component Spec — Layer 0: BSON Codec

**Version:** 0.1  
**Layer:** 0 (Foundation — no dependencies)  
**Module:** `commonMain/codec/BsonCodec`

-----

## 1. Purpose

The BSON Codec serialises and deserialises all KDB internal data structures to and from BSON binary format (bsonspec.org, Apache 2.0). It is the single binary representation used across all three storage tiers (hot/warm/cold/ice) and the peer wire protocol. All other KDB modules that need binary I/O call this module; nothing else in the engine touches raw bytes for structured data.

-----

## 2. Dependencies

None. Layer 0 has no KDB dependencies. This module depends only on Kotlin stdlib and `kotlinx.io` for byte-level I/O primitives available in `commonMain`.

-----

## 3. Public Interface

```kotlin
package dev.kdb.codec

import kotlinx.io.Buffer
import kotlinx.io.Sink
import kotlinx.io.Source

// ── Top-level entry points ────────────────────────────────────────────────────

/**
 * Encode a BsonDocument to bytes.
 * The returned ByteArray is a complete, self-contained BSON document.
 */
fun BsonDocument.toBytes(): ByteArray

/**
 * Encode a BsonDocument into an existing Sink (zero-copy on JVM/native).
 */
fun BsonDocument.writeTo(sink: Sink)

/**
 * Decode a complete BSON document from a ByteArray.
 * @throws BsonDecodeException if bytes are malformed or truncated.
 */
fun BsonDocument.Companion.fromBytes(bytes: ByteArray): BsonDocument

/**
 * Decode a complete BSON document from a Source.
 * Reads exactly as many bytes as the embedded length field declares.
 * @throws BsonDecodeException if stream is malformed or truncated.
 */
fun BsonDocument.Companion.fromSource(source: Source): BsonDocument

// ── Codec registry / extension point ─────────────────────────────────────────

/**
 * Codec for a specific Kotlin type T → BsonValue round-trip.
 * Register custom codecs via BsonCodecRegistry.
 */
interface BsonCodec<T> {
    fun encode(value: T): BsonValue
    fun decode(bson: BsonValue): T
}

/**
 * Global registry for type codecs.
 * Built-in codecs for all KDB internal types are pre-registered.
 */
object BsonCodecRegistry {
    fun <T : Any> register(kClass: kotlin.reflect.KClass<T>, codec: BsonCodec<T>)
    fun <T : Any> get(kClass: kotlin.reflect.KClass<T>): BsonCodec<T>?
    fun <T : Any> getOrThrow(kClass: kotlin.reflect.KClass<T>): BsonCodec<T>
}

// ── Encode / decode helpers for KDB domain types ─────────────────────────────

/** Encode any registered type to a BsonValue. */
fun <T : Any> T.toBsonValue(): BsonValue

/** Decode a BsonValue to the target type via registered codec. */
inline fun <reified T : Any> BsonValue.decode(): T

// ── KDB-specific BSON convention helpers ──────────────────────────────────────

/**
 * Encode a UUID as BsonBinary(subtype=0x04, 16 bytes, big-endian).
 * Used for: document IDs, node IDs, transaction IDs.
 */
fun java.util.UUID.toBsonBinary(): BsonBinary    // expect/actual for KMP UUID
fun KdbUuid.toBsonBinary(): BsonBinary

/**
 * Decode BsonBinary(subtype=0x04) back to a KdbUuid.
 * @throws BsonDecodeException if subtype != 0x04 or length != 16.
 */
fun BsonBinary.toKdbUuid(): KdbUuid

/**
 * Encode a SHA-256 hash as BsonBinary(subtype=0x00, 32 bytes).
 * Used for: commit hashes, blob hashes.
 */
fun KdbHash.toBsonBinary(): BsonBinary

/**
 * Decode BsonBinary(subtype=0x00) back to a KdbHash.
 * @throws BsonDecodeException if subtype != 0x00 or length != 32.
 */
fun BsonBinary.toKdbHash(): KdbHash

/**
 * Encode a microsecond-precision timestamp as BSON Date (int64 milliseconds).
 * Microseconds are encoded in the top bits of a companion BSON field "_us"
 * when sub-millisecond precision must be preserved.
 */
fun KdbTimestamp.toBsonDate(): BsonDateTime

/**
 * Decode a BSON Date (+ optional "_us" companion field) to a KdbTimestamp.
 */
fun BsonDateTime.toKdbTimestamp(microRemainder: Long = 0L): KdbTimestamp

// ── JSON ↔ BSON bridge ────────────────────────────────────────────────────────

/**
 * Parse a JSON string into a BsonDocument.
 * The document preserves BSON-representable JSON types exactly.
 * Numbers without decimal points → Int32 or Int64 based on magnitude.
 * Numbers with decimal points → Double.
 * @throws BsonDecodeException if JSON is malformed.
 */
fun BsonDocument.Companion.fromJson(json: String): BsonDocument

/**
 * Render a BsonDocument as canonical JSON.
 * BsonBinary values are rendered as Base64-encoded strings.
 * BsonDateTime values are rendered as ISO-8601 strings.
 */
fun BsonDocument.toJson(): String

/**
 * Render a BsonDocument as pretty-printed JSON.
 */
fun BsonDocument.toPrettyJson(indent: Int = 2): String

// ── Size estimation ───────────────────────────────────────────────────────────

/**
 * Returns the exact encoded byte length of this document without allocating
 * the full byte array. Used by the wire protocol for frame-length headers.
 */
fun BsonDocument.encodedSize(): Int
fun BsonValue.encodedSize(): Int
```

-----

## 4. Data Structures

```kotlin
package dev.kdb.codec

// ── KDB primitive types (owned by this module) ────────────────────────────────

/**
 * A 128-bit UUID. Multiplatform replacement for java.util.UUID.
 * Stored as two Long values (msb, lsb) for KMP compatibility.
 */
data class KdbUuid(val msb: Long, val lsb: Long) {
    override fun toString(): String  // standard 8-4-4-4-12 format
    companion object {
        fun random(): KdbUuid
        fun fromString(s: String): KdbUuid
        fun fromBytes(bytes: ByteArray): KdbUuid  // must be 16 bytes
    }
}

/**
 * A 32-byte SHA-256 hash.
 */
@JvmInline
value class KdbHash(val bytes: ByteArray) {
    init { require(bytes.size == 32) }
    fun toHex(): String
    companion object {
        fun fromHex(hex: String): KdbHash
        fun fromBytes(bytes: ByteArray): KdbHash
    }
}

/**
 * A microsecond-precision timestamp.
 * [epochMillis] is the millisecond epoch; [microRemainder] is 0..999.
 */
data class KdbTimestamp(
    val epochMillis: Long,
    val microRemainder: Int = 0
) : Comparable<KdbTimestamp> {
    fun toEpochMicros(): Long
    companion object {
        fun now(): KdbTimestamp
        fun fromEpochMicros(micros: Long): KdbTimestamp
        fun fromIso8601(s: String): KdbTimestamp
    }
}

// ── BSON value hierarchy ──────────────────────────────────────────────────────

/** Sum type for all BSON values. */
sealed class BsonValue {
    abstract val bsonType: BsonType
}

/** BSON 0x02 — UTF-8 string. */
data class BsonString(val value: String) : BsonValue() {
    override val bsonType get() = BsonType.STRING
}

/** BSON 0x10 — 32-bit signed integer. */
data class BsonInt32(val value: Int) : BsonValue() {
    override val bsonType get() = BsonType.INT32
}

/** BSON 0x12 — 64-bit signed integer. */
data class BsonInt64(val value: Long) : BsonValue() {
    override val bsonType get() = BsonType.INT64
}

/** BSON 0x01 — IEEE 754 double. */
data class BsonDouble(val value: Double) : BsonValue() {
    override val bsonType get() = BsonType.DOUBLE
}

/** BSON 0x08 — boolean. */
data class BsonBoolean(val value: Boolean) : BsonValue() {
    override val bsonType get() = BsonType.BOOLEAN
}

/** BSON 0x09 — UTC datetime, milliseconds since epoch. */
data class BsonDateTime(val epochMillis: Long) : BsonValue() {
    override val bsonType get() = BsonType.DATETIME
}

/** BSON 0x05 — binary blob with subtype byte. */
data class BsonBinary(val subtype: Byte, val data: ByteArray) : BsonValue() {
    override val bsonType get() = BsonType.BINARY
    override fun equals(other: Any?): Boolean
    override fun hashCode(): Int
}

/** BSON 0x0A — null. */
object BsonNull : BsonValue() {
    override val bsonType get() = BsonType.NULL
}

/** BSON 0x03 — embedded document. */
data class BsonDocument(
    /** Preserves insertion order; field names must be unique within a document. */
    val fields: LinkedHashMap<String, BsonValue> = LinkedHashMap()
) : BsonValue() {
    override val bsonType get() = BsonType.DOCUMENT

    operator fun get(key: String): BsonValue?
    operator fun set(key: String, value: BsonValue)
    fun getString(key: String): String?
    fun getInt32(key: String): Int?
    fun getInt64(key: String): Long?
    fun getDouble(key: String): Double?
    fun getBoolean(key: String): Boolean?
    fun getDocument(key: String): BsonDocument?
    fun getArray(key: String): BsonArray?
    fun getBinary(key: String): BsonBinary?
    fun getDateTime(key: String): BsonDateTime?
    fun containsKey(key: String): Boolean
    fun keys(): Set<String>
    fun isEmpty(): Boolean

    companion object  // extension point for fromBytes/fromSource/fromJson
}

/** BSON 0x04 — array (document with string integer keys "0","1",…). */
data class BsonArray(val elements: MutableList<BsonValue> = mutableListOf()) : BsonValue() {
    override val bsonType get() = BsonType.ARRAY

    operator fun get(index: Int): BsonValue
    fun size(): Int
    fun isEmpty(): Boolean
    fun add(value: BsonValue)
}

/** BSON type byte constants. */
enum class BsonType(val byte: Byte) {
    DOUBLE(0x01),
    STRING(0x02),
    DOCUMENT(0x03),
    ARRAY(0x04),
    BINARY(0x05),
    BOOLEAN(0x08),
    DATETIME(0x09),
    NULL(0x0A),
    INT32(0x10),
    INT64(0x12),
}

// ── Binary subtype constants ──────────────────────────────────────────────────

object BsonBinarySubtype {
    const val GENERIC: Byte  = 0x00  // used for KdbHash (32-byte SHA-256)
    const val UUID: Byte     = 0x04  // used for KdbUuid (16-byte UUID)
}

// ── Exceptions ────────────────────────────────────────────────────────────────

/**
 * Thrown when BSON bytes are structurally invalid, truncated,
 * or violate a KDB-specific encoding convention.
 */
class BsonDecodeException(
    message: String,
    val offset: Int = -1,
    cause: Throwable? = null
) : Exception(message, cause)

/**
 * Thrown when a type codec cannot encode a value
 * (e.g. NaN or Infinity in a context where they are forbidden).
 */
class BsonEncodeException(
    message: String,
    cause: Throwable? = null
) : Exception(message, cause)
```

-----

## 5. Contracts

### `BsonDocument.toBytes()`

- **Pre:** document contains only supported `BsonType` values (no custom extensions)
- **Post:** returned `ByteArray` is a valid BSON document; `BsonDocument.fromBytes(result)` produces a structurally equal document
- **Guarantee:** pure function; no side effects; thread-safe

### `BsonDocument.fromBytes(bytes)`

- **Pre:** `bytes` is a complete BSON document (starts with valid int32 length)
- **Post:** all fields present in `bytes` are present in the result with identical types and values
- **Throws:** `BsonDecodeException` if length field is negative, exceeds `bytes.size`, type byte is unrecognised, or a string is not valid UTF-8

### `BsonDocument.fromSource(source)`

- **Pre:** `source` is positioned at the start of a BSON document
- **Post:** reads exactly `length` bytes from `source` as declared in the BSON length prefix
- **Guarantee:** does not read beyond the declared document boundary; leaves `source` positioned immediately after the document

### `KdbUuid.toBsonBinary()` / `BsonBinary.toKdbUuid()`

- **Guarantee:** round-trip lossless; `uuid.toBsonBinary().toKdbUuid() == uuid`
- **Throws:** `BsonDecodeException` if subtype ≠ 0x04 or `data.size` ≠ 16

### `KdbHash.toBsonBinary()` / `BsonBinary.toKdbHash()`

- **Guarantee:** round-trip lossless; `hash.toBsonBinary().toKdbHash() == hash`
- **Throws:** `BsonDecodeException` if subtype ≠ 0x00 or `data.size` ≠ 32

### `KdbTimestamp.toBsonDate()` / `BsonDateTime.toKdbTimestamp()`

- **Guarantee:** millisecond precision always preserved; microsecond remainder preserved when `_us` companion field is present
- **Note:** callers that only need millisecond precision may omit the `_us` field

### `BsonDocument.fromJson(json)`

- **Pre:** `json` is a well-formed JSON string (object at top level)
- **Post:** all JSON fields appear in the result; number representation follows KDB convention (see §9)
- **Throws:** `BsonDecodeException` wrapping a JSON parse error

### `BsonDocument.encodedSize()`

- **Guarantee:** `doc.encodedSize() == doc.toBytes().size` — exact, not an estimate
- **Guarantee:** O(n) in document element count; does not allocate a byte array

-----

## 6. Error Cases

|Exception                                   |When thrown                                                                                                                                                                                                                  |
|--------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|`BsonDecodeException`                       |BSON length field negative or exceeds buffer; unknown type byte; binary blob shorter than declared length; UUID binary not 16 bytes; hash binary not 32 bytes; string contains invalid UTF-8; source stream ends mid-document|
|`BsonEncodeException`                       |Encoding `Double.NaN` or `Double.POSITIVE_INFINITY` / `NEGATIVE_INFINITY`; encoding a string longer than 2 GiB (BSON spec limit)                                                                                             |
|`BsonDecodeException` (JSON)                |Malformed JSON; number overflow beyond Int64 range; JSON array at top level (BSON root must be a document)                                                                                                                   |
|`NoSuchElementException` (from `getOrThrow`)|Requested codec not registered in `BsonCodecRegistry`                                                                                                                                                                        |

-----

## 7. Test Cases

|# |Name                               |Input                                                             |Expected                                                                                    |
|--|-----------------------------------|------------------------------------------------------------------|--------------------------------------------------------------------------------------------|
|1 |**round-trip empty document**      |`BsonDocument()`                                                  |`fromBytes(toBytes())` equals original                                                      |
|2 |**round-trip all scalar types**    |Document with one field of each `BsonType`                        |All fields survive encode/decode with identical types and values                            |
|3 |**round-trip nested document**     |Doc containing sub-doc containing array containing doc            |Full structural equality after decode                                                       |
|4 |**UUID encode/decode**             |`KdbUuid.random()`                                                |`toBsonBinary().toKdbUuid()` equals original; subtype byte is `0x04`; binary length is 16   |
|5 |**hash encode/decode**             |`KdbHash(ByteArray(32) { it.toByte() })`                          |`toBsonBinary().toKdbHash()` equals original; subtype byte is `0x00`; binary length is 32   |
|6 |**timestamp microsecond precision**|`KdbTimestamp(epochMillis=1_700_000_000_000L, microRemainder=987)`|After encode + decode with `_us` field, `microRemainder == 987`                             |
|7 |**encodedSize accuracy**           |Arbitrary 50-field document                                       |`encodedSize() == toBytes().size`                                                           |
|8 |**JSON → BSON → JSON round-trip**  |`{"a":1,"b":"hello","c":true,"d":null,"e":[1,2,3]}`               |`fromJson(json).toJson()` produces semantically equivalent JSON                             |
|9 |**large document**                 |10,000-field document, each field a 100-char string               |Encodes and decodes without error; total size within BSON spec limit                        |
|10|**truncated bytes — error case**   |Valid BSON document truncated by 1 byte                           |`BsonDecodeException` with meaningful message and non-negative offset                       |
|11|**unknown type byte — error case** |Bytes with type byte `0xFF`                                       |`BsonDecodeException`                                                                       |
|12|**fromSource boundary**            |Two concatenated BSON documents in one Source                     |First `fromSource` reads exactly the first document; source is positioned at start of second|

-----

## 8. Non-Goals

- **No BSON extended types** — deprecated types (Symbol, DBPointer, Undefined, Code, Timestamp BSON internal type) are not supported; attempting to decode them throws `BsonDecodeException`
- **No Decimal128** — not required by KDB; `Double` covers all needed precision
- **No object-graph mapping** — this module does not annotate or reflect over Kotlin data classes; that is the responsibility of individual domain-model codecs in their respective modules
- **No compression** — zstd compression/decompression is handled by the Storage Tier Manager; this module outputs and accepts raw BSON bytes
- **No JSONPath evaluation** — JSON path queries are handled by the JSON Functions Engine (Layer 1)
- **No schema validation** — the Schema Engine (Layer 2) owns validation; this module only encodes and decodes structure
- **No streaming partial reads** — the module reads a complete BSON document atomically; partial/streaming reads of large arrays are out of scope for v1

-----

## 9. Implementation Notes

### Number representation in JSON → BSON

- Integers that fit in `Int` range → `BsonInt32`
- Integers that fit in `Long` range → `BsonInt64`
- Integers exceeding `Long.MAX_VALUE` → `BsonDecodeException` (BSON has no BigInteger)
- Any number with `.` or `e`/`E` → `BsonDouble`
- `NaN`, `Infinity` are illegal in canonical JSON and illegal in KDB BSON; throw `BsonEncodeException`

### Multiplatform byte I/O

- Use `kotlinx.io` (`Buffer`, `Source`, `Sink`) throughout; it is the KMP-safe I/O abstraction
- All multi-byte integers are little-endian per the BSON spec
- Do not use `java.io` anywhere in `commonMain`

### Field-name uniqueness

- `BsonDocument` uses `LinkedHashMap` to preserve insertion order (required by spec)
- Duplicate field names during decode: last value wins and a warning is logged (not an error — some historic BSON producers emit duplicates)

### KMP expect/actual boundaries

- `KdbUuid.random()` requires `expect/actual` for platform-specific UUID generation
- `KdbTimestamp.now()` requires `expect/actual` for platform clock access
- All codec logic itself lives entirely in `commonMain`

### Performance

- `encodedSize()` must traverse the document tree without allocating; it is called on the hot wire-write path
- Pre-size the output `Buffer` using `encodedSize()` before writing to avoid buffer growth copies
- `BsonArray` and `BsonDocument` use mutable collections internally; expose read-only views at API boundaries where appropriate

### BSON length prefix

- Every BSON document starts with a little-endian `int32` total byte length (including the 4-byte length field itself and the terminating `0x00` byte)
- Validate that the length field equals `bytes.size` in `fromBytes`; in `fromSource`, trust the length field and read exactly that many bytes

-----

## 10. Estimated Lines

|Sub-component                                 |Est. NBNC lines|
|----------------------------------------------|---------------|
|BSON type hierarchy + BsonDocument / BsonArray|350            |
|Encoder (write path)                          |400            |
|Decoder (read path)                           |450            |
|KDB convention helpers (UUID, Hash, Timestamp)|200            |
|JSON ↔ BSON bridge                            |350            |
|BsonCodecRegistry + BsonCodec interface       |150            |
|KdbUuid / KdbHash / KdbTimestamp primitives   |200            |
|expect/actual stubs (UUID random, clock)      |50             |
|Error types                                   |50             |
|Unit tests                                    |300            |
|**Total**                                     |**~2,500**     |