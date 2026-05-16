# KDB Component Spec — Layer 0: Type System & Codec

**Version:** 0.2  
**Layer:** 0 (Foundation — no dependencies)  
**Module:** `kdb-codec`

-----

## 1. Purpose

Layer 0 defines the **KDB type system**: the set of physical types that can be encoded on the wire and in storage, and the logical types that give them semantic meaning. It owns:

- The in-memory typed value model (`KdbValue`)
- The schema model for named and referenceable types (`KdbSchema`)
- A binary codec (physical encoding to/from bytes)
- A JSON ↔ typed model bridge (with schema context)
- A registry for custom logical types

Binary encoding is an implementation detail of this layer; the primary abstraction is the type system and the typed value model. All other KDB modules that need binary I/O or type-aware data manipulation call this module. Nothing outside this module touches raw bytes for structured data.

> **Relation to BSON** — a `BsonDocument` / `BsonValue` representation is retained internally as an intermediate form for JSON parsing convenience, but it is not the public data model. External callers work with `KdbValue` and `KdbSchema`.

-----

## 2. Dependencies

None. Layer 0 has no KDB dependencies. This module depends only on:

- Kotlin stdlib
- `kotlinx.io` — byte-level I/O primitives (`Buffer`, `Source`, `Sink`) available in `commonMain`
- `kotlinx.datetime` — `Instant`, `LocalDate`, `LocalTime` on all platforms

-----

## 3. Design Principles

- **Schema drives meaning.** The same bytes can represent different things depending on schema context. The schema is always the authority on type semantics.
- **Internally typed, never stringly.** No `Map<String, Any>`, no `JsonElement`, no `String`-keyed untyped maps inside the engine. All values flowing through the engine are `KdbValue` instances.
- **JSON at the boundary only.** JSON + a schema reference is the public input/output format. Internally, every value has a concrete Kotlin type; codecs handle the translation.
- **Extensible logical types.** The built-in logical type set can be augmented via `LogicalTypeRegistry` at configuration time without modifying core code.
- **Non-string map keys.** Maps are modelled as ordered lists of `(key, value)` pairs at the physical level; the key may be any type in the type system.
- **Named types are referenceable.** Records, enums, and fixed-size types are registered by fully-qualified name and may be referenced from any schema without embedding the full definition inline.

-----

## 4. Type System

### 4.1 Physical Types

Physical types are the primitive wire/storage representations. Each has a 1-byte tag. All multi-byte integers are **little-endian**.

| Tag | Name | Wire representation |
|-----|------|---------------------|
| `0x00` | `NULL` | 0 bytes |
| `0x01` | `BOOLEAN` | 1 byte: `0x00` false / `0x01` true |
| `0x02` | `INT8` | 1 signed byte |
| `0x03` | `INT16` | 2-byte signed integer |
| `0x04` | `INT32` | 4-byte signed integer |
| `0x05` | `INT64` | 8-byte signed integer |
| `0x06` | `FLOAT32` | 4-byte IEEE 754 single |
| `0x07` | `FLOAT64` | 8-byte IEEE 754 double |
| `0x08` | `BYTES` | varint byte-count prefix + raw bytes |
| `0x09` | `STRING` | varint byte-count prefix + UTF-8 bytes |
| `0x0A` | `ARRAY` | varint element count + element values |
| `0x0B` | `MAP` | varint entry count + (key-value) pairs; key is any physical type |
| `0x0C` | `RECORD` | fields written in field-ID order per schema; each field: varint field-ID + value |
| `0x0D` | `ENUM` | `INT32` ordinal |
| `0x0E` | `UNION` | `INT8` branch index + branch value |
| `0x0F` | `FIXED` | exactly *n* bytes (size specified in schema, not on wire) |

Varint encoding is unsigned LEB-128 (same as Protocol Buffers).

### 4.2 Logical Types

Logical types annotate a physical type with a semantic meaning. They exist **in the schema only** and do not change the wire tag byte.

| Logical type | Physical base | Schema params | Description |
|---|---|---|---|
| `date` | `INT32` | — | Calendar date; value = days since 1970-01-01 (proleptic Gregorian, UTC) |
| `time-micros` | `INT64` | — | Time of day; value = microseconds since midnight |
| `timestamp-micros` | `INT64` | `timezone?: String` | Instant; value = microseconds since Unix epoch. `null` / absent ⇒ UTC |
| `timestamp-millis` | `INT64` | `timezone?: String` | Millisecond-precision instant (interop / legacy) |
| `uuid` | `FIXED(16)` | — | RFC 4122 UUID, most-significant byte first |
| `decimal` | `BYTES` | `precision: Int, scale: Int` | Unscaled value as two's-complement big-endian bytes × 10⁻ˢᶜᵃˡᵉ |
| `big-integer` | `BYTES` | — | Arbitrary-precision integer, two's-complement big-endian |
| `big-decimal` | `BYTES` | — | 4-byte little-endian scale prefix + big-integer unscaled (see §9) |
| `duration` | `FIXED(12)` | — | 3 × unsigned `INT32` LE: months, days, microseconds |
| `enum` | `ENUM` | `name, symbols[]` | Named enum type; symbols list is part of the schema |

Additional logical types may be registered at runtime via `LogicalTypeRegistry` (see §7).

### 4.3 Named / Referenceable Types

Three constructs produce **named types** that are registered in `KdbTypeRegistry` and may be referenced by fully-qualified name from any schema anywhere in the engine.

**Record** — an ordered list of named, typed fields. Each field carries a stable numeric field-ID for binary evolution. Field names are for human readability and JSON mapping; field-IDs govern the binary format.

**Enum** — a named ordered list of symbol strings. Wire value is `INT32` ordinal. New symbols must be **appended** to preserve backward compatibility.

**Fixed** — a named fixed-length byte array. Used as the physical base for `uuid`, `duration`, and any custom fixed-size primitive.

Fully-qualified names follow `<namespace>.<Name>`, for example `dev.kdb.model.CommitHeader`. Names are globally unique within a `KdbTypeRegistry` instance and must be stable across releases.

-----

## 5. Schema Model

```kotlin
package dev.kdb.codec.schema

// ── Type expressions ──────────────────────────────────────────────────────────

/**
 * A type expression: a structural description of a value's shape and meaning.
 * All KdbType instances are immutable and comparable.
 */
sealed class KdbType {
    /** Physical primitive + optional logical annotation. */
    data class Primitive(
        val physical: PhysicalKind,
        val logical: LogicalAnnotation? = null,
    ) : KdbType()

    /** Reference to a named Record, Enum, or Fixed type by fully-qualified name. */
    data class Ref(val fullyQualifiedName: String) : KdbType()

    /** Nullable: UNION of [NULL, inner]. Shorthand for the common case. */
    data class Nullable(val inner: KdbType) : KdbType()

    /** Homogeneous array. */
    data class Array(val element: KdbType) : KdbType()

    /**
     * Map with potentially non-string key type.
     * Encoded as an ordered list of (key, value) pairs on the wire.
     */
    data class Map(val key: KdbType, val value: KdbType) : KdbType()

    /**
     * Explicit union of 2+ branches.
     * Branch index (INT8) precedes the value on the wire.
     */
    data class Union(val branches: List<KdbType>) : KdbType()
}

enum class PhysicalKind {
    NULL, BOOLEAN,
    INT8, INT16, INT32, INT64,
    FLOAT32, FLOAT64,
    BYTES, STRING,
    ARRAY, MAP, RECORD, ENUM, UNION, FIXED,
}

// ── Named type definitions ────────────────────────────────────────────────────

/** Schema for a named Record type. */
data class RecordSchema(
    val name: String,
    val namespace: String,
    val doc: String? = null,
    val fields: List<FieldSchema>,
) {
    val fullyQualifiedName: String get() = "$namespace.$name"
}

data class FieldSchema(
    /**
     * Stable numeric ID used in the binary format.
     * Must not change after the field is first published.
     * New fields must have new IDs.
     */
    val id: Int,
    val name: String,
    val type: KdbType,
    val default: KdbValue? = null,
    val doc: String? = null,
)

/** Schema for a named Enum type. */
data class EnumSchema(
    val name: String,
    val namespace: String,
    val symbols: List<String>,
    val doc: String? = null,
) {
    val fullyQualifiedName: String get() = "$namespace.$name"
}

/** Schema for a named Fixed-size byte array type. */
data class FixedSchema(
    val name: String,
    val namespace: String,
    val size: Int,
    val logical: LogicalAnnotation? = null,
    val doc: String? = null,
) {
    val fullyQualifiedName: String get() = "$namespace.$name"
}

// ── Logical type annotations ──────────────────────────────────────────────────

/** Sealed for built-ins; extensible via LogicalTypeRegistry for custom annotations. */
sealed class LogicalAnnotation {
    object Date                                              : LogicalAnnotation()
    object TimeMicros                                        : LogicalAnnotation()
    data class TimestampMicros(val timezone: String? = null) : LogicalAnnotation()
    data class TimestampMillis(val timezone: String? = null) : LogicalAnnotation()
    object Uuid                                              : LogicalAnnotation()
    data class Decimal(val precision: Int, val scale: Int)   : LogicalAnnotation()
    object BigInteger                                         : LogicalAnnotation()
    object BigDecimal                                         : LogicalAnnotation()
    object Duration                                           : LogicalAnnotation()
    /** Extension point; `id` is a name registered in LogicalTypeRegistry. */
    data class Custom(val id: String, val params: Map<String, String> = emptyMap()) : LogicalAnnotation()
}
```

-----

## 6. Value Model

```kotlin
package dev.kdb.codec

/**
 * Sum type for all in-memory typed values.
 * KdbValue instances are immutable. ByteArray fields are defensive copies.
 */
sealed class KdbValue {

    // ── Physical primitives ───────────────────────────────────────────────────

    data object Null                                                  : KdbValue()
    data class Bool(val v: Boolean)                                   : KdbValue()
    data class Int8Val(val v: Byte)                                   : KdbValue()
    data class Int16Val(val v: Short)                                 : KdbValue()
    data class Int32Val(val v: Int)                                   : KdbValue()
    data class Int64Val(val v: Long)                                  : KdbValue()
    data class Float32Val(val v: Float)                               : KdbValue()
    data class Float64Val(val v: Double)                              : KdbValue()
    data class BytesVal(val v: ByteArray)                             : KdbValue()
    data class StringVal(val v: String)                               : KdbValue()
    data class ArrayVal(val elements: List<KdbValue>)                 : KdbValue()
    /** Entries are in declaration order; key may be any KdbValue. */
    data class MapVal(val entries: List<Pair<KdbValue, KdbValue>>)    : KdbValue()
    /** Fields keyed by stable field-ID (matches FieldSchema.id). */
    data class RecordVal(val fields: Map<Int, KdbValue>)              : KdbValue()
    data class EnumVal(val ordinal: Int, val symbol: String)          : KdbValue()
    data class UnionVal(val branch: Int, val value: KdbValue)         : KdbValue()
    data class FixedVal(val v: ByteArray)                             : KdbValue()

    // ── Logical / rich types (decoded in-memory representation) ──────────────

    /** Calendar date, days since 1970-01-01. */
    data class DateVal(val daysSinceEpoch: Int)                       : KdbValue()

    /** Time of day, microseconds since midnight. */
    data class TimeMicrosVal(val microsSinceMidnight: Long)           : KdbValue()

    /** Instant with microsecond resolution; tz is IANA zone id or null for UTC. */
    data class TimestampVal(val epochMicros: Long, val tz: String?)   : KdbValue()

    /** RFC 4122 UUID stored as two 64-bit halves, most-significant first. */
    data class UuidVal(val msb: Long, val lsb: Long)                  : KdbValue()

    /**
     * Arbitrary-precision decimal.
     * [unscaled] is a two's-complement big-endian byte array.
     * Logical value = unscaled × 10⁻ˢᶜᵃˡᵉ.
     * Platform-specific conversions: see KdbBigMath expect/actual.
     */
    data class DecimalVal(val unscaled: ByteArray, val scale: Int)    : KdbValue()

    /**
     * Arbitrary-precision integer.
     * [magnitude] is a two's-complement big-endian byte array.
     * Platform-specific conversions: see KdbBigMath expect/actual.
     */
    data class BigIntegerVal(val magnitude: ByteArray)                : KdbValue()

    /**
     * Arbitrary-precision decimal without fixed scale.
     * [unscaled] + [scale] encode the same way as DecimalVal.
     * Scale is stored alongside; unlike Decimal there is no schema-level precision/scale.
     */
    data class BigDecimalVal(val unscaled: ByteArray, val scale: Int) : KdbValue()

    /** Calendar duration: whole months, whole days, and residual microseconds. */
    data class DurationVal(val months: Int, val days: Int, val micros: Long) : KdbValue()
}
```

### 6.1 Platform-Specific Big-Number Conversions

`java.math.BigInteger` and `java.math.BigDecimal` are JVM-only. The multiplatform interface is:

```kotlin
// commonMain
expect object KdbBigMath {
    fun bigIntegerToBytes(v: Any): ByteArray   // v is platform's BigInteger equivalent
    fun bytesToBigInteger(bytes: ByteArray): Any
    fun bigDecimalToBytes(v: Any): Pair<ByteArray, Int>
    fun bytesToBigDecimal(unscaled: ByteArray, scale: Int): Any
}

// jvmMain
actual object KdbBigMath {
    actual fun bigIntegerToBytes(v: Any): ByteArray = (v as java.math.BigInteger).toByteArray()
    actual fun bytesToBigInteger(bytes: ByteArray): Any = java.math.BigInteger(bytes)
    actual fun bigDecimalToBytes(v: Any): Pair<ByteArray, Int> {
        v as java.math.BigDecimal
        return Pair(v.unscaledValue().toByteArray(), v.scale())
    }
    actual fun bytesToBigDecimal(unscaled: ByteArray, scale: Int): Any =
        java.math.BigDecimal(java.math.BigInteger(unscaled), scale)
}
// jsMain / nativeMain: use equivalent third-party or manual implementations
```

-----

## 7. Registries

### 7.1 KdbTypeRegistry — Named Type Registry

```kotlin
package dev.kdb.codec.schema

/**
 * Registry for named Record, Enum, and Fixed schemas.
 * All schemas must be registered before encoding or decoding begins.
 * Registries are immutable after calling freeze(); further registration throws.
 */
class KdbTypeRegistry {
    fun registerRecord(schema: RecordSchema)
    fun registerEnum(schema: EnumSchema)
    fun registerFixed(schema: FixedSchema)

    fun resolveRecord(fqn: String): RecordSchema
    fun resolveEnum(fqn: String): EnumSchema
    fun resolveFixed(fqn: String): FixedSchema

    /** Resolve any named type by FQN; returns RecordSchema | EnumSchema | FixedSchema. */
    fun resolve(fqn: String): Any

    fun freeze()
    val isFrozen: Boolean

    companion object {
        /** Pre-populated with all KDB built-in types. */
        fun builtin(): KdbTypeRegistry
    }
}
```

### 7.2 LogicalTypeRegistry — Extensible Logical Types

```kotlin
package dev.kdb.codec.schema

/**
 * Registry for custom logical type annotations.
 * Built-in logical types (date, uuid, decimal, etc.) are always available.
 * Third-party code may add custom logical type handlers for encoding/decoding.
 */
object LogicalTypeRegistry {
    fun register(id: String, handler: LogicalTypeHandler)
    fun resolve(id: String): LogicalTypeHandler?
}

interface LogicalTypeHandler {
    /** Validate that [annotation] is compatible with [physical] before schema freeze. */
    fun validate(annotation: LogicalAnnotation.Custom, physical: PhysicalKind)

    /** Encode a higher-level value to the base KdbValue for the physical type. */
    fun encode(value: KdbValue, annotation: LogicalAnnotation.Custom): KdbValue

    /** Decode a physical KdbValue back to the rich in-memory value. */
    fun decode(value: KdbValue, annotation: LogicalAnnotation.Custom): KdbValue
}
```

### 7.3 KdbCodecRegistry — Kotlin Type Codecs

```kotlin
package dev.kdb.codec

/**
 * Maps a Kotlin class to a codec that bridges it to/from KdbValue.
 * Use this when you have a concrete domain class (e.g. CommitHeader)
 * and want to encode it to/from the typed value model without manual field access.
 */
object KdbCodecRegistry {
    fun <T : Any> register(kClass: KClass<T>, codec: KdbCodec<T>)
    fun <T : Any> get(kClass: KClass<T>): KdbCodec<T>?
    fun <T : Any> getOrThrow(kClass: KClass<T>): KdbCodec<T>
}

/**
 * Codec that converts a concrete Kotlin type T to/from KdbValue.
 * Implementations should be stateless and thread-safe.
 */
interface KdbCodec<T> {
    /** The schema describing the KdbValue shape this codec produces/consumes. */
    val schema: KdbType

    fun encode(value: T): KdbValue
    fun decode(value: KdbValue): T
}

/** Encode any registered type to KdbValue. */
fun <T : Any> T.toKdbValue(): KdbValue

/** Decode a KdbValue to the target type via the registered codec. */
inline fun <reified T : Any> KdbValue.decode(): T
```

-----

## 8. Public Interface

### 8.1 Binary Codec (typed value ↔ bytes)

```kotlin
package dev.kdb.codec

import dev.kdb.codec.schema.KdbType
import dev.kdb.codec.schema.KdbTypeRegistry
import kotlinx.io.Sink
import kotlinx.io.Source

/**
 * Encodes a KdbValue to bytes given its type and a type registry.
 * @throws KdbEncodeException if the value is incompatible with the schema type.
 */
fun KdbValue.encodeToBytes(type: KdbType, registry: KdbTypeRegistry): ByteArray
fun KdbValue.encodeTo(sink: Sink, type: KdbType, registry: KdbTypeRegistry)

/**
 * Decodes a KdbValue from bytes given its expected type.
 * @throws KdbDecodeException if bytes are malformed or type-incompatible.
 */
fun KdbValue.Companion.decodeFromBytes(
    bytes: ByteArray,
    type: KdbType,
    registry: KdbTypeRegistry,
): KdbValue

fun KdbValue.Companion.decodeFrom(
    source: Source,
    type: KdbType,
    registry: KdbTypeRegistry,
): KdbValue

/** Returns the exact encoded byte count without allocating. O(n) in value tree size. */
fun KdbValue.encodedSize(type: KdbType, registry: KdbTypeRegistry): Int
```

### 8.2 JSON ↔ Typed Model Bridge

```kotlin
package dev.kdb.codec

import dev.kdb.codec.schema.KdbType
import dev.kdb.codec.schema.KdbTypeRegistry

/**
 * Parse a JSON string into a typed KdbValue, guided by a schema type.
 * Schema context resolves ambiguities (e.g. "2024-01-15" → DateVal vs StringVal).
 * @throws KdbDecodeException if JSON is malformed or schema-incompatible.
 */
fun KdbValue.Companion.fromJson(
    json: String,
    type: KdbType,
    registry: KdbTypeRegistry,
): KdbValue

/**
 * Render a KdbValue as canonical JSON.
 * Rich types use their natural JSON representation:
 *   DateVal       → "2024-01-15"
 *   TimestampVal  → "2024-01-15T12:00:00.000000Z"
 *   UuidVal       → "550e8400-e29b-41d4-a716-446655440000"
 *   DecimalVal    → numeric string preserving scale (e.g. "3.14")
 *   BigIntegerVal → numeric string (no quotes for values in safe integer range; quoted otherwise)
 *   BytesVal      → Base64-encoded string
 * @throws KdbEncodeException if a value cannot be represented in JSON.
 */
fun KdbValue.toJson(type: KdbType, registry: KdbTypeRegistry): String
fun KdbValue.toPrettyJson(type: KdbType, registry: KdbTypeRegistry, indent: Int = 2): String
```

### 8.3 Primitive Helpers (KdbUuid, KdbHash, KdbTimestamp)

The `KdbUuid`, `KdbHash`, and `KdbTimestamp` Kotlin types defined in the previous spec are retained and promoted to first-class KDB primitive helpers. They are not `KdbValue` subtypes; instead they are convenience types used by engine internals (node IDs, content hashes, commit timestamps). Conversion helpers bridge them into/out of the `KdbValue` hierarchy when needed:

```kotlin
fun KdbUuid.toUuidVal(): KdbValue.UuidVal
fun KdbValue.UuidVal.toKdbUuid(): KdbUuid

fun KdbTimestamp.toTimestampVal(): KdbValue.TimestampVal
fun KdbValue.TimestampVal.toKdbTimestamp(): KdbTimestamp
```

-----

## 9. Data Contracts

### Binary Codec

| Contract | Detail |
|---|---|
| Round-trip | `decodeFromBytes(value.encodeToBytes(t, reg), t, reg) == value` for all values matching type `t` |
| Deterministic | Same `(value, type)` always produces the same bytes |
| Bounded reads | `decodeFrom(source)` reads exactly `encodedSize()` bytes; does not read beyond |
| `encodedSize` | `value.encodedSize(t, reg) == value.encodeToBytes(t, reg).size` — exact, not an estimate; O(n), no allocation |

### JSON Bridge

| Contract | Detail |
|---|---|
| Schema-guided | `fromJson` and `toJson` require a matching `KdbType`; calling without schema context is a programming error |
| Lossless logical round-trip | `fromJson(toJson(v, t, reg), t, reg) == v` for all built-in logical types |
| BigDecimal JSON | Emitted as a numeric string preserving all digits; parsed back without loss via schema context |
| Map keys | Non-string map keys are serialised as JSON using KDB Extended JSON conventions (see §10.4) |

### KdbCodec<T>

| Contract | Detail |
|---|---|
| Stateless | Codec implementations must be stateless and safe to call from multiple coroutines concurrently |
| Schema agreement | `codec.schema` must correctly describe the `KdbValue` shape that `encode` produces; mismatch is a programming error detected at registration time if the registry is frozen |

-----

## 10. Implementation Notes

### 10.1 Varint Encoding

Use unsigned LEB-128. The maximum varint is 10 bytes (covering the full `Long` range). Field IDs in records use the same encoding.

### 10.2 Record Field Evolution

- Reader encounters unknown field-ID → skip the value using its physical type tag.
- Writer omits a field whose value equals `FieldSchema.default` → reader fills in the default.
- Field IDs must be allocated monotonically; reuse of a field-ID is a schema-level error caught at `KdbTypeRegistry.freeze()`.

### 10.3 BigDecimal Wire Format

`BigDecimalVal` on the wire: 4-byte little-endian `INT32` scale, followed by the two's-complement big-endian unscaled integer byte array (varint-length-prefixed as per BYTES).

### 10.4 Extended JSON for Non-String Map Keys

When rendering a `MapVal` whose key type is not `STRING` to JSON, keys are wrapped in a tagged object:

```json
{ "$key": <json-representation-of-key> }
```

Full specification of KDB Extended JSON is a Layer 1 concern (JSON Functions Engine). Layer 0 only applies the convention when `toJson` encounters a non-string map key.

### 10.5 Multiplatform Byte I/O

- Use `kotlinx.io` (`Buffer`, `Source`, `Sink`) throughout; do not use `java.io` in `commonMain`.
- All multi-byte integers in the binary format are **little-endian** except where explicitly noted (UUID bytes are big-endian per RFC 4122).

### 10.6 KMP expect/actual Boundaries

| Surface | Reason |
|---|---|
| `KdbUuid.random()` | Platform-specific secure random |
| `KdbTimestamp.now()` | Platform clock |
| `KdbBigMath` | `BigInteger`/`BigDecimal` APIs are JVM-only |
| `LogicalTypeHandler` for `decimal`/`big-integer`/`big-decimal` | Delegate to `KdbBigMath` |

All codec logic itself lives in `commonMain`.

### 10.7 Schema Registry Freeze

Call `KdbTypeRegistry.freeze()` before any encode/decode operation in production. Frozen registries are thread-safe and allow compile-time validation of all `Ref` types. Mutable registries must be used only during configuration / test setup.

-----

## 11. Error Cases

| Exception | When thrown |
|---|---|
| `KdbDecodeException(message, offset)` | Malformed bytes; unknown physical type tag; varint overflow; string not valid UTF-8; FIXED length mismatch; field-ID refers to unknown type in frozen registry; source stream ends mid-value |
| `KdbEncodeException(message)` | `Double.NaN` or `Double.Infinity` outside a union branch that allows them; `DecimalVal` unscaled bytes exceed schema `precision`; `EnumVal.ordinal` out of range for schema symbol count |
| `KdbSchemaException(message)` | Duplicate field ID; unknown `Ref` FQN in frozen registry; `LogicalAnnotation` incompatible with physical kind; `freeze()` called when schema is invalid |
| `KdbDecodeException` (JSON) | Malformed JSON; numeric value exceeds `INT64` range for a non-big-integer type; JSON array at top level when schema expects a record |
| `NoSuchElementException` (registry) | `KdbCodecRegistry.getOrThrow` for an unregistered Kotlin class |

-----

## 12. Test Cases

| # | Name | Input | Expected |
|---|------|-------|----------|
| 1 | Round-trip null | `KdbValue.Null`, type `NULL` | Decode(encode) == Null |
| 2 | Round-trip all scalar physical types | One value of each physical kind | All survive encode/decode with identical type and value |
| 3 | Round-trip nested record | `RecordVal` containing `ArrayVal` containing `MapVal` | Full structural equality after decode |
| 4 | UUID encode/decode | `UuidVal(msb, lsb)` | `decode(encode) == original`; wire is 16 bytes big-endian |
| 5 | Decimal round-trip | `DecimalVal(unscaled=314, scale=2)` → 3.14 | Encode; decode; `DecimalVal.unscaled` and `scale` match |
| 6 | BigInteger large value | 256-bit integer | Bytes survive encode/decode; conversion via `KdbBigMath` matches original |
| 7 | BigDecimal round-trip | Arbitrary precision value | `unscaled + scale` survive encode/decode losslessly |
| 8 | Date JSON bridge | `"2024-01-15"` + `date` schema type | Parsed to `DateVal(daysSinceEpoch=19737)` |
| 9 | Timestamp micros JSON bridge | `"2024-01-15T12:00:00.000123Z"` + `timestamp-micros` schema | `TimestampVal(epochMicros=...)` preserves microseconds |
| 10 | Map with non-string keys | `MapVal([(Int32Val(1), StringVal("a"))])` | `toJson` uses extended key syntax; `fromJson` round-trips |
| 11 | Enum evolution — unknown symbol | Reader schema has 3 symbols; writer encoded ordinal 3 (symbol 4) | Reader returns `EnumVal(3, "<unknown>")` without throwing |
| 12 | Record field evolution — new field added | Writer writes field-ID 5 not in reader schema | Reader skips field 5; result contains only known fields |
| 13 | Record field evolution — field removed | Writer omits field-ID 3; reader schema declares field-ID 3 with default | Reader fills in default value for field 3 |
| 14 | `encodedSize` accuracy | Arbitrary 100-field record | `encodedSize(t, reg) == encodeToBytes(t, reg).size` |
| 15 | Truncated bytes error | Valid encoded value truncated by 1 byte | `KdbDecodeException` with non-negative offset |
| 16 | Unknown physical type tag | Byte stream with tag `0xFF` | `KdbDecodeException` |
| 17 | `decodeFrom` boundary | Two concatenated encoded values in one Source | First `decodeFrom` reads exactly the first value; source at start of second |
| 18 | Custom logical type round-trip | `LogicalAnnotation.Custom("my-type", ...)` + registered handler | Encode → decode returns original value via handler |

-----

## 13. Non-Goals

- **No BSON compatibility guarantee** — BSON may be used as an internal parsing aid (for JSON ingestion via existing libs) but is not part of the public contract; callers never see `BsonValue`
- **No schema compatibility rules** — backward/forward/full compatibility enforcement is a Schema Registry concern (Layer 2); Layer 0 only defines the structural encoding
- **No compression** — zstd compression is handled by the Storage Tier Manager; Layer 0 produces and consumes uncompressed bytes
- **No JSONPath evaluation** — handled by the JSON Functions Engine (Layer 1)
- **No object-graph reflection** — Layer 0 does not reflect over Kotlin data classes; each domain type provides its own `KdbCodec<T>` implementation
- **No streaming partial reads** — each encode/decode call operates on one complete value; sub-value streaming is out of scope for v1

-----

## 14. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| `KdbValue` hierarchy | 200 |
| Schema model (`KdbType`, `RecordSchema`, etc.) | 300 |
| `KdbTypeRegistry` + `LogicalTypeRegistry` | 250 |
| Binary encoder | 450 |
| Binary decoder | 500 |
| JSON ↔ typed model bridge | 400 |
| `KdbCodecRegistry` + `KdbCodec` interface | 150 |
| Built-in logical type handlers (date, timestamp, uuid, decimal, big-*) | 350 |
| `KdbBigMath` expect/actual (JVM + JS + native) | 150 |
| Primitive helpers (`KdbUuid`, `KdbHash`, `KdbTimestamp`) | 200 |
| expect/actual stubs (random, clock) | 50 |
| Error types | 60 |
| Unit tests | 400 |
| **Total** | **~3,500** |
