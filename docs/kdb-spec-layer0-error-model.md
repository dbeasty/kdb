# KDB Component Spec — Layer 0: Error Model

**Version:** 0.2  
**Layer:** 0 (Foundation — no dependencies)  
**Module:** `kdb-error` (`commonMain`)

**Companion:** [`kdb-spec-layer0-codec.md`](kdb-spec-layer0-codec.md) — `KdbDecodeException` / `KdbEncodeException` are thrown at binary and schema-guided JSON boundaries in `dev.kdb.codec`.

-----

## 1. Purpose

The Error Model defines every exception type the KDB engine can throw, their structured payload fields, and the conventions callers use to handle them. It is the single, exhaustive error vocabulary for the entire engine — every other module throws only types defined here. Centralising error types in Layer 0 means all dependent layers can reference the same sealed hierarchy without circular dependencies.

Layer 0 codec failures use **`KDB_DECODE_ERROR`** / **`KDB_ENCODE_ERROR`** (numeric legacy of removed BSON-named codes). Registry / type-binding failures use **`KDB_SCHEMA_ERROR`**.

-----

## 2. Dependencies

None. Layer 0 has no KDB dependencies. The only external dependency is Kotlin stdlib (`Exception`, `kotlin.reflect`).

-----

## 3. Public Interface

```kotlin
package dev.kdb.error

// ── Root exception ─────────────────────────────────────────────────────────────

/**
 * Base class for all KDB exceptions.
 * All engine exceptions are KdbException subclasses.
 * Callers that want to catch any KDB error catch this type.
 */
sealed class KdbException(
    message: String,
    cause: Throwable? = null
) : Exception(message, cause) {
    /** Machine-readable error code. Stable across versions. */
    abstract val code: KdbErrorCode
}

// ── Error code enum ────────────────────────────────────────────────────────────

/**
 * Stable machine-readable error codes.
 * Numeric values are fixed and must not be changed once published.
 * New codes are always additive.
 */
enum class KdbErrorCode(val numericCode: Int) {
    // Layer 0 — typed codec + schema registry (see kdb-spec-layer0-codec.md)
    /** Decode failure at binary or schema-guided JSON boundary. Numeric legacy of BSON decode. */
    KDB_DECODE_ERROR(1001),
    /** Encode failure at binary or schema-guided JSON boundary. Numeric legacy of BSON encode. */
    KDB_ENCODE_ERROR(1002),
    /** Frozen registry violations, unknown field IDs, incompatible logical bindings, etc. */
    KDB_SCHEMA_ERROR(1005),

    // Layer 1 — JSON path
    JSON_PATH_ERROR(2001),

    // Layer 2 — schema + DAG
    SCHEMA_VIOLATION(3001),
    SCHEMA_MIGRATION_FAILED(3002),
    VERSION_NOT_FOUND(3101),
    ICE_STORAGE(3102),
    COMPACTION_BOUNDARY(3103),

    // Layer 3 — write path + storage adapter
    CONFLICT(4001),
    STORAGE_TIER_ERROR(4101),
    NAMESPACE_NOT_FOUND(4201),

    // Layer 4 — index
    INDEX_CORRUPTION(5001),

    // Layer 6 — wire protocol
    UNSUPPORTED_PROTOCOL_VERSION(6001),
    ENCODING_NEGOTIATION_FAILURE(6002),

    // Layer 7 — ice restore
    ARCHIVE_RESTORE(7001),
}

// ── Exception hierarchy ────────────────────────────────────────────────────────

// ---------- Layer 0: typed codec + registry -----------------------------------

/**
 * Binary or schema-guided JSON input is malformed, truncated, or inconsistent with [KdbType].
 */
class KdbDecodeException(
    message: String,
    /** Byte offset within the input where decoding failed. -1 if unknown or not applicable (e.g. JSON). */
    val offset: Int = -1,
    cause: Throwable? = null
) : KdbException(message, cause) {
    override val code get() = KdbErrorCode.KDB_DECODE_ERROR
}

/**
 * A value cannot be encoded (e.g. NaN in double wire form, oversized payload for constraints).
 */
class KdbEncodeException(
    message: String,
    cause: Throwable? = null
) : KdbException(message, cause) {
    override val code get() = KdbErrorCode.KDB_ENCODE_ERROR
}

/**
 * Schema registry misuse: duplicate IDs, unresolved refs after freeze, incompatible logical annotation, etc.
 */
class KdbSchemaException(
    message: String,
    cause: Throwable? = null
) : KdbException(message, cause) {
    override val code get() = KdbErrorCode.KDB_SCHEMA_ERROR
}

/** @deprecated Binary-era names; prefer [KdbDecodeException]. */
@Deprecated("Use KdbDecodeException", ReplaceWith("KdbDecodeException"))
typealias BsonDecodeException = KdbDecodeException

/** @deprecated Binary-era names; prefer [KdbEncodeException]. */
@Deprecated("Use KdbEncodeException", ReplaceWith("KdbEncodeException"))
typealias BsonEncodeException = KdbEncodeException

// ---------- Layer 1: JSON path errors ----------------------------------------

/**
 * A JSONPath expression is syntactically invalid or does not match the
 * document structure in a context where a match is required.
 */
class JsonPathException(
    message: String,
    /** The invalid or non-matching path expression. */
    val path: String,
    cause: Throwable? = null
) : KdbException(message, cause) {
    override val code get() = KdbErrorCode.JSON_PATH_ERROR
}

// ---------- Layer 2: schema errors -------------------------------------------

/**
 * A write violates one or more declared schema constraints.
 * [violations] lists every failing field so the caller can surface them all.
 */
class SchemaViolationException(
    message: String,
    /** Per-field violation details. Never empty. */
    val violations: List<FieldViolation>
) : KdbException(message) {
    init {
        require(violations.isNotEmpty()) { "violations must be non-empty" }
    }

    override val code get() = KdbErrorCode.SCHEMA_VIOLATION
}

/** Details of a single field constraint violation. */
data class FieldViolation(
    val fieldName: String,
    val violationType: ViolationType,
    /** Human-readable description of the constraint that was violated. */
    val detail: String
)

enum class ViolationType {
    REQUIRED_FIELD_MISSING,
    TYPE_MISMATCH,
    UNIQUE_CONSTRAINT,
    ENUM_VALUE_NOT_DECLARED,
    CUSTOM_CONSTRAINT,
}

/**
 * A schema migration failed. The namespace has been automatically rolled back
 * to the pre-migration state.
 */
class SchemaMigrationException(
    message: String,
    val namespaceName: String,
    val failedStep: String,
    cause: Throwable? = null
) : KdbException(message, cause) {
    override val code get() = KdbErrorCode.SCHEMA_MIGRATION_FAILED
}

// ---------- Layer 2: DAG / version errors ------------------------------------

/**
 * The requested commit hash, branch, tag, or timestamp does not exist in
 * the local DAG.
 */
class VersionNotFoundException(
    message: String,
    val namespaceName: String,
    /** The hash, branch name, tag name, or timestamp string that was not found. */
    val reference: String
) : KdbException(message) {
    override val code get() = KdbErrorCode.VERSION_NOT_FOUND
}

/**
 * The requested commit has been archived to ice storage.
 * The caller must restore the archive before accessing this commit.
 */
class IceStorageException(
    message: String,
    val namespaceName: String,
    val commitHash: String,
    /** Location hint for the ice archive (e.g. object store URI). May be null if unknown. */
    val archiveLocation: String?
) : KdbException(message) {
    override val code get() = KdbErrorCode.ICE_STORAGE
}

/**
 * A peer's transaction base hash has been compacted away.
 * The peer must perform a full snapshot exchange before syncing.
 */
class CompactionBoundaryException(
    message: String,
    val namespaceName: String,
    val requestedBaseHash: String,
    val compactionBoundaryHash: String
) : KdbException(message) {
    override val code get() = KdbErrorCode.COMPACTION_BOUNDARY
}

// ---------- Layer 3: write path errors ---------------------------------------

/**
 * Transaction replay detected irreconcilable conflicts in STRICT or CUSTOM mode.
 * [report] carries full conflict detail for application-level resolution.
 */
class ConflictException(
    message: String,
    val report: ConflictReport
) : KdbException(message) {
    init {
        require(report.conflicts.isNotEmpty()) { "report.conflicts must be non-empty" }
    }

    override val code get() = KdbErrorCode.CONFLICT
}

/** Full conflict report returned with a ConflictException. */
data class ConflictReport(
    val transactionId: String,
    val baseHash: String,
    val targetHash: String,
    val conflicts: List<ConflictItem>
)

/** One conflicting operation within a transaction replay. */
data class ConflictItem(
    val documentId: String,
    val operationType: ConflictOperationType,
    /** The local version of the document at conflict time. Null if document was deleted. */
    val localDoc: String?,
    /** The incoming version of the document. Null if incoming op was a delete. */
    val incomingDoc: String?,
)

enum class ConflictOperationType {
    CONCURRENT_WRITE,
    WRITE_DELETE,
    DELETE_WRITE,
    SCHEMA_INCOMPATIBLE,
}

// ---------- Layer 3: storage adapter errors ----------------------------------

/**
 * A storage tier transition failed (e.g. object store unreachable, disk full).
 */
class StorageTierException(
    message: String,
    val tier: String,
    cause: Throwable? = null
) : KdbException(message, cause) {
    override val code get() = KdbErrorCode.STORAGE_TIER_ERROR
}

/**
 * The requested namespace does not exist on this node.
 */
class NamespaceNotFoundException(
    message: String,
    val namespaceName: String
) : KdbException(message) {
    override val code get() = KdbErrorCode.NAMESPACE_NOT_FOUND
}

// ---------- Layer 4: index errors --------------------------------------------

/**
 * An index is inconsistent with the document tree.
 * The engine has automatically triggered an index rebuild.
 * The caller should retry the operation after the rebuild completes.
 */
class IndexCorruptionException(
    message: String,
    val namespaceName: String,
    val indexName: String,
    cause: Throwable? = null
) : KdbException(message, cause) {
    override val code get() = KdbErrorCode.INDEX_CORRUPTION
}

// ---------- Layer 6: wire protocol errors ------------------------------------

/**
 * A remote peer requires a wire protocol version that this node does not support.
 */
class UnsupportedProtocolVersionException(
    message: String,
    val peerRequiredVersion: Int,
    val localMaxVersion: Int
) : KdbException(message) {
    override val code get() = KdbErrorCode.UNSUPPORTED_PROTOCOL_VERSION
}

/**
 * Handshake failed because there is no mutually supported encoding
 * (e.g. JSON-only peer vs typed-binary-only peer).
 */
class EncodingNegotiationFailureException(
    message: String,
    val localEncodings: List<String>,
    val peerEncodings: List<String>
) : KdbException(message) {
    override val code get() = KdbErrorCode.ENCODING_NEGOTIATION_FAILURE
}

// ---------- Layer 7: archive restore errors ----------------------------------

/**
 * Retrieval of an ice archive failed (network error, checksum mismatch,
 * corrupt archive).
 */
class ArchiveRestoreException(
    message: String,
    val archiveLocation: String,
    cause: Throwable? = null
) : KdbException(message, cause) {
    override val code get() = KdbErrorCode.ARCHIVE_RESTORE
}

// ── Result type helpers ────────────────────────────────────────────────────────

/**
 * KDB-specific result type.
 * Used by functions that need to return errors without throwing
 * (e.g. batch operations where partial failure is valid).
 */
sealed class KdbResult<out T> {
    data class Success<T>(val value: T) : KdbResult<T>()
    data class Failure(val exception: KdbException) : KdbResult<Nothing>()

    val isSuccess: Boolean get() = this is Success
    val isFailure: Boolean get() = this is Failure

    fun getOrNull(): T? = (this as? Success)?.value
    fun exceptionOrNull(): KdbException? = (this as? Failure)?.exception
    fun getOrThrow(): T = when (this) {
        is Success -> value
        is Failure -> throw exception
    }
    inline fun <R> map(transform: (T) -> R): KdbResult<R>
    inline fun onSuccess(action: (T) -> Unit): KdbResult<T>
    inline fun onFailure(action: (KdbException) -> Unit): KdbResult<T>
}

/** Wrap a block in a KdbResult, catching only KdbException. */
inline fun <T> kdbRunCatching(block: () -> T): KdbResult<T> =
    try { KdbResult.Success(block()) }
    catch (e: KdbException) { KdbResult.Failure(e) }
```

-----

## 4. Data Structures

All data structures are defined in Section 3 inline with their owning exception class. Key owned types summary:

|Type                   |Purpose                                                        |
|-----------------------|---------------------------------------------------------------|
|`KdbException`         |Sealed root; all engine exceptions extend this                 |
|`KdbErrorCode`         |Stable numeric code per exception type                         |
|`KdbDecodeException`, `KdbEncodeException`, `KdbSchemaException` |Layer 0 typed codec decode/encode and frozen-registry errors |
|`FieldViolation`       |Per-field schema constraint failure detail                     |
|`ViolationType`        |Enum of schema violation categories                            |
|`ConflictReport`       |Full payload for a failed transaction replay                   |
|`ConflictItem`         |One conflicting operation with local and incoming doc snapshots|
|`ConflictOperationType`|Enum of conflict categories                                    |
|`KdbResult<T>`         |Discriminated union for non-throwing error propagation         |

-----

## 5. Contracts

### `KdbException` hierarchy

- **Guarantee:** every exception the engine throws is a `KdbException` subclass; no engine code throws raw `RuntimeException` or `IllegalArgumentException` to callers
- **Guarantee:** `code` is always non-null and uniquely identifies the exception type

### `SchemaViolationException`

- **Pre:** `violations` must be non-empty (if there are no violations, the exception must not be thrown)
- **Guarantee:** `violations` contains one entry per failing field; fields that passed validation are absent

### `ConflictException`

- **Guarantee:** `report.conflicts` is non-empty
- **Guarantee:** `report.baseHash` and `report.targetHash` are the exact hashes the engine used during replay

### `KdbResult`

- **Guarantee:** `getOrThrow()` either returns the value or throws the wrapped `KdbException` — never a different exception type
- **Guarantee:** `kdbRunCatching` only catches `KdbException`; non-KDB exceptions propagate normally

### `KdbErrorCode` numeric values

- **Guarantee:** numeric codes are stable across versions; existing values are never reassigned
- **Guarantee:** new codes are always added at the end of their group; gaps are acceptable

-----

## 6. Error Cases

This module is the error model itself — it does not throw errors. The only situation where this module can fail is programmer error:

|Situation                                                          |Behaviour                                                              |
|-------------------------------------------------------------------|-----------------------------------------------------------------------|
|`SchemaViolationException` constructed with empty `violations` list|`IllegalArgumentException` in the constructor (programming error guard)|
|`KdbResult.getOrThrow()` called on `Failure`                       |Re-throws the wrapped `KdbException`                                   |
|`kdbRunCatching` block throws a non-`KdbException`                 |Exception propagates uncaught — correct behaviour                      |

-----

## 7. Test Cases

|# |Name                                                  |Input / Scenario                                        |Expected                                                       |
|--|------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------------|
|1 |**code uniqueness**                                   |All `KdbErrorCode` entries                              |No two codes share the same `numericCode` value                |
|2 |**KdbDecodeException fields**                          |Construct with `offset=42`                              |`exception.offset == 42`; `exception.code == KDB_DECODE_ERROR` |
|3 |**SchemaViolationException — all violations returned**|Construct with 3 `FieldViolation`s                      |`exception.violations.size == 3`; each field accessible        |
|4 |**SchemaViolationException — empty violations guard** |Construct with empty list                               |`IllegalArgumentException` thrown                              |
|5 |**ConflictReport round-trip**                         |Construct `ConflictReport` with 2 `ConflictItem`s       |All fields accessible; `report.conflicts.size == 2`            |
|6 |**KdbResult.Success**                                 |`kdbRunCatching { 42 }`                                 |`isSuccess == true`; `getOrThrow() == 42`                      |
|7 |**KdbResult.Failure**                                 |`kdbRunCatching { throw SchemaViolationException(...) }`|`isFailure == true`; `exceptionOrNull()` returns the exception |
|8 |**KdbResult non-KDB exception propagates**            |`kdbRunCatching { throw NullPointerException() }`       |`NullPointerException` propagates out uncaught                 |
|9 |**catch by root type**                                |Throw `IceStorageException`; catch as `KdbException`    |Caught successfully; `code == ICE_STORAGE`                     |
|10|**IceStorageException — null archiveLocation**        |Construct with `archiveLocation = null`                 |`exception.archiveLocation == null`; no NPE                    |
|11|**KdbResult.map on Success**                          |`KdbResult.Success(5).map { it * 2 }`                   |`KdbResult.Success(10)`                                        |
|12|**KdbResult.map on Failure — error case**             |`KdbResult.Failure(e).map { it }`                       |Returns `KdbResult.Failure(e)` unchanged                       |

-----

## 8. Non-Goals

- **No logging** — this module does not log; logging is the responsibility of the calling layer
- **No i18n / localisation** — exception messages are English developer strings; user-facing messages are the application’s responsibility
- **No error recovery logic** — this module only defines the taxonomy; recovery strategies live in their respective layer modules
- **No serialisation of exceptions** — wire-protocol representation of errors is handled by the Wire Protocol module (Layer 6); this module does not define encoding of exceptions on the wire
- **No cause-chaining policy** — individual modules decide whether to wrap lower-level exceptions as `cause`; this module imposes no rule beyond making `cause` available on all types
- **No HTTP status mapping** — mapping `KdbErrorCode` to HTTP status codes is the application’s responsibility

-----

## 9. Implementation Notes

### Sealed hierarchy enforcement

- `KdbException` is `sealed`; all subclasses are defined in this module; no other module can extend it directly. This ensures the full set of error types is known at compile time.

### Kotlin Multiplatform constraints

- `Exception` and `Throwable` are available in `commonMain`; no platform-specific types are used
- Do not extend `RuntimeException` or `IOException` — these are JVM-centric; `Exception` is the correct `commonMain` base
- On the JVM target, all exceptions will naturally extend `java.lang.Exception` through the Kotlin hierarchy — this is correct for JDBC interop (JDBC catches `java.lang.Exception`)

### KdbResult vs throw

- Throwing exceptions is the default for all synchronous engine operations
- `KdbResult` is provided for batch APIs where partial success is meaningful (e.g. bulk write returning per-document results)
- Do not use `KdbResult` in internal engine code; use it only at public API boundaries where batch semantics apply

### Immutability

- All exception payload classes (`FieldViolation`, `ConflictReport`, `ConflictItem`) are `data class` with immutable fields
- `violations: List<FieldViolation>` and `conflicts: List<ConflictItem>` are stored as `List` (read-only); callers cannot mutate them

### Numeric code assignment policy

- Groups: 1000s = codec + Layer 0 schema registry (`1005`), 2000s = document/JSON path, 3000s = schema/DAG, 4000s = write path, 5000s = indexes, 6000s = wire, 7000s = archive
- **`1003`–`1004`** intentionally unused today; **`1005`** is `KDB_SCHEMA_ERROR`
- Leave gaps within groups for later additions without renumbering

-----

## 10. Estimated Lines

|Sub-component                                              |Est. NBNC lines|
|-----------------------------------------------------------|---------------|
|`KdbException` sealed root + `KdbErrorCode` enum           |60             |
|Codec exceptions (`KdbDecodeException`, `KdbEncodeException`, `KdbSchemaException`, deprecated BSON aliases)|35             |
|Schema exceptions + payload data classes                   |80             |
|DAG/version exceptions                                     |60             |
|Write path / conflict exceptions + payload                 |80             |
|Storage / network / archive exceptions                     |60             |
|`KdbResult<T>` + `kdbRunCatching`                          |60             |
|Unit tests                                                 |70             |
|**Total**                                                  |**~505**       |