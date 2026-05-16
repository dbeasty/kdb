# KDB — Component Spec: Schema Engine

**Layer:** 2  
**Component:** 5  
**Package:** `dev.kdb.schema`  
**File:** `kdb-spec-layer2-component5-schema-engine.md`  
**Status:** Implementation-ready

-----

## 1. Purpose

The Schema Engine declares, validates, stores, and evolves the typed field lens that SQL can index and query over each namespace. It enforces type correctness, required-field presence, uniqueness constraints, and enum membership on every document write, while explicitly ignoring any extra fields beyond the declared schema. It also manages schema versioning: every schema change produces a content-addressed identity (`schemaHash` on `KdbSchema`) that is referenced by commits in the DAG, so historical checkouts always see the schema that was in effect at that point in time.

-----

## 2. Dependencies

|Module                                                |Interfaces used                                                                                                                                                                                                                                                                                                                    |
|------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|`dev.kdb.codec` (Layer 0 — Type System & Codec)       |`KdbUuid`, `KdbHash`, `KdbTimestamp`, `KdbValue`, `KdbType`, `KdbTypeRegistry`, `encodeToBytes`, `decodeFromBytes` — per `kdb-spec-layer0-codec.md`. BSON is not a public interchange format.                                                                                                                                       |
|`dev.kdb.error` (Layer 0 — Error Model)               |`KdbException`, `KdbErrorCode`, `SchemaViolationException`, `SchemaMigrationException`, `FieldViolation`, `ViolationType`, `KdbDecodeException`, `KdbResult`, `kdbRunCatching`                                                                                                                                                        |
|`dev.kdb.document` (Layer 1 — Document + Commit Model)|`KdbDocument` (canonical body in `.json`; identity `id`; content hashing via Layer 0 record encoding — see Layer 1 component 3 spec), `KdbOp.SchemaMigration`                                                                                                                                                                     |
|`dev.kdb.json` (Layer 1 — JSON Functions Engine)      |`JsonValue`, `kdbJsonGet`, `kdbJsonType`                                                                                                                                                                                                                                                                                           |

-----

## 3. Public Interface

```kotlin
package dev.kdb.schema

import dev.kdb.codec.*
import dev.kdb.document.KdbDocument
import dev.kdb.error.*
import dev.kdb.json.JsonValue

// ── Layer 0 registry (schema wire shapes) ───────────────────────────────────────

/** Builtin schemas for every `dev.kdb.schema.*` record / enum / union used by encode/decode in this module. */
fun KdbSchemaWireRegistry(): KdbTypeRegistry

// ── Field type hierarchy ───────────────────────────────────────────────────────

sealed class KdbFieldType {
    object StringType    : KdbFieldType()
    object Int32Type     : KdbFieldType()
    object Int64Type     : KdbFieldType()
    object Float64Type   : KdbFieldType()
    object BoolType      : KdbFieldType()
    object TimestampType : KdbFieldType()
    object UuidType      : KdbFieldType()
    object ObjectType    : KdbFieldType()
    object ArrayType     : KdbFieldType()
    data class EnumType(val values: Set<String>) : KdbFieldType() {
        init { require(values.isNotEmpty()) { "EnumType must have at least one value" } }
    }

    fun sqlTypeName(): String

    /** JDBC / introspection hint aligned with Layer 0 physical mapping (not BSON). */
    fun codecTypeLabel(): String
}

// ── Field declaration ─────────────────────────────────────────────────────────

data class SchemaField(
    /** Field name as it appears in JSON documents and SQL columns. */
    val name: String,
    /** Declared type. Validation enforces this on every write. */
    val type: KdbFieldType,
    /** If true, documents missing this field fail validation. */
    val required: Boolean,
    /** If true, a SQL index is maintained for this field. */
    val indexed: Boolean,
    /** If true, no two documents in the namespace may share this value. */
    val unique: Boolean = false,
) {
    init {
        require(name.matches(Regex("[a-zA-Z_][a-zA-Z0-9_]*"))) {
            "Field name must be a valid identifier: $name"
        }
        require(!(unique && !indexed)) { "unique=true requires indexed=true: $name" }
    }
}

// ── Schema declaration ─────────────────────────────────────────────────────────

/**
 * The full schema for one namespace at one point in time.
 * Immutable once constructed. Identity is [schemaHash].
 */
data class KdbSchema(
    /** Stable content-addressed identity. SHA-256 of canonical Layer 0 encoding of the schema record. */
    val schemaHash: KdbHash,
    /** Ordered list of declared fields. Order determines SQL column order. */
    val fields: List<SchemaField>,
    /** Monotonically increasing version counter within a namespace. */
    val version: Int,
    /** When this schema version was created. */
    val createdAt: KdbTimestamp,
    /** Human-readable description of this version (optional). */
    val description: String = "",
) {
    /** Map from field name → SchemaField for O(1) lookup. */
    val fieldsByName: Map<String, SchemaField>

    fun hasField(name: String): Boolean
    fun field(name: String): SchemaField?
    fun fieldOrThrow(name: String): SchemaField
    fun indexedFields(): List<SchemaField>
    fun requiredFields(): List<SchemaField>
    fun uniqueFields(): List<SchemaField>

    companion object {
        /** Sentinel for schema-less namespaces. */
        val NONE: KdbSchema

        fun build(fields: List<SchemaField>, version: Int = 1, createdAt: KdbTimestamp = KdbTimestamp.now(), description: String = ""): KdbSchema
    }
}

val KdbSchema.isNone: Boolean

// ── Schema serialisation (Layer 0) ────────────────────────────────────────────

fun KdbSchema.toKdbValue(): KdbValue
fun KdbSchema.Companion.fromKdbValue(value: KdbValue): KdbSchema

/** Canonical typed-binary encoding using [KdbSchemaWireRegistry] and the registered schema wire type. */
fun KdbSchema.toBytes(): ByteArray
fun KdbSchema.Companion.fromBytes(bytes: ByteArray): KdbSchema

// ── Migration DSL ─────────────────────────────────────────────────────────────

/**
 * Describes a single backward-compatible or breaking schema change.
 * Produced by [SchemaMigrationBuilder] and applied by [SchemaEngine.applyMigration].
 */
data class SchemaMigration(
    val migrationId: KdbUuid,
    val fromVersion: Int,
    val toVersion: Int,
    val steps: List<MigrationStep>,
    val description: String = "",
) {
    companion object {
        fun fromKdbValue(value: KdbValue): SchemaMigration
        fun fromBytes(bytes: ByteArray): SchemaMigration
    }
}

sealed class MigrationStep {
    data class AddField(val field: SchemaField)                                    : MigrationStep()
    data class DropField(val fieldName: String)                                    : MigrationStep()
    data class RenameField(val oldName: String, val newName: String)               : MigrationStep()
    data class ChangeType(val fieldName: String, val newType: KdbFieldType)        : MigrationStep()
    data class AddIndex(val fieldName: String)                                     : MigrationStep()
    data class DropIndex(val fieldName: String)                                    : MigrationStep()
    data class SetRequired(val fieldName: String, val required: Boolean)           : MigrationStep()
    data class SetUnique(val fieldName: String, val unique: Boolean)               : MigrationStep()
    data class WidenEnum(val fieldName: String, val addValues: Set<String>)        : MigrationStep()
    data class NarrowEnum(val fieldName: String, val removeValues: Set<String>)    : MigrationStep()
}

fun MigrationStep.isBreaking(): Boolean

/** DSL builder. */
class SchemaMigrationBuilder(private val baseSchema: KdbSchema) {
    fun addField(name: String, type: KdbFieldType, required: Boolean = false, indexed: Boolean = false, unique: Boolean = false): SchemaMigrationBuilder
    fun dropField(name: String): SchemaMigrationBuilder
    fun renameField(oldName: String, newName: String): SchemaMigrationBuilder
    fun changeType(fieldName: String, newType: KdbFieldType): SchemaMigrationBuilder
    fun addIndex(fieldName: String): SchemaMigrationBuilder
    fun dropIndex(fieldName: String): SchemaMigrationBuilder
    fun setRequired(fieldName: String, required: Boolean): SchemaMigrationBuilder
    fun setUnique(fieldName: String, unique: Boolean): SchemaMigrationBuilder
    fun widenEnum(fieldName: String, vararg addValues: String): SchemaMigrationBuilder
    fun narrowEnum(fieldName: String, vararg removeValues: String): SchemaMigrationBuilder
    fun description(text: String): SchemaMigrationBuilder

    /** Validates all steps against [baseSchema] and returns a ready-to-apply [SchemaMigration]. */
    fun build(migrationId: KdbUuid = KdbUuid.random()): SchemaMigration
}

fun KdbSchema.migrate(block: SchemaMigrationBuilder.() -> Unit): SchemaMigration

// ── Schema engine ─────────────────────────────────────────────────────────────

/**
 * Core operations on schemas: validation, migration application, hashing.
 * Stateless — callers supply all inputs; the engine returns results.
 * Storage of schema versions is the caller's responsibility.
 */
object SchemaEngine {

    /**
     * Validate [document] against [schema].
     * Returns [KdbResult.Success] with the document unchanged if valid.
     * Returns [KdbResult.Failure] wrapping [SchemaViolationException] if any field fails.
     * Extension fields (not in schema) are never checked.
     */
    fun validate(document: KdbDocument, schema: KdbSchema): KdbResult<KdbDocument>

    /**
     * Apply a [SchemaMigration] to [currentSchema] and return the new [KdbSchema].
     * Does not touch documents — callers handle document migration separately.
     * Throws [SchemaMigrationException] if any step is inconsistent with [currentSchema].
     */
    fun applyMigration(currentSchema: KdbSchema, migration: SchemaMigration): KdbResult<KdbSchema>

    /**
     * Compute the content hash (SHA-256 of canonical Layer 0 bytes) for a schema.
     * Deterministic: same fields in same order always produce the same hash.
     */
    fun computeSchemaHash(schema: KdbSchema): KdbHash

    /**
     * Check whether [migration] is backward-compatible with [currentSchema].
     * Returns true if no breaking steps are present.
     */
    fun isBackwardCompatible(currentSchema: KdbSchema, migration: SchemaMigration): Boolean

    /**
     * Produce a human-readable diff between two schema versions.
     */
    fun diff(from: KdbSchema, to: KdbSchema): SchemaDiff

    /**
     * Check whether [value] is type-compatible with [field].
     * Used internally by validate; exposed for testing and external callers.
     */
    fun checkFieldValue(field: SchemaField, value: JsonValue?): FieldViolation?
}

// ── Schema diff ───────────────────────────────────────────────────────────────

data class SchemaDiff(
    val addedFields: List<SchemaField>,
    val removedFields: List<SchemaField>,
    val modifiedFields: List<FieldDiff>,
    val fromVersion: Int,
    val toVersion: Int,
) {
    val isEmpty: Boolean
    val isBreaking: Boolean
}

data class FieldDiff(
    val fieldName: String,
    val changes: List<FieldChange>,
)

sealed class FieldChange {
    data class TypeChanged(val from: KdbFieldType, val to: KdbFieldType)     : FieldChange()
    data class RequiredChanged(val from: Boolean, val to: Boolean)           : FieldChange()
    data class IndexedChanged(val from: Boolean, val to: Boolean)            : FieldChange()
    data class UniqueChanged(val from: Boolean, val to: Boolean)             : FieldChange()
    data class EnumValuesChanged(val added: Set<String>, val removed: Set<String>) : FieldChange()
}

// ── Migration serialisation (Layer 0) ─────────────────────────────────────────

fun SchemaMigration.toKdbValue(): KdbValue
fun SchemaMigration.toBytes(): ByteArray

// ── Exceptions ────────────────────────────────────────────────────────────────

class SchemaDecodeException(
    message: String,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.KDB_DECODE_ERROR
}

class SchemaMigrationConflictException(
    message: String,
    val step: MigrationStep,
    val reason: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.SCHEMA_MIGRATION_FAILED
}
```

-----

## 4. Data Structures

### `KdbFieldType`

Sealed hierarchy of all supported field types. `EnumType` carries its allowed string values. `ObjectType` and `ArrayType` are stored and returned as JSON but are not index-eligible unless accessed via JSON functions such as `kdbJsonGet`. `sqlTypeName()` returns the SQL column type string for the query engine; `codecTypeLabel()` returns the Layer 0 physical-type hint used for JDBC/introspection (aligned with `kdb-spec-layer0-codec.md`, not BSON).

### `SchemaField`

Immutable declaration of one field: name, type, required, indexed, unique. Name must satisfy `[a-zA-Z_][a-zA-Z0-9_]*`. `unique` implies `indexed`.

### `KdbSchema`

Immutable snapshot of all fields at one schema version. Identity is `schemaHash` (SHA-256 of the canonical Layer 0 byte encoding of the schema wire record). `KdbSchema.NONE` is the sentinel for schema-less namespaces: `isNone == true`, `fields` is empty, `schemaHash` is a fixed well-known hash (see implementation notes). `fieldsByName` is a `LinkedHashMap` preserving declaration order.

### `SchemaMigration`

Ordered list of `MigrationStep` values describing a transition from `fromVersion` to `toVersion`. Steps are applied in sequence; earlier steps affect later ones (e.g. rename then drop uses the new name). `migrationId` is a UUID used by the commit DAG to deduplicate replayed migrations.

### `MigrationStep` variants

|Step                      |Breaking?|Notes                                   |
|--------------------------|---------|----------------------------------------|
|`AddField(required=false)`|No       |Safe to add optional fields             |
|`AddField(required=true)` |Yes      |Existing docs lack the field            |
|`DropField`               |Yes      |Removes column from SQL                 |
|`RenameField`             |Yes      |SQL column name changes                 |
|`ChangeType`              |Yes      |Existing values may be incompatible     |
|`AddIndex`                |No       |Only affects index layer                |
|`DropIndex`               |No       |Only affects index layer                |
|`SetRequired(true)`       |Yes      |Existing docs may lack the field        |
|`SetRequired(false)`      |No       |Relaxes constraint                      |
|`SetUnique(true)`         |Yes      |Existing docs may violate uniqueness    |
|`SetUnique(false)`        |No       |Relaxes constraint                      |
|`WidenEnum`               |No       |New values; old values still valid      |
|`NarrowEnum`              |Yes      |Existing docs may contain removed values|

### `SchemaDiff`

Human-readable description of the difference between two schema versions. `isBreaking` is true if any breaking step is present.

-----

## 5. Contracts

### `SchemaEngine.validate(document, schema)`

- **Preconditions:** `document.json` is valid JSON; `schema` is not `NONE` (if `NONE`, callers must skip validation).
- **Postconditions on success:** Every required field is present and non-null; every present schema field’s value is type-compatible; all unique constraints have been declared (actual uniqueness is checked by the index layer, not here).
- **Postconditions on failure:** Returns `KdbResult.Failure` wrapping `SchemaViolationException` with one `FieldViolation` per failing field. Never throws directly.
- **Extension fields:** Fields in the document not declared in `schema` are silently ignored — never flagged as violations.
- **`KdbSchema.NONE`:** If `schema.isNone`, returns `KdbResult.Success(document)` immediately.

### `SchemaEngine.applyMigration(currentSchema, migration)`

- **Preconditions:** `migration.fromVersion == currentSchema.version`.
- **Postconditions on success:** Returns a new `KdbSchema` with `version == migration.toVersion`, a freshly computed `schemaHash`, and all steps applied in order.
- **Postconditions on failure:** Returns `KdbResult.Failure` wrapping `SchemaMigrationException` if any step references a field that does not exist, attempts to add a field that already exists, or narrows an enum to remove a value not currently in the enum.
- **Idempotency:** Applying the same `SchemaMigration` (same `migrationId`) twice to a schema that is already at `toVersion` returns the current schema unchanged (detected by version equality).

### `SchemaEngine.computeSchemaHash(schema)`

- **Determinism:** Same fields in same declaration order → identical hash.
- **Canonical Layer 0 encoding:** `fields` serialised as a deterministic `KdbValue` graph registered in `KdbSchemaWireRegistry()` (record/array layouts per `kdb-spec-layer0-codec.md`: stable field ordinals, deterministic ordering rules for nested collections). `schemaHash` is SHA-256 of `encodeToBytes(schema.toKdbValue(), SchemaWireType, registry)` (exact wire type name is an implementation detail fixed in the registry).

### `SchemaMigrationBuilder.build()`

- **Validates** all steps against the base schema before returning. Throws `SchemaMigrationConflictException` if any step is invalid.
- **`fromVersion`** is set to `baseSchema.version`; **`toVersion`** is `baseSchema.version + 1`.

### `KdbFieldType.sqlTypeName()`

Returns the SQL column type string used by the query engine. Mapping:

|KdbFieldType |SQL type   |
|-------------|-----------|
|StringType   |`TEXT`     |
|Int32Type    |`INTEGER`  |
|Int64Type    |`BIGINT`   |
|Float64Type  |`REAL`     |
|BoolType     |`BOOLEAN`  |
|TimestampType|`TIMESTAMP`|
|UuidType     |`TEXT`     |
|ObjectType   |`JSON`     |
|ArrayType    |`JSON`     |
|EnumType     |`TEXT`     |

-----

## 6. Error Cases

|Exception                         |When thrown                                                                                                                                                                                   |
|----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|`SchemaViolationException`        |`validate()` finds one or more field violations. `violations` list contains one entry per failing field.                                                                                      |
|`SchemaMigrationException`        |`applyMigration()` detects an inconsistency between a migration step and the current schema (e.g. adding a field that already exists, dropping a field that does not exist, version mismatch).|
|`SchemaMigrationConflictException`|`SchemaMigrationBuilder.build()` validation fails.                                                                                                                                            |
|`SchemaDecodeException`           |`KdbSchema.Companion.fromBytes()` / `fromKdbValue()` or `SchemaMigration.Companion.fromBytes()` / `fromKdbValue()` receives malformed or truncated Layer 0 bytes / values.                                                                                       |

-----

## 7. Test Cases

### TC-01 — Valid document passes validation

**Input:** document `{"userId":"abc","email":"a@b.com","status":"active","createdAt":"2024-01-01T00:00:00Z"}`, schema with those four required fields.  
**Expected:** `KdbResult.Success(document)`

### TC-02 — Missing required field fails validation

**Input:** document `{"userId":"abc","status":"active"}` (missing `email`), same schema.  
**Expected:** `KdbResult.Failure(SchemaViolationException)` with `violations` containing `FieldViolation("email", REQUIRED_FIELD_MISSING, ...)`

### TC-03 — Type mismatch fails validation

**Input:** document `{"userId":"abc","email":"a@b.com","status":"active","score":"not-a-number"}`, schema with `score: Int32Type`.  
**Expected:** `KdbResult.Failure` with `ViolationType.TYPE_MISMATCH` for `score`.

### TC-04 — Extension fields are ignored

**Input:** document with all required schema fields plus `{"clientField":{"source":"ios"},"tags":["a","b"]}`.  
**Expected:** `KdbResult.Success` — extension fields never cause violations.

### TC-05 — Enum value not declared

**Input:** document with `status = "deleted"`, schema declares `EnumType("active","inactive")`.  
**Expected:** `KdbResult.Failure` with `ViolationType.ENUM_VALUE_NOT_DECLARED`.

### TC-06 — Backward-compatible migration: add optional field

**Input:** schema v1 with fields `[userId, email]`; migration `addField("score", Int32Type, required=false)`.  
**Expected:** `KdbResult.Success(schemaV2)` with `fields` containing `score`; `isBackwardCompatible == true`.

### TC-07 — Breaking migration: add required field

**Input:** schema v1; migration `addField("phone", StringType, required=true)`.  
**Expected:** `applyMigration` returns `Success(schemaV2)`; `isBackwardCompatible == false`; `MigrationStep.AddField(required=true).isBreaking() == true`.

### TC-08 — Migration version mismatch is rejected

**Input:** schema at version 3; migration with `fromVersion = 1`.  
**Expected:** `KdbResult.Failure(SchemaMigrationException)`.

### TC-09 — Schema hash is deterministic

**Input:** same two `KdbSchema` objects built independently with identical field lists in identical order.  
**Expected:** `schema1.schemaHash == schema2.schemaHash`.

### TC-10 — `KdbSchema.NONE` passes validation unconditionally

**Input:** any document, `KdbSchema.NONE`.  
**Expected:** `KdbResult.Success(document)`.

### TC-11 — `SchemaMigration` Layer 0 round-trip

**Input:** a `SchemaMigration` with all ten step types.  
**Expected:** `SchemaMigration.fromBytes(migration.toBytes())` yields an equal migration (same steps, versions, id); equivalently `SchemaMigration.fromKdbValue(migration.toKdbValue())`.

### TC-12 — WidenEnum then NarrowEnum round-trip on diff

**Input:** schema v1 with `EnumType("a","b")`; migrate to v2 widening to `{"a","b","c"}`; diff v1 → v2.  
**Expected:** `diff.modifiedFields[0].changes` contains `EnumValuesChanged(added={"c"}, removed={})`.

-----

## 8. Non-Goals

- **Does not persist schema versions** — storage is the caller’s responsibility; the engine only computes and transforms.
- **Does not enforce unique constraints** — that is the index layer’s job. `SchemaEngine.validate()` declares the intent; the index layer enforces it at write time.
- **Does not migrate documents** — `applyMigration` only changes the schema record. Document backfill is a separate concern handled by the Transaction Engine (Layer 3).
- **Does not handle SQL DDL parsing** — schema declarations come in via the Kotlin API or Layer 0–encoded migrations (`toBytes` / `fromBytes`), not SQL strings.
- **Does not version namespaces** — schema versions are referenced by commits; namespace lifecycle management belongs to the Commit DAG (Component 6) and higher layers.
- **Does not index fields** — indexing is the Index Layer’s concern (Component 8).

-----

## 9. Implementation Notes

### Canonical Layer 0 encoding for hash stability

Follow `kdb-spec-layer0-codec.md`: register explicit wire types for `KdbSchema`, `SchemaField`, and `SchemaMigration` / `MigrationStep` in `KdbSchemaWireRegistry()`. Use stable record field ordinals (numeric keys), deterministic ordering for variable-length collections (e.g. enum value sets sorted lexicographically), and the single canonical `encodeToBytes` path — never alternate JSON or ad hoc maps for hashing.

### `KdbSchema.NONE` identity

Use a fixed well-known `schemaHash` constant (e.g. SHA-256 of the canonical encoding of an empty schema record). Never compute it lazily — it must be identical across all platforms and engine versions.

### EnumType serialisation

Represent allowed enum strings as a deterministically ordered structure in `KdbValue` (e.g. sorted array of strings). Sorting is required for hash stability.

### Validation performance

`SchemaEngine.validate()` will be called on every document write. Pre-compute `fieldsByName` on `KdbSchema` construction and cache it. Avoid allocating intermediate collections during validation — iterate `schema.fields` once, collecting all violations, then return.

### `kdbJsonGet` usage in validation

Use `dev.kdb.json.kdbJsonGet(document.json, "$.fieldName")` to extract each schema field’s value. This keeps type-extraction logic in the JSON Functions Engine where it belongs. Do not re-implement JSON traversal here.

### Migration step ordering

Steps are applied in sequence. A `RenameField` in step 1 means step 2 must use the new name. The builder validates this as each step is added.

### Kotlin Multiplatform

All code in `commonMain`. No platform-specific types. SHA-256 must use a `commonMain` implementation — `kotlinx-crypto` or a bundled pure-Kotlin implementation (same approach as `:kdb-document`). `KdbTimestamp.now()` uses the shared Layer 0 timestamp primitive already used elsewhere in the codebase.

-----

## 10. Estimated Lines

|Section                                      |Est. lines|
|---------------------------------------------|----------|
|`KdbFieldType` hierarchy + helpers           |150       |
|`SchemaField` + validation                   |80        |
|`KdbSchema` + `fieldsByName` + helpers       |200       |
|Schema Layer 0 wire encoding / decoding      |250       |
|`SchemaMigrationBuilder` + DSL               |300       |
|`MigrationStep` sealed class + `isBreaking()`|120       |
|`SchemaMigration` + Layer 0 round-trip        |150       |
|`SchemaEngine.validate()`                    |200       |
|`SchemaEngine.applyMigration()`              |250       |
|`SchemaEngine.computeSchemaHash()`           |80        |
|`SchemaEngine.isBackwardCompatible()`        |60        |
|`SchemaEngine.diff()` + `SchemaDiff` types   |200       |
|`SchemaEngine.checkFieldValue()`             |150       |
|Exceptions                                   |60        |
|Tests                                        |400       |
|**Total**                                    |**~2,650**|