package dev.kdb.schema

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.encodeToBytes
import dev.kdb.document.KdbDocument
import dev.kdb.document.kdbSha256
import dev.kdb.error.FieldViolation
import dev.kdb.error.KdbResult
import dev.kdb.error.SchemaMigrationException
import dev.kdb.error.SchemaViolationException
import dev.kdb.error.ViolationType
import dev.kdb.json.JsonValue
import dev.kdb.json.kdbJsonGet
import kotlin.math.abs

/** Stateless schema validation + migration transforms ([Layer 2 §5]). */
public object SchemaEngine {

    public fun validate(
        document: KdbDocument,
        schema: KdbSchema,
    ): KdbResult<KdbDocument> {
        if (schema.isNone) {
            return KdbResult.Success(document)
        }
        val violations = mutableListOf<FieldViolation>()
        for (field in schema.fields) {
            val path = "$.${field.name}"
            val raw: JsonValue? =
                try {
                    kdbJsonGet(document.json, path)
                } catch (_: Throwable) {
                    violations.add(
                        FieldViolation(
                            field.name,
                            ViolationType.TYPE_MISMATCH,
                            "invalid JSON path access for ${field.name}",
                        ),
                    )
                    continue
                }
            val absentOrNull = raw == null || raw === JsonValue.JNull
            if (field.required && absentOrNull) {
                violations.add(
                    FieldViolation(
                        field.name,
                        ViolationType.REQUIRED_FIELD_MISSING,
                        "required field missing or null",
                    ),
                )
                continue
            }
            if (!field.required && absentOrNull) {
                continue
            }
            checkFieldValue(field, raw)?.let { violations.add(it) }
        }
        return if (violations.isEmpty()) {
            KdbResult.Success(document)
        } else {
            KdbResult.Failure(SchemaViolationException("schema validation failed", violations))
        }
    }

    public fun applyMigration(
        currentSchema: KdbSchema,
        migration: SchemaMigration,
    ): KdbResult<KdbSchema> {
        if (currentSchema.version == migration.toVersion) {
            return KdbResult.Success(currentSchema)
        }
        if (migration.fromVersion != currentSchema.version) {
            return migrationFailure("migration.fromVersion (${migration.fromVersion}) != current.version (${currentSchema.version})")
        }
        if (migration.toVersion != currentSchema.version + 1) {
            return migrationFailure("migration.toVersion (${migration.toVersion}) must equal current.version + 1")
        }
        return try {
            val nextFields = replayMigrationSteps(currentSchema.fields, migration.steps)
            val desc = migration.description.ifEmpty { currentSchema.description }
            val built =
                KdbSchema.build(
                    nextFields,
                    migration.toVersion,
                    KdbTimestamp.now(),
                    desc,
                )
            KdbResult.Success(built)
        } catch (e: SchemaMigrationConflictException) {
            KdbResult.Failure(
                SchemaMigrationException(
                    e.message ?: "schema migration failed",
                    "",
                    describeStep(e.step),
                    e,
                ),
            )
        }
    }

    public fun computeSchemaHash(schema: KdbSchema): KdbHash {
        val bytes =
            schemaBodyToValue(schema.fields, schema.uniqueConstraints, schema.version, schema.createdAt, schema.description).encodeToBytes(
                SchemaBodyWireType,
                KdbSchemaWireRegistry(),
            )
        return KdbHash.fromBytes(kdbSha256(bytes))
    }

    public fun isBackwardCompatible(
        currentSchema: KdbSchema,
        migration: SchemaMigration,
    ): Boolean {
        if (migration.fromVersion != currentSchema.version) {
            return false
        }
        return migration.steps.none { it.isBreaking() }
    }

    public fun diff(
        from: KdbSchema,
        to: KdbSchema,
    ): SchemaDiff {
        val fa = from.fields.associateBy { it.name }
        val fb = to.fields.associateBy { it.name }
        // Sorted by field name, matching Go's DiffSchemas. Kotlin's associateBy keeps declaration
        // order, so this side was already deterministic - but the two implementations returned
        // the same diff in different orders, which anything rendering or comparing a diff across
        // them would see as a difference that is not one.
        val added = fb.keys.filterNot { it in fa }.map { fb.getValue(it) }.sortedBy { it.name }
        val removed = fa.keys.filterNot { it in fb }.map { fa.getValue(it) }.sortedBy { it.name }
        val modified =
            fa.keys.intersect(fb.keys).mapNotNull { name ->
                val a = fa.getValue(name)
                val b = fb.getValue(name)
                val changes = diffSingleField(a, b)
                if (changes.isEmpty()) null else FieldDiff(name, changes)
            }.sortedBy { it.fieldName }
        return SchemaDiff(added, removed, modified, from.version, to.version)
    }

    public fun checkFieldValue(
        field: SchemaField,
        value: JsonValue?,
    ): FieldViolation? {
        if (value == null || value === JsonValue.JNull) {
            return null
        }
        return when (val t = field.type) {
            KdbFieldType.StringType ->
                if (value is JsonValue.JString) null
                else typeMismatch(field.name, "expected string")

            KdbFieldType.Int32Type ->
                when (value) {
                    is JsonValue.JInt ->
                        if (value.value in Int.MIN_VALUE.toLong()..Int.MAX_VALUE.toLong()) null
                        else typeMismatch(field.name, "int32 out of range")
                    is JsonValue.JNumber ->
                        if (isIntegralDouble(value.value) && value.value >= Int.MIN_VALUE && value.value <= Int.MAX_VALUE) {
                            null
                        } else {
                            typeMismatch(field.name, "expected int32")
                        }
                    else -> typeMismatch(field.name, "expected int32")
                }

            KdbFieldType.Int64Type ->
                when (value) {
                    // A JInt is already a Long, so it is in range by construction.
                    is JsonValue.JInt -> null
                    is JsonValue.JNumber ->
                        // Bounded explicitly, like Int32Type just above. isIntegralDouble happens
                        // to reject most out-of-range doubles as a side effect of Double.toLong()
                        // saturating at Long.MAX_VALUE, but not exactly 2^63: that saturates to
                        // Long.MAX_VALUE, whose Double form is 2^63 again, so the difference is
                        // zero and the value passed - one past the largest Long there is. Double
                        // cannot represent Long.MAX_VALUE at all, so the upper bound is a strict
                        // "less than 2^63" rather than a "<= Long.MAX_VALUE" that would round up
                        // to 2^63 and admit it. Go's checkFieldValue enforces the same bound.
                        if (isIntegralDouble(value.value) &&
                            value.value >= LONG_MIN_AS_DOUBLE &&
                            value.value < TWO_TO_THE_63
                        ) {
                            null
                        } else {
                            typeMismatch(field.name, "expected int64")
                        }
                    else -> typeMismatch(field.name, "expected int64")
                }

            KdbFieldType.Float64Type ->
                when (value) {
                    is JsonValue.JNumber, is JsonValue.JInt -> null
                    else -> typeMismatch(field.name, "expected number")
                }

            KdbFieldType.BoolType ->
                if (value is JsonValue.JBool) null else typeMismatch(field.name, "expected boolean")

            KdbFieldType.TimestampType ->
                if (value is JsonValue.JString) {
                    try {
                        KdbTimestamp.fromIso8601(value.value)
                        null
                    } catch (_: Throwable) {
                        typeMismatch(field.name, "invalid ISO-8601 timestamp")
                    }
                } else {
                    typeMismatch(field.name, "expected timestamp string")
                }

            KdbFieldType.UuidType ->
                if (value is JsonValue.JString) {
                    try {
                        dev.kdb.codec.KdbUuid.fromString(value.value)
                        null
                    } catch (_: Throwable) {
                        typeMismatch(field.name, "invalid UUID string")
                    }
                } else {
                    typeMismatch(field.name, "expected UUID string")
                }

            KdbFieldType.ObjectType ->
                if (value is JsonValue.JObject) null else typeMismatch(field.name, "expected JSON object")

            KdbFieldType.ArrayType ->
                if (value is JsonValue.JArray) null else typeMismatch(field.name, "expected JSON array")

            is KdbFieldType.EnumType ->
                if (value !is JsonValue.JString) {
                    typeMismatch(field.name, "expected string enum")
                } else if (value.value !in t.values) {
                    FieldViolation(field.name, ViolationType.ENUM_VALUE_NOT_DECLARED, "value not in enum")
                } else {
                    null
                }
        }
    }

    private fun typeMismatch(
        field: String,
        detail: String,
    ): FieldViolation = FieldViolation(field, ViolationType.TYPE_MISMATCH, detail)

    /** One past Long.MAX_VALUE, which Double cannot represent exactly. */
    private const val TWO_TO_THE_63: Double = 9.223372036854775808E18

    /** Long.MIN_VALUE is exactly -2^63 and does have an exact Double form. */
    private const val LONG_MIN_AS_DOUBLE: Double = -9.223372036854775808E18

    private fun isIntegralDouble(d: Double): Boolean = abs(d - d.toLong()) < 1e-9

    private fun diffSingleField(
        a: SchemaField,
        b: SchemaField,
    ): List<FieldChange> {
        val changes = mutableListOf<FieldChange>()
        when {
            a.type is KdbFieldType.EnumType && b.type is KdbFieldType.EnumType -> {
                val ea = (a.type as KdbFieldType.EnumType).values
                val eb = (b.type as KdbFieldType.EnumType).values
                if (ea != eb) {
                    changes.add(FieldChange.EnumValuesChanged(eb - ea, ea - eb))
                }
            }

            else -> {
                if (a.type != b.type) {
                    changes.add(FieldChange.TypeChanged(a.type, b.type))
                }
            }
        }
        if (a.required != b.required) {
            changes.add(FieldChange.RequiredChanged(a.required, b.required))
        }
        if (a.indexed != b.indexed) {
            changes.add(FieldChange.IndexedChanged(a.indexed, b.indexed))
        }
        if (a.unique != b.unique) {
            changes.add(FieldChange.UniqueChanged(a.unique, b.unique))
        }
        return changes
    }

    private fun migrationFailure(message: String): KdbResult<KdbSchema> =
        KdbResult.Failure(SchemaMigrationException(message, "", ""))

    private fun describeStep(step: MigrationStep): String =
        when (step) {
            is MigrationStep.AddField -> "AddField(${step.field.name})"
            is MigrationStep.DropField -> "DropField(${step.fieldName})"
            is MigrationStep.RenameField -> "RenameField(${step.oldName}->${step.newName})"
            is MigrationStep.ChangeType -> "ChangeType(${step.fieldName})"
            is MigrationStep.AddIndex -> "AddIndex(${step.fieldName})"
            is MigrationStep.DropIndex -> "DropIndex(${step.fieldName})"
            is MigrationStep.SetRequired -> "SetRequired(${step.fieldName})"
            is MigrationStep.SetUnique -> "SetUnique(${step.fieldName})"
            is MigrationStep.WidenEnum -> "WidenEnum(${step.fieldName})"
            is MigrationStep.NarrowEnum -> "NarrowEnum(${step.fieldName})"
        }
}

internal fun replayMigrationSteps(
    baseFields: List<SchemaField>,
    steps: List<MigrationStep>,
): List<SchemaField> {
    val fields = baseFields.map { it }.toMutableList()
    for (step in steps) {
        try {
            applyMigrationStep(fields, step)
        } catch (e: IllegalArgumentException) {
            throw SchemaMigrationConflictException(e.message ?: "invalid migration step", step, e.message ?: "")
        }
    }
    return fields
}

private fun MutableList<SchemaField>.requireField(name: String): SchemaField {
    val ix = indexOfFirst { it.name == name }
    require(ix >= 0) { "unknown field $name" }
    return this[ix]
}

private fun applyMigrationStep(
    fields: MutableList<SchemaField>,
    step: MigrationStep,
) {
    when (step) {
        is MigrationStep.AddField -> {
            require(fields.none { it.name == step.field.name }) { "field ${step.field.name} already exists" }
            fields.add(step.field)
        }

        is MigrationStep.DropField -> {
            val ix = fields.indexOfFirst { it.name == step.fieldName }
            require(ix >= 0) { "cannot drop unknown field ${step.fieldName}" }
            fields.removeAt(ix)
        }

        is MigrationStep.RenameField -> {
            val ix = fields.indexOfFirst { it.name == step.oldName }
            require(ix >= 0) { "unknown field ${step.oldName}" }
            require(fields.none { it.name == step.newName }) { "target name ${step.newName} already exists" }
            val cur = fields[ix]
            fields[ix] =
                SchemaField(step.newName, cur.type, cur.required, cur.indexed, cur.unique)
        }

        is MigrationStep.ChangeType -> {
            val ix = fields.indexOfFirst { it.name == step.fieldName }
            require(ix >= 0) { "unknown field ${step.fieldName}" }
            val cur = fields[ix]
            fields[ix] =
                SchemaField(cur.name, step.newType, cur.required, cur.indexed, cur.unique)
        }

        is MigrationStep.AddIndex -> {
            val ix = fields.indexOfFirst { it.name == step.fieldName }
            require(ix >= 0) { "unknown field ${step.fieldName}" }
            val cur = fields[ix]
            fields[ix] = SchemaField(cur.name, cur.type, cur.required, indexed = true, unique = cur.unique)
        }

        is MigrationStep.DropIndex -> {
            val ix = fields.indexOfFirst { it.name == step.fieldName }
            require(ix >= 0) { "unknown field ${step.fieldName}" }
            val cur = fields[ix]
            fields[ix] =
                SchemaField(cur.name, cur.type, cur.required, indexed = false, unique = false)
        }

        is MigrationStep.SetRequired -> {
            val ix = fields.indexOfFirst { it.name == step.fieldName }
            require(ix >= 0) { "unknown field ${step.fieldName}" }
            val cur = fields[ix]
            fields[ix] =
                SchemaField(cur.name, cur.type, required = step.required, cur.indexed, cur.unique)
        }

        is MigrationStep.SetUnique -> {
            val ix = fields.indexOfFirst { it.name == step.fieldName }
            require(ix >= 0) { "unknown field ${step.fieldName}" }
            val cur = fields[ix]
            val indexed = if (step.unique) true else cur.indexed
            fields[ix] =
                SchemaField(cur.name, cur.type, cur.required, indexed = indexed, unique = step.unique)
        }

        is MigrationStep.WidenEnum -> {
            val cur = fields.requireField(step.fieldName)
            val et =
                cur.type as? KdbFieldType.EnumType
                    ?: throw IllegalArgumentException("${step.fieldName} is not an enum field")
            val merged = et.values + step.addValues
            fields.replace(step.fieldName, cur.copy(type = KdbFieldType.EnumType(merged)))
        }

        is MigrationStep.NarrowEnum -> {
            val cur = fields.requireField(step.fieldName)
            val et =
                cur.type as? KdbFieldType.EnumType
                    ?: throw IllegalArgumentException("${step.fieldName} is not an enum field")
            require(step.removeValues.all { it in et.values }) {
                "cannot remove enum symbols that are not declared on ${step.fieldName}"
            }
            val rest = et.values - step.removeValues
            require(rest.isNotEmpty()) { "enum cannot become empty on ${step.fieldName}" }
            fields.replace(step.fieldName, cur.copy(type = KdbFieldType.EnumType(rest)))
        }
    }
}

private fun MutableList<SchemaField>.replace(
    name: String,
    newField: SchemaField,
) {
    val ix = indexOfFirst { it.name == name }
    require(ix >= 0)
    this[ix] = newField
}
