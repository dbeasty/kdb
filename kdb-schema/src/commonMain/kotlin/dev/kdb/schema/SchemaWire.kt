package dev.kdb.schema

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbValue
import dev.kdb.codec.encodeToBytes
import dev.kdb.codec.toKdbUuid
import dev.kdb.codec.toKdbTimestamp
import dev.kdb.codec.toTimestampVal
import dev.kdb.codec.toUuidVal
import dev.kdb.codec.schema.FieldSchema
import dev.kdb.codec.schema.FixedSchema
import dev.kdb.codec.schema.KdbType
import dev.kdb.codec.schema.KdbTypeRegistry
import dev.kdb.codec.schema.LogicalAnnotation
import dev.kdb.codec.schema.PhysicalKind
import dev.kdb.codec.schema.RecordSchema

internal object SchemaFqn {
    const val NS = "dev.kdb.schema"

    const val HASH32 = "$NS.Hash32"
    const val ENUM_VALUES_PAYLOAD = "$NS.EnumValuesPayload"

    const val FK_STRING = "$NS.FieldKindString"
    const val FK_INT32 = "$NS.FieldKindInt32"
    const val FK_INT64 = "$NS.FieldKindInt64"
    const val FK_FLOAT64 = "$NS.FieldKindFloat64"
    const val FK_BOOL = "$NS.FieldKindBool"
    const val FK_TIMESTAMP = "$NS.FieldKindTimestamp"
    const val FK_UUID = "$NS.FieldKindUuid"
    const val FK_OBJECT = "$NS.FieldKindObject"
    const val FK_ARRAY = "$NS.FieldKindArray"

    const val SCHEMA_FIELD = "$NS.SchemaFieldWire"
    const val SCHEMA_BODY = "$NS.KdbSchemaBody"

    const val MS_ADD_FIELD = "$NS.MigrationAddField"
    const val MS_DROP_FIELD = "$NS.MigrationDropField"
    const val MS_RENAME_FIELD = "$NS.MigrationRenameField"
    const val MS_CHANGE_TYPE = "$NS.MigrationChangeType"
    const val MS_ADD_INDEX = "$NS.MigrationAddIndex"
    const val MS_DROP_INDEX = "$NS.MigrationDropIndex"
    const val MS_SET_REQUIRED = "$NS.MigrationSetRequired"
    const val MS_SET_UNIQUE = "$NS.MigrationSetUnique"
    const val MS_WIDEN_ENUM = "$NS.MigrationWidenEnum"
    const val MS_NARROW_ENUM = "$NS.MigrationNarrowEnum"

    const val SCHEMA_MIGRATION = "$NS.SchemaMigrationWire"
}

private val uuidPrim = KdbType.Primitive(PhysicalKind.FIXED, LogicalAnnotation.Uuid)

private val timestampPrim =
    KdbType.Primitive(PhysicalKind.INT64, LogicalAnnotation.TimestampMicros(null))

private fun buildRegistry(): KdbTypeRegistry {
    val reg = KdbTypeRegistry.create()
    reg.registerFixed(
        FixedSchema(
            name = "Hash32",
            namespace = SchemaFqn.NS,
            size = 32,
        ),
    )

    val fkString = RecordSchema("FieldKindString", SchemaFqn.NS, fields = emptyList())
    val fkInt32 = RecordSchema("FieldKindInt32", SchemaFqn.NS, fields = emptyList())
    val fkInt64 = RecordSchema("FieldKindInt64", SchemaFqn.NS, fields = emptyList())
    val fkFloat64 = RecordSchema("FieldKindFloat64", SchemaFqn.NS, fields = emptyList())
    val fkBool = RecordSchema("FieldKindBool", SchemaFqn.NS, fields = emptyList())
    val fkTimestamp = RecordSchema("FieldKindTimestamp", SchemaFqn.NS, fields = emptyList())
    val fkUuid = RecordSchema("FieldKindUuid", SchemaFqn.NS, fields = emptyList())
    val fkObject = RecordSchema("FieldKindObject", SchemaFqn.NS, fields = emptyList())
    val fkArray = RecordSchema("FieldKindArray", SchemaFqn.NS, fields = emptyList())

    val enumPayload =
        RecordSchema(
            name = "EnumValuesPayload",
            namespace = SchemaFqn.NS,
            fields =
                listOf(
                    FieldSchema(1, "values", KdbType.Array(KdbType.Primitive(PhysicalKind.STRING))),
                ),
        )

    listOf(fkString, fkInt32, fkInt64, fkFloat64, fkBool, fkTimestamp, fkUuid, fkObject, fkArray, enumPayload).forEach {
        reg.registerRecord(it)
    }

    val fieldTypeUnion =
        KdbType.Union(
            listOf(
                KdbType.Ref(SchemaFqn.FK_STRING),
                KdbType.Ref(SchemaFqn.FK_INT32),
                KdbType.Ref(SchemaFqn.FK_INT64),
                KdbType.Ref(SchemaFqn.FK_FLOAT64),
                KdbType.Ref(SchemaFqn.FK_BOOL),
                KdbType.Ref(SchemaFqn.FK_TIMESTAMP),
                KdbType.Ref(SchemaFqn.FK_UUID),
                KdbType.Ref(SchemaFqn.FK_OBJECT),
                KdbType.Ref(SchemaFqn.FK_ARRAY),
                KdbType.Ref(SchemaFqn.ENUM_VALUES_PAYLOAD),
            ),
        )

    val schemaFieldWire =
        RecordSchema(
            name = "SchemaFieldWire",
            namespace = SchemaFqn.NS,
            fields =
                listOf(
                    FieldSchema(1, "name", KdbType.Primitive(PhysicalKind.STRING)),
                    FieldSchema(2, "type", fieldTypeUnion),
                    FieldSchema(3, "required", KdbType.Primitive(PhysicalKind.BOOLEAN)),
                    FieldSchema(4, "indexed", KdbType.Primitive(PhysicalKind.BOOLEAN)),
                    FieldSchema(5, "unique", KdbType.Primitive(PhysicalKind.BOOLEAN)),
                ),
        )
    reg.registerRecord(schemaFieldWire)

    val schemaBodyWire =
        RecordSchema(
            name = "KdbSchemaBody",
            namespace = SchemaFqn.NS,
            fields =
                listOf(
                    FieldSchema(1, "fields", KdbType.Array(KdbType.Ref(SchemaFqn.SCHEMA_FIELD))),
                    FieldSchema(2, "version", KdbType.Primitive(PhysicalKind.INT32)),
                    FieldSchema(3, "createdAt", timestampPrim),
                    FieldSchema(4, "description", KdbType.Primitive(PhysicalKind.STRING)),
                ),
        )
    reg.registerRecord(schemaBodyWire)

    val msAdd =
        RecordSchema(
            name = "MigrationAddField",
            namespace = SchemaFqn.NS,
            fields = listOf(FieldSchema(1, "field", KdbType.Ref(SchemaFqn.SCHEMA_FIELD))),
        )
    val msDrop =
        RecordSchema(
            name = "MigrationDropField",
            namespace = SchemaFqn.NS,
            fields = listOf(FieldSchema(1, "fieldName", KdbType.Primitive(PhysicalKind.STRING))),
        )
    val msRename =
        RecordSchema(
            name = "MigrationRenameField",
            namespace = SchemaFqn.NS,
            fields =
                listOf(
                    FieldSchema(1, "oldName", KdbType.Primitive(PhysicalKind.STRING)),
                    FieldSchema(2, "newName", KdbType.Primitive(PhysicalKind.STRING)),
                ),
        )
    val msChange =
        RecordSchema(
            name = "MigrationChangeType",
            namespace = SchemaFqn.NS,
            fields =
                listOf(
                    FieldSchema(1, "fieldName", KdbType.Primitive(PhysicalKind.STRING)),
                    FieldSchema(2, "newType", fieldTypeUnion),
                ),
        )
    val msAddIx =
        RecordSchema(
            name = "MigrationAddIndex",
            namespace = SchemaFqn.NS,
            fields = listOf(FieldSchema(1, "fieldName", KdbType.Primitive(PhysicalKind.STRING))),
        )
    val msDropIx =
        RecordSchema(
            name = "MigrationDropIndex",
            namespace = SchemaFqn.NS,
            fields = listOf(FieldSchema(1, "fieldName", KdbType.Primitive(PhysicalKind.STRING))),
        )
    val msReq =
        RecordSchema(
            name = "MigrationSetRequired",
            namespace = SchemaFqn.NS,
            fields =
                listOf(
                    FieldSchema(1, "fieldName", KdbType.Primitive(PhysicalKind.STRING)),
                    FieldSchema(2, "required", KdbType.Primitive(PhysicalKind.BOOLEAN)),
                ),
        )
    val msUnique =
        RecordSchema(
            name = "MigrationSetUnique",
            namespace = SchemaFqn.NS,
            fields =
                listOf(
                    FieldSchema(1, "fieldName", KdbType.Primitive(PhysicalKind.STRING)),
                    FieldSchema(2, "unique", KdbType.Primitive(PhysicalKind.BOOLEAN)),
                ),
        )
    val msWiden =
        RecordSchema(
            name = "MigrationWidenEnum",
            namespace = SchemaFqn.NS,
            fields =
                listOf(
                    FieldSchema(1, "fieldName", KdbType.Primitive(PhysicalKind.STRING)),
                    FieldSchema(2, "addValues", KdbType.Array(KdbType.Primitive(PhysicalKind.STRING))),
                ),
        )
    val msNarrow =
        RecordSchema(
            name = "MigrationNarrowEnum",
            namespace = SchemaFqn.NS,
            fields =
                listOf(
                    FieldSchema(1, "fieldName", KdbType.Primitive(PhysicalKind.STRING)),
                    FieldSchema(2, "removeValues", KdbType.Array(KdbType.Primitive(PhysicalKind.STRING))),
                ),
        )

    listOf(msAdd, msDrop, msRename, msChange, msAddIx, msDropIx, msReq, msUnique, msWiden, msNarrow).forEach {
        reg.registerRecord(it)
    }

    val migrationStepUnion =
        KdbType.Union(
            listOf(
                KdbType.Ref(SchemaFqn.MS_ADD_FIELD),
                KdbType.Ref(SchemaFqn.MS_DROP_FIELD),
                KdbType.Ref(SchemaFqn.MS_RENAME_FIELD),
                KdbType.Ref(SchemaFqn.MS_CHANGE_TYPE),
                KdbType.Ref(SchemaFqn.MS_ADD_INDEX),
                KdbType.Ref(SchemaFqn.MS_DROP_INDEX),
                KdbType.Ref(SchemaFqn.MS_SET_REQUIRED),
                KdbType.Ref(SchemaFqn.MS_SET_UNIQUE),
                KdbType.Ref(SchemaFqn.MS_WIDEN_ENUM),
                KdbType.Ref(SchemaFqn.MS_NARROW_ENUM),
            ),
        )

    val migrationWire =
        RecordSchema(
            name = "SchemaMigrationWire",
            namespace = SchemaFqn.NS,
            fields =
                listOf(
                    FieldSchema(1, "migrationId", uuidPrim),
                    FieldSchema(2, "fromVersion", KdbType.Primitive(PhysicalKind.INT32)),
                    FieldSchema(3, "toVersion", KdbType.Primitive(PhysicalKind.INT32)),
                    FieldSchema(4, "steps", KdbType.Array(migrationStepUnion)),
                    FieldSchema(5, "description", KdbType.Primitive(PhysicalKind.STRING)),
                ),
        )
    reg.registerRecord(migrationWire)

    reg.freeze()
    return reg
}

private val registryLazy = lazy { buildRegistry() }

/** Builtin schemas for every `dev.kdb.schema.*` wire shape ([Layer 2 §3]). */
public fun KdbSchemaWireRegistry(): KdbTypeRegistry = registryLazy.value

/** Root schema snapshot wire type (fields + metadata only; identity hash is SHA-256 of these bytes). */
public val SchemaBodyWireType: KdbType = KdbType.Ref(SchemaFqn.SCHEMA_BODY)

/** Layer 0 wire type for [SchemaMigration]. */
public val SchemaMigrationWireType: KdbType = KdbType.Ref(SchemaFqn.SCHEMA_MIGRATION)

internal fun sortedStrings(values: Collection<String>): List<String> = values.distinct().sorted()

internal fun fieldTypeToWireUnion(
    type: KdbFieldType,
): KdbValue.UnionVal =
    when (type) {
        KdbFieldType.StringType -> KdbValue.UnionVal(0, KdbValue.RecordVal(emptyMap()))
        KdbFieldType.Int32Type -> KdbValue.UnionVal(1, KdbValue.RecordVal(emptyMap()))
        KdbFieldType.Int64Type -> KdbValue.UnionVal(2, KdbValue.RecordVal(emptyMap()))
        KdbFieldType.Float64Type -> KdbValue.UnionVal(3, KdbValue.RecordVal(emptyMap()))
        KdbFieldType.BoolType -> KdbValue.UnionVal(4, KdbValue.RecordVal(emptyMap()))
        KdbFieldType.TimestampType -> KdbValue.UnionVal(5, KdbValue.RecordVal(emptyMap()))
        KdbFieldType.UuidType -> KdbValue.UnionVal(6, KdbValue.RecordVal(emptyMap()))
        KdbFieldType.ObjectType -> KdbValue.UnionVal(7, KdbValue.RecordVal(emptyMap()))
        KdbFieldType.ArrayType -> KdbValue.UnionVal(8, KdbValue.RecordVal(emptyMap()))
        is KdbFieldType.EnumType ->
            KdbValue.UnionVal(
                9,
                KdbValue.RecordVal(
                    mapOf(
                        1 to
                            KdbValue.ArrayVal(
                                sortedStrings(type.values).map { KdbValue.StringVal(it) },
                            ),
                    ),
                ),
            )
    }

internal fun parseFieldTypeWire(uv: KdbValue.UnionVal): KdbFieldType =
    when (uv.branch) {
        0 -> KdbFieldType.StringType
        1 -> KdbFieldType.Int32Type
        2 -> KdbFieldType.Int64Type
        3 -> KdbFieldType.Float64Type
        4 -> KdbFieldType.BoolType
        5 -> KdbFieldType.TimestampType
        6 -> KdbFieldType.UuidType
        7 -> KdbFieldType.ObjectType
        8 -> KdbFieldType.ArrayType
        9 -> {
            val rec = uv.value as? KdbValue.RecordVal ?: throw SchemaDecodeException("EnumValuesPayload expected record")
            val arr = rec.fields[1] as? KdbValue.ArrayVal ?: throw SchemaDecodeException("Enum values array")
            val values =
                arr.elements.map {
                    (it as? KdbValue.StringVal)?.v ?: throw SchemaDecodeException("Enum symbol")
                }
            require(values.isNotEmpty()) { "enum requires symbols" }
            KdbFieldType.EnumType(values.toSet())
        }
        else -> throw SchemaDecodeException("unknown field type union branch ${uv.branch}")
    }

internal fun schemaFieldToWireRecord(field: SchemaField): KdbValue.RecordVal =
    KdbValue.RecordVal(
        mapOf(
            1 to KdbValue.StringVal(field.name),
            2 to fieldTypeToWireUnion(field.type),
            3 to KdbValue.Bool(field.required),
            4 to KdbValue.Bool(field.indexed),
            5 to KdbValue.Bool(field.unique),
        ),
    )

internal fun parseSchemaFieldWire(rec: KdbValue.RecordVal): SchemaField {
    val name =
        (rec.fields[1] as? KdbValue.StringVal)?.v ?: throw SchemaDecodeException("schema field name")
    val typeUv =
        rec.fields[2] as? KdbValue.UnionVal ?: throw SchemaDecodeException("schema field type")
    val required =
        (rec.fields[3] as? KdbValue.Bool)?.v ?: throw SchemaDecodeException("schema field required")
    val indexed =
        (rec.fields[4] as? KdbValue.Bool)?.v ?: throw SchemaDecodeException("schema field indexed")
    val unique =
        (rec.fields[5] as? KdbValue.Bool)?.v ?: throw SchemaDecodeException("schema field unique")
    return SchemaField(name, parseFieldTypeWire(typeUv), required, indexed, unique)
}

internal fun schemaBodyToValue(
    fields: List<SchemaField>,
    version: Int,
    createdAt: KdbTimestamp,
    description: String,
): KdbValue.RecordVal =
    KdbValue.RecordVal(
        mapOf(
            1 to KdbValue.ArrayVal(fields.map { schemaFieldToWireRecord(it) }),
            2 to KdbValue.Int32Val(version),
            3 to createdAt.toTimestampVal(null),
            4 to KdbValue.StringVal(description),
        ),
    )

internal fun parseSchemaBodyRecord(rec: KdbValue.RecordVal): KdbSchema {
    val fa = rec.fields[1] as? KdbValue.ArrayVal ?: throw SchemaDecodeException("schema fields array")
    val fields =
        fa.elements.map {
            parseSchemaFieldWire(it as? KdbValue.RecordVal ?: throw SchemaDecodeException("schema field record"))
        }
    val version =
        (rec.fields[2] as? KdbValue.Int32Val)?.v ?: throw SchemaDecodeException("schema version")
    val ts =
        (rec.fields[3] as? KdbValue.TimestampVal)?.toKdbTimestamp()
            ?: throw SchemaDecodeException("createdAt")
    val desc =
        (rec.fields[4] as? KdbValue.StringVal)?.v ?: throw SchemaDecodeException("description")
    require(fields.distinctBy { it.name }.size == fields.size) { "duplicate schema field names" }
    val hash =
        dev.kdb.document.kdbSha256(
            schemaBodyToValue(fields, version, ts, desc).encodeToBytes(SchemaBodyWireType, KdbSchemaWireRegistry()),
        )
    return KdbSchema(KdbHash.fromBytes(hash), fields, version, ts, desc)
}

internal fun migrationStepToWire(step: MigrationStep): KdbValue.UnionVal =
    when (step) {
        is MigrationStep.AddField ->
            KdbValue.UnionVal(
                0,
                KdbValue.RecordVal(mapOf(1 to schemaFieldToWireRecord(step.field))),
            )
        is MigrationStep.DropField ->
            KdbValue.UnionVal(1, KdbValue.RecordVal(mapOf(1 to KdbValue.StringVal(step.fieldName))))
        is MigrationStep.RenameField ->
            KdbValue.UnionVal(
                2,
                KdbValue.RecordVal(
                    mapOf(
                        1 to KdbValue.StringVal(step.oldName),
                        2 to KdbValue.StringVal(step.newName),
                    ),
                ),
            )
        is MigrationStep.ChangeType ->
            KdbValue.UnionVal(
                3,
                KdbValue.RecordVal(
                    mapOf(
                        1 to KdbValue.StringVal(step.fieldName),
                        2 to fieldTypeToWireUnion(step.newType),
                    ),
                ),
            )
        is MigrationStep.AddIndex ->
            KdbValue.UnionVal(4, KdbValue.RecordVal(mapOf(1 to KdbValue.StringVal(step.fieldName))))
        is MigrationStep.DropIndex ->
            KdbValue.UnionVal(5, KdbValue.RecordVal(mapOf(1 to KdbValue.StringVal(step.fieldName))))
        is MigrationStep.SetRequired ->
            KdbValue.UnionVal(
                6,
                KdbValue.RecordVal(
                    mapOf(
                        1 to KdbValue.StringVal(step.fieldName),
                        2 to KdbValue.Bool(step.required),
                    ),
                ),
            )
        is MigrationStep.SetUnique ->
            KdbValue.UnionVal(
                7,
                KdbValue.RecordVal(
                    mapOf(
                        1 to KdbValue.StringVal(step.fieldName),
                        2 to KdbValue.Bool(step.unique),
                    ),
                ),
            )
        is MigrationStep.WidenEnum ->
            KdbValue.UnionVal(
                8,
                KdbValue.RecordVal(
                    mapOf(
                        1 to KdbValue.StringVal(step.fieldName),
                        2 to
                            KdbValue.ArrayVal(
                                sortedStrings(step.addValues).map { KdbValue.StringVal(it) },
                            ),
                    ),
                ),
            )
        is MigrationStep.NarrowEnum ->
            KdbValue.UnionVal(
                9,
                KdbValue.RecordVal(
                    mapOf(
                        1 to KdbValue.StringVal(step.fieldName),
                        2 to
                            KdbValue.ArrayVal(
                                sortedStrings(step.removeValues).map { KdbValue.StringVal(it) },
                            ),
                    ),
                ),
            )
    }

internal fun parseMigrationStepWire(uv: KdbValue.UnionVal): MigrationStep {
    val rec = uv.value as? KdbValue.RecordVal ?: throw SchemaDecodeException("migration step payload")
    return when (uv.branch) {
        0 ->
            MigrationStep.AddField(
                parseSchemaFieldWire(rec.fields[1] as? KdbValue.RecordVal ?: throw SchemaDecodeException("AddField")),
            )
        1 ->
            MigrationStep.DropField(
                (rec.fields[1] as? KdbValue.StringVal)?.v ?: throw SchemaDecodeException("DropField"),
            )
        2 ->
            MigrationStep.RenameField(
                (rec.fields[1] as? KdbValue.StringVal)?.v ?: throw SchemaDecodeException("Rename old"),
                (rec.fields[2] as? KdbValue.StringVal)?.v ?: throw SchemaDecodeException("Rename new"),
            )
        3 ->
            MigrationStep.ChangeType(
                (rec.fields[1] as? KdbValue.StringVal)?.v ?: throw SchemaDecodeException("ChangeType name"),
                parseFieldTypeWire(rec.fields[2] as? KdbValue.UnionVal ?: throw SchemaDecodeException("ChangeType type")),
            )
        4 ->
            MigrationStep.AddIndex(
                (rec.fields[1] as? KdbValue.StringVal)?.v ?: throw SchemaDecodeException("AddIndex"),
            )
        5 ->
            MigrationStep.DropIndex(
                (rec.fields[1] as? KdbValue.StringVal)?.v ?: throw SchemaDecodeException("DropIndex"),
            )
        6 ->
            MigrationStep.SetRequired(
                (rec.fields[1] as? KdbValue.StringVal)?.v ?: throw SchemaDecodeException("SetRequired name"),
                (rec.fields[2] as? KdbValue.Bool)?.v ?: throw SchemaDecodeException("SetRequired flag"),
            )
        7 ->
            MigrationStep.SetUnique(
                (rec.fields[1] as? KdbValue.StringVal)?.v ?: throw SchemaDecodeException("SetUnique name"),
                (rec.fields[2] as? KdbValue.Bool)?.v ?: throw SchemaDecodeException("SetUnique flag"),
            )
        8 ->
            MigrationStep.WidenEnum(
                (rec.fields[1] as? KdbValue.StringVal)?.v ?: throw SchemaDecodeException("WidenEnum name"),
                readStringArray(rec.fields[2], "WidenEnum values").toSet(),
            )
        9 ->
            MigrationStep.NarrowEnum(
                (rec.fields[1] as? KdbValue.StringVal)?.v ?: throw SchemaDecodeException("NarrowEnum name"),
                readStringArray(rec.fields[2], "NarrowEnum values").toSet(),
            )
        else -> throw SchemaDecodeException("unknown migration branch ${uv.branch}")
    }
}

internal fun schemaMigrationToRecord(m: SchemaMigration): KdbValue.RecordVal =
    KdbValue.RecordVal(
        mapOf(
            1 to m.migrationId.toUuidVal(),
            2 to KdbValue.Int32Val(m.fromVersion),
            3 to KdbValue.Int32Val(m.toVersion),
            4 to KdbValue.ArrayVal(m.steps.map { migrationStepToWire(it) }),
            5 to KdbValue.StringVal(m.description),
        ),
    )

internal fun parseSchemaMigrationRecord(rec: KdbValue.RecordVal): SchemaMigration {
    val mid =
        (rec.fields[1] as? KdbValue.UuidVal)?.toKdbUuid()
            ?: throw SchemaDecodeException("migration id")
    val fromV =
        (rec.fields[2] as? KdbValue.Int32Val)?.v ?: throw SchemaDecodeException("fromVersion")
    val toV =
        (rec.fields[3] as? KdbValue.Int32Val)?.v ?: throw SchemaDecodeException("toVersion")
    val stepsArr =
        rec.fields[4] as? KdbValue.ArrayVal ?: throw SchemaDecodeException("migration steps")
    val steps =
        stepsArr.elements.map {
            parseMigrationStepWire(it as? KdbValue.UnionVal ?: throw SchemaDecodeException("step union"))
        }
    val desc =
        (rec.fields[5] as? KdbValue.StringVal)?.v ?: throw SchemaDecodeException("migration description")
    return SchemaMigration(mid, fromV, toV, steps, desc)
}

private fun readStringArray(
    v: KdbValue?,
    ctx: String,
): List<String> {
    val arr = v as? KdbValue.ArrayVal ?: throw SchemaDecodeException(ctx)
    return arr.elements.map {
        (it as? KdbValue.StringVal)?.v ?: throw SchemaDecodeException("$ctx element")
    }
}
