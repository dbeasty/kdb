package dev.kdb.schema

import dev.kdb.codec.KdbValue
import dev.kdb.codec.encodeToBytes

/** Wire projection for hashing / interchange ([Layer 2 §3]). Excludes redundant hash field on disk. */
public fun KdbSchema.toKdbValue(): KdbValue =
    schemaBodyToValue(fields, uniqueConstraints, version, createdAt, description)

public fun KdbSchema.Companion.fromKdbValue(value: KdbValue): KdbSchema {
    val rec = value as? KdbValue.RecordVal ?: throw SchemaDecodeException("schema body: expected record")
    return parseSchemaBodyRecord(rec)
}

/** Canonical typed-binary encoding ([Layer 2 §3]). */
public fun KdbSchema.toBytes(): ByteArray =
    toKdbValue().encodeToBytes(SchemaBodyWireType, KdbSchemaWireRegistry())

public fun KdbSchema.Companion.fromBytes(bytes: ByteArray): KdbSchema {
    val reg = KdbSchemaWireRegistry()
    val v =
        try {
            KdbValue.decodeFromBytes(bytes, SchemaBodyWireType, reg)
        } catch (e: Throwable) {
            throw SchemaDecodeException("schema body decode failed", e)
        }
    return KdbSchema.fromKdbValue(v)
}

public fun SchemaMigration.toKdbValue(): KdbValue = schemaMigrationToRecord(this)

public fun SchemaMigration.Companion.fromKdbValue(value: KdbValue): SchemaMigration {
    val rec = value as? KdbValue.RecordVal ?: throw SchemaDecodeException("migration: expected record")
    return parseSchemaMigrationRecord(rec)
}

public fun SchemaMigration.toBytes(): ByteArray =
    toKdbValue().encodeToBytes(SchemaMigrationWireType, KdbSchemaWireRegistry())

public fun SchemaMigration.Companion.fromBytes(bytes: ByteArray): SchemaMigration {
    val reg = KdbSchemaWireRegistry()
    val v =
        try {
            KdbValue.decodeFromBytes(bytes, SchemaMigrationWireType, reg)
        } catch (e: Throwable) {
            throw SchemaDecodeException("migration decode failed", e)
        }
    return SchemaMigration.fromKdbValue(v)
}
