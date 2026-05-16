package dev.kdb.schema

import dev.kdb.codec.encodeToBytes
import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.document.kdbSha256

/**
 * Full schema snapshot for one namespace version ([Layer 2 §3]).
 * Identity is [schemaHash] — SHA-256 of canonical Layer 0 bytes of the wire body (fields + metadata).
 */
public data class KdbSchema(
    val schemaHash: KdbHash,
    val fields: List<SchemaField>,
    val version: Int,
    val createdAt: KdbTimestamp,
    val description: String = "",
) {
    /** Map from field name → field declaration (declaration order preserved). */
    val fieldsByName: Map<String, SchemaField> =
        fields.associateByTo(LinkedHashMap()) { it.name }

    public fun hasField(name: String): Boolean = fieldsByName.containsKey(name)

    public fun field(name: String): SchemaField? = fieldsByName[name]

    public fun fieldOrThrow(name: String): SchemaField =
        fieldsByName[name] ?: throw NoSuchElementException("unknown schema field: $name")

    public fun indexedFields(): List<SchemaField> = fields.filter { it.indexed }

    public fun requiredFields(): List<SchemaField> = fields.filter { it.required }

    public fun uniqueFields(): List<SchemaField> = fields.filter { it.unique }

    public companion object {
        private val noneLazy =
            lazy {
                val ts = KdbTimestamp(0, 0)
                val reg = KdbSchemaWireRegistry()
                val bytes =
                    schemaBodyToValue(emptyList(), 0, ts, "").encodeToBytes(SchemaBodyWireType, reg)
                val h = KdbHash.fromBytes(kdbSha256(bytes))
                KdbSchema(h, emptyList(), 0, ts, "")
            }

        /** Sentinel for schema-less namespaces ([Layer 2 §4]). */
        public val NONE: KdbSchema get() = noneLazy.value

        public fun build(
            fields: List<SchemaField>,
            version: Int = 1,
            createdAt: KdbTimestamp = KdbTimestamp.now(),
            description: String = "",
        ): KdbSchema {
            require(version >= 1) { "schema version must be >= 1" }
            require(fields.distinctBy { it.name }.size == fields.size) { "duplicate field names" }
            val reg = KdbSchemaWireRegistry()
            val bytes =
                schemaBodyToValue(fields, version, createdAt, description).encodeToBytes(SchemaBodyWireType, reg)
            val h = KdbHash.fromBytes(kdbSha256(bytes))
            return KdbSchema(h, fields, version, createdAt, description)
        }
    }
}

public val KdbSchema.isNone: Boolean get() = schemaHash == KdbSchema.NONE.schemaHash
