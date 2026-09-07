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
    /**
     * Compound (multi-field) unique constraints (Layer 16, Component 73). Single-field uniqueness is
     * still [SchemaField.unique]; [uniqueTuples] merges both views. Wire field 5 of KdbSchemaBody with
     * an empty default, so a schema without compound constraints hashes exactly as before.
     */
    val uniqueConstraints: List<UniqueConstraint> = emptyList(),
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

    /**
     * Every unique constraint as an ordered field tuple: one 1-tuple per [SchemaField.unique] field in
     * declaration order, followed by [uniqueConstraints].
     */
    public fun uniqueTuples(): List<List<String>> =
        fields.filter { it.unique }.map { listOf(it.name) } + uniqueConstraints.map { it.fields }

    public fun hasUniqueConstraints(): Boolean = fields.any { it.unique } || uniqueConstraints.isNotEmpty()

    public companion object {
        private val noneLazy =
            lazy {
                val ts = KdbTimestamp(0, 0)
                val reg = KdbSchemaWireRegistry()
                val bytes =
                    schemaBodyToValue(emptyList(), emptyList(), 0, ts, "").encodeToBytes(SchemaBodyWireType, reg)
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
            uniqueConstraints: List<UniqueConstraint> = emptyList(),
        ): KdbSchema {
            require(version >= 1) { "schema version must be >= 1" }
            require(fields.distinctBy { it.name }.size == fields.size) { "duplicate field names" }
            validateUniqueConstraints(fields, uniqueConstraints)
            val reg = KdbSchemaWireRegistry()
            val bytes =
                schemaBodyToValue(fields, uniqueConstraints, version, createdAt, description)
                    .encodeToBytes(SchemaBodyWireType, reg)
            val h = KdbHash.fromBytes(kdbSha256(bytes))
            return KdbSchema(h, fields, version, createdAt, description, uniqueConstraints)
        }

        internal fun validateUniqueConstraints(
            fields: List<SchemaField>,
            constraints: List<UniqueConstraint>,
        ) {
            val names = fields.map { it.name }.toSet()
            for (c in constraints) {
                require(c.fields.isNotEmpty()) { "unique constraint must name at least one field" }
                require(c.fields.distinct().size == c.fields.size) { "unique constraint repeats a field: ${c.fields}" }
                for (n in c.fields) {
                    require(n in names) { "unique constraint references unknown field: $n" }
                }
            }
        }
    }
}

public val KdbSchema.isNone: Boolean get() = schemaHash == KdbSchema.NONE.schemaHash

/**
 * One compound unique constraint: the ordered tuple of field names whose combined values must be
 * unique across live documents. A document in which any part is absent or JSON null claims nothing.
 */
public data class UniqueConstraint(val fields: List<String>) {
    public constructor(vararg fields: String) : this(fields.toList())
}
