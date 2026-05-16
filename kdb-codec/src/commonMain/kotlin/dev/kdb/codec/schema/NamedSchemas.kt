package dev.kdb.codec.schema

import dev.kdb.codec.KdbValue
import dev.kdb.error.KdbSchemaException

/** Field within a Record ([Layer 0 spec §5]). */
public data class FieldSchema(
    val id: Int,
    val name: String,
    val type: KdbType,
    val default: KdbValue? = null,
    val doc: String? = null,
)

/** Named Record schema ([Layer 0 spec §5]). */
public data class RecordSchema(
    val name: String,
    val namespace: String,
    val doc: String? = null,
    val fields: List<FieldSchema>,
) {
    val fullyQualifiedName: String get() = "$namespace.$name"

    val fieldsById: Map<Int, FieldSchema> get() = fields.associateBy { it.id }
}

public data class EnumSchema(
    val name: String,
    val namespace: String,
    val symbols: List<String>,
    val doc: String? = null,
) {
    val fullyQualifiedName: String get() = "$namespace.$name"
}

public data class FixedSchema(
    val name: String,
    val namespace: String,
    val size: Int,
    val logical: LogicalAnnotation? = null,
    val doc: String? = null,
) {
    val fullyQualifiedName: String get() = "$namespace.$name"

    init {
        require(size >= 0) { "fixed size must be ≥0" }
    }
}
