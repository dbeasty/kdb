package dev.kdb.embed

import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import kotlinx.serialization.Serializable

@Serializable
public data class EmbedSchemaFieldDto(
    val name: String,
    val type: String,
    val required: Boolean = false,
    val indexed: Boolean = false,
    val unique: Boolean = false,
)

@Serializable
public data class EmbedSchemaDto(
    val fields: List<EmbedSchemaFieldDto> = emptyList(),
)

public fun EmbedSchemaDto.toKdbSchema(): KdbSchema {
    if (fields.isEmpty()) return KdbSchema.NONE
    return KdbSchema.build(
        fields.map { f ->
            SchemaField(
                name = f.name,
                type = parseFieldType(f.type),
                required = f.required,
                indexed = f.indexed,
                unique = f.unique,
            )
        },
    )
}

private fun parseFieldType(raw: String): KdbFieldType =
    when (raw.lowercase()) {
        "string", "text" -> KdbFieldType.StringType
        "int32", "integer" -> KdbFieldType.Int32Type
        "int64", "bigint" -> KdbFieldType.Int64Type
        "float64", "real", "double" -> KdbFieldType.Float64Type
        "bool", "boolean" -> KdbFieldType.BoolType
        "timestamp" -> KdbFieldType.TimestampType
        "uuid" -> KdbFieldType.UuidType
        "object", "json" -> KdbFieldType.ObjectType
        "array" -> KdbFieldType.ArrayType
        else -> throw IllegalArgumentException("unknown schema field type: $raw")
    }
