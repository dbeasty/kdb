package dev.kdb.schema

/** Declared SQL / codec projection for one schema field ([Layer 2 §4]). */
public sealed class KdbFieldType {
    public data object StringType : KdbFieldType()

    public data object Int32Type : KdbFieldType()

    public data object Int64Type : KdbFieldType()

    public data object Float64Type : KdbFieldType()

    public data object BoolType : KdbFieldType()

    public data object TimestampType : KdbFieldType()

    public data object UuidType : KdbFieldType()

    public data object ObjectType : KdbFieldType()

    public data object ArrayType : KdbFieldType()

    public data class EnumType(
        val values: Set<String>,
    ) : KdbFieldType() {
        init {
            require(values.isNotEmpty()) { "EnumType must have at least one value" }
        }
    }

    public fun sqlTypeName(): String =
        when (this) {
            StringType -> "TEXT"
            Int32Type -> "INTEGER"
            Int64Type -> "BIGINT"
            Float64Type -> "REAL"
            BoolType -> "BOOLEAN"
            TimestampType -> "TIMESTAMP"
            UuidType -> "TEXT"
            ObjectType -> "JSON"
            ArrayType -> "JSON"
            is EnumType -> "TEXT"
        }

    /** JDBC / introspection hint aligned with Layer 0 physical mapping. */
    public fun codecTypeLabel(): String =
        when (this) {
            StringType -> "STRING"
            Int32Type -> "INT32"
            Int64Type -> "INT64"
            Float64Type -> "FLOAT64"
            BoolType -> "BOOLEAN"
            TimestampType -> "TIMESTAMP"
            UuidType -> "UUID"
            ObjectType -> "JSON_OBJECT"
            ArrayType -> "JSON_ARRAY"
            is EnumType -> "ENUM_AS_STRING"
        }
}
