package dev.kdb.schema

/** Immutable declaration of one schema column ([Layer 2 §4]). */
public data class SchemaField(
    val name: String,
    val type: KdbFieldType,
    val required: Boolean,
    val indexed: Boolean,
    val unique: Boolean = false,
) {
    init {
        require(name.matches(Regex("[a-zA-Z_][a-zA-Z0-9_]*"))) {
            "Field name must be a valid identifier: $name"
        }
        require(!(unique && !indexed)) { "unique=true requires indexed=true: $name" }
    }
}
