package dev.kdb.schema

/** Human-readable schema delta ([Layer 2 §3]). */
public data class SchemaDiff(
    val addedFields: List<SchemaField>,
    val removedFields: List<SchemaField>,
    val modifiedFields: List<FieldDiff>,
    val fromVersion: Int,
    val toVersion: Int,
) {
    public val isEmpty: Boolean
        get() = addedFields.isEmpty() && removedFields.isEmpty() && modifiedFields.isEmpty()

    public val isBreaking: Boolean
        get() =
            removedFields.isNotEmpty() ||
                addedFields.any { it.required } ||
                modifiedFields.any { fd ->
                    fd.changes.any { change ->
                        when (change) {
                            is FieldChange.TypeChanged -> true
                            is FieldChange.RequiredChanged -> change.to
                            is FieldChange.UniqueChanged -> change.to
                            is FieldChange.EnumValuesChanged -> change.removed.isNotEmpty()
                            is FieldChange.IndexedChanged -> false
                        }
                    }
                }
}

public data class FieldDiff(
    val fieldName: String,
    val changes: List<FieldChange>,
)

public sealed class FieldChange {
    public data class TypeChanged(
        val from: KdbFieldType,
        val to: KdbFieldType,
    ) : FieldChange()

    public data class RequiredChanged(
        val from: Boolean,
        val to: Boolean,
    ) : FieldChange()

    public data class IndexedChanged(
        val from: Boolean,
        val to: Boolean,
    ) : FieldChange()

    public data class UniqueChanged(
        val from: Boolean,
        val to: Boolean,
    ) : FieldChange()

    public data class EnumValuesChanged(
        val added: Set<String>,
        val removed: Set<String>,
    ) : FieldChange()
}
