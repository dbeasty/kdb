package dev.kdb.schema

import dev.kdb.codec.KdbUuid

/** Ordered schema evolution operation ([Layer 2 §4]). */
public sealed class MigrationStep {
    public data class AddField(
        val field: SchemaField,
    ) : MigrationStep()

    public data class DropField(
        val fieldName: String,
    ) : MigrationStep()

    public data class RenameField(
        val oldName: String,
        val newName: String,
    ) : MigrationStep()

    public data class ChangeType(
        val fieldName: String,
        val newType: KdbFieldType,
    ) : MigrationStep()

    public data class AddIndex(
        val fieldName: String,
    ) : MigrationStep()

    public data class DropIndex(
        val fieldName: String,
    ) : MigrationStep()

    public data class SetRequired(
        val fieldName: String,
        val required: Boolean,
    ) : MigrationStep()

    public data class SetUnique(
        val fieldName: String,
        val unique: Boolean,
    ) : MigrationStep()

    public data class WidenEnum(
        val fieldName: String,
        val addValues: Set<String>,
    ) : MigrationStep()

    public data class NarrowEnum(
        val fieldName: String,
        val removeValues: Set<String>,
    ) : MigrationStep()
}

public fun MigrationStep.isBreaking(): Boolean =
    when (this) {
        is MigrationStep.AddField -> field.required
        is MigrationStep.DropField -> true
        is MigrationStep.RenameField -> true
        is MigrationStep.ChangeType -> true
        is MigrationStep.AddIndex -> false
        is MigrationStep.DropIndex -> false
        is MigrationStep.SetRequired -> required
        is MigrationStep.SetUnique -> unique
        is MigrationStep.WidenEnum -> false
        is MigrationStep.NarrowEnum -> true
    }

/** Describes a backward-compatible or breaking schema transition ([Layer 2 §3]). */
public data class SchemaMigration(
    val migrationId: KdbUuid,
    val fromVersion: Int,
    val toVersion: Int,
    val steps: List<MigrationStep>,
    val description: String = "",
) {
    public companion object
}
