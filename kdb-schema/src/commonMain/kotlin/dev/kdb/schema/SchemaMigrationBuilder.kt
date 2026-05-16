package dev.kdb.schema

import dev.kdb.codec.KdbUuid

/** DSL builder for [SchemaMigration] ([Layer 2 §3]). */
public class SchemaMigrationBuilder(
    private val baseSchema: KdbSchema,
) {
    private val steps = mutableListOf<MigrationStep>()
    private var migrationDescription: String = ""

    public fun addField(
        name: String,
        type: KdbFieldType,
        required: Boolean = false,
        indexed: Boolean = false,
        unique: Boolean = false,
    ): SchemaMigrationBuilder {
        steps.add(MigrationStep.AddField(SchemaField(name, type, required, indexed, unique)))
        return this
    }

    public fun dropField(name: String): SchemaMigrationBuilder {
        steps.add(MigrationStep.DropField(name))
        return this
    }

    public fun renameField(
        oldName: String,
        newName: String,
    ): SchemaMigrationBuilder {
        steps.add(MigrationStep.RenameField(oldName, newName))
        return this
    }

    public fun changeType(
        fieldName: String,
        newType: KdbFieldType,
    ): SchemaMigrationBuilder {
        steps.add(MigrationStep.ChangeType(fieldName, newType))
        return this
    }

    public fun addIndex(fieldName: String): SchemaMigrationBuilder {
        steps.add(MigrationStep.AddIndex(fieldName))
        return this
    }

    public fun dropIndex(fieldName: String): SchemaMigrationBuilder {
        steps.add(MigrationStep.DropIndex(fieldName))
        return this
    }

    public fun setRequired(
        fieldName: String,
        required: Boolean,
    ): SchemaMigrationBuilder {
        steps.add(MigrationStep.SetRequired(fieldName, required))
        return this
    }

    public fun setUnique(
        fieldName: String,
        unique: Boolean,
    ): SchemaMigrationBuilder {
        steps.add(MigrationStep.SetUnique(fieldName, unique))
        return this
    }

    public fun widenEnum(
        fieldName: String,
        vararg addValues: String,
    ): SchemaMigrationBuilder {
        steps.add(MigrationStep.WidenEnum(fieldName, addValues.toSet()))
        return this
    }

    public fun narrowEnum(
        fieldName: String,
        vararg removeValues: String,
    ): SchemaMigrationBuilder {
        steps.add(MigrationStep.NarrowEnum(fieldName, removeValues.toSet()))
        return this
    }

    public fun description(text: String): SchemaMigrationBuilder {
        migrationDescription = text
        return this
    }

    /** Validates full sequence against [baseSchema]; throws [SchemaMigrationConflictException] if invalid. */
    public fun build(migrationId: KdbUuid = KdbUuid.random()): SchemaMigration {
        replayMigrationSteps(baseSchema.fields, steps)
        return SchemaMigration(
            migrationId,
            baseSchema.version,
            baseSchema.version + 1,
            steps.toList(),
            migrationDescription,
        )
    }
}

public fun KdbSchema.migrate(block: SchemaMigrationBuilder.() -> Unit): SchemaMigration =
    SchemaMigrationBuilder(this).apply(block).build()
