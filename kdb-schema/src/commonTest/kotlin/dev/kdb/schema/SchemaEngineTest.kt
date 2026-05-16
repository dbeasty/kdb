package dev.kdb.schema

import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbDocument
import dev.kdb.error.KdbResult
import dev.kdb.error.ViolationType
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class SchemaEngineTest {
    @Test
    fun tc01_validDocumentPasses() {
        val schema =
            KdbSchema.build(
                listOf(
                    SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                    SchemaField("email", KdbFieldType.StringType, required = true, indexed = true),
                    SchemaField("status", KdbFieldType.EnumType(setOf("active", "inactive")), required = true, indexed = true),
                    SchemaField("createdAt", KdbFieldType.TimestampType, required = true, indexed = false),
                ),
            )
        val doc =
            KdbDocument.fromJson(
                """{"userId":"abc","email":"a@b.com","status":"active","createdAt":"2024-01-01T00:00:00Z"}""",
            )
        val r = SchemaEngine.validate(doc, schema)
        assertIs<KdbResult.Success<KdbDocument>>(r)
    }

    @Test
    fun tc02_missingRequiredFails() {
        val schema =
            KdbSchema.build(
                listOf(
                    SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                    SchemaField("email", KdbFieldType.StringType, required = true, indexed = true),
                    SchemaField("status", KdbFieldType.StringType, required = true, indexed = true),
                ),
            )
        val doc = KdbDocument.fromJson("""{"userId":"abc","status":"active"}""")
        val r = SchemaEngine.validate(doc, schema)
        val f = assertIs<KdbResult.Failure>(r)
        val sv = assertIs<dev.kdb.error.SchemaViolationException>(f.exception)
        assertEquals("email", sv.violations.single().fieldName)
        assertEquals(ViolationType.REQUIRED_FIELD_MISSING, sv.violations.single().violationType)
    }

    @Test
    fun tc04_noneSchemaAlwaysPasses() {
        val doc = KdbDocument.fromJson("""{"x":1}""")
        val r = SchemaEngine.validate(doc, KdbSchema.NONE)
        assertIs<KdbResult.Success<KdbDocument>>(r)
    }

    @Test
    fun migrationRoundTripWire() {
        val base =
            KdbSchema.build(
                listOf(SchemaField("s", KdbFieldType.EnumType(setOf("a", "b")), required = false, indexed = true)),
            )
        val m =
            base.migrate {
                widenEnum("s", "c")
                narrowEnum("s", "b")
                description("zigzag")
            }
        val bytes = m.toBytes()
        val back = SchemaMigration.fromBytes(bytes)
        assertEquals(m.migrationId, back.migrationId)
        assertEquals(m.steps.size, back.steps.size)
    }

    @Test
    fun schemaHashDeterministic() {
        val f =
            listOf(
                SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
            )
        val ts = KdbTimestamp.fromEpochMicros(1)
        val a = KdbSchema.build(f, createdAt = ts, description = "")
        val b = KdbSchema.build(f, createdAt = ts, description = "")
        assertEquals(a.schemaHash, b.schemaHash)
        assertEquals(SchemaEngine.computeSchemaHash(a), a.schemaHash)
    }
}
