package dev.kdb.schema

import dev.kdb.json.JsonValue
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * Mirrors go/kdb/schema/engine_test.go's bounds and ordering cases. Both implementations decide
 * whether a document may be written and whether a schema change may land, so they have to give
 * the same answer at their own limits.
 */
class SchemaBoundsAndDiffOrderTest {
    private val int64Field = SchemaField("n", KdbFieldType.Int64Type, required = false, indexed = false)

    /**
     * A JSON number outside Long's range must be rejected rather than validated and then stored
     * as the Long the field declares. `isIntegralDouble` rejected most such values already, as a
     * side effect of `Double.toLong()` saturating - but not exactly 2^63, which saturated to
     * Long.MAX_VALUE whose Double form is 2^63 again, so the difference was zero and it passed.
     */
    @Test
    fun int64FieldRejectsValuesOutsideLong() {
        val twoToThe63 = 9.223372036854775808E18
        for (v in listOf(1e30, -1e30, 9.3e18, -9.3e18, twoToThe63)) {
            assertNotNull(
                SchemaEngine.checkFieldValue(int64Field, JsonValue.JNumber(v)),
                "int64 field accepted $v, which is outside Long",
            )
        }
    }

    @Test
    fun int64FieldAcceptsValuesThatFit() {
        val twoToThe63 = 9.223372036854775808E18
        for (v in listOf(-9.223372036854775808E18, twoToThe63 - 1024.0, 0.0, -1.0, 9.007199254740992E15)) {
            assertNull(
                SchemaEngine.checkFieldValue(int64Field, JsonValue.JNumber(v)),
                "int64 field rejected $v, which fits",
            )
        }
        // A JInt is a Long already, so every value one can hold is in range by construction.
        assertNull(SchemaEngine.checkFieldValue(int64Field, JsonValue.JInt(Long.MAX_VALUE)))
        assertNull(SchemaEngine.checkFieldValue(int64Field, JsonValue.JInt(Long.MIN_VALUE)))
    }

    /** Int32 was already bounded; pinned here so the two integer types cannot drift apart. */
    @Test
    fun int32FieldRejectsValuesOutsideInt() {
        val f = SchemaField("n", KdbFieldType.Int32Type, required = false, indexed = false)
        assertNotNull(SchemaEngine.checkFieldValue(f, JsonValue.JNumber(2147483648.0)))
        assertNotNull(SchemaEngine.checkFieldValue(f, JsonValue.JNumber(-2147483649.0)))
        assertNull(SchemaEngine.checkFieldValue(f, JsonValue.JNumber(2147483647.0)))
        assertNull(SchemaEngine.checkFieldValue(f, JsonValue.JNumber(-2147483648.0)))
    }

    /**
     * The three lists in a diff are sorted by field name, matching Go's DiffSchemas. Kotlin's
     * associateBy keeps declaration order, so this side was already deterministic - but the two
     * implementations returned the same diff in different orders.
     */
    @Test
    fun diffListsAreSortedByFieldName() {
        fun f(name: String, type: KdbFieldType = KdbFieldType.StringType) =
            SchemaField(name, type, required = false, indexed = false)

        val from =
            KdbSchema.build(
                listOf(f("r3"), f("r1"), f("r2"), f("m2"), f("m1")),
                version = 1,
            )
        val to =
            KdbSchema.build(
                listOf(
                    f("a3"), f("a1"), f("a2"),
                    f("m2", KdbFieldType.Int32Type), f("m1", KdbFieldType.Int32Type),
                ),
                version = 2,
            )

        val d = SchemaEngine.diff(from, to)
        assertEquals(listOf("a1", "a2", "a3"), d.addedFields.map { it.name })
        assertEquals(listOf("r1", "r2", "r3"), d.removedFields.map { it.name })
        assertEquals(listOf("m1", "m2"), d.modifiedFields.map { it.fieldName })
    }
}
