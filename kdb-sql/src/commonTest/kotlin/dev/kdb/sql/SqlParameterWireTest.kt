package dev.kdb.sql

import kotlin.test.Test
import kotlin.test.assertEquals

class SqlParameterWireTest {
    @Test
    fun roundTripParameters() {
        val original =
            listOf(
                SqlParameter.StringParam("u1"),
                SqlParameter.IntParam(42),
                SqlParameter.DoubleParam(1.5),
                SqlParameter.BoolParam(true),
                SqlParameter.NullParam,
            )
        val json = encodeSqlParameters(original)!!
        val decoded = decodeSqlParameters(json)
        assertEquals(original.size, decoded.size)
        assertEquals("u1", (decoded[0] as SqlParameter.StringParam).value)
        assertEquals(42L, (decoded[1] as SqlParameter.IntParam).value)
        assertEquals(1.5, (decoded[2] as SqlParameter.DoubleParam).value)
        assertEquals(true, (decoded[3] as SqlParameter.BoolParam).value)
        assertEquals(SqlParameter.NullParam, decoded[4])
    }
}
