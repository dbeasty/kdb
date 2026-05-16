package dev.kdb.error

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue

class ErrorModelTest {
    @Test
    fun kdb_error_codes_numeric_unique() {
        val nums = enumValues<KdbErrorCode>().map { it.numericCode }
        assertEquals(nums.size, nums.toSet().size)
    }

    @Test
    fun bson_decode_exception_offset_and_code() {
        val e = BsonDecodeException("oops", offset = 42)
        assertEquals(42, e.offset)
        assertEquals(KdbErrorCode.BSON_DECODE_ERROR, e.code)
    }

    @Test
    fun schema_violations_all_preserved() {
        val violations = listOf(
            FieldViolation("a", ViolationType.TYPE_MISMATCH, "x"),
            FieldViolation("b", ViolationType.REQUIRED_FIELD_MISSING, "y"),
            FieldViolation("c", ViolationType.UNIQUE_CONSTRAINT, "z"),
        )
        val e = SchemaViolationException("bad", violations)
        assertEquals(3, e.violations.size)
        assertEquals("c", e.violations[2].fieldName)
    }

    @Test
    fun schema_violation_empty_list_guard() {
        assertFailsWith<IllegalArgumentException> {
            SchemaViolationException("x", emptyList())
        }
    }

    @Test
    fun conflict_exception_empty_guard() {
        assertFailsWith<IllegalArgumentException> {
            ConflictException(
                "x",
                ConflictReport("tid", "base", "target", emptyList()),
            )
        }
    }

    @Test
    fun conflict_report_accessible() {
        val conflicts = listOf(
            ConflictItem("d1", ConflictOperationType.CONCURRENT_WRITE, "{a}", "{b}"),
            ConflictItem("d2", ConflictOperationType.WRITE_DELETE, "{c}", null),
        )
        val r = ConflictReport("tid", "b", "t", conflicts)
        assertEquals(2, r.conflicts.size)
        assertEquals(ConflictOperationType.WRITE_DELETE, r.conflicts[1].operationType)
    }

    @Test
    fun kdb_result_success() {
        val r = kdbRunCatching { 42 }
        assertTrue(r.isSuccess)
        assertEquals(42, r.getOrThrow())
    }

    @Test
    fun kdb_result_failure_wraps_schema_violation() {
        val v = FieldViolation("f", ViolationType.TYPE_MISMATCH, "d")
        val r = kdbRunCatching {
            throw SchemaViolationException("!", listOf(v))
        }
        assertTrue(r.isFailure)
        val ex = r.exceptionOrNull()!!
        assertEquals(KdbErrorCode.SCHEMA_VIOLATION, ex.code)
        check(ex is SchemaViolationException)
        assertEquals("f", ex.violations[0].fieldName)
    }

    @Test
    fun kdb_result_non_kdb_propagates() {
        assertFailsWith<IllegalStateException> {
            kdbRunCatching { throw IllegalStateException("no") }
        }
    }

    @Test
    fun catch_as_kdb_exception_root_type() {
        val ex = assertFailsWith<KdbException> {
            throw IceStorageException("ice", "ns", "dead", archiveLocation = null)
        }
        assertEquals(KdbErrorCode.ICE_STORAGE, ex.code)
        check(ex is IceStorageException)
        assertEquals(null, ex.archiveLocation)
    }

    @Test
    fun ice_archive_location_can_be_null() {
        val e = IceStorageException("m", "n", "h", archiveLocation = null)
        assertEquals(null, e.archiveLocation)
    }

    @Test
    fun map_success() {
        val r = KdbResult.Success(5).map { it * 2 }
        check(r is KdbResult.Success<Int>)
        assertEquals(10, r.value)
    }

    @Test
    fun map_failure_preserves_failure() {
        val ex = BsonDecodeException("x")
        val r = KdbResult.Failure(ex).map { _: Nothing -> kotlin.test.fail("unexpected") }
        assertSame(ex, assertIs<KdbResult.Failure>(r).exception)
    }

    @Suppress("SameParameterValue")
    private inline fun <reified T> assertIs(any: Any): T {
        assertTrue(any is T, "expected ${T::class.simpleName}")
        @Suppress("UNCHECKED_CAST")
        return any as T
    }
}
