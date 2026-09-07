package dev.kdb.sql

import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.SchemaField
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** Component 69 — column resolution and the total comparator (Layer 16 §2, §3). */
class SqlColumnResolutionTest {
    private val fields =
        listOf(
            SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
            SchemaField("status", KdbFieldType.StringType, required = false, indexed = true),
            SchemaField("rank", KdbFieldType.Int64Type, required = false, indexed = true),
        )

    private suspend fun seeded(): SqlTestRuntime {
        val fx = schemaRuntime(fields)
        fx.put("""{"userId":"u1","status":"active","rank":2}""")
        fx.put("""{"userId":"u2","status":"idle","rank":1}""")
        return fx
    }

    /** Rule 1: an unknown column in WHERE is a planning error, not a silently empty match. */
    @Test
    fun unknownColumnInWhereThrows() =
        runTest {
            val fx = seeded()
            val e = assertFailsWith<SqlPlanningException> { fx.query("SELECT userId FROM users WHERE nope = 'x'") }
            assertEquals("unknown column: nope", e.message)
        }

    /** Rule 1: `<>` on an unknown column used to match every row; it must now be a planning error. */
    @Test
    fun unknownColumnInNotEqualsWhereThrows() =
        runTest {
            val fx = seeded()
            assertFailsWith<SqlPlanningException> { fx.query("SELECT userId FROM users WHERE nope <> 'x'") }
        }

    /** Rule 1: an unknown ORDER BY column is a planning error, not a silently ignored sort. */
    @Test
    fun unknownColumnInOrderByThrows() =
        runTest {
            val fx = seeded()
            val e = assertFailsWith<SqlPlanningException> { fx.query("SELECT userId FROM users ORDER BY nope DESC") }
            assertEquals("unknown column: nope", e.message)
        }

    /** Rule 1 covers GROUP BY too. */
    @Test
    fun unknownColumnInGroupByThrows() =
        runTest {
            val fx = seeded()
            assertFailsWith<SqlPlanningException> { fx.query("SELECT COUNT(*) FROM users GROUP BY nope") }
        }

    /** Rule 1 covers function arguments. */
    @Test
    fun unknownColumnInFunctionArgumentThrows() =
        runTest {
            val fx = seeded()
            assertFailsWith<SqlPlanningException> { fx.query("SELECT COUNT(nope) FROM users") }
        }

    /** Rule 1 covers projections (the only case the planner checked before Layer 16). */
    @Test
    fun unknownColumnInProjectionThrows() =
        runTest {
            val fx = seeded()
            assertFailsWith<SqlPlanningException> { fx.query("SELECT nope FROM users") }
        }

    /** `kdb_id` and `_doc` resolve outside the document and are never "unknown". */
    @Test
    fun reservedColumnsResolveUnderASchema() =
        runTest {
            val fx = seeded()
            val result = fx.query("SELECT kdb_id, _doc FROM users WHERE userId = 'u1'")
            assertEquals(1, result.rows.size)
            assertTrue(result.rows.first().values[0].str().isNotEmpty())
            assertTrue(result.rows.first().values[1] is SqlCell.JsonVal)
        }

    /** Rule 1 checks only the root segment, so nested paths under a declared field are allowed. */
    @Test
    fun nestedPathUnderDeclaredFieldResolves() =
        runTest {
            val fx =
                schemaRuntime(
                    listOf(
                        SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                        SchemaField("meta", KdbFieldType.ObjectType, required = false, indexed = false),
                    ),
                )
            fx.put("""{"userId":"u1","meta":{"reviewed":true}}""")
            val result = fx.query("SELECT userId FROM users WHERE meta.reviewed = true")
            assertEquals(listOf("u1"), result.strings())
        }

    /**
     * Rule 2: a schemaless namespace resolves every column by JSON path, which is what makes
     * `SELECT kdb_id, _doc … WHERE title = 'alpha'` work against documents written by `kdb put`.
     */
    @Test
    fun schemalessNamespaceResolvesByJsonPath() =
        runTest {
            val fx = schemalessRuntime()
            fx.put("""{"title":"alpha","tags":["x"]}""")
            fx.put("""{"title":"beta"}""")
            val result = fx.query("SELECT kdb_id, _doc FROM tasks WHERE title = 'alpha'")
            assertEquals(1, result.rows.size)
        }

    /** Rule 2: an absent path is NULL rather than an error. */
    @Test
    fun schemalessAbsentPathIsNull() =
        runTest {
            val fx = schemalessRuntime()
            fx.put("""{"title":"alpha"}""")
            val result = fx.query("SELECT missing FROM tasks")
            assertEquals(SqlCell.Null, result.rows.single().values[0])
            assertEquals(1, fx.query("SELECT title FROM tasks WHERE missing IS NULL").rows.size)
        }

    /** §2: NULL compared with anything is unknown, so `= NULL` and `<> NULL` are both false. */
    @Test
    fun nullComparesUnknownInPredicates() =
        runTest {
            val fx = schemalessRuntime()
            fx.put("""{"title":"alpha","score":null}""")
            assertEquals(0, fx.query("SELECT title FROM tasks WHERE score = NULL").rows.size)
            assertEquals(0, fx.query("SELECT title FROM tasks WHERE score <> NULL").rows.size)
            assertEquals(1, fx.query("SELECT title FROM tasks WHERE score IS NULL").rows.size)
        }

    /** §2: mismatched types are incomparable — `=` false, `<>` true, ordering false. */
    @Test
    fun mismatchedTypesAreIncomparable() =
        runTest {
            val fx = schemalessRuntime()
            fx.put("""{"title":"alpha","score":"7"}""")
            assertEquals(0, fx.query("SELECT title FROM tasks WHERE score = 7").rows.size)
            assertEquals(1, fx.query("SELECT title FROM tasks WHERE score <> 7").rows.size)
            assertEquals(0, fx.query("SELECT title FROM tasks WHERE score < 8").rows.size)
            assertEquals(0, fx.query("SELECT title FROM tasks WHERE score > 6").rows.size)
        }

    /** §2: integers and doubles still compare numerically across the two cell types. */
    @Test
    fun intAndDoubleCompareNumerically() =
        runTest {
            val fx = schemalessRuntime()
            fx.put("""{"title":"a","score":2}""")
            fx.put("""{"title":"b","score":1.5}""")
            assertEquals(listOf("b"), fx.query("SELECT title FROM tasks WHERE score < 2").strings())
            assertEquals(listOf("a"), fx.query("SELECT title FROM tasks WHERE score = 2.0").strings())
        }

    /** §2 sort comparator is total: NULL first ascending, last descending, NULL vs NULL equal. */
    @Test
    fun nullSortsFirstAscendingAndLastDescending() =
        runTest {
            val fx = schemalessRuntime()
            fx.put("""{"title":"a","score":2}""")
            fx.put("""{"title":"b"}""")
            fx.put("""{"title":"c","score":1}""")
            assertEquals(listOf("b", "c", "a"), fx.query("SELECT title FROM tasks ORDER BY score ASC").strings())
            assertEquals(listOf("a", "c", "b"), fx.query("SELECT title FROM tasks ORDER BY score DESC").strings())
        }

    /** DISTINCT runs after projection and before LIMIT, so LIMIT bounds deduplicated rows (§3). */
    @Test
    fun distinctIsAppliedBeforeLimit() =
        runTest {
            val fx = schemalessRuntime()
            fx.put("""{"status":"active"}""")
            fx.put("""{"status":"active"}""")
            fx.put("""{"status":"idle"}""")
            val result = fx.query("SELECT DISTINCT status FROM tasks ORDER BY status ASC LIMIT 2")
            assertEquals(listOf("active", "idle"), result.strings())
        }
}
