package dev.kdb.sql

import dev.kdb.json.kdbJsonGet
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.SchemaField
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** Component 71 — aggregates, group keys and mutating SQL (Layer 16 §5). */
class SqlAggregateAndUpdateTest {
    private suspend fun scored(): SqlTestRuntime {
        val fx = schemalessRuntime()
        fx.put("""{"status":"active","points":3,"owner":"u1"}""")
        fx.put("""{"status":"active","points":4,"owner":"u2"}""")
        fx.put("""{"status":"idle","points":10,"owner":"u1"}""")
        return fx
    }

    /** The group-key column projects the group's value; it used to come back NULL. */
    @Test
    fun groupKeyColumnIsProjected() =
        runTest {
            val fx = scored()
            val result = fx.query("SELECT status, COUNT(*) AS n FROM tasks GROUP BY status")
            assertEquals(listOf("active", "idle"), result.strings(0))
            assertEquals(listOf(2L, 1L), result.column(1).map { it.long() })
        }

    /** Without ORDER BY, groups come out in ascending group-key order (total comparator). */
    @Test
    fun groupsAreEmittedInAscendingKeyOrder() =
        runTest {
            val fx = schemalessRuntime()
            fx.put("""{"status":"zulu"}""")
            fx.put("""{"status":"alpha"}""")
            fx.put("""{"status":"mike"}""")
            val result = fx.query("SELECT status, COUNT(*) AS n FROM tasks GROUP BY status")
            assertEquals(listOf("alpha", "mike", "zulu"), result.strings(0))
        }

    /** A NULL group key sorts first, like every other NULL (§2). */
    @Test
    fun nullGroupKeySortsFirst() =
        runTest {
            val fx = schemalessRuntime()
            fx.put("""{"status":"alpha"}""")
            fx.put("""{"other":1}""")
            val result = fx.query("SELECT status, COUNT(*) AS n FROM tasks GROUP BY status")
            assertEquals(SqlCell.Null, result.rows.first().values[0])
        }

    /** SUM over integers only is a Long; any double input makes it a Double. */
    @Test
    fun sumTypingFollowsItsInputs() =
        runTest {
            val fx = scored()
            val ints = fx.query("SELECT SUM(points) AS s FROM tasks")
            assertEquals(17L, ints.rows.single().values[0].long())

            val mixed = schemalessRuntime()
            mixed.put("""{"points":1}""")
            mixed.put("""{"points":1.5}""")
            assertEquals(2.5, mixed.query("SELECT SUM(points) AS s FROM tasks").rows.single().values[0].dbl())
        }

    /** AVG is always a Double. */
    @Test
    fun avgIsAlwaysDouble() =
        runTest {
            val fx = schemalessRuntime()
            fx.put("""{"points":1}""")
            fx.put("""{"points":2}""")
            assertEquals(1.5, fx.query("SELECT AVG(points) AS a FROM tasks").rows.single().values[0].dbl())
        }

    /** MIN/MAX ignore NULL rather than letting it win the comparison. */
    @Test
    fun minAndMaxIgnoreNull() =
        runTest {
            val fx = schemalessRuntime()
            fx.put("""{"points":5}""")
            fx.put("""{"other":1}""")
            fx.put("""{"points":2}""")
            val result = fx.query("SELECT MIN(points) AS lo, MAX(points) AS hi FROM tasks")
            assertEquals(2L, result.rows.single().values[0].long())
            assertEquals(5L, result.rows.single().values[1].long())
        }

    /** Over zero rows every aggregate is NULL except COUNT, which is 0. */
    @Test
    fun aggregatesOverZeroRows() =
        runTest {
            val fx = scored()
            val result =
                fx.query("SELECT COUNT(*) AS n, COUNT(points) AS c, SUM(points) AS s, AVG(points) AS a, MIN(points) AS lo, MAX(points) AS hi FROM tasks WHERE status = 'nope'")
            val row = result.rows.single().values
            assertEquals(0L, row[0].long())
            assertEquals(0L, row[1].long())
            assertEquals(SqlCell.Null, row[2])
            assertEquals(SqlCell.Null, row[3])
            assertEquals(SqlCell.Null, row[4])
            assertEquals(SqlCell.Null, row[5])
        }

    /** COUNT(col) counts non-NULL values, COUNT(*) counts rows. */
    @Test
    fun countColumnSkipsNulls() =
        runTest {
            val fx = schemalessRuntime()
            fx.put("""{"points":1}""")
            fx.put("""{"other":2}""")
            val result = fx.query("SELECT COUNT(*) AS all, COUNT(points) AS some FROM tasks")
            assertEquals(2L, result.rows.single().values[0].long())
            assertEquals(1L, result.rows.single().values[1].long())
        }

    /** Aggregates group by several keys and can be ordered by an aggregate alias. */
    @Test
    fun groupByTwoKeysOrderedByAggregate() =
        runTest {
            val fx = scored()
            val result = fx.query("SELECT owner, SUM(points) AS total FROM tasks GROUP BY owner ORDER BY total DESC")
            assertEquals(listOf("u1", "u2"), result.strings(0))
            assertEquals(listOf(13L, 4L), result.column(1).map { it.long() })
        }

    /** UPDATE writes one op per matched document and reports it as rowsAffected. */
    @Test
    fun updateReportsRowsAffected() =
        runTest {
            val fx = scored()
            val dml = fx.dml("UPDATE tasks SET status = 'done' WHERE status = 'active'")
            assertEquals(2, dml.rowsAffected)
            assertEquals(2, fx.query("SELECT status FROM tasks WHERE status = 'done'").rows.size)
        }

    /** `SET path = value` writes a nested JSON path rather than a flat key. */
    @Test
    fun updateSetsNestedPath() =
        runTest {
            val fx = schemalessRuntime()
            val id = fx.put("""{"title":"a","meta":{"points":1}}""")
            fx.dml("UPDATE tasks SET meta.reviewed = true WHERE title = 'a'")
            val json = fx.docJson(id)!!
            assertEquals("true", kdbJsonGet(json, "$.meta.reviewed")!!.toJsonString())
            assertEquals("1", kdbJsonGet(json, "$.meta.points")!!.toJsonString())
        }

    /** An assignment expression sees the pre-update document. */
    @Test
    fun updateExpressionReadsPreUpdateColumns() =
        runTest {
            val fx = schemalessRuntime()
            val id = fx.put("""{"title":"a","points":7,"copy":0}""")
            fx.dml("UPDATE tasks SET copy = points, points = 0 WHERE title = 'a'")
            val json = fx.docJson(id)!!
            assertEquals("7", kdbJsonGet(json, "$.copy")!!.toJsonString())
            assertEquals("0", kdbJsonGet(json, "$.points")!!.toJsonString())
        }

    /** A bound parameter is a legal assignment value. */
    @Test
    fun updateWithParameter() =
        runTest {
            val fx = schemalessRuntime()
            val id = fx.put("""{"title":"a","status":"new"}""")
            fx.dml("UPDATE tasks SET status = ? WHERE title = ?", listOf(SqlParameter.StringParam("done"), SqlParameter.StringParam("a")))
            assertEquals("\"done\"", kdbJsonGet(fx.docJson(id)!!, "$.status")!!.toJsonString())
        }

    /**
     * `SET _doc = …` supplies a whole-document body, but the transaction engine applies a
     * `WriteOp` body as a shallow root-level *merge*, so a top-level key absent from the new body
     * survives (Layer 16 §5, known limitation). That is pre-existing engine behaviour shared with
     * wire UPSERT — true replacement needs a replace-capable document op, deferred to its own
     * layer — so the SQL layer emits the body verbatim and both trees merge identically. This
     * test pins the merge so a future replace-capable op is a deliberate change, not a surprise.
     */
    @Test
    fun updateWithDocBodyMergesRatherThanReplacing() =
        runTest {
            val fx = schemalessRuntime()
            val id = fx.put("""{"title":"a","points":1}""")
            fx.dml("UPDATE tasks SET _doc = ? WHERE title = 'a'", listOf(SqlParameter.StringParam("""{"title":"b"}""")))
            val json = fx.docJson(id)!!
            assertEquals("\"b\"", kdbJsonGet(json, "$.title")!!.toJsonString())
            assertEquals("1", kdbJsonGet(json, "$.points")!!.toJsonString(), "points survived the merge")
        }

    /**
     * `NOT` is two-valued over the §2 comparison rules: `NOT BETWEEN` / `NOT IN` / `NOT LIKE`
     * against a NULL or absent path return the row, because the inner comparison is false and
     * `NOT` negates it. Standard SQL's three-valued logic would exclude it; both trees
     * deliberately behave this way (Layer 16 §5).
     */
    @Test
    fun notIsTwoValuedOverNullAndAbsentPaths() =
        runTest {
            val fx = schemalessRuntime()
            fx.put("""{"title":"present","points":5,"label":"x"}""")
            fx.put("""{"title":"absent"}""")
            fx.put("""{"title":"null-valued","points":null,"label":null}""")

            assertEquals(
                listOf("absent", "null-valued"),
                fx.query("SELECT title FROM tasks WHERE points NOT BETWEEN 1 AND 10 ORDER BY title ASC").strings(),
            )
            assertEquals(
                listOf("absent", "null-valued"),
                fx.query("SELECT title FROM tasks WHERE points NOT IN (5) ORDER BY title ASC").strings(),
            )
            assertEquals(
                listOf("absent", "null-valued"),
                fx.query("SELECT title FROM tasks WHERE label NOT LIKE 'x' ORDER BY title ASC").strings(),
            )
        }

    /** A document that would violate the schema after assignment fails the statement. */
    @Test
    fun updateValidatesAgainstTheSchema() =
        runTest {
            val fx =
                schemaRuntime(
                    listOf(
                        SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                        SchemaField("rank", KdbFieldType.Int64Type, required = false, indexed = true),
                    ),
                )
            fx.put("""{"userId":"u1","rank":1}""")
            assertFailsWith<SqlPlanningException> { fx.dml("UPDATE users SET rank = 'not a number' WHERE userId = 'u1'") }
        }

    /** DELETE reports the number of documents removed. */
    @Test
    fun deleteReportsRowsAffected() =
        runTest {
            val fx = scored()
            val dml = fx.dml("DELETE FROM tasks WHERE status = 'active'")
            assertEquals(2, dml.rowsAffected)
            assertEquals(1, fx.query("SELECT status FROM tasks").rows.size)
        }

    /** DELETE without WHERE removes every document. */
    @Test
    fun deleteWithoutWhereRemovesEverything() =
        runTest {
            val fx = scored()
            assertEquals(3, fx.dml("DELETE FROM tasks").rowsAffected)
            assertTrue(fx.query("SELECT status FROM tasks").rows.isEmpty())
        }

    /** UPDATE against an unknown column is a planning error, like any other column reference. */
    @Test
    fun updateOnUnknownColumnThrowsUnderASchema() =
        runTest {
            val fx =
                schemaRuntime(listOf(SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true)))
            fx.put("""{"userId":"u1"}""")
            assertFailsWith<SqlPlanningException> { fx.dml("UPDATE users SET nope = 'x' WHERE userId = 'u1'") }
        }
    /**
     * INSERT INTO t (_doc) VALUES (...) supplies the whole body. Kotlin briefly rejected this as a
     * "reserved column", which both broke stored procedures and diverged from the Go executor,
     * where every INSERT column goes through the same whole-body assignment path.
     */
    @Test
    fun insertIntoDocSuppliesTheWholeBody() =
        runTest {
            val fx = schemalessRuntime()
            fx.dml("""INSERT INTO tasks (_doc) VALUES ('{"title":"written via _doc","n":7}')""")
            val result = fx.query("SELECT title, n FROM tasks")
            assertEquals(listOf("written via _doc"), result.strings(0))
            assertEquals(listOf(7L), result.column(1).map { it.long() })
        }

    /** kdb_id stays engine-minted: assigning it in an INSERT is still an error. */
    @Test
    fun insertIntoKdbIdIsRejected() =
        runTest {
            val fx = schemalessRuntime()
            assertFailsWith<SqlPlanningException> {
                fx.dml("INSERT INTO tasks (kdb_id) VALUES ('x')")
            }
        }
}
