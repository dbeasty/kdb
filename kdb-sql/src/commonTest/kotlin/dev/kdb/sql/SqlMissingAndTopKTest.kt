package dev.kdb.sql

import kotlinx.coroutines.test.runTest
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Kotlin parity for the qtask production plan's SQL additions (Go `sql/missing_test.go`,
 * `sql/topk_test.go`, `sql/conformance_test.go`): IS [NOT] MISSING / EXISTS / ALL_VALUES, booleans
 * projected as booleans, and the bounded top-k behind ORDER BY ... LIMIT.
 */
class SqlMissingAndTopKTest {
    /** Go's conformance corpus, so the expectations below are Go's, row for row. */
    private suspend fun corpus(): SqlTestRuntime {
        val fx = schemalessRuntime(ns = "app/conf-missing")
        for (json in listOf(
            """{"title":"alpha","status":"open","priority":3,"tags":["x","y"],"projectIds":["p1","p2"],""" +
                """"collaborators":[{"userId":"u1"},{"userId":"u2"}],"steps":[{"text":"plan"},{"text":"deploy staging"}],""" +
                """"flag":true,"score":1.5}""",
            """{"title":"bravo","status":"done","priority":1,"tags":["y"],"projectIds":["p2"],""" +
                """"collaborators":[{"userId":"u3"}],"steps":[],"flag":false,"score":2}""",
            """{"title":"charlie","status":"open","priority":2,"tags":["x","y","z"],"projectIds":[],"flag":true}""",
            """{"title":"delta","status":"blocked","priority":null,"tags":[],"score":0.5}""",
            """{"title":"Echo","status":"done","priority":5,"tags":["z"],"projectIds":["p1"],"score":"high"}""",
        )) {
            fx.put(json)
        }
        return fx
    }

    private fun cellString(c: SqlCell): String =
        when (c) {
            SqlCell.Null -> "NULL"
            is SqlCell.StringVal -> c.value
            is SqlCell.LongVal -> c.value.toString()
            is SqlCell.DoubleVal -> c.value.toString()
            is SqlCell.BoolVal -> c.value.toString()
            is SqlCell.JsonVal -> c.json
        }

    private suspend fun SqlTestRuntime.rows(sql: String): List<String> =
        query(sql).rows.map { r -> r.values.joinToString("|") { cellString(it) } }

    private suspend fun SqlTestRuntime.expect(
        sql: String,
        vararg want: String,
    ) = assertEquals(want.toList(), rows(sql), sql)

    @Test
    fun missingExistsAndAllValues() =
        runTest {
            val f = corpus()
            // delta stores priority null; every other document has a number or nothing.
            f.expect("SELECT title FROM tasks WHERE priority IS NULL ORDER BY title", "delta")
            f.expect("SELECT title FROM tasks WHERE priority IS MISSING ORDER BY title")
            f.expect("SELECT title FROM tasks WHERE score IS MISSING ORDER BY title", "charlie")
            f.expect(
                "SELECT title FROM tasks WHERE projectIds IS NOT MISSING AND score IS NOT MISSING ORDER BY title",
                "Echo",
                "alpha",
                "bravo",
            )
            f.expect("SELECT title, EXISTS(priority), EXISTS(flag) FROM tasks WHERE title = 'delta'", "delta|true|false")
            f.expect("SELECT title FROM tasks WHERE EXISTS(collaborators.userId) ORDER BY title", "alpha", "bravo")
            // A path through an array: the bare column is its first value (spec §3.5), ALL_VALUES is all.
            f.expect(
                "SELECT collaborators.userId, ALL_VALUES(collaborators.userId) FROM tasks WHERE title = 'alpha'",
                """u1|["u1","u2"]""",
            )
            f.expect("SELECT ALL_VALUES(steps.text) FROM tasks WHERE title = 'alpha'", """["plan","deploy staging"]""")
            f.expect("SELECT ALL_VALUES(collaborators.userId) FROM tasks WHERE title = 'delta'", "NULL")
            f.expect("SELECT EXISTS(kdb_id) FROM tasks WHERE title = 'delta'", "true")
            val typed = f.query("SELECT EXISTS(flag) AS e, ALL_VALUES(tags) AS v FROM tasks WHERE title = 'delta'")
            assertEquals(listOf("BOOLEAN", "JSON"), typed.columns.map { it.sqlType })

            val one = assertFailsWith<SqlPlanningException> { f.query("SELECT EXISTS(1) FROM tasks") }
            assertTrue(one.message!!.contains("path"), one.message)
            val two = assertFailsWith<SqlPlanningException> { f.query("SELECT ALL_VALUES(title, status) FROM tasks") }
            assertTrue(two.message!!.contains("one path"), two.message)
        }

    /** A projected JSON boolean is a boolean cell (Go: CellBool, typed BOOL on the wire). */
    @Test
    fun projectedBooleansAreBooleans() =
        runTest {
            val f = corpus()
            f.expect(
                "SELECT DISTINCT status, flag FROM tasks ORDER BY status, flag",
                "blocked|NULL",
                "done|NULL",
                "done|false",
                "open|true",
            )
            f.expect("SELECT DISTINCT flag FROM tasks WHERE flag", "true")
        }

    /**
     * ORDER BY ... LIMIT served by the bounded top-k returns exactly what sorting every match and
     * cutting it returns - ties in candidate order, NULLs, absent fields, mixed types, arrays,
     * several sort keys, both directions, OFFSET.
     */
    @Test
    fun topKMatchesTheFullSort() =
        runTest {
            val f = schemalessRuntime(ns = "app/topk")
            val r = Random(7)
            for (i in 0 until 400) {
                val a =
                    when (r.nextInt(8)) {
                        0 -> "\"a\":null,"
                        1 -> ""
                        2 -> "\"a\":\"s${r.nextInt(5)}\","
                        3 -> "\"a\":[${r.nextInt(3)},${r.nextInt(3)}],"
                        4 -> "\"a\":${r.nextBoolean()},"
                        else -> "\"a\":${r.nextInt(10)},"
                    }
                f.put("""{$a"b":${r.nextInt(4)},"g":"g${i % 3}","n":$i}""")
            }
            for (order in listOf("a", "a DESC", "b, a", "b DESC, n", "a, b DESC, n", "g DESC, a DESC")) {
                for (where in listOf("", " WHERE n >= 50", " WHERE g = 'g1'")) {
                    val full = f.rows("SELECT n, a, b FROM t$where ORDER BY $order")
                    for ((limit, offset) in listOf(1 to 0, 7 to 0, 25 to 10, 0 to 0, 500 to 0)) {
                        val q = "SELECT n, a, b FROM t$where ORDER BY $order LIMIT $limit" + if (offset > 0) " OFFSET $offset" else ""
                        assertEquals(full.drop(offset).take(limit), f.rows(q), q)
                    }
                }
            }
        }

    @Test
    fun topKKeepsOnlyKInArrivalOrderOnTies() {
        val top = SqlTopK<Pair<Int, String>>(3) { a, b -> a.first.compareTo(b.first) }
        for (v in listOf(2 to "a", 1 to "b", 2 to "c", 1 to "d", 0 to "e", 2 to "f", 1 to "g")) top.offer(v)
        assertEquals(listOf(0 to "e", 1 to "b", 1 to "d"), top.sorted())
        assertEquals(emptyList(), SqlTopK<Int>(0) { a, b -> a.compareTo(b) }.also { it.offer(1) }.sorted())
    }
}
