package dev.kdb.sql

import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * `maxRows` bounds what a query may return, never what it may look at (the Go tree's
 * `scan_result_bound_test.go`, `search_result_bound_test.go`, and the shared fixture
 * `go/testdata/golden/sql/result_bound.json`, which jvmTest runs). These cover what the fixture
 * cannot: a ranking longer than the bound, and DML, which resolves its targets by scanning too.
 */
class SqlResultBoundTest {
    /** Twelve documents ranked best-first, only the three lowest-ranked of them 'open'. */
    private suspend fun rankedCorpus(): SqlTestRuntime {
        val ns = "app/tasks"
        val dag = inMemoryCommitDag(ns)
        val storage = InMemoryStorageAdapter()
        val ids = List(12) { KdbUuid.random() }
        val factory =
            FakeSearchStoreFactory(
                dag,
                storage,
                textRankings = mapOf("x" to ranked(*ids.mapIndexed { i, id -> id to (12 - i).toFloat() }.toTypedArray())),
            )
        val fx = schemalessRuntime(ns = ns, storeFactory = factory)
        ids.forEachIndexed { i, id ->
            val status = if (i >= 9) "open" else "closed"
            fx.put("""{"title":"t${(i + 1).toString().padStart(2, '0')}","status":"$status"}""", id)
        }
        fx.query("CREATE INDEX tasks_text ON tasks (title) USING FULLTEXT")
        return fx
    }

    @Test
    fun matchWithAFilterSeesHitsRankedPastMaxRows() =
        runTest {
            val fx = rankedCorpus()
            val ctx = fx.ctx().copy(maxRows = 5)
            val result =
                fx.engine.execute(
                    "SELECT title FROM tasks WHERE MATCH(tasks_text, 'x') AND status = 'open' ORDER BY title",
                    ctx,
                )
            assertEquals(listOf("t10", "t11", "t12"), result.strings())
        }

    @Test
    fun aRankingLargerThanMaxRowsIsAnErrorNotACut() =
        runTest {
            val fx = rankedCorpus()
            val ctx = fx.ctx().copy(maxRows = 5)
            val e = assertFailsWith<SqlResultLimitException> { fx.engine.execute("SELECT title FROM tasks WHERE MATCH(tasks_text, 'x')", ctx) }
            assertEquals(5, e.limit)
        }

    private suspend fun docs(n: Int): SqlTestRuntime {
        val fx = schemalessRuntime(ns = "app/t")
        for (i in 0 until n) fx.put("""{"id":"doc-${i.toString().padStart(3, '0')}","v":"before"}""")
        return fx
    }

    @Test
    fun updateAndDeleteActOnDocumentsPastMaxRows() =
        runTest {
            val fx = docs(20)
            val ctx = fx.ctx().copy(maxRows = 5)

            val one = fx.engine.executeDml("UPDATE t SET v = 'after' WHERE id = 'doc-019'", ctx)
            assertEquals(1, one.rowsAffected)

            // A DML predicate may match more rows than a reply could carry; nothing is returned.
            val many = fx.engine.executeDml("UPDATE t SET v = 'bulk' WHERE id >= 'doc-000'", ctx)
            assertEquals(20, many.rowsAffected)
            assertEquals(20, many.operations.size)

            val gone = fx.engine.executeDml("DELETE FROM t WHERE id >= 'doc-000'", ctx)
            assertEquals(20, gone.rowsAffected)
        }

    @Test
    fun deleteByDocumentIdPastMaxRows() =
        runTest {
            val fx = docs(20)
            val ctx = fx.ctx().copy(maxRows = 5)
            val last = fx.query("SELECT kdb_id FROM t WHERE id = 'doc-019'").strings().single()
            val deleted = fx.engine.executeDml("DELETE FROM t WHERE kdb_id = '$last'", ctx)
            assertEquals(1, deleted.rowsAffected)
        }
}
