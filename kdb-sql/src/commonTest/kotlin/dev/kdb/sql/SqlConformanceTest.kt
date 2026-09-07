package dev.kdb.sql

import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.index.IndexType
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Component 69 §3.5 — conformance suite: one test per clause the parser accepts, asserting the
 * clause either takes effect or errors, against an in-memory runtime over a fixed corpus. The Go
 * tree runs the same corpus, so the two trees can be compared clause by clause.
 */
class SqlConformanceTest {
    private val t1 = KdbUuid.random()
    private val t2 = KdbUuid.random()
    private val t3 = KdbUuid.random()
    private val t4 = KdbUuid.random()

    /**
     * The shared corpus: four documents with a mix of present, absent and null fields, an array of
     * scalars, an array of objects, and a FULLTEXT + VECTOR index answered from canned rankings.
     */
    private suspend fun corpus(): Pair<SqlTestRuntime, FakeSearchStoreFactory> {
        val ns = "app/tasks"
        val dag = inMemoryCommitDag(ns)
        val storage = InMemoryStorageAdapter()
        val factory =
            FakeSearchStoreFactory(
                dag,
                storage,
                textRankings = mapOf("deploy" to ranked(t1 to 3.0f, t2 to 1.5f)),
                vectorRanking = ranked(t2 to 0.9f, t3 to 0.4f),
            )
        val fx = schemalessRuntime(ns = ns, storeFactory = factory)
        fx.put("""{"title":"Deploy staging","status":"open","points":3,"tags":["ops"],"steps":[{"text":"prepare"}]}""", t1)
        fx.put("""{"title":"Deploy prod","status":"open","points":8,"tags":["ops","urgent"],"steps":[{"text":"verify"}]}""", t2)
        fx.put("""{"title":"Write docs","status":"done","points":1,"tags":["docs"],"steps":[]}""", t3)
        fx.put("""{"title":"Untriaged","status":null,"tags":[]}""", t4)
        fx.query("CREATE INDEX tasks_text ON tasks (title) USING FULLTEXT")
        fx.query("CREATE INDEX tasks_vec ON tasks (embedding) USING VECTOR WITH (dimensions = 2)")
        return fx to factory
    }

    /** Projection: named columns, aliases, `*`, and the reserved names. */
    @Test
    fun projectionClause() =
        runTest {
            val (fx, _) = corpus()
            val named = fx.query("SELECT title AS name FROM tasks WHERE title = 'Write docs'")
            assertEquals(listOf("name"), named.columns.map { it.name })
            assertEquals(listOf("Write docs"), named.strings())

            val star = fx.query("SELECT * FROM tasks WHERE title = 'Write docs'")
            assertEquals(listOf("kdb_id", "_doc"), star.columns.map { it.name })

            val reserved = fx.query("SELECT kdb_id, _doc FROM tasks WHERE title = 'Write docs'")
            assertEquals(t3.toString(), reserved.rows.single().values[0].str())
            assertTrue(reserved.rows.single().values[1] is SqlCell.JsonVal)
        }

    /** WHERE: equality, inequality, ordering, AND/OR, NOT, IS NULL / IS NOT NULL. */
    @Test
    fun whereClause() =
        runTest {
            val (fx, _) = corpus()
            assertEquals(listOf("Write docs"), fx.query("SELECT title FROM tasks WHERE status = 'done'").strings())
            assertEquals(2, fx.query("SELECT title FROM tasks WHERE status <> 'done'").rows.size)
            assertEquals(listOf("Deploy prod"), fx.query("SELECT title FROM tasks WHERE points > 3").strings())
            assertEquals(
                listOf("Deploy staging"),
                fx.query("SELECT title FROM tasks WHERE status = 'open' AND points < 5").strings(),
            )
            assertEquals(3, fx.query("SELECT title FROM tasks WHERE status = 'done' OR status = 'open'").rows.size)
            assertEquals(1, fx.query("SELECT title FROM tasks WHERE NOT status IS NOT NULL").rows.size)
            assertEquals(3, fx.query("SELECT title FROM tasks WHERE status IS NOT NULL").rows.size)
        }

    /** ORDER BY: both directions, with NULL first ascending and last descending. */
    @Test
    fun orderByClause() =
        runTest {
            val (fx, _) = corpus()
            assertEquals(
                listOf("Untriaged", "Write docs", "Deploy staging", "Deploy prod"),
                fx.query("SELECT title FROM tasks ORDER BY points ASC").strings(),
            )
            assertEquals(
                listOf("Deploy prod", "Deploy staging", "Write docs", "Untriaged"),
                fx.query("SELECT title FROM tasks ORDER BY points DESC").strings(),
            )
            assertEquals(
                listOf("Deploy prod", "Deploy staging", "Untriaged", "Write docs"),
                fx.query("SELECT title FROM tasks ORDER BY title ASC").strings(),
                "no direction means ascending",
            )
        }

    /** ORDER BY over several keys applies them left to right. */
    @Test
    fun orderByMultipleKeys() =
        runTest {
            val (fx, _) = corpus()
            val result = fx.query("SELECT title FROM tasks ORDER BY status ASC, points DESC")
            assertEquals(listOf("Untriaged", "Write docs", "Deploy prod", "Deploy staging"), result.strings())
        }

    /** DISTINCT deduplicates projected rows, first occurrence winning. */
    @Test
    fun distinctClause() =
        runTest {
            val (fx, _) = corpus()
            val result = fx.query("SELECT DISTINCT status FROM tasks ORDER BY status ASC")
            assertEquals(3, result.rows.size)
            assertEquals(SqlCell.Null, result.rows.first().values[0])
        }

    /** LIMIT and OFFSET bound the final row set, after ORDER BY. */
    @Test
    fun limitAndOffsetClauses() =
        runTest {
            val (fx, _) = corpus()
            assertEquals(listOf("Deploy prod", "Deploy staging"), fx.query("SELECT title FROM tasks ORDER BY points DESC LIMIT 2").strings())
            assertEquals(listOf("Deploy staging", "Write docs"), fx.query("SELECT title FROM tasks ORDER BY points DESC LIMIT 2 OFFSET 1").strings())
            assertEquals(0, fx.query("SELECT title FROM tasks ORDER BY points DESC LIMIT 2 OFFSET 10").rows.size)
        }

    /** GROUP BY emits one row per key, in ascending key order, with the key projectable. */
    @Test
    fun groupByClause() =
        runTest {
            val (fx, _) = corpus()
            val result = fx.query("SELECT status, COUNT(*) AS n FROM tasks GROUP BY status")
            assertEquals(3, result.rows.size)
            assertEquals(SqlCell.Null, result.rows[0].values[0])
            assertEquals(listOf("done", "open"), result.column(0).drop(1).map { it.str() })
            assertEquals(listOf(1L, 1L, 2L), result.column(1).map { it.long() })
        }

    /** Each aggregate over the corpus, including its zero-row behaviour. */
    @Test
    fun aggregateFunctions() =
        runTest {
            val (fx, _) = corpus()
            val row = fx.query("SELECT COUNT(*) AS n, COUNT(points) AS c, SUM(points) AS s, AVG(points) AS a, MIN(points) AS lo, MAX(points) AS hi FROM tasks").rows.single().values
            assertEquals(4L, row[0].long())
            assertEquals(3L, row[1].long(), "COUNT(col) skips the document without points")
            assertEquals(12L, row[2].long(), "SUM over integers stays integral")
            assertEquals(4.0, row[3].dbl(), "AVG is always a double")
            assertEquals(1L, row[4].long())
            assertEquals(8L, row[5].long())

            val empty = fx.query("SELECT COUNT(*) AS n, SUM(points) AS s FROM tasks WHERE status = 'nope'").rows.single().values
            assertEquals(0L, empty[0].long())
            assertEquals(SqlCell.Null, empty[1])
        }

    /** LIKE / ILIKE and their NOT forms. */
    @Test
    fun likeClause() =
        runTest {
            val (fx, _) = corpus()
            assertEquals(2, fx.query("SELECT title FROM tasks WHERE title LIKE 'Deploy%'").rows.size)
            assertEquals(0, fx.query("SELECT title FROM tasks WHERE title LIKE 'deploy%'").rows.size)
            assertEquals(2, fx.query("SELECT title FROM tasks WHERE title ILIKE 'deploy%'").rows.size)
            assertEquals(2, fx.query("SELECT title FROM tasks WHERE title NOT LIKE 'Deploy%'").rows.size)
        }

    /** IN and NOT IN over a literal list. */
    @Test
    fun inClause() =
        runTest {
            val (fx, _) = corpus()
            assertEquals(2, fx.query("SELECT title FROM tasks WHERE title IN ('Deploy prod', 'Write docs')").rows.size)
            assertEquals(2, fx.query("SELECT title FROM tasks WHERE title NOT IN ('Deploy prod', 'Write docs')").rows.size)
            assertEquals(1, fx.query("SELECT title FROM tasks WHERE tags IN ('urgent')").rows.size)
        }

    /** BETWEEN is inclusive at both ends; NOT BETWEEN is its two-valued complement. */
    @Test
    fun betweenClause() =
        runTest {
            val (fx, _) = corpus()
            assertEquals(
                setOf("Deploy staging", "Write docs"),
                fx.query("SELECT title FROM tasks WHERE points BETWEEN 1 AND 3").strings().toSet(),
            )
            assertEquals(
                setOf("Deploy prod", "Untriaged"),
                fx.query("SELECT title FROM tasks WHERE points NOT BETWEEN 1 AND 3").strings().toSet(),
            )
        }

    /** MATCH as predicate and as score projection. */
    @Test
    fun matchClause() =
        runTest {
            val (fx, factory) = corpus()
            val hits = fx.query("SELECT kdb_id FROM tasks WHERE MATCH(tasks_text, 'deploy')")
            assertEquals(setOf(t1.toString(), t2.toString()), hits.strings().toSet())
            assertTrue(factory.store(IndexType.FULLTEXT).searchCalls > 0)

            val scored = fx.query("SELECT kdb_id, MATCH(tasks_text, 'deploy') AS score FROM tasks ORDER BY score DESC LIMIT 2")
            assertEquals(listOf(t1.toString(), t2.toString()), scored.strings())
            assertEquals(3.0, scored.column(1).first().dbl())
        }

    /** MATCH without a FULLTEXT index errors instead of falling back to a substring scan. */
    @Test
    fun matchWithoutIndexErrors() =
        runTest {
            val (fx, _) = corpus()
            assertFailsWith<SqlPlanningException> { fx.query("SELECT title FROM tasks WHERE MATCH(status, 'open')") }
        }

    /** SIMILARITY orders by the vector index's metric score. */
    @Test
    fun similarityClause() =
        runTest {
            val (fx, _) = corpus()
            val result =
                fx.query(
                    "SELECT kdb_id, SIMILARITY(embedding, ?) AS score FROM tasks ORDER BY score DESC LIMIT 5",
                    listOf(SqlParameter.VectorParam(floatArrayOf(1f, 0f))),
                )
            assertEquals(listOf(t2.toString(), t3.toString()), result.strings())
            assertEquals(0.9, result.column(1).first().dbl())
        }

    /** SIMILARITY without a VECTOR index errors. */
    @Test
    fun similarityWithoutIndexErrors() =
        runTest {
            val (fx, _) = corpus()
            assertFailsWith<SqlPlanningException> {
                fx.query("SELECT kdb_id FROM tasks ORDER BY SIMILARITY(title, [1.0, 0.0]) DESC")
            }
        }

    /** FUSE combines both arms into one ranking. */
    @Test
    fun fuseClause() =
        runTest {
            val (fx, _) = corpus()
            val result =
                fx.query(
                    "SELECT kdb_id, FUSE(MATCH(tasks_text, 'deploy'), SIMILARITY(embedding, ?), 'rrf') AS score " +
                        "FROM tasks ORDER BY score DESC LIMIT 3",
                    listOf(SqlParameter.VectorParam(floatArrayOf(1f, 0f))),
                )
            assertEquals(listOf(t2.toString(), t1.toString(), t3.toString()), result.strings())
        }

    /** Parameters bind positionally across the whole statement. */
    @Test
    fun parameterBinding() =
        runTest {
            val (fx, _) = corpus()
            val prepared = fx.engine.prepare("SELECT title FROM tasks WHERE status = ? AND points > ?", fx.ctx())
            assertEquals(2, prepared.parameterCount)
            val result =
                prepared.execute(listOf(SqlParameter.StringParam("open"), SqlParameter.IntParam(3)), fx.ctx())
            assertEquals(listOf("Deploy prod"), result.rows.map { it.values[0].str() })
        }

    /** INSERT, UPDATE and DELETE each report the documents they touched. */
    @Test
    fun dmlStatements() =
        runTest {
            val (fx, _) = corpus()
            assertEquals(1, fx.dml("INSERT INTO tasks (title, status) VALUES ('New task', 'open')").rowsAffected)
            assertEquals(5, fx.query("SELECT title FROM tasks").rows.size)
            assertEquals(1, fx.dml("UPDATE tasks SET status = 'done' WHERE title = 'New task'").rowsAffected)
            assertEquals(2, fx.query("SELECT title FROM tasks WHERE status = 'done'").rows.size)
            assertEquals(1, fx.dml("DELETE FROM tasks WHERE title = 'New task'").rowsAffected)
            assertEquals(4, fx.query("SELECT title FROM tasks").rows.size)
        }

    /** Transaction-control statements parse and are left to the session host. */
    @Test
    fun transactionControlIsRejectedByTheEngine() =
        runTest {
            val (fx, _) = corpus()
            for (sql in listOf("BEGIN", "COMMIT", "ROLLBACK")) {
                assertFailsWith<SqlPlanningException> { fx.engine.execute(sql, fx.ctx()) }
            }
        }
}
