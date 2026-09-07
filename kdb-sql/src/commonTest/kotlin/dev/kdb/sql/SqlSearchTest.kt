package dev.kdb.sql

import dev.kdb.codec.KdbUuid
import dev.kdb.index.IndexType
import dev.kdb.index.RankedResult
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.SchemaField
import dev.kdb.storage.mem.InMemoryStorageAdapter
import dev.kdb.dag.inMemoryCommitDag
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * SQL half of Component 67 — search in SQL and its DDL (Layer 16 §9.1–§9.3). Rankings come from
 * [FakeSearchStore] so the assertions prove the executor consulted the index rather than the
 * documents.
 */
class SqlSearchTest {
    private val docA = KdbUuid.random()
    private val docB = KdbUuid.random()
    private val docC = KdbUuid.random()

    /** Schemaless namespace with a FULLTEXT and a VECTOR index, both answered from canned rankings. */
    private suspend fun searchable(
        textRankings: Map<String, List<RankedResult>> = mapOf("deploy staging" to ranked(docA to 2.5f, docB to 0.9f)),
        vectorRanking: List<RankedResult> = ranked(docB to 0.75f, docC to 0.25f),
    ): Pair<SqlTestRuntime, FakeSearchStoreFactory> {
        val ns = "app/tasks"
        val dag = inMemoryCommitDag(ns)
        val storage = InMemoryStorageAdapter()
        val factory = FakeSearchStoreFactory(dag, storage, textRankings, vectorRanking)
        val fx = schemalessRuntime(ns = ns, storeFactory = factory)
        fx.put("""{"title":"Deploy staging","embedding":[1.0,0.0]}""", docA)
        fx.put("""{"title":"Deploy prod","embedding":[0.0,1.0]}""", docB)
        fx.put("""{"title":"Write docs","embedding":[0.5,0.5]}""", docC)
        fx.query("CREATE INDEX tasks_text ON tasks (title WEIGHT 3, steps.text) USING FULLTEXT")
        fx.query("CREATE INDEX tasks_vec ON tasks (embedding) USING VECTOR WITH (dimensions = 2, metric = 'cosine')")
        return fx to factory
    }

    // ---------------------------------------------------------------- MATCH

    /** MATCH as a predicate is answered from the FULLTEXT index, not by scanning documents. */
    @Test
    fun matchPredicateUsesTheFullTextIndex() =
        runTest {
            val (fx, factory) = searchable()
            val result = fx.query("SELECT kdb_id FROM tasks WHERE MATCH(tasks_text, 'deploy staging')")
            assertEquals(setOf(docA.toString(), docB.toString()), result.strings().toSet())
            val store = factory.store(IndexType.FULLTEXT)
            assertTrue(store.searchCalls > 0, "the executor never consulted the full-text index")
            assertEquals("deploy staging", store.lastQuery)
        }

    /** MATCH as a projection is the BM25 score, 0 for non-hits, in a DOUBLE column. */
    @Test
    fun matchProjectionIsAScoreColumn() =
        runTest {
            val (fx, _) = searchable()
            val result =
                fx.query("SELECT kdb_id, MATCH(tasks_text, 'deploy staging') AS score FROM tasks ORDER BY kdb_id ASC")
            assertEquals("DOUBLE", result.columns[1].sqlType)
            val byId = result.rows.associate { it.values[0].str() to it.values[1].dbl() }
            assertEquals(2.5, byId[docA.toString()])
            assertEquals(0.0, byId[docC.toString()], "a non-hit scores 0")
        }

    /** A float32 score widens through its shortest round-trip: 0.9f is 0.9, not 0.8999999761581421. */
    @Test
    fun scoreWideningUsesShortestRoundTrip() =
        runTest {
            val (fx, _) = searchable()
            val result =
                fx.query("SELECT MATCH(tasks_text, 'deploy staging') AS score FROM tasks ORDER BY score DESC")
            assertEquals(0.9, result.column(0)[1].dbl())
        }

    /** MATCH against a name with no FULLTEXT index is a planning error (the substring fallback is gone). */
    @Test
    fun matchWithoutAnIndexIsAPlanningError() =
        runTest {
            val fx = schemalessRuntime()
            fx.put("""{"title":"Deploy staging"}""")
            val e =
                assertFailsWith<SqlPlanningException> {
                    fx.query("SELECT title FROM tasks WHERE MATCH(title, 'deploy')")
                }
            assertEquals("no FULLTEXT index for title", e.message)
        }

    /** The first argument of MATCH also resolves as an indexed field, not only as an index name. */
    @Test
    fun matchResolvesTheIndexByItsFirstField() =
        runTest {
            val (fx, factory) = searchable()
            fx.query("SELECT kdb_id FROM tasks WHERE MATCH(title, 'deploy staging')")
            assertTrue(factory.store(IndexType.FULLTEXT).searchCalls > 0)
        }

    /** A MATCH query may be a bound parameter. */
    @Test
    fun matchQueryFromAParameter() =
        runTest {
            val (fx, factory) = searchable()
            val result =
                fx.query(
                    "SELECT kdb_id FROM tasks WHERE MATCH(tasks_text, ?)",
                    listOf(SqlParameter.StringParam("deploy staging")),
                )
            assertEquals(2, result.rows.size)
            assertEquals("deploy staging", factory.store(IndexType.FULLTEXT).lastQuery)
        }

    /** A residual filter still applies on top of the ranking. */
    @Test
    fun residualFilterAppliesOverTheRanking() =
        runTest {
            val (fx, _) = searchable()
            val result =
                fx.query("SELECT kdb_id FROM tasks WHERE MATCH(tasks_text, 'deploy staging') AND title = 'Deploy prod'")
            assertEquals(listOf(docB.toString()), result.strings())
        }

    // ---------------------------------------------------------------- SIMILARITY

    /** SIMILARITY needs a VECTOR index on the field. */
    @Test
    fun similarityWithoutAnIndexIsAPlanningError() =
        runTest {
            val fx = schemalessRuntime()
            fx.put("""{"embedding":[1.0,0.0]}""")
            val e =
                assertFailsWith<SqlPlanningException> {
                    fx.query("SELECT kdb_id FROM tasks ORDER BY SIMILARITY(embedding, [1.0, 0.0]) DESC")
                }
            assertEquals("no VECTOR index for embedding", e.message)
        }

    /** A SIMILARITY-ordered query plans a VectorAnn lookup and orders by the metric score. */
    @Test
    fun similarityOrderedQueryPlansVectorAnn() =
        runTest {
            val (fx, factory) = searchable()
            val sql = "SELECT kdb_id, SIMILARITY(embedding, ?) AS score FROM tasks ORDER BY score DESC LIMIT 10"
            val params = listOf(SqlParameter.VectorParam(floatArrayOf(1.0f, 0.0f)))
            val plan = fx.engine.explain(sql, fx.ctx(params))
            assertTrue(plan.accessPath.contains("VectorAnn"), "expected a VectorAnn access path, got ${plan.accessPath}")

            val result = fx.query(sql, params)
            assertEquals(listOf(docB.toString(), docC.toString()), result.strings())
            assertEquals(0.75, result.column(1).first().dbl())
            assertTrue(floatArrayOf(1.0f, 0.0f).contentEquals(factory.store(IndexType.VECTOR).lastVector))
        }

    /** A vector literal works in place of a bound vector parameter. */
    @Test
    fun similarityAcceptsAVectorLiteral() =
        runTest {
            val (fx, factory) = searchable()
            fx.query("SELECT kdb_id FROM tasks ORDER BY SIMILARITY(embedding, [1.0, 0.0]) DESC LIMIT 2")
            assertTrue(floatArrayOf(1.0f, 0.0f).contentEquals(factory.store(IndexType.VECTOR).lastVector))
        }

    /** A non-vector parameter bound to SIMILARITY is a planning error, not a silent zero score. */
    @Test
    fun similarityRejectsANonVectorParameter() =
        runTest {
            val (fx, _) = searchable()
            assertFailsWith<SqlPlanningException> {
                fx.query(
                    "SELECT kdb_id FROM tasks ORDER BY SIMILARITY(embedding, ?) DESC",
                    listOf(SqlParameter.StringParam("nope")),
                )
            }
        }

    // ---------------------------------------------------------------- FUSE

    /**
     * FUSE combines both arms by reciprocal rank (k = 60). Text ranks docA, docB; vector ranks
     * docB, docC — so docB (1/61 + 1/62) outranks docA (1/61) and docC (1/62).
     */
    @Test
    fun fuseOrdersByReciprocalRankFusion() =
        runTest {
            val (fx, _) = searchable()
            val result =
                fx.query(
                    "SELECT kdb_id, FUSE(MATCH(tasks_text, 'deploy staging'), SIMILARITY(embedding, [1.0, 0.0]), 'rrf') AS score " +
                        "FROM tasks ORDER BY score DESC LIMIT 10",
                )
            assertEquals(listOf(docB.toString(), docA.toString(), docC.toString()), result.strings())
            assertTrue(result.column(1).first().dbl() > result.column(1)[1].dbl())
        }

    /** The weighted mode is accepted and produces its own ordering. */
    @Test
    fun fuseAcceptsTheWeightedMode() =
        runTest {
            val (fx, _) = searchable()
            val result =
                fx.query(
                    "SELECT kdb_id FROM tasks ORDER BY " +
                        "FUSE(MATCH(tasks_text, 'deploy staging'), SIMILARITY(embedding, [1.0, 0.0]), 'weighted') DESC",
                )
            assertEquals(3, result.rows.size)
        }

    /** An unknown fusion mode is a planning error. */
    @Test
    fun fuseRejectsAnUnknownMode() =
        runTest {
            val (fx, _) = searchable()
            assertFailsWith<SqlPlanningException> {
                fx.query(
                    "SELECT kdb_id FROM tasks ORDER BY " +
                        "FUSE(MATCH(tasks_text, 'a'), SIMILARITY(embedding, [1.0, 0.0]), 'magic') DESC",
                )
            }
        }

    // ---------------------------------------------------------------- depth rule

    /** A score-ordered LIMIT query fetches min(1000, max(50, 4·(n+m))) candidates per arm (§9.1). */
    @Test
    fun scoreOrderedLimitAppliesTheDepthRule() =
        runTest {
            val (fx, factory) = searchable()
            fx.query("SELECT kdb_id FROM tasks ORDER BY MATCH(tasks_text, 'deploy staging') DESC LIMIT 10")
            assertEquals(50, factory.store(IndexType.FULLTEXT).lastLimit, "4*(10+0) floors at 50")

            fx.query("SELECT kdb_id FROM tasks ORDER BY MATCH(tasks_text, 'deploy staging') DESC LIMIT 100 OFFSET 50")
            assertEquals(600, factory.store(IndexType.FULLTEXT).lastLimit, "4*(100+50)")

            fx.query("SELECT kdb_id FROM tasks ORDER BY MATCH(tasks_text, 'deploy staging') DESC LIMIT 500")
            assertEquals(1000, factory.store(IndexType.FULLTEXT).lastLimit, "capped at 1000")
        }

    /** Without a LIMIT the query takes every hit. */
    @Test
    fun scoreOrderedQueryWithoutLimitTakesEveryHit() =
        runTest {
            val (fx, factory) = searchable()
            fx.query("SELECT kdb_id FROM tasks ORDER BY MATCH(tasks_text, 'deploy staging') DESC")
            assertEquals(Int.MAX_VALUE, factory.store(IndexType.FULLTEXT).lastLimit)
        }

    // ---------------------------------------------------------------- range strictness / EXPLAIN

    /** `>` must not be planned as `>=`: the boundary row is excluded (§9.3). */
    @Test
    fun rangeStrictnessExcludesTheBoundary() =
        runTest {
            val fx =
                schemaRuntime(
                    listOf(
                        SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                        SchemaField("rank", KdbFieldType.Int64Type, required = true, indexed = true),
                    ),
                )
            fx.put("""{"userId":"a","rank":1}""")
            fx.put("""{"userId":"b","rank":2}""")
            fx.put("""{"userId":"c","rank":3}""")

            assertEquals(listOf("c"), fx.query("SELECT userId FROM users WHERE rank > 2").strings())
            assertEquals(setOf("b", "c"), fx.query("SELECT userId FROM users WHERE rank >= 2").strings().toSet())
            assertEquals(listOf("a"), fx.query("SELECT userId FROM users WHERE rank < 2").strings())
            assertEquals(setOf("a", "b"), fx.query("SELECT userId FROM users WHERE rank <= 2").strings().toSet())

            val plan = fx.explain("SELECT userId FROM users WHERE rank > 2")
            assertTrue(plan.accessPath.contains("Range(>"), "expected a strict range, got ${plan.accessPath}")
        }

    /** EXPLAIN names the chosen access path so tests can assert an index was used. */
    @Test
    fun explainNamesTheAccessPath() =
        runTest {
            val fx =
                schemaRuntime(listOf(SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true)))
            fx.put("""{"userId":"u1"}""")
            assertTrue(fx.explain("SELECT userId FROM users WHERE userId = 'u1'").accessPath.startsWith("IndexScan(userId"))
            assertTrue(fx.explain("SELECT userId FROM users").accessPath.startsWith("FullTableScan("))
        }

    /** A score-ordered plan names its arms. */
    @Test
    fun explainNamesTheScoredAccessPath() =
        runTest {
            val (fx, _) = searchable()
            val path = fx.explain("SELECT kdb_id FROM tasks ORDER BY MATCH(tasks_text, 'deploy staging') DESC LIMIT 5").accessPath
            assertTrue(path.startsWith("ScoredScan("), path)
            assertTrue(path.contains("FullText"), path)
        }

    // ---------------------------------------------------------------- DDL (§9.2)

    /** CREATE INDEX records the SQL name, the field weights and the vector options on the descriptor. */
    @Test
    fun createIndexFillsDescriptorOptions() =
        runTest {
            val (_, factory) = searchable()
            val text = factory.descriptors.single { it.type == IndexType.FULLTEXT }
            assertEquals("tasks_text", text.options["index_name"])
            assertEquals("title=3,steps.text=1", text.options["weights"])
            assertEquals(listOf("title", "steps.text"), text.fields)
            assertEquals("title", text.fieldName, "the first field is the registry key")

            val vector = factory.descriptors.single { it.type == IndexType.VECTOR }
            assertEquals("tasks_vec", vector.options["index_name"])
            assertEquals("2", vector.options["dimensions"])
            assertEquals("cosine", vector.options["metric"])
        }

    /** Every documented vector option is carried through. */
    @Test
    fun createVectorIndexCarriesEveryOption() =
        runTest {
            val ns = "app/tasks"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val factory = FakeSearchStoreFactory(dag, storage)
            val fx = schemalessRuntime(ns = ns, storeFactory = factory)
            fx.query(
                "CREATE INDEX v ON tasks (embedding) USING VECTOR " +
                    "WITH (dimensions = 768, metric = 'l2', m = 16, ef_construction = 200, ef_search = 64)",
            )
            val d = factory.descriptors.single { it.type == IndexType.VECTOR }
            assertEquals("768", d.options["dimensions"])
            assertEquals("l2", d.options["metric"])
            assertEquals("16", d.options["m"])
            assertEquals("200", d.options["ef_construction"])
            assertEquals("64", d.options["ef_search"])
        }

    /** A VECTOR index without `dimensions`, or with an unknown option or metric, is rejected. */
    @Test
    fun createVectorIndexValidatesItsOptions() =
        runTest {
            val fx = schemalessRuntime()
            assertFailsWith<SqlPlanningException> { fx.query("CREATE INDEX v ON tasks (e) USING VECTOR") }
            assertFailsWith<SqlPlanningException> {
                fx.query("CREATE INDEX v ON tasks (e) USING VECTOR WITH (dimensions = 8, nope = 1)")
            }
            assertFailsWith<SqlPlanningException> {
                fx.query("CREATE INDEX v ON tasks (e) USING VECTOR WITH (dimensions = 8, metric = 'manhattan')")
            }
        }

    /** FULLTEXT covers several fields; HASH and BTREE still take exactly one declared field. */
    @Test
    fun multiFieldIndexesAreOnlyForFullText() =
        runTest {
            val fx = schemalessRuntime()
            fx.query("CREATE INDEX t1 ON tasks (title, body, steps.text) USING FULLTEXT")
            assertFailsWith<SqlPlanningException> { fx.query("CREATE INDEX h ON tasks (a, b) USING HASH") }
        }

    /** FULLTEXT/VECTOR are allowed on a schemaless namespace; HASH/BTREE need declared fields (§9.2). */
    @Test
    fun hashAndBtreeRequireDeclaredFields() =
        runTest {
            val schemaless = schemalessRuntime()
            schemaless.query("CREATE INDEX t ON tasks (title) USING FULLTEXT")
            assertFailsWith<SqlPlanningException> { schemaless.query("CREATE INDEX h ON tasks (title) USING HASH") }

            val declared =
                schemaRuntime(listOf(SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true)))
            declared.query("CREATE INDEX h ON users (userId) USING HASH")
            assertFailsWith<SqlPlanningException> { declared.query("CREATE INDEX h2 ON users (nope) USING HASH") }
        }

    /** `CREATE UNIQUE INDEX` (prefix form) parses as well as the trailing `UNIQUE`. */
    @Test
    fun uniqueIndexPrefixAndSuffixFormsBothParse() {
        val parser = defaultSqlParser()
        val prefix = parser.parse("CREATE UNIQUE INDEX i ON t (a) USING HASH") as SqlStatement.CreateIndex
        assertTrue(prefix.ddl.unique)
        val suffix = parser.parse("CREATE INDEX i ON t (a) USING HASH UNIQUE") as SqlStatement.CreateIndex
        assertTrue(suffix.ddl.unique)
        assertTrue(!(parser.parse("CREATE INDEX i ON t (a) USING HASH") as SqlStatement.CreateIndex).ddl.unique)
    }

    /** `WEIGHT n` per field parses, defaulting to 1, and is rejected for non-FULLTEXT indexes. */
    @Test
    fun weightClauseParsesPerField() {
        val parser = defaultSqlParser()
        val stmt =
            parser.parse("CREATE INDEX i ON t (title WEIGHT 3, description, tags WEIGHT 2) USING FULLTEXT")
                as SqlStatement.CreateIndex
        assertEquals(mapOf("title" to 3, "tags" to 2), stmt.ddl.weights)
        assertEquals(listOf("title", "description", "tags"), stmt.ddl.fields)
        assertFailsWith<SqlParseException> { parser.parse("CREATE INDEX i ON t (a WEIGHT 2) USING HASH") }
    }

    /** `UNIQUE (a, b)` table constraints reach the built schema (§9.2/§9.6). */
    @Test
    fun createTableCarriesUniqueConstraints() =
        runTest {
            val fx = schemalessRuntime(ns = "app/orders")
            val result =
                fx.engine.execute(
                    "CREATE TABLE orders (tenant VARCHAR NOT NULL, code VARCHAR NOT NULL, sku VARCHAR UNIQUE, UNIQUE (tenant, code))",
                    fx.ctx(),
                )
            val schema = assertNotNull(result.appliedSchema)
            assertEquals(listOf(listOf("tenant", "code")), schema.uniqueConstraints.map { it.fields })
            assertTrue(schema.fieldsByName.getValue("sku").unique, "column-level UNIQUE survives")
            assertTrue(schema.uniqueTuples().contains(listOf("tenant", "code")))
        }

    /** A UNIQUE constraint naming an undeclared column fails the statement. */
    @Test
    fun createTableRejectsUnknownUniqueConstraintField() =
        runTest {
            val fx = schemalessRuntime(ns = "app/orders")
            assertFailsWith<SqlPlanningException> {
                fx.engine.execute("CREATE TABLE orders (a VARCHAR NOT NULL, UNIQUE (a, missing))", fx.ctx())
            }
        }

    /** A vector parameter round-trips over the wire encoding under tag "v". */
    @Test
    fun vectorParameterRoundTripsOverTheWire() {
        val original = listOf(SqlParameter.VectorParam(floatArrayOf(0.1f, -0.5f, 2f)), SqlParameter.IntParam(3))
        val decoded = decodeSqlParameters(encodeSqlParameters(original)!!)
        val vector = decoded[0] as SqlParameter.VectorParam
        assertTrue(floatArrayOf(0.1f, -0.5f, 2f).contentEquals(vector.asFloatArray()))
        assertEquals(3L, (decoded[1] as SqlParameter.IntParam).value)
    }

    /** DROP INDEX removes the SQL index, and MATCH against it then fails to plan again. */
    @Test
    fun dropIndexRemovesTheSearchPath() =
        runTest {
            val (fx, _) = searchable()
            fx.query("DROP INDEX tasks_text ON tasks")
            assertFailsWith<SqlPlanningException> {
                fx.query("SELECT kdb_id FROM tasks WHERE MATCH(tasks_text, 'deploy staging')")
            }
        }
}
