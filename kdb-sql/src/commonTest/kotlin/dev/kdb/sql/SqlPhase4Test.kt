package dev.kdb.sql

import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.KdbTransaction
import dev.kdb.index.IndexManager
import dev.kdb.index.productionIndexManager
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.isNone
import dev.kdb.storage.StorageAdapter
import dev.kdb.storage.mem.InMemoryStorageAdapter
import dev.kdb.transaction.ConflictPolicy
import dev.kdb.transaction.TransactionResult
import dev.kdb.transaction.transactionEngine
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SqlPhase4Test {
    @Test
    fun createTableAndInsert() =
        runTest {
            val fx = emptyFixtureSuspend()
            val created =
                fx.engine.execute(
                    """CREATE TABLE users (
                    userId VARCHAR NOT NULL,
                    status VARCHAR NOT NULL
                )""",
                    QueryContext(fx.ns, KdbSchema.NONE),
                )
            assertFalse(created.appliedSchema!!.isNone)
            val inserted =
                fx.executeDml(
                    """INSERT INTO users (userId, status) VALUES ('u1', 'active')""",
                    created.appliedSchema!!,
                )
            assertEquals(1, inserted.rowsAffected)
            assertEquals(1, inserted.generatedIds.size)
        }

    @Test
    fun multiRowInsert() =
        runTest {
            val fx = emptyFixtureSuspend()
            val schema =
                fx.engine
                    .execute(
                        "CREATE TABLE t (id VARCHAR NOT NULL)",
                        QueryContext(fx.ns, KdbSchema.NONE),
                    )
                    .appliedSchema!!
            val dml =
                fx.executeDml(
                    "INSERT INTO t (id) VALUES ('a'), ('b')",
                    schema,
                )
            assertEquals(2, dml.rowsAffected)
            assertEquals(2, dml.generatedIds.size)
        }

    @Test
    fun alterTableAddColumn() =
        runTest {
            val fx = emptyFixtureSuspend()
            val base =
                fx.engine
                    .execute(
                        "CREATE TABLE t (id VARCHAR NOT NULL)",
                        QueryContext(fx.ns, KdbSchema.NONE),
                    )
                    .appliedSchema!!
            val altered =
                fx.engine.execute(
                    "ALTER TABLE t ADD COLUMN score BIGINT NOT NULL",
                    QueryContext(fx.ns, base),
                )
            assertTrue(altered.appliedSchema!!.hasField("score"))
        }

    @Test
    fun countAfterCreate() =
        runTest {
            val fx = emptyFixtureSuspend()
            val schema =
                fx.engine
                    .execute(
                        "CREATE TABLE t (id VARCHAR NOT NULL)",
                        QueryContext(fx.ns, KdbSchema.NONE),
                    )
                    .appliedSchema!!
            fx.executeDml(
                "INSERT INTO t (id) VALUES ('x')",
                schema,
            )
            val count =
                fx.engine.execute(
                    "SELECT COUNT(*) AS n FROM t",
                    QueryContext(fx.ns, schema),
                )
            assertEquals(1L, (count.rows.first().values[0] as SqlCell.LongVal).value)
        }

    private class Fixture(
        val engine: SqlEngine,
        val ns: String,
        private val dag: CommitDag,
        private val storage: StorageAdapter,
        private val manager: IndexManager,
    ) {
        suspend fun executeDml(
            sql: String,
            schema: KdbSchema,
        ): DmlResult {
            val dml = engine.executeDml(sql, QueryContext(ns, schema))
            if (dml.operations.isEmpty()) {
                return dml
            }
            val txEngine = transactionEngine(ConflictPolicy.STRICT)
            val parent = dag.head()
            val tx =
                KdbTransaction(
                    id = KdbUuid.random(),
                    baseVersion = parent,
                    operations = dml.operations,
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                )
            when (val result = txEngine.commit(tx, dag, storage, schema)) {
                is TransactionResult.Success -> {
                    if (!schema.isNone) {
                        manager.writer.applyCommit(
                            result.commit,
                            manager.registryFor(ns),
                            storage,
                            schema,
                        )
                    }
                }
                is TransactionResult.Conflict ->
                    error("transaction conflict: ${result.report.conflicts.size} operation(s)")
                is TransactionResult.SchemaError ->
                    error("schema rejection: ${result.violations.size} violation(s)")
            }
            return dml
        }
    }

    private suspend fun emptyFixtureSuspend(): Fixture {
        val ns = "app/users"
        val dag = inMemoryCommitDag(ns)
        val storage = InMemoryStorageAdapter()
        val manager = productionIndexManager(dag, storage)
        manager.bindNamespace(ns, dag)
        val engine = sqlEngine(manager, storage, dag)
        return Fixture(engine, ns, dag, storage, manager)
    }
}
