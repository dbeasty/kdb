package dev.kdb.script

import dev.kdb.auth.AuthAction
import dev.kdb.auth.Authorizer
import dev.kdb.auth.KdbAuthorizationException
import dev.kdb.auth.Principal
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.index.compositeIndexStoreFactory
import dev.kdb.index.productionIndexManager
import dev.kdb.policy.inMemoryNamespacePolicyRegistry
import dev.kdb.query.hybrid.HybridQueryRequest
import dev.kdb.schema.KdbSchema
import dev.kdb.sql.sqlEngine
import dev.kdb.storage.mem.InMemoryStorageAdapter
import dev.kdb.transaction.ConflictPolicy
import dev.kdb.transaction.transactionEngine
import dev.kdb.query.hybrid.hybridQueryEngine
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Exercises the whole define/execute flow from Component 32 spec §7.1 against real (in-memory)
 * engines: a procedure that reads, writes, and queries `orders`, with per-call authorization
 * (spec §5.1) verified by a fake [Authorizer] that only allows reads.
 */
class GraalProcedureRuntimeTest {
    private val ns = "orders"

    private class FakeAuthorizer(
        private val allowedKinds: Set<String>,
    ) : Authorizer {
        override suspend fun authorize(
            principal: Principal,
            action: AuthAction,
        ) {
            val kind =
                when (action) {
                    is AuthAction.SqlExec -> if (action.readOnly) "read" else "write"
                    is AuthAction.ProcExec -> "proc"
                    is AuthAction.ProcManage -> "proc-manage"
                    else -> "other"
                }
            if (kind !in allowedKinds) {
                throw KdbAuthorizationException("principal ${principal.id} lacks $kind on ${namespaceOf(action)}")
            }
        }

        private fun namespaceOf(action: AuthAction): String =
            when (action) {
                is AuthAction.SqlExec -> action.namespace
                is AuthAction.ProcExec -> action.namespace
                is AuthAction.ProcManage -> action.namespace
                else -> "?"
            }
    }

    private fun harness(allowedKinds: Set<String>) =
        runBlocking {
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val indexManager = productionIndexManager(dag, storage)
            indexManager.bindNamespace(ns, dag)
            val policies = inMemoryNamespacePolicyRegistry()
            val sql = sqlEngine(indexManager, storage, dag)
            val hybrid = hybridQueryEngine(sql, dag, policies, indexManager, storage)
            val txEngine = transactionEngine(ConflictPolicy.STRICT)
            val registry = inMemoryProcedureRegistry()
            val runtime =
                graalProcedureRuntime(
                    registry = registry,
                    hybrid = hybrid,
                    dag = dag,
                    storage = storage,
                    schema = KdbSchema.NONE,
                    txEngine = txEngine,
                    indexManager = indexManager,
                    authorizer = FakeAuthorizer(allowedKinds),
                )
            Harness(dag, storage, hybrid, registry, runtime)
        }

    private data class Harness(
        val dag: dev.kdb.dag.CommitDag,
        val storage: dev.kdb.storage.StorageAdapter,
        val hybrid: dev.kdb.query.hybrid.HybridQueryEngine,
        val registry: ProcedureRegistry,
        val runtime: ProcedureRuntime,
    )

    private val shipOrderSource =
        """
        function main(args) {
          const doc = kdb.get(args.id);
          if (!doc) throw new Error("order " + args.id + " not found");
          kdb.put(Object.assign({}, doc, { status: "shipped" }));
          const rows = kdb.query("SELECT _doc FROM $ns", []);
          kdb.log("shipped " + args.id + "; " + rows.length + " total docs");
          return { ok: true, id: args.id };
        }
        """.trimIndent()

    @Test
    fun defineThenExecute_writesAndCommitsAtomically() {
        runBlocking {
            val h = harness(allowedKinds = setOf("read", "write", "proc"))
            h.registry.put(ProcedureDefinition(namespaceId = ns, name = "shipOrder", source = shipOrderSource))

            val insertResult =
                h.hybrid.execute(
                    "INSERT INTO $ns (_doc) VALUES ('{\"status\":\"pending\"}')",
                    HybridQueryRequest(namespaceId = ns, schema = KdbSchema.NONE),
                )
            val docId = insertResult.result.generatedIds.single()

            val principal = Principal(id = "writer", roles = setOf("writer"))
            val result =
                h.runtime.invoke(
                    principal,
                    ns,
                    "shipOrder",
                    """{"id":"$docId"}""",
                )

            assertTrue(result.value.contains("\"ok\":true"))
            assertEquals(listOf("shipped $docId; 1 total docs"), result.logs)

            val after =
                h.hybrid.execute(
                    "SELECT _doc FROM $ns WHERE kdb_id = ?",
                    HybridQueryRequest(
                        namespaceId = ns,
                        schema = KdbSchema.NONE,
                        parameters = listOf(dev.kdb.sql.SqlParameter.StringParam(docId)),
                    ),
                )
            assertTrue(singleDocJsonCell(after.result)!!.contains("\"status\":\"shipped\""))
        }
    }

    @Test
    fun invocationAllowed_butWriteDenied_rollsBackAndThrows() {
        runBlocking {
            // Allowed to invoke the procedure and to read, but not to write - mirrors spec
            // §5.1/§5.2: being allowed to call a procedure never implies being allowed to do
            // what it attempts. The per-call authorize() inside kdb.put must independently deny.
            val h = harness(allowedKinds = setOf("read", "proc"))
            h.registry.put(ProcedureDefinition(namespaceId = ns, name = "shipOrder", source = shipOrderSource))

            val insertResult =
                h.hybrid.execute(
                    "INSERT INTO $ns (_doc) VALUES ('{\"status\":\"pending\"}')",
                    HybridQueryRequest(namespaceId = ns, schema = KdbSchema.NONE),
                )
            val docId = insertResult.result.generatedIds.single()
            val headBefore = h.dag.head()

            val principal = Principal(id = "reader", roles = setOf("reader"))
            assertFailsWith<ProcException.Denied> {
                h.runtime.invoke(principal, ns, "shipOrder", """{"id":"$docId"}""")
            }

            assertEquals(headBefore, h.dag.head())
        }
    }

    @Test
    fun invocationItselfDenied_scriptNeverRuns() {
        runBlocking {
            val h = harness(allowedKinds = setOf("read", "write"))
            h.registry.put(ProcedureDefinition(namespaceId = ns, name = "shipOrder", source = shipOrderSource))
            val principal = Principal(id = "outsider", roles = setOf("outsider"))
            assertFailsWith<ProcException.Denied> {
                h.runtime.invoke(principal, ns, "shipOrder", """{"id":"whatever"}""")
            }
        }
    }

    @Test
    fun unknownProcedure_throwsNotFound() {
        runBlocking {
            val h = harness(allowedKinds = setOf("read", "write", "proc"))
            assertFailsWith<ProcException.NotFound> {
                h.runtime.invoke(Principal(id = "writer"), ns, "doesNotExist", "{}")
            }
        }
    }

    @Test
    fun sandboxHasNoHostClassAccess() {
        runBlocking {
            val h = harness(allowedKinds = setOf("read", "write", "proc"))
            h.registry.put(
                ProcedureDefinition(
                    namespaceId = ns,
                    name = "escape",
                    source = "function main(args) { return Java.type('java.lang.System').exit(1); }",
                ),
            )
            assertFailsWith<ProcException.ScriptRuntimeError> {
                h.runtime.invoke(Principal(id = "writer"), ns, "escape", "{}")
            }
        }
    }

    @Test
    fun infiniteLoop_isInterruptedByWallClockTimeout() {
        runBlocking {
            val h = harness(allowedKinds = setOf("read", "write", "proc"))
            h.registry.put(
                ProcedureDefinition(
                    namespaceId = ns,
                    name = "spin",
                    source = "function main(args) { while (true) {} }",
                ),
            )
            assertFailsWith<ProcException.Timeout> {
                h.runtime.invoke(Principal(id = "writer"), ns, "spin", "{}", ProcLimits(wallClockMillis = 300))
            }
        }
    }
}
