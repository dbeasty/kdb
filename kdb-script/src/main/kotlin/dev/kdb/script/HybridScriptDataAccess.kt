package dev.kdb.script

import dev.kdb.auth.AuthAction
import dev.kdb.auth.Authorizer
import dev.kdb.auth.Principal
import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.error.ConflictException
import dev.kdb.index.IndexManager
import dev.kdb.query.hybrid.HybridQueryEngine
import dev.kdb.query.hybrid.HybridQueryRequest
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.isNone
import dev.kdb.sql.SqlParameter
import dev.kdb.sql.defaultSqlParser
import dev.kdb.sql.isDmlStatement
import dev.kdb.storage.StorageAdapter
import dev.kdb.transaction.TransactionEngine
import dev.kdb.transaction.TransactionResult
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Default [ScriptDataAccess]: every call maps to ordinary SQL against `HybridQueryEngine`
 * (`_doc`/`kdb_id`, same as the wire protocol's `SqlExec`), so no new storage path is added —
 * see Component 32 spec §2. Writes are staged (`deferCommit = true`) into one [KdbTransaction]
 * per procedure invocation and committed atomically by [commitPending] once the script body
 * returns; on any failure the caller must simply discard this instance (nothing was committed).
 */
public class HybridScriptDataAccess(
    private val principal: Principal,
    private val namespaceId: String,
    private val hybrid: HybridQueryEngine,
    private val dag: CommitDag,
    private val storage: StorageAdapter,
    private val schema: KdbSchema,
    private val txEngine: TransactionEngine,
    private val indexManager: IndexManager,
    private val authorizer: Authorizer,
    private val limits: ProcLimits,
    private val baseVersion: KdbHash,
    private val callProcedure: suspend (String, String) -> String,
) : ScriptDataAccess {
    private val guard = Mutex()
    private var callCount = 0
    private val logBuffer = StringBuilder()
    private val loggedLines = mutableListOf<String>()
    private val pendingOps = mutableListOf<KdbOp>()

    override val logs: List<String> get() = loggedLines.toList()

    private suspend fun chargeCall() {
        guard.withLock {
            callCount += 1
            if (callCount > limits.maxHostCalls) {
                throw ProcException.ResourceLimitExceeded(
                    "exceeded max host calls per invocation (${limits.maxHostCalls})",
                )
            }
        }
    }

    private suspend fun authorize(readOnly: Boolean) {
        try {
            authorizer.authorize(principal, AuthAction.SqlExec(namespaceId, readOnly = readOnly))
        } catch (e: Exception) {
            throw ProcException.Denied(e.message ?: "denied: ${principal.id} on $namespaceId")
        }
    }

    override suspend fun get(id: String): String? {
        chargeCall()
        authorize(readOnly = true)
        val result =
            hybrid.execute(
                "SELECT _doc FROM $namespaceId WHERE kdb_id = ?",
                HybridQueryRequest(
                    namespaceId = namespaceId,
                    schema = schema,
                    parameters = listOf(SqlParameter.StringParam(id)),
                ),
            )
        val raw = singleDocJsonCell(result.result) ?: return null
        return withKdbId(raw, id)
    }

    override suspend fun put(docJson: String): String {
        chargeCall()
        authorize(readOnly = false)
        val existingId = docIdFromJson(docJson)
        val body = withoutKdbId(docJson)
        val sql: String
        val params: List<SqlParameter>
        if (existingId != null) {
            sql = "UPDATE $namespaceId SET _doc = ? WHERE kdb_id = ?"
            params = listOf(SqlParameter.StringParam(body), SqlParameter.StringParam(existingId))
        } else {
            sql = "INSERT INTO $namespaceId (_doc) VALUES (?)"
            params = listOf(SqlParameter.StringParam(body))
        }
        val result =
            hybrid.execute(
                sql,
                HybridQueryRequest(
                    namespaceId = namespaceId,
                    schema = schema,
                    parameters = params,
                    deferCommit = true,
                    transactionBase = baseVersion,
                    bufferOps = { ops -> guard.withLock { pendingOps.addAll(ops) } },
                ),
            )
        return existingId ?: result.result.generatedIds.firstOrNull() ?: ""
    }

    override suspend fun delete(id: String): Boolean {
        chargeCall()
        authorize(readOnly = false)
        val result =
            hybrid.execute(
                "DELETE FROM $namespaceId WHERE kdb_id = ?",
                HybridQueryRequest(
                    namespaceId = namespaceId,
                    schema = schema,
                    parameters = listOf(SqlParameter.StringParam(id)),
                    deferCommit = true,
                    transactionBase = baseVersion,
                    bufferOps = { ops -> guard.withLock { pendingOps.addAll(ops) } },
                ),
            )
        return result.result.rowsAffected > 0
    }

    override suspend fun query(
        sql: String,
        paramsJson: String,
    ): String {
        chargeCall()
        val stmt = defaultSqlParser().parse(sql.trim())
        if (isDmlStatement(stmt)) {
            throw ProcException.ScriptRuntimeError(
                "kdb.query does not accept write statements; use kdb.put/kdb.delete",
            )
        }
        authorize(readOnly = true)
        val result =
            hybrid.execute(
                sql,
                HybridQueryRequest(
                    namespaceId = namespaceId,
                    schema = schema,
                    parameters = parseJsonArrayParams(paramsJson),
                ),
            )
        return queryResultToJsonArray(result.result).toString()
    }

    override fun log(message: String) {
        if (logBuffer.length + message.length > limits.maxLogBytes) {
            throw ProcException.ResourceLimitExceeded("kdb.log exceeded ${limits.maxLogBytes} bytes")
        }
        logBuffer.append(message)
        loggedLines += message
    }

    override suspend fun callProc(
        name: String,
        argsJson: String,
    ): String {
        chargeCall()
        return callProcedure(name, argsJson)
    }

    /** Commits everything staged by `put`/`delete` as one transaction. Returns `null` if nothing was staged. */
    public suspend fun commitPending(): KdbHash? {
        val ops = guard.withLock { pendingOps.toList() }
        if (ops.isEmpty()) return null
        val tx =
            KdbTransaction(
                id = KdbUuid.random(),
                baseVersion = baseVersion,
                operations = ops,
                timestamp = KdbTimestamp.now(),
                authorNodeId = KdbUuid.random(),
            )
        return when (val result = txEngine.commit(tx, dag, storage, schema)) {
            is TransactionResult.Success -> {
                if (!schema.isNone) {
                    indexManager.writer.applyCommit(
                        result.commit,
                        indexManager.registryFor(namespaceId),
                        storage,
                        schema,
                    )
                }
                result.commit.hash
            }
            is TransactionResult.Conflict ->
                throw ConflictException(
                    "stored procedure transaction conflict: ${result.report.conflicts.size} operation(s)",
                    result.report,
                )
            is TransactionResult.SchemaError ->
                throw ProcException.ScriptRuntimeError(
                    "schema rejection: ${result.violations.size} violation(s)",
                )
        }
    }
}
