package dev.kdb.script

import dev.kdb.auth.AuthAction
import dev.kdb.auth.Authorizer
import dev.kdb.auth.Principal
import dev.kdb.dag.CommitDag
import dev.kdb.index.IndexManager
import dev.kdb.query.hybrid.HybridQueryEngine
import dev.kdb.schema.KdbSchema
import dev.kdb.storage.StorageAdapter
import dev.kdb.transaction.TransactionEngine
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.graalvm.polyglot.Context
import org.graalvm.polyglot.HostAccess
import org.graalvm.polyglot.PolyglotException
import org.graalvm.polyglot.ResourceLimits
import org.graalvm.polyglot.Value
import org.graalvm.polyglot.proxy.ProxyExecutable

/**
 * [ProcedureRuntime] backed by a locked-down GraalVM JS `Context` per invocation. See Component
 * 32 spec §5.4 for the exact sandbox boundary this configures, and §7.1 for a worked example of
 * the whole define/execute flow this class sits at the bottom of.
 *
 * The `kdb` binding never crosses via [HostAccess] / Java reflection: every function is a
 * [ProxyExecutable] exchanging plain strings, and `HostAccess.NONE` plus
 * `allowHostClassLookup { false }` mean the script has no way to reach any other JVM class even
 * if it tried (no `Java.type`, no reflective escape from a proxy).
 */
internal class GraalProcedureRuntime(
    private val registry: ProcedureRegistry,
    private val hybrid: HybridQueryEngine,
    private val dag: CommitDag,
    private val storage: StorageAdapter,
    private val schema: KdbSchema,
    private val txEngine: TransactionEngine,
    private val indexManager: IndexManager,
    private val authorizer: Authorizer,
    private val maxCallDepth: Int,
) : ProcedureRuntime {
    override suspend fun invoke(
        principal: Principal,
        namespaceId: String,
        name: String,
        argsJson: String,
        limits: ProcLimits,
    ): ProcResult = invokeInternal(principal, namespaceId, name, argsJson, limits, depth = 0)

    private suspend fun invokeInternal(
        principal: Principal,
        namespaceId: String,
        name: String,
        argsJson: String,
        limits: ProcLimits,
        depth: Int,
    ): ProcResult {
        if (depth >= maxCallDepth) {
            throw ProcException.ResourceLimitExceeded("kdb.callProc recursion depth exceeded ($maxCallDepth)")
        }
        val def = registry.get(namespaceId, name) ?: throw ProcException.NotFound(namespaceId, name)

        // Gate 1 (spec §5.1): may this principal invoke this procedure at all?
        try {
            authorizer.authorize(principal, AuthAction.ProcExec(namespaceId, name, readOnly = false))
        } catch (e: Exception) {
            throw ProcException.Denied(e.message ?: "principal ${principal.id} may not exec $namespaceId/$name")
        }

        val access =
            HybridScriptDataAccess(
                principal = principal,
                namespaceId = namespaceId,
                hybrid = hybrid,
                dag = dag,
                storage = storage,
                schema = schema,
                txEngine = txEngine,
                indexManager = indexManager,
                authorizer = authorizer,
                limits = limits,
                baseVersion = dag.head(),
                callProcedure = { calleeName, calleeArgs ->
                    invokeInternal(principal, namespaceId, calleeName, calleeArgs, limits, depth + 1).value
                },
            )

        val returned = runSandboxed(def.source, argsJson, access, limits)
        access.commitPending()
        return ProcResult(value = returned, logs = access.logs)
    }

    private suspend fun runSandboxed(
        source: String,
        argsJson: String,
        access: ScriptDataAccess,
        limits: ProcLimits,
    ): String {
        val context = buildContext(limits)
        return try {
            coroutineScope {
                // Kotlin coroutine cancellation is cooperative and cannot interrupt a
                // non-suspending, CPU-bound guest loop (`while(true){}`) running on the
                // Dispatchers.IO thread below. The actual interrupt mechanism is GraalVM's
                // own `Context.close(true)`, which is safe to call from another thread and
                // forcibly stops guest execution at its next Truffle safepoint - that is what
                // enforces `limits.wallClockMillis`, not the coroutine timeout by itself.
                val watchdog =
                    launch {
                        delay(limits.wallClockMillis)
                        runCatching { context.close(true) }
                    }
                val result =
                    withContext(Dispatchers.IO) {
                        evalOnContext(context, source, argsJson, access, limits.wallClockMillis)
                    }
                watchdog.cancel()
                result
            }
        } finally {
            runCatching { context.close(true) }
        }
    }

    private fun evalOnContext(
        context: Context,
        source: String,
        argsJson: String,
        access: ScriptDataAccess,
        wallClockMillis: Long,
    ): String {
        val bindings = context.getBindings("js")
        bindings.putMember("__kdb_get", ProxyExecutable { args -> access.blockingGet(args[0].asString()) })
        bindings.putMember(
            "__kdb_put",
            ProxyExecutable { args -> runBlocking { access.put(args[0].asString()) } },
        )
        bindings.putMember(
            "__kdb_delete",
            ProxyExecutable { args -> runBlocking { access.delete(args[0].asString()) } },
        )
        bindings.putMember(
            "__kdb_query",
            ProxyExecutable { args -> runBlocking { access.query(args[0].asString(), args[1].asString()) } },
        )
        bindings.putMember(
            "__kdb_call_proc",
            ProxyExecutable { args -> runBlocking { access.callProc(args[0].asString(), args[1].asString()) } },
        )
        bindings.putMember("__kdb_log", ProxyExecutable { args -> access.log(args[0].asString()); null })
        bindings.putMember("__args_json", argsJson)

        try {
            context.eval("js", KDB_PRELUDE)
            context.eval("js", source)
            val result: Value = context.eval("js", "JSON.stringify(main(args))")
            return if (result.isNull) "null" else result.asString()
        } catch (e: PolyglotException) {
            // A ProcException thrown from inside a ProxyExecutable (e.g. HybridScriptDataAccess's
            // per-call authorize()) crosses back into Java wrapped as a host exception - unwrap it
            // so callers see the original Denied/ResourceLimitExceeded/etc., not a generic wrapper.
            if (e.isHostException() && e.asHostException() is ProcException) {
                throw e.asHostException() as ProcException
            }
            if (e.isCancelled || e.isInterrupted) {
                throw ProcException.Timeout(wallClockMillis)
            }
            if (e.isResourceExhausted) {
                throw ProcException.ResourceLimitExceeded(e.message ?: "resource limit exceeded")
            }
            if (e.message?.contains("main is not defined") == true) {
                throw ProcException.CompileError("procedure source must define a top-level function main(args)", e)
            }
            if (e.isSyntaxError) {
                throw ProcException.CompileError(e.message ?: "syntax error", e)
            }
            throw ProcException.ScriptRuntimeError(e.message ?: e.toString(), e)
        }
    }

    private fun ScriptDataAccess.blockingGet(id: String): String? = runBlocking { get(id) }

    private fun buildContext(limits: ProcLimits): Context =
        Context
            .newBuilder("js")
            .allowHostAccess(HostAccess.NONE)
            .allowHostClassLookup { false }
            .allowIO(false)
            .allowCreateProcess(false)
            .allowCreateThread(false)
            .allowNativeAccess(false)
            .allowPolyglotAccess(org.graalvm.polyglot.PolyglotAccess.NONE)
            .option("engine.WarnInterpreterOnly", "false")
            .resourceLimits(
                ResourceLimits
                    .newBuilder()
                    .statementLimit(limits.maxStatements, null)
                    .build(),
            ).build()

    private companion object {
        /**
         * Builds the JS-visible `kdb` object from the low-level `__kdb_*` proxies bound as
         * globals by [evalOnContext]. The `__kdb_*` names stay reachable afterward (the closures
         * below capture them by reference, evaluated at call time, not at prelude-eval time) —
         * that's harmless: calling `__kdb_get` directly carries identical authorization to
         * calling `kdb.get`, so hiding it would be cosmetic only, and deleting it here would
         * break these very closures.
         */
        const val KDB_PRELUDE = """
            (function () {
                globalThis.args = JSON.parse(__args_json);
                globalThis.kdb = {
                    get: function (id) {
                        var s = __kdb_get(id);
                        return s === null || s === undefined ? null : JSON.parse(s);
                    },
                    put: function (doc) {
                        return __kdb_put(JSON.stringify(doc));
                    },
                    delete: function (id) {
                        return __kdb_delete(id);
                    },
                    query: function (sql, params) {
                        return JSON.parse(__kdb_query(sql, JSON.stringify(params || [])));
                    },
                    log: function (msg) {
                        __kdb_log(String(msg));
                    },
                    callProc: function (name, procArgs) {
                        return JSON.parse(__kdb_call_proc(name, JSON.stringify(procArgs || {})));
                    },
                };
            })();
        """
    }
}
