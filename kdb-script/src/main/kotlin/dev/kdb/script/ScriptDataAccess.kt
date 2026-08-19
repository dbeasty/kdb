package dev.kdb.script

/**
 * The data-access surface bound into a running script as the `kdb` object (Component 32 §4).
 * Every method independently re-authorizes its own action against the invoking principal
 * before touching storage — see [HybridScriptDataAccess] — so being allowed to *invoke* a
 * procedure never implies being allowed to *do* what it attempts (spec §5.1/§5.2).
 *
 * All payloads cross this boundary as JSON text rather than typed objects: the Graal binding
 * layer only ever exchanges strings with the JS side (see `GraalProcedureRuntime`'s prelude),
 * so no host object is ever reflectively reachable from script code.
 */
public interface ScriptDataAccess {
    /** Returns the document's JSON, or `null` if no document has that `kdb_id`. */
    public suspend fun get(id: String): String?

    /** Inserts (if `docJson` has no `kdb_id`) or updates (if it does). Returns the `kdb_id`. */
    public suspend fun put(docJson: String): String

    /** Returns whether a document was deleted. */
    public suspend fun delete(id: String): Boolean

    /** Read-only SQL; `paramsJson` is a plain JSON array. Returns a JSON array of row objects. */
    public suspend fun query(
        sql: String,
        paramsJson: String,
    ): String

    public fun log(message: String)

    /** Recurses into another procedure under the *same calling principal*. */
    public suspend fun callProc(
        name: String,
        argsJson: String,
    ): String

    /** Ops staged by `put`/`delete` during this invocation, not yet committed. */
    public val logs: List<String>
}
