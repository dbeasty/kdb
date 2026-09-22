package dev.kdb.script

import dev.kdb.document.KdbDocument
import dev.kdb.transaction.ConflictResolver
import dev.kdb.transaction.DocumentConflict

/**
 * A stored procedure as a local transaction's conflict resolver, for
 * [dev.kdb.transaction.ConflictPolicy.CUSTOM]. The counterpart of Go's `script.ConflictResolver`,
 * running the same source.
 *
 * A local commit conflicts when the document it writes has changed since the version the
 * transaction was based on - one process, one document, no second node to agree with - so the
 * engine asks its resolver for the body to store, and that is all it can accept. Take, doc and
 * defer mean something here; fork and delete belong to a merge, which has a commit to put the
 * extra document or the deletion in, so asking for one here is refused rather than quietly
 * collapsed into storing one side.
 */
public class ProcedureConflictResolver(
    private val source: String,
    private val runtime: PureProcedureRuntime = PureProcedureRuntime(),
) : ConflictResolver {
    override suspend fun resolve(conflict: DocumentConflict): KdbDocument? {
        val args =
            conflictArgs(
                ConflictInput(
                    docId = conflict.docId.toString(),
                    base = conflict.baseDoc?.json,
                    local = conflict.existingDoc?.json,
                    remote = conflict.incomingDoc?.json,
                ),
            )
        return when (val decision = parseDecision(runtime.call(source, args))) {
            // The engine reads null as "report the conflict", which is what defer means here.
            is ProcDecision.Defer -> null
            is ProcDecision.Take -> {
                val side = listOf(conflict.existingDoc, conflict.incomingDoc)[decision.side]
                    ?: throw ProcException.ScriptRuntimeError(
                        "the chosen side of ${conflict.docId} is a delete, which a local commit's resolver cannot return",
                    )
                KdbDocument(conflict.docId, side.json)
            }
            is ProcDecision.Doc -> KdbDocument(conflict.docId, decision.json)
            else -> throw ProcException.ScriptRuntimeError(
                "only take, doc and defer mean anything when resolving a local commit",
            )
        }
    }
}
