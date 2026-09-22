package dev.kdb.script

import dev.kdb.json.JsonValue
import dev.kdb.json.diffConflict

/**
 * The conflict-resolution calling convention, shared with Go's `kdb/script`. The two build the
 * same argument from the same conflict and read the same result, which is what lets one procedure
 * source run on either runtime; `go/testdata/golden/conflict_corpus` holds the cases that say so.
 *
 * A procedure is handed one conflicting document: both sides in full, the value they last agreed
 * on, who wrote each side, and the list of fields that actually differ. It returns one of five
 * decisions.
 *
 * "local" and "remote" name the two sides, but which is which depends on where the call happens.
 * In a merge they are canonical sides - side 0 is the merge's first parent's - so both nodes hand
 * the procedure the same two sides in the same order. A procedure that must not care can say
 * side0/side1 instead.
 */

/** The commit that produced one side of a conflict. */
public data class ProcOrigin(
    val nodeId: String = "",
    val commit: String = "",
    val timestampMicros: Long = 0,
)

/**
 * One conflicting document as the caller knows it. A null body means that side deleted the
 * document (or, for [base], that it did not exist when the sides parted).
 */
public data class ConflictInput(
    val docId: String,
    val base: String?,
    val local: String?,
    val remote: String?,
    val localOrigin: ProcOrigin = ProcOrigin(),
    val remoteOrigin: ProcOrigin = ProcOrigin(),
)

/** What a procedure decided. */
public sealed class ProcDecision {
    /** No opinion; the caller moves on to whatever is next. */
    public data object Defer : ProcDecision()

    /** Keep one side whole, discarding the other. [side] is 0 or 1. */
    public data class Take(val side: Int) : ProcDecision()

    /** Store a document the procedure built. */
    public data class Doc(val json: String) : ProcDecision()

    /** Keep [side] at this document's id and write the other to a new document. */
    public data class Fork(val side: Int) : ProcDecision()

    /** The document should not survive. */
    public data object Delete : ProcDecision()
}

/** Renders [input] as the JSON a procedure receives. */
public fun conflictArgs(input: ConflictInput): String {
    fun parse(
        body: String?,
        side: String,
    ): JsonValue? =
        body?.let {
            try {
                JsonValue.fromJsonString(it)
            } catch (e: Exception) {
                throw ProcException.ScriptRuntimeError("the $side side of ${input.docId} is not valid JSON: ${e.message}", e)
            }
        }

    val base = parse(input.base, "base")
    val local = parse(input.local, "local")
    val remote = parse(input.remote, "remote")
    fun raw(v: JsonValue?): String = v?.toJsonString() ?: "null"
    fun origin(o: ProcOrigin): String =
        """{"nodeId":${JsonValue.JString(o.nodeId).toJsonString()},""" +
            """"commit":${JsonValue.JString(o.commit).toJsonString()},""" +
            """"timestampMicros":${o.timestampMicros}}"""

    fun present(
        b: Boolean,
        l: Boolean,
        r: Boolean,
    ): String = """{"base":$b,"local":$l,"remote":$r,"side0":$l,"side1":$r}"""

    val fields =
        diffConflict(base, local, remote).joinToString(",") { f ->
            buildString {
                append("""{"path":${JsonValue.JString(f.path).toJsonString()},"kind":"${f.kind}"""")
                if (f.basePresent) append(""","base":${raw(f.base)}""")
                if (f.localPresent) append(""","local":${raw(f.local)}""")
                if (f.remotePresent) append(""","remote":${raw(f.remote)}""")
                append(""","present":${present(f.basePresent, f.localPresent, f.remotePresent)}}""")
            }
        }
    return buildString {
        append("""{"docId":${JsonValue.JString(input.docId).toJsonString()}""")
        append(""","base":${raw(base)},"local":${raw(local)},"remote":${raw(remote)}""")
        append(""","side0":${raw(local)},"side1":${raw(remote)}""")
        append(""","present":${present(base != null, local != null, remote != null)}""")
        append(""","origins":{"local":${origin(input.localOrigin)},"remote":${origin(input.remoteOrigin)}""")
        append(""","side0":${origin(input.localOrigin)},"side1":${origin(input.remoteOrigin)}}""")
        append(""","fields":[$fields]}""")
    }
}

/**
 * Validates a procedure's returned JSON.
 *
 * Strict on purpose: a result that could be read two ways is a result two nodes might read
 * differently. Exactly one decision must be present, a side must be named, and a returned
 * document must be a JSON object.
 */
public fun parseDecision(text: String): ProcDecision {
    val root =
        try {
            JsonValue.fromJsonString(text)
        } catch (e: Exception) {
            throw ProcException.ScriptRuntimeError("procedure returned invalid JSON: ${e.message}", e)
        }
    val obj = root as? JsonValue.JObject ?: throw invalid("a procedure returns an object, not $text")
    val known = setOf("take", "doc", "fork", "delete", "defer")
    obj.fields.keys.firstOrNull { it !in known }?.let { throw invalid("unknown key \"$it\"; use take, doc, fork, delete or defer") }
    if (obj.fields.size != 1) {
        throw invalid("name exactly one of take, doc, fork, delete or defer (found ${obj.fields.size})")
    }
    val (key, value) = obj.fields.entries.first()
    return when (key) {
        "take" -> ProcDecision.Take(side(value))
        "doc" -> {
            val doc = value as? JsonValue.JObject ?: throw invalid("doc must be an object")
            ProcDecision.Doc(doc.toJsonString())
        }
        "fork" -> {
            val fork = value as? JsonValue.JObject ?: throw invalid("fork must be an object naming the side to keep")
            ProcDecision.Fork(side(fork.fields["keep"] ?: throw invalid("fork needs keep")))
        }
        "delete" -> {
            if (value != JsonValue.JBool(true)) throw invalid("delete must be true; to decide nothing, return {defer:true}")
            ProcDecision.Delete
        }
        else -> {
            if (value != JsonValue.JBool(true)) throw invalid("defer must be true; to decide nothing, return {defer:true}")
            ProcDecision.Defer
        }
    }
}

private fun side(v: JsonValue): Int =
    when ((v as? JsonValue.JString)?.value) {
        "local", "side0" -> 0
        "remote", "side1" -> 1
        else -> throw invalid("${v.toJsonString()} is not a side; use local/remote or side0/side1")
    }

private fun invalid(detail: String): ProcException = ProcException.ScriptRuntimeError("procedure returned an invalid result: $detail")
