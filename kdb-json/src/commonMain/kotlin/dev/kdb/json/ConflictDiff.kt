package dev.kdb.json

/**
 * Three-way field diff, for telling a conflict resolver which fields actually conflict. The
 * mirror of Go's `kdb/json/diff.go`, field for field: the two produce the same list for the same
 * three documents, which is what lets one stored procedure run on either runtime.
 *
 * A resolver handed two whole documents would have to work out itself which fields the two sides
 * disagree about, and every resolver would do it slightly differently. Doing it once here keeps
 * that answer the same everywhere - and, because an in-merge resolver's result is covered by a
 * merge commit's hash, the same on every node.
 *
 * The walk recurses into objects and compares arrays whole: an array's identity is its order, so
 * "element 2 changed" is not a fact two nodes can be relied on to derive alike from two arrays of
 * different lengths.
 */
public object ChangeKind {
    /** Only the local side moved away from the base. */
    public const val LOCAL: String = "local"

    /** Only the remote side moved away from the base. */
    public const val REMOTE: String = "remote"

    /** Both sides moved away from the base, to different values - the set a resolver exists for. */
    public const val BOTH: String = "both"

    /**
     * Both sides moved away from the base and landed on the same value. Not a conflict, but a
     * resolver is told so that a rule like "keep the field the other side also set" can see it.
     */
    public const val SAME: String = "same"
}

/**
 * One field's three values. A field both sides left at the base is not reported.
 *
 * Absent and null are different: a field deleted on one side has `present` false there, while a
 * field set to null has `present` true and a [JsonValue.JNull]. A resolver that conflated them
 * would resurrect deleted fields.
 *
 * @property path an RFC 6901 JSON Pointer from the document root.
 */
public data class FieldChange(
    val path: String,
    val base: JsonValue?,
    val local: JsonValue?,
    val remote: JsonValue?,
    val basePresent: Boolean,
    val localPresent: Boolean,
    val remotePresent: Boolean,
    val kind: String,
)

/**
 * Lists the fields on which [local] and [remote] differ, measured against [base]. A null value
 * means the whole document is absent on that side (a delete).
 *
 * The order is by path compared as UTF-8 bytes. Kotlin's own `String.compareTo` is UTF-16 and
 * orders some astral characters differently from every byte-wise comparison, so it is not used
 * here - otherwise two runtimes would hand the same procedure the same fields in different
 * orders, and a procedure that reads `fields[0]` would decide differently on each.
 */
public fun diffConflict(
    base: JsonValue?,
    local: JsonValue?,
    remote: JsonValue?,
): List<FieldChange> {
    val out = mutableListOf<FieldChange>()
    walkConflict("", base, local, remote, base != null, local != null, remote != null, out)
    return out.sortedWith { a, b -> compareUtf8(a.path, b.path) }
}

/** Compares two strings by their UTF-8 bytes. */
internal fun compareUtf8(
    a: String,
    b: String,
): Int {
    val ba = a.encodeToByteArray()
    val bb = b.encodeToByteArray()
    val n = minOf(ba.size, bb.size)
    for (i in 0 until n) {
        val d = (ba[i].toInt() and 0xff) - (bb[i].toInt() and 0xff)
        if (d != 0) return d
    }
    return ba.size - bb.size
}

private fun walkConflict(
    path: String,
    base: JsonValue?,
    local: JsonValue?,
    remote: JsonValue?,
    basePresent: Boolean,
    localPresent: Boolean,
    remotePresent: Boolean,
    out: MutableList<FieldChange>,
) {
    if (sameValue(local, localPresent, remote, remotePresent) && sameValue(base, basePresent, local, localPresent)) return
    val lo = local as? JsonValue.JObject
    val ro = remote as? JsonValue.JObject
    if (localPresent && remotePresent && lo != null && ro != null) {
        val bo = base as? JsonValue.JObject
        if (!basePresent || bo != null) {
            val baseHere = basePresent && bo != null
            for (k in unionKeys(bo, baseHere, lo, ro)) {
                walkConflict(
                    path + "/" + escapePointer(k),
                    if (baseHere) bo!!.fields[k] else null,
                    lo.fields[k],
                    ro.fields[k],
                    baseHere && bo!!.fields.containsKey(k),
                    lo.fields.containsKey(k),
                    ro.fields.containsKey(k),
                    out,
                )
            }
            return
        }
    }
    out +=
        FieldChange(
            path = path, base = base, local = local, remote = remote,
            basePresent = basePresent, localPresent = localPresent, remotePresent = remotePresent,
            kind = classify(base, basePresent, local, localPresent, remote, remotePresent),
        )
}

private fun classify(
    base: JsonValue?,
    bp: Boolean,
    local: JsonValue?,
    lp: Boolean,
    remote: JsonValue?,
    rp: Boolean,
): String {
    val localChanged = !sameValue(base, bp, local, lp)
    val remoteChanged = !sameValue(base, bp, remote, rp)
    return when {
        localChanged && remoteChanged && sameValue(local, lp, remote, rp) -> ChangeKind.SAME
        localChanged && remoteChanged -> ChangeKind.BOTH
        localChanged -> ChangeKind.LOCAL
        else -> ChangeKind.REMOTE
    }
}

/**
 * Every key any side has, in a fixed order: the local side's, then the remote side's additions,
 * then the base's. Sorting happens once at the end, over full paths.
 */
private fun unionKeys(
    base: JsonValue.JObject?,
    basePresent: Boolean,
    local: JsonValue.JObject,
    remote: JsonValue.JObject,
): List<String> {
    val keys = LinkedHashSet<String>()
    keys += local.fields.keys
    keys += remote.fields.keys
    if (basePresent && base != null) keys += base.fields.keys
    return keys.toList()
}

/**
 * Compares two possibly-absent values. Two absent values are the same; an absent value never
 * equals a present one, including a present null.
 */
private fun sameValue(
    a: JsonValue?,
    aPresent: Boolean,
    b: JsonValue?,
    bPresent: Boolean,
): Boolean {
    if (aPresent != bPresent) return false
    if (!aPresent) return true
    return canonicalText(a!!) == canonicalText(b!!)
}

/**
 * Renders a value with every object's keys sorted, for comparison: two documents that differ only
 * in the order their fields were written are the same document.
 */
public fun canonicalText(v: JsonValue): String =
    when (v) {
        is JsonValue.JObject ->
            v.fields.keys
                .sortedWith { a, b -> compareUtf8(a, b) }
                .joinToString(",", "{", "}") { k -> JsonValue.JString(k).toJsonString() + ":" + canonicalText(v.fields[k]!!) }
        is JsonValue.JArray -> v.elements.joinToString(",", "[", "]") { canonicalText(it) }
        else -> v.toJsonString()
    }

/** Encodes one JSON Pointer reference token (RFC 6901 §3). */
private fun escapePointer(k: String): String = k.replace("~", "~0").replace("/", "~1")
