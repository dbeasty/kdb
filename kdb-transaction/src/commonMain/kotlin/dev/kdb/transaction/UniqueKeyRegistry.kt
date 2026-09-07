package dev.kdb.transaction

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.error.FieldViolation
import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException
import dev.kdb.error.ViolationType
import dev.kdb.json.JsonValue
import dev.kdb.json.kdbJsonGet
import dev.kdb.schema.KdbSchema
import dev.kdb.storage.StorageAdapter
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * One claimed value tuple of one unique constraint in one namespace (Layer 16 §9.6). [fields] is the
 * constraint's ordered field tuple (a single-field `UNIQUE` is the 1-tuple case); [values] holds the
 * canonical JSON of each part, so `1` and `1.0` collide and strings compare byte-wise.
 */
public data class UniqueKey(
    val namespaceId: String,
    val fields: List<String>,
    val values: List<String>,
) {
    override fun toString(): String = "$namespaceId.(${fields.joinToString(",")})=[${values.joinToString(",")}]"
}

/**
 * Thrown (or surfaced through [TransactionResult.SchemaError] with [ViolationType.UNIQUE_CONSTRAINT])
 * when a write claims a value tuple another live document already holds. Carries both document ids
 * because "who already has it" is what a client needs and what a bare message never says. Same error
 * class family as a schema violation (Go: UNIQUE_VIOLATION is a SchemaViolationError subtype).
 */
public class UniqueConstraintViolationException(
    message: String,
    public val namespaceId: String,
    public val fields: List<String>,
    public val values: List<String>,
    public val ownerDocId: KdbUuid?,
    public val docId: KdbUuid,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.SCHEMA_VIOLATION

    public companion object {
        /** Rebuilds the exception from a violation produced by the engine's unique-key planning, or
         * null if [violation] is not a unique violation. Lets callers that only see a
         * [TransactionResult.SchemaError] (kdb-embed's commitViaEngine) surface the typed error. */
        public fun fromViolation(
            namespaceId: String,
            violation: OperationViolation,
        ): UniqueConstraintViolationException? {
            val fv = violation.violations.firstOrNull { it.violationType == ViolationType.UNIQUE_CONSTRAINT } ?: return null
            val docId =
                when (val op = violation.op) {
                    is KdbOp.Write -> op.docId
                    is KdbOp.Delete -> op.docId
                    else -> KdbUuid(0, 0)
                }
            val owner = OWNER_PATTERN.find(fv.detail)?.groupValues?.get(1)?.let { runCatching { KdbUuid.fromString(it) }.getOrNull() }
            val values = VALUES_PATTERN.find(fv.detail)?.groupValues?.get(1)?.let { listOf(it) } ?: emptyList()
            return UniqueConstraintViolationException(
                "unique constraint violated on $namespaceId.(${fv.fieldName}): ${fv.detail} (attempted by $docId)",
                namespaceId,
                fv.fieldName.split(','),
                values,
                owner,
                docId,
            )
        }

        private val OWNER_PATTERN = Regex("held by document ([0-9a-fA-F-]{36})")
        private val VALUES_PATTERN = Regex("^value (\\[.*\\]) already")
    }
}

/**
 * The authoritative owner map for every unique constraint value tuple in a runtime:
 * `(namespace, fields, values) -> the single live document holding it`. Mirrors Go's
 * `transaction.UniqueKeyRegistry`, generalised to compound tuples.
 *
 * Ordinary conflict detection is content-addressed and per-document; two clients creating two
 * *different* documents with the same email never trip it. The registry answers "is this value already
 * spoken for" and is consulted and mutated only from inside the engine's commit, which the server
 * serialises per namespace (WriteCoordinator), so check and claim cannot interleave.
 *
 * Derived state: rebuilt from the document tree at open ([rebuild]) and whenever a commit changes the
 * schema, never persisted.
 */
public class UniqueKeyRegistry {
    private val mutex = Mutex()
    private val owners = mutableMapOf<UniqueKey, KdbUuid>()

    public suspend fun owner(key: UniqueKey): KdbUuid? = mutex.withLock { owners[key] }

    public suspend fun size(): Int = mutex.withLock { owners.size }

    /** Retracts then claims as one step, so a value moving between documents in one transaction never
     * transiently frees its key. A retraction naming a different owner than the current one is ignored -
     * a stale retraction can never free somebody else's key. */
    public suspend fun apply(
        retract: Map<UniqueKey, KdbUuid>,
        claim: Map<UniqueKey, KdbUuid>,
    ) {
        mutex.withLock {
            for ((key, owner) in retract) {
                if (owners[key] == owner) owners.remove(key)
            }
            owners.putAll(claim)
        }
    }

    public suspend fun reset() {
        mutex.withLock { owners.clear() }
    }

    /** Repopulates from every document in [namespaceId] at [treeHash] under [schema]. A duplicate found
     * during the scan throws [UniqueConstraintViolationException] rather than silently picking a winner:
     * data on disk already violates the constraint and hiding that would make it permanent. */
    public suspend fun rebuild(
        namespaceId: String,
        storage: StorageAdapter,
        treeHash: KdbHash,
        schema: KdbSchema,
    ) {
        val fresh = scanOwners(namespaceId, storage, treeHash, schema)
        mutex.withLock {
            owners.clear()
            owners.putAll(fresh)
        }
    }

    internal suspend fun replaceWith(other: UniqueKeyRegistry) {
        val snapshot = other.mutex.withLock { other.owners.toMap() }
        mutex.withLock {
            owners.clear()
            owners.putAll(snapshot)
        }
    }

    internal companion object {
        internal suspend fun scanOwners(
            namespaceId: String,
            storage: StorageAdapter,
            treeHash: KdbHash,
            schema: KdbSchema,
        ): Map<UniqueKey, KdbUuid> {
            val fresh = mutableMapOf<UniqueKey, KdbUuid>()
            if (!schema.hasUniqueConstraints()) return fresh
            storage.scanDocuments(namespaceId, treeHash, 256) { batch ->
                for (doc in batch) {
                    for (key in uniqueKeysFor(namespaceId, schema, doc)) {
                        val existing = fresh[key]
                        if (existing != null && existing != doc.id) {
                            throw UniqueConstraintViolationException(
                                "unique constraint violated on $key: value already held by document $existing (attempted by ${doc.id})",
                                namespaceId,
                                key.fields,
                                key.values,
                                existing,
                                doc.id,
                            )
                        }
                        fresh[key] = doc.id
                    }
                }
            }
            return fresh
        }
    }
}

/**
 * The keys [doc] claims under [schema]: one per unique tuple whose every part is present and non-null
 * (sparse semantics - a document missing any part of a tuple claims nothing for it, matching SQL where
 * NULLs never collide). A document whose JSON cannot be evaluated claims nothing.
 */
public fun uniqueKeysFor(
    namespaceId: String,
    schema: KdbSchema,
    doc: KdbDocument,
): List<UniqueKey> {
    if (!schema.hasUniqueConstraints()) return emptyList()
    val out = mutableListOf<UniqueKey>()
    tuples@ for (fields in schema.uniqueTuples()) {
        val values = ArrayList<String>(fields.size)
        for (field in fields) {
            val raw =
                try {
                    kdbJsonGet(doc.json, "$.$field")
                } catch (_: Throwable) {
                    null
                }
            if (raw == null || raw is JsonValue.JNull) continue@tuples
            values += canonicalJson(raw)
        }
        out += UniqueKey(namespaceId, fields, values)
    }
    return out
}

/** Canonical JSON for registry comparison: object keys sorted, integral doubles rendered as integers
 * (so `1` and `1.0` collide, as Go's encoding/json re-marshal makes them), strings escaped byte-wise. */
internal fun canonicalJson(v: JsonValue): String =
    when (v) {
        is JsonValue.JNull -> "null"
        is JsonValue.JBool -> v.value.toString()
        is JsonValue.JInt -> v.value.toString()
        is JsonValue.JNumber -> {
            val d = v.value
            if (d.isFinite() && d == kotlin.math.floor(d) && kotlin.math.abs(d) < 1e15) d.toLong().toString() else d.toString()
        }
        is JsonValue.JString -> v.toJsonString()
        is JsonValue.JArray -> v.elements.joinToString(",", "[", "]") { canonicalJson(it) }
        is JsonValue.JObject ->
            v.fields.entries
                .sortedBy { it.key }
                .joinToString(",", "{", "}") { JsonValue.JString(it.key).toJsonString() + ":" + canonicalJson(it.value) }
    }

/** One transaction's net effect on the registry, resolved before anything is applied. */
internal class UniquePlan(
    val retract: MutableMap<UniqueKey, KdbUuid> = mutableMapOf(),
    val claim: MutableMap<UniqueKey, KdbUuid> = mutableMapOf(),
)

/**
 * Resolves the transaction's effect on [registry] and reports violations without mutating anything.
 * Runs against [targetTreeHash] - the tree the transaction lands on - so a stale-based transaction is
 * still checked against reality. A claim is legal when nobody holds the key, when this same document
 * holds it, or when the holder releases it in this same transaction; two ops in one transaction
 * claiming the same key is a violation (atomicity does not launder a self-collision).
 */
internal suspend fun planUniqueKeys(
    transaction: KdbTransaction,
    namespaceId: String,
    storage: StorageAdapter,
    targetTreeHash: KdbHash,
    schema: KdbSchema,
    registry: UniqueKeyRegistry?,
    projectedWrites: Map<Int, KdbDocument>,
): Pair<UniquePlan, List<OperationViolation>> {
    val plan = UniquePlan()
    if (registry == null || !schema.hasUniqueConstraints()) return plan to emptyList()

    val releasedBy = mutableMapOf<UniqueKey, KdbUuid>()
    for (op in transaction.operations) {
        val docId =
            when (op) {
                is KdbOp.Write -> op.docId
                is KdbOp.Delete -> op.docId
                else -> continue
            }
        val existing = storage.getDocument(namespaceId, docId, targetTreeHash) ?: continue
        for (key in uniqueKeysFor(namespaceId, schema, existing)) {
            releasedBy[key] = docId
            plan.retract[key] = docId
        }
    }

    val violations = mutableListOf<OperationViolation>()
    for ((index, op) in transaction.operations.withIndex()) {
        if (op !is KdbOp.Write) continue
        val doc = projectedWrites[index] ?: continue
        for (key in uniqueKeysFor(namespaceId, schema, doc)) {
            val claimant = plan.claim[key]
            if (claimant != null && claimant != op.docId) {
                violations += uniqueViolation(index, op, key, claimant)
                continue
            }
            val owner = registry.owner(key)
            when {
                owner == null -> Unit
                owner == op.docId -> Unit
                releasedBy[key] == owner -> Unit
                else -> {
                    violations += uniqueViolation(index, op, key, owner)
                    continue
                }
            }
            plan.claim[key] = op.docId
            if (plan.retract[key] == op.docId) plan.retract.remove(key)
        }
    }
    return plan to violations
}

private fun uniqueViolation(
    index: Int,
    op: KdbOp,
    key: UniqueKey,
    owner: KdbUuid,
): OperationViolation =
    OperationViolation(
        opIndex = index,
        op = op,
        violations =
            listOf(
                FieldViolation(
                    fieldName = key.fields.joinToString(","),
                    violationType = ViolationType.UNIQUE_CONSTRAINT,
                    detail = "value [${key.values.joinToString(",")}] already held by document $owner",
                ),
            ),
    )
