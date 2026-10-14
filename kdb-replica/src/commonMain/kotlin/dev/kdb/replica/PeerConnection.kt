package dev.kdb.replica

import dev.kdb.compression.ZstdCompression
import dev.kdb.stream.WireConnection
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put

/** The host answered a request with PEER_ERROR. */
public class RemoteException(
    public val code: String,
    public val remoteMessage: String,
    public val namespace: String,
) : RuntimeException("kdb replica: peer refused: $code: $remoteMessage")

/** The host broke the protocol, or the connection ended. */
public class ProtocolException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

public const val SYNC_PROTOCOL_VERSION: Int = 2

/** Limits the host enforces (go/kdb/peersync/repair.go, refs.go). */
public const val MAX_TREE_NODE_PREFIXES: Int = 4096
public const val MAX_OBJECT_FETCH_ITEMS: Int = 1024
public const val DEFAULT_TREE_ENTRY_LIMIT: Int = 32
public const val DEFAULT_PAGE_BYTES: Int = 4 shl 20

/** The most a pack's commits may inflate to (go/kdb/wire maxPackPayload). */
private const val MAX_PACK_PAYLOAD = 256 shl 20

/** A namespace's refs as the host reports them. */
public data class NamespaceRefs(
    val namespace: String,
    val branches: Map<String, String>,
    val historyFloorHex: String = "",
    val shallow: List<String> = emptyList(),
    /** "" both directions, "pull" or "push" only. */
    val access: String = "",
)

/** go/kdb/error/payloads.go ConflictReport. */
public data class ConflictReport(
    val transactionId: String,
    val baseHash: String,
    val targetHash: String,
    val conflicts: List<ConflictItem>,
)

public data class ConflictItem(
    val documentId: String,
    val operationType: String,
    val localDoc: String? = null,
    val incomingDoc: String? = null,
    val actualContentHash: String? = null,
)

public data class SnapshotPage(
    val commit: ReplicaCommit,
    val docs: List<ReplicaDocument>,
    val next: String,
    val done: Boolean,
    val total: Int,
)

public data class SquashHead(
    val commit: ReplicaCommit?,
    val descends: Boolean,
    val backlog: Long,
    val refused: String,
)

public data class WireTreeNode(
    val prefix: String,
    val hash: String,
    val children: List<String>,
    val hasEntries: Boolean,
    val entries: Map<String, String>,
)

public data class ProjectWriteResult(
    /** "applied", "conflict" or "refused". */
    val outcome: String,
    val commitHex: String,
    val reason: String,
    val current: List<CurrentDoc>,
)

public data class RefUpdateAck(
    val outcome: String,
    val headHex: String,
    val stored: Int,
    val conflict: ConflictReport?,
)

/** Opens the byte transport to a host URL: WebSocket by default (kdb-transport-ws). */
public typealias PeerConnector = suspend (url: String) -> WireConnection

/**
 * A peer sync v2 connection, from the client side: the same frames and messages a Go node's v2
 * client sends (go/kdb/peersync/v2_client.go, go/kdb/wire/sync_v2_ops.go).
 *
 * Two things differ from the client protocol kdb-wire's codec speaks:
 * - v2 is Go-only on the host side, so `[]byte` fields are standard base64 strings, not the
 *   integer arrays Kotlin's v1 DTOs use;
 * - every request gets exactly one reply, and a failure comes back as PEER_ERROR (0x25), never
 *   as silence. The one exception is the watch: after its reply, REFS_CHANGED frames keep coming
 *   under the REFS_WATCH's correlation id, unasked.
 */
public class PeerConnection private constructor(
    private val transport: WireConnection,
    private val timeoutMs: Long,
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val lock = Mutex()
    private val pending = HashMap<Int, CompletableDeferred<SyncFrame>>()
    private var next = 1
    private var watch: Pair<Int, (List<NamespaceRefs>) -> Unit>? = null
    private val closeHandlers = ArrayList<(Throwable) -> Unit>()

    @kotlin.concurrent.Volatile
    private var closedCause: Throwable? = null
    private var reader: Job? = null

    public var caps: List<String> = emptyList()
        private set
    public var remoteNodeId: String = ""
        private set

    /** The refs as of the hello, or as of the last [refreshRefs]. */
    @kotlin.concurrent.Volatile
    public var refs: List<NamespaceRefs> = emptyList()
        private set

    public companion object {
        /** Dials [url] and completes the SYNC_HELLO handshake. */
        public suspend fun open(
            url: String,
            nodeId: String,
            token: String?,
            namespaces: List<String>,
            connect: PeerConnector,
            timeoutMs: Long = 30_000,
        ): PeerConnection {
            val conn = PeerConnection(connect(url), timeoutMs)
            conn.startReader()
            try {
                val hello =
                    buildJsonObject {
                        put("nodeId", nodeId)
                        put("protocol", SYNC_PROTOCOL_VERSION)
                        // zstd-pack: kdb-compression decodes zstd, so packs may come compressed.
                        // Pushes go uncompressed; the host accepts both.
                        put("capabilities", stringArray(listOf("snapshot", "repair", "squash", "filter", "writeback", "watch", "zstd-pack")))
                        put("namespaces", stringArray(namespaces))
                        if (token != null) put("token", token)
                    }
                val ack = conn.request(SyncMsg.SYNC_HELLO, hello, "syncHelloAck")
                if (!ack.bool("accepted")) throw RemoteException("REFUSED", ack.str("reason").ifEmpty { "hello refused" }, "")
                conn.caps = ack.strings("capabilities")
                conn.remoteNodeId = ack.str("nodeId")
                conn.refs = ack.objects("refs").map(::refsFrom)
                return conn
            } catch (e: Throwable) {
                conn.close()
                throw e
            }
        }
    }

    private fun startReader() {
        reader =
            scope.launch {
                var cause: Throwable = ProtocolException("kdb replica: connection closed")
                try {
                    transport.incoming().collect { onFrame(it) }
                } catch (e: Throwable) {
                    cause = e as? ProtocolException ?: ProtocolException("kdb replica: connection failed: ${e.message}", e)
                }
                fail(cause)
            }
    }

    private suspend fun onFrame(bytes: ByteArray) {
        val frame = decodeSyncFrame(bytes)
        val (waiter, w) =
            lock.withLock {
                pending.remove(frame.correlationId) to watch
            }
        if (waiter != null) {
            waiter.complete(frame)
            return
        }
        // An unasked REFS_CHANGED, under the watch's correlation id.
        if (w != null && frame.correlationId == w.first && frame.kind == "refsChanged") {
            val refs = (frame.envelope["refsChanged"] as? JsonObject)?.objects("refs").orEmpty().map(::refsFrom)
            try {
                w.second(refs)
            } catch (_: Exception) {
                // the listener's failure is its own
            }
        }
    }

    private suspend fun fail(err: Throwable) {
        val (waiters, handlers) =
            lock.withLock {
                if (closedCause != null) return
                closedCause = err
                val ws = pending.values.toList()
                pending.clear()
                ws to closeHandlers.toList()
            }
        for (w in waiters) w.completeExceptionally(err)
        for (h in handlers) {
            try {
                h(err)
            } catch (_: Exception) {
            }
        }
    }

    /** Whether the connection has ended. */
    public val closed: Boolean get() = closedCause != null

    /** Called once when the connection ends, for whatever reason. */
    public suspend fun onClose(handler: (Throwable) -> Unit) {
        val already = lock.withLock { closedCause ?: run { closeHandlers.add(handler); null } }
        if (already != null) handler(already)
    }

    /**
     * Sends one request and returns the body of its reply, which must be of kind [expect]. When
     * [watcher] is given, it is registered under this request's correlation id before the request
     * leaves, so no report can arrive before it is in place.
     */
    internal suspend fun request(
        code: Int,
        body: JsonObject,
        expect: String,
        watcher: ((List<NamespaceRefs>) -> Unit)? = null,
    ): JsonObject {
        val kind = SyncMsg.requestKinds[code] ?: throw ProtocolException("kdb replica: no request kind for 0x${code.toString(16)}")
        val waiter = CompletableDeferred<SyncFrame>()
        val id =
            lock.withLock {
                closedCause?.let { throw it }
                val id = next++
                pending[id] = waiter
                if (watcher != null) watch = id to watcher
                id
            }
        try {
            transport.send(encodeSyncFrame(code, id, envelope(kind, body)))
        } catch (e: Throwable) {
            lock.withLock { pending.remove(id) }
            throw ProtocolException("kdb replica: send failed: ${e.message}", e)
        }
        val frame =
            withTimeoutOrNull(timeoutMs) { waiter.await() }
                ?: run {
                    lock.withLock { pending.remove(id) }
                    throw ProtocolException("kdb replica: no reply to $kind within $timeoutMs ms")
                }
        if (frame.kind == "peerError") {
            val pe = frame.envelope["peerError"] as? JsonObject ?: JsonObject(emptyMap())
            throw RemoteException(pe.str("code").ifEmpty { "UNKNOWN" }, pe.str("message"), pe.str("namespace"))
        }
        if (frame.kind != expect) throw ProtocolException("kdb replica: expected $expect, got ${frame.kind}")
        return frame.envelope[expect] as? JsonObject ?: JsonObject(emptyMap())
    }

    public suspend fun close() {
        fail(ProtocolException("kdb replica: connection closed"))
        try {
            transport.close()
        } catch (_: Exception) {
        }
        scope.cancel()
    }

    public fun has(cap: String): Boolean = cap in caps

    // --- typed requests ----------------------------------------------------------------------

    /** REFS_REQUEST: the refs of [namespaces] now; also replaces [refs]. */
    public suspend fun refreshRefs(namespaces: List<String>): List<NamespaceRefs> {
        val r = request(SyncMsg.REFS_REQUEST, buildJsonObject { put("namespaces", stringArray(namespaces)) }, "refsResult")
        return r.objects("refs").map(::refsFrom).also { refs = it }
    }

    /**
     * Watches [namespaces] (REFS_WATCH): returns their refs now, then calls [onChange] with the
     * refs of those that move, until the connection ends. Replaces any earlier watch on this
     * connection. [onChange] runs on the connection's reader, so it must not block.
     */
    public suspend fun watch(namespaces: List<String>, onChange: (List<NamespaceRefs>) -> Unit): List<NamespaceRefs> {
        val r = request(SyncMsg.REFS_WATCH, buildJsonObject { put("namespaces", stringArray(namespaces)) }, "refsChanged", onChange)
        return r.objects("refs").map(::refsFrom)
    }

    public suspend fun fetch(
        namespace: String,
        wants: List<String>,
        haves: List<String>,
        maxBytes: Int = DEFAULT_PAGE_BYTES,
    ): Pair<List<ReplicaCommit>, Boolean> {
        val r =
            request(
                SyncMsg.FETCH_REQUEST,
                buildJsonObject {
                    put("namespace", namespace)
                    put("wants", stringArray(wants))
                    put("haves", stringArray(haves))
                    put("maxBytes", maxBytes)
                },
                "packPage",
            )
        if (r.objects("stubs").isNotEmpty()) {
            throw ProtocolException("kdb replica: the peer sent archived commit stubs, which a replica cannot apply")
        }
        var bytes = fromBase64(r.strOrNull("commitsPayload"))
        if (r.bool("commitsZstd")) bytes = ZstdCompression.decompress(bytes, MAX_PACK_PAYLOAD)
        return decodeCommitList(bytes) to r.bool("done")
    }

    public suspend fun snapshot(namespace: String, atHex: String, after: String, maxBytes: Int = DEFAULT_PAGE_BYTES): SnapshotPage {
        val r =
            request(
                SyncMsg.SNAPSHOT_FETCH,
                buildJsonObject {
                    put("namespace", namespace)
                    put("atHex", atHex)
                    put("after", after)
                    put("maxBytes", maxBytes)
                },
                "snapshotPage",
            )
        val commits = decodeCommitList(fromBase64(r.strOrNull("commitPayload")))
        if (commits.size != 1) throw ProtocolException("kdb replica: a snapshot page must carry exactly one commit")
        return SnapshotPage(
            commit = commits[0],
            docs = r.objects("docs").map { ReplicaDocument(it.str("id"), it.str("body")) },
            next = r.str("next"),
            done = r.bool("done"),
            total = r.int("total"),
        )
    }

    public suspend fun squashHead(namespace: String, have: String): SquashHead {
        val r =
            request(
                SyncMsg.SQUASH_HEAD,
                buildJsonObject {
                    put("namespace", namespace)
                    put("have", have)
                },
                "squashHeadResult",
            )
        val payload = r.strOrNull("commitPayload")
        return SquashHead(
            commit = if (payload.isNullOrEmpty()) null else decodeCommitList(fromBase64(payload)).firstOrNull(),
            descends = r.bool("descends"),
            backlog = r.long("backlog"),
            refused = r.str("refused"),
        )
    }

    public suspend fun treeNodes(
        namespace: String,
        treeHex: String,
        prefixes: List<String>,
        entryLimit: Int = DEFAULT_TREE_ENTRY_LIMIT,
    ): List<WireTreeNode> {
        val r =
            request(
                SyncMsg.TREE_NODES,
                buildJsonObject {
                    put("namespace", namespace)
                    put("treeHex", treeHex)
                    put("prefixes", stringArray(prefixes))
                    put("entryLimit", entryLimit)
                },
                "treeNodesResult",
            )
        return r.objects("nodes").map { n ->
            WireTreeNode(
                prefix = n.str("prefix"),
                hash = n.str("hash"),
                children = n.strings("children"),
                hasEntries = n.bool("hasEntries"),
                entries = (n["entries"] as? JsonObject)?.mapValues { (it.value as? JsonPrimitive)?.contentOrNull.orEmpty() }.orEmpty(),
            )
        }
    }

    /** OBJECT_FETCH: bodies by (id, content hash) at [treeHex]; returns the docs and the missing ids. */
    public suspend fun objectFetch(
        namespace: String,
        items: List<Pair<String, String>>,
        treeHex: String,
    ): Pair<List<ReplicaDocument>, List<String>> {
        val r =
            request(
                SyncMsg.OBJECT_FETCH,
                buildJsonObject {
                    put("namespace", namespace)
                    put(
                        "items",
                        buildJsonArray {
                            for ((id, h) in items) {
                                add(
                                    buildJsonObject {
                                        put("id", id)
                                        put("contentHash", h)
                                        put("treeHex", treeHex)
                                    },
                                )
                            }
                        },
                    )
                },
                "objectFetchResult",
            )
        return r.objects("docs").map { ReplicaDocument(it.str("id"), it.str("body")) } to r.strings("missing")
    }

    public suspend fun projectFetch(
        namespace: String,
        filter: String,
        fromHex: String,
        atHex: String,
        after: String,
        maxBytes: Int = DEFAULT_PAGE_BYTES,
    ): ProjectPage {
        val r =
            request(
                SyncMsg.PROJECT_FETCH,
                buildJsonObject {
                    put("namespace", namespace)
                    put("filter", filter)
                    put("fromHex", fromHex)
                    put("atHex", atHex)
                    put("after", after)
                    put("maxBytes", maxBytes)
                },
                "projectPage",
            )
        return ProjectPage(
            atHex = r.str("atHex"),
            reset = r.bool("reset"),
            writes = r.objects("writes").map { ReplicaDocument(it.str("id"), it.str("body")) },
            deletes = r.strings("deletes"),
            next = r.str("next"),
            done = r.bool("done"),
        )
    }

    public suspend fun projectWrite(namespace: String, txId: String, docs: List<WriteBackDoc>): ProjectWriteResult {
        val r =
            request(
                SyncMsg.PROJECT_WRITE,
                buildJsonObject {
                    put("namespace", namespace)
                    put("txId", txId)
                    put(
                        "docs",
                        buildJsonArray {
                            for (d in docs) {
                                add(
                                    buildJsonObject {
                                        put("docId", d.docId)
                                        if (d.body != null) put("body", d.body)
                                        if (d.deleted) put("deleted", true)
                                        if (d.baseHash != null) put("baseHash", d.baseHash)
                                    },
                                )
                            }
                        },
                    )
                },
                "projectWriteResult",
            )
        return ProjectWriteResult(
            outcome = r.str("outcome"),
            commitHex = r.str("commitHex"),
            reason = r.str("reason"),
            current = r.objects("current").map { CurrentDoc(it.str("docId"), it.strOrNull("body"), it.bool("absent")) },
        )
    }

    public suspend fun refUpdate(
        namespace: String,
        oldHex: String,
        newHex: String,
        commits: List<ReplicaCommit>,
        more: Boolean,
    ): RefUpdateAck {
        val r =
            request(
                SyncMsg.REF_UPDATE,
                buildJsonObject {
                    put("namespace", namespace)
                    put("kind", "branch")
                    put("ref", "main")
                    put("oldHex", oldHex)
                    put("newHex", newHex)
                    put("commitsPayload", toBase64(encodeCommitList(commits)))
                    put("more", more)
                },
                "refUpdateAck",
            )
        val conflictBytes = r.strOrNull("conflictBytes")
        return RefUpdateAck(
            outcome = r.str("outcome"),
            headHex = r.str("headHex"),
            stored = r.int("stored"),
            conflict = if (conflictBytes.isNullOrEmpty()) null else conflictFrom(fromBase64(conflictBytes).decodeToString()),
        )
    }
}

// --- JSON helpers ------------------------------------------------------------------------------

internal fun stringArray(xs: List<String>): JsonArray = JsonArray(xs.map { JsonPrimitive(it) })

private fun JsonElement?.prim(): JsonPrimitive? = if (this is JsonPrimitive && this !is JsonNull) this else null

internal fun JsonObject.strOrNull(k: String): String? = this[k].prim()?.contentOrNull

internal fun JsonObject.str(k: String): String = strOrNull(k).orEmpty()

internal fun JsonObject.bool(k: String): Boolean = this[k].prim()?.booleanOrNull ?: false

internal fun JsonObject.int(k: String): Int = this[k].prim()?.intOrNull ?: 0

internal fun JsonObject.long(k: String): Long = this[k].prim()?.longOrNull ?: 0L

internal fun JsonObject.strings(k: String): List<String> =
    (this[k] as? JsonArray)?.mapNotNull { it.prim()?.contentOrNull }.orEmpty()

internal fun JsonObject.objects(k: String): List<JsonObject> = (this[k] as? JsonArray)?.filterIsInstance<JsonObject>().orEmpty()

private fun refsFrom(o: JsonObject): NamespaceRefs =
    NamespaceRefs(
        namespace = o.str("namespace"),
        branches = (o["branches"] as? JsonObject)?.mapValues { it.value.prim()?.contentOrNull.orEmpty() }.orEmpty(),
        historyFloorHex = o.str("historyFloorHex"),
        shallow = o.strings("shallow"),
        access = o.str("access"),
    )

private fun conflictFrom(text: String): ConflictReport {
    val o = wireJson.parseToJsonElement(text) as? JsonObject ?: JsonObject(emptyMap())
    return ConflictReport(
        transactionId = o.str("transactionId"),
        baseHash = o.str("baseHash"),
        targetHash = o.str("targetHash"),
        conflicts =
            o.objects("conflicts").map {
                ConflictItem(
                    documentId = it.str("documentId"),
                    operationType = it.str("operationType"),
                    localDoc = it.strOrNull("localDoc"),
                    incomingDoc = it.strOrNull("incomingDoc"),
                    actualContentHash = it.strOrNull("actualContentHash"),
                )
            },
    )
}
