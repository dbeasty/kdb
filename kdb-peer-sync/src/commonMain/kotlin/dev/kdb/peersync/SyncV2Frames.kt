package dev.kdb.peersync

import dev.kdb.codec.KdbHash
import dev.kdb.compression.ZstdCompression
import dev.kdb.document.KdbCommit
import dev.kdb.wire.CommitPushCodec
import dev.kdb.wire.DEFAULT_MAX_FRAME_BYTES
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.validateFrameLength
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonObjectBuilder
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

/**
 * Peer sync protocol v2 (go/kdb/wire/sync_v2_ops.go): one connection serves every namespace the
 * peer may sync, and every branch and tag in each.
 *
 *     SYNC_HELLO    -> SYNC_HELLO_ACK   authenticate, grant namespaces, advertise their refs
 *     REFS_REQUEST  -> REFS_RESULT      re-read refs mid-session
 *     FETCH_REQUEST -> PACK_PAGE        commits the fetcher lacks, parents first, paged by bytes
 *     REF_UPDATE    -> REF_UPDATE_ACK   push a page of commits; on the last page, propose a ref
 *
 * Every request gets a reply; failures are PEER_ERROR. These frames are JSON envelopes
 * (`{"kind": k, k: {...}}`), as Go writes them. kdb-wire's WireCodec types frames by an enum that
 * has no v2 codes, so they are framed here (as kdb-replica does) - the attachment frames that ride
 * the same session (BLOB_FETCH, ATTACHMENT_UPLOAD) are kdb-wire's own, always binary.
 */
public object SyncV2 {
    public const val PROTOCOL: Int = 2

    public const val PEER_ERROR: Int = 0x25
    public const val SYNC_HELLO: Int = 0x26
    public const val SYNC_HELLO_ACK: Int = 0x27
    public const val REFS_REQUEST: Int = 0x28
    public const val REFS_RESULT: Int = 0x29
    public const val FETCH_REQUEST: Int = 0x2a
    public const val PACK_PAGE: Int = 0x2b
    public const val REF_UPDATE: Int = 0x2c
    public const val REF_UPDATE_ACK: Int = 0x2d
    public const val SNAPSHOT_FETCH: Int = 0x2e
    public const val SNAPSHOT_PAGE: Int = 0x2f

    // Capabilities (go/kdb/wire SyncCap*). A Kotlin node advertises only what it implements: the
    // session uses the intersection, so a Go peer never sends it snapshot, graft, squash, filter or
    // tree-node frames, and never answers it with them.
    public const val CAP_BRANCHES: String = "branches"
    public const val CAP_TAGS: String = "tags"
    public const val CAP_ZSTD_PACK: String = "zstd-pack"
    public const val CAP_ATTACHMENTS: String = "attachments"

    /** SNAPSHOT_FETCH/PAGE: a fresh node bootstraps from a peer's state at a commit instead of its
     * history - which a peer holding a shallow root or a retention floor cannot send. */
    public const val CAP_SNAPSHOT: String = "snapshot"

    /** What this build's v2 host and client do. Not "stubs": neither side stores an archived
     * parent sent in place of its commit, and a page carrying one is refused. */
    public val CAPABILITIES: List<String> = listOf(CAP_BRANCHES, CAP_TAGS, CAP_SNAPSHOT, CAP_ZSTD_PACK, CAP_ATTACHMENTS)

    public const val ACCESS_PULL: String = "pull"
    public const val ACCESS_PUSH: String = "push"

    public const val REF_BRANCH: String = "branch"
    public const val REF_TAG: String = "tag"

    public const val OUTCOME_STORED: String = "stored"
    public const val OUTCOME_FAST_FORWARDED: String = "fast-forwarded"
    public const val OUTCOME_MERGED: String = "merged"
    public const val OUTCOME_NOOP: String = "noop"
    public const val OUTCOME_CREATED: String = "created"
    public const val OUTCOME_CONFLICT: String = "conflict"

    /** Whether [code] is a v2 sync frame this module frames itself. */
    public fun isSyncFrame(code: Int): Boolean = code in PEER_ERROR..SNAPSHOT_PAGE
}

/** Go's wire.ErrorCode values a peer sends in PEER_ERROR and in per-entry blob errors. */
public object PeerErrorCodes {
    public const val CONFLICT: String = "CONFLICT"
    public const val SCHEMA_VIOLATION: String = "SCHEMA_VIOLATION"
    public const val UNAUTHORIZED: String = "UNAUTHORIZED"
    public const val UNSUPPORTED: String = "UNSUPPORTED"
    public const val NAMESPACE_MISMATCH: String = "NAMESPACE_MISMATCH"
    public const val INTEGRITY: String = "INTEGRITY"
    public const val NOT_FOUND: String = "NOT_FOUND"
    public const val INTERNAL: String = "INTERNAL"
}

/** One namespace's refs as a node advertises them. */
public data class NamespaceRefs(
    val namespace: String,
    /** Branch name to head commit hex; holds "main" once the namespace has a commit. */
    val branches: Map<String, String> = emptyMap(),
    val tags: Map<String, String> = emptyMap(),
    val historyFloorHex: String = "",
    val shallow: List<String> = emptyList(),
    val resolutionHash: String = "",
    /** "" both directions, [SyncV2.ACCESS_PULL] or [SyncV2.ACCESS_PUSH] one only. */
    val access: String = "",
)

/** An archived parent sent in place of its commit (Go's document.CommitStub). */
public data class WireCommitStub(
    val originalHashHex: String,
    val archiveLocation: String,
    val stubbedAtMicros: Long,
)

/** A decoded v2 sync frame. */
public sealed class SyncV2Message {
    public abstract val correlationId: Int

    public data class PeerError(
        override val correlationId: Int,
        val namespace: String,
        val code: String,
        val message: String,
    ) : SyncV2Message()

    public data class Hello(
        override val correlationId: Int,
        val nodeId: String,
        val protocol: Int,
        val capabilities: List<String>,
        val namespaces: List<String>,
        val user: String? = null,
        val password: String? = null,
        val token: String? = null,
    ) : SyncV2Message()

    public data class HelloAck(
        override val correlationId: Int,
        val accepted: Boolean,
        val nodeId: String,
        val protocol: Int,
        val capabilities: List<String> = emptyList(),
        val refs: List<NamespaceRefs> = emptyList(),
        val reason: String = "",
    ) : SyncV2Message()

    public data class RefsRequest(
        override val correlationId: Int,
        val namespaces: List<String>,
    ) : SyncV2Message()

    public data class RefsResult(
        override val correlationId: Int,
        val refs: List<NamespaceRefs>,
    ) : SyncV2Message()

    public data class FetchRequest(
        override val correlationId: Int,
        val namespace: String,
        val wants: List<KdbHash>,
        val haves: List<KdbHash> = emptyList(),
        val maxBytes: Int = 0,
    ) : SyncV2Message()

    public data class PackPage(
        override val correlationId: Int,
        val namespace: String,
        val commits: List<KdbCommit>,
        val stubs: List<WireCommitStub> = emptyList(),
        val done: Boolean,
        /** Send the commits zstd-compressed; only to a peer with [SyncV2.CAP_ZSTD_PACK]. */
        val zstd: Boolean = false,
    ) : SyncV2Message()

    public data class RefUpdate(
        override val correlationId: Int,
        val namespace: String,
        val kind: String,
        val ref: String,
        val oldHex: String = "",
        val newHex: String,
        val commits: List<KdbCommit>,
        val stubs: List<WireCommitStub> = emptyList(),
        val more: Boolean = false,
        val zstd: Boolean = false,
    ) : SyncV2Message()

    /** One page of a peer's state at a commit, in document-id order after [after] (Go's
     * SnapshotFetchMessage). [atHex] empty: the host's main head, which the page names. */
    public data class SnapshotFetch(
        override val correlationId: Int,
        val namespace: String,
        val atHex: String = "",
        val after: String = "",
        val maxBytes: Int = 0,
    ) : SyncV2Message()

    /** Answers SNAPSHOT_FETCH. [commit] is on every page; the last ([done]) carries the commit's
     * packed attachment table when it has one (Go's SnapshotPageMessage). */
    public data class SnapshotPage(
        override val correlationId: Int,
        val namespace: String,
        val commit: KdbCommit,
        val docs: List<SnapshotDoc>,
        val next: String = "",
        val done: Boolean,
        val total: Int,
        val attachmentTables: ByteArray? = null,
    ) : SyncV2Message() {
        override fun equals(other: Any?): Boolean =
            other is SnapshotPage && correlationId == other.correlationId && namespace == other.namespace &&
                commit == other.commit && docs == other.docs && next == other.next && done == other.done &&
                total == other.total && (attachmentTables?.contentEquals(other.attachmentTables ?: ByteArray(0)) ?: (other.attachmentTables == null))

        override fun hashCode(): Int = listOf(correlationId, namespace, commit.hash, docs, next, done, total).hashCode()
    }

    public data class RefUpdateAck(
        override val correlationId: Int,
        val namespace: String,
        val kind: String,
        val ref: String,
        val outcome: String,
        val headHex: String = "",
        val stored: Int = 0,
        /** The JSON ConflictReport when [outcome] is conflict. */
        val conflictBytes: ByteArray? = null,
    ) : SyncV2Message() {
        override fun equals(other: Any?): Boolean =
            other is RefUpdateAck && correlationId == other.correlationId && namespace == other.namespace &&
                kind == other.kind && ref == other.ref && outcome == other.outcome && headHex == other.headHex &&
                stored == other.stored && (conflictBytes?.contentEquals(other.conflictBytes ?: ByteArray(0)) ?: (other.conflictBytes == null))

        override fun hashCode(): Int = listOf(correlationId, namespace, kind, ref, outcome, headHex, stored).hashCode()
    }
}

/** One document of a snapshot page. */
public data class SnapshotDoc(
    val docId: String,
    val body: String,
)

/** A frame this module could not read: the connection is dropped, as Go does. */
public class SyncV2DecodeException(message: String) : PeerSyncException(message)

private const val FRAME_HEADER_SIZE = 12
private const val ENCODING_JSON: Byte = 1

/** Bounds a decompressed commits payload, as Go's maxPackPayload: a hostile peer's small frame
 * must not inflate into gigabytes. */
private const val MAX_PACK_PAYLOAD = 256 shl 20

private val syncJson = Json { ignoreUnknownKeys = true }

/** The message type code of a framed message, or -1 for a frame too short to carry a header. */
public fun frameTypeCode(frame: ByteArray): Int =
    if (frame.size < FRAME_HEADER_SIZE) -1 else (frame[4].toInt() and 0xff) or ((frame[5].toInt() and 0xff) shl 8)

/** The correlation id of a framed message (0 for a frame too short to carry one). */
public fun frameCorrelationId(frame: ByteArray): Int = if (frame.size < FRAME_HEADER_SIZE) 0 else readLe(frame, 8, 4)

public object SyncV2Codec {
    public fun encode(msg: SyncV2Message): ByteArray {
        val (code, kind, body) =
            when (msg) {
                is SyncV2Message.PeerError ->
                    Triple(
                        SyncV2.PEER_ERROR,
                        "peerError",
                        buildJsonObject {
                            put("namespace", msg.namespace)
                            put("code", msg.code)
                            put("message", msg.message)
                        },
                    )
                is SyncV2Message.Hello ->
                    Triple(
                        SyncV2.SYNC_HELLO,
                        "syncHello",
                        buildJsonObject {
                            put("nodeId", msg.nodeId)
                            put("protocol", msg.protocol)
                            if (msg.capabilities.isNotEmpty()) put("capabilities", strings(msg.capabilities))
                            put("namespaces", strings(msg.namespaces))
                            msg.user?.let { put("user", it) }
                            msg.password?.let { put("password", it) }
                            msg.token?.let { put("token", it) }
                        },
                    )
                is SyncV2Message.HelloAck ->
                    Triple(
                        SyncV2.SYNC_HELLO_ACK,
                        "syncHelloAck",
                        buildJsonObject {
                            put("accepted", msg.accepted)
                            put("nodeId", msg.nodeId)
                            put("protocol", msg.protocol)
                            if (msg.capabilities.isNotEmpty()) put("capabilities", strings(msg.capabilities))
                            if (msg.refs.isNotEmpty()) put("refs", JsonArray(msg.refs.map(::refsJson)))
                            if (msg.reason.isNotEmpty()) put("reason", msg.reason)
                        },
                    )
                is SyncV2Message.RefsRequest ->
                    Triple(SyncV2.REFS_REQUEST, "refsRequest", buildJsonObject { put("namespaces", strings(msg.namespaces)) })
                is SyncV2Message.RefsResult ->
                    Triple(SyncV2.REFS_RESULT, "refsResult", buildJsonObject { put("refs", JsonArray(msg.refs.map(::refsJson))) })
                is SyncV2Message.FetchRequest ->
                    Triple(
                        SyncV2.FETCH_REQUEST,
                        "fetchRequest",
                        buildJsonObject {
                            put("namespace", msg.namespace)
                            put("wants", strings(msg.wants.map { it.toHex() }))
                            if (msg.haves.isNotEmpty()) put("haves", strings(msg.haves.map { it.toHex() }))
                            if (msg.maxBytes != 0) put("maxBytes", msg.maxBytes)
                        },
                    )
                is SyncV2Message.PackPage ->
                    Triple(
                        SyncV2.PACK_PAGE,
                        "packPage",
                        buildJsonObject {
                            put("namespace", msg.namespace)
                            put("commitsPayload", toBase64(commitsPayload(msg.commits, msg.zstd)))
                            stubs(msg.stubs)
                            put("done", msg.done)
                            if (msg.zstd) put("commitsZstd", true)
                        },
                    )
                is SyncV2Message.RefUpdate ->
                    Triple(
                        SyncV2.REF_UPDATE,
                        "refUpdate",
                        buildJsonObject {
                            put("namespace", msg.namespace)
                            put("kind", msg.kind)
                            put("ref", msg.ref)
                            if (msg.oldHex.isNotEmpty()) put("oldHex", msg.oldHex)
                            put("newHex", msg.newHex)
                            put("commitsPayload", toBase64(commitsPayload(msg.commits, msg.zstd)))
                            stubs(msg.stubs)
                            if (msg.more) put("more", true)
                            if (msg.zstd) put("commitsZstd", true)
                        },
                    )
                is SyncV2Message.SnapshotFetch ->
                    Triple(
                        SyncV2.SNAPSHOT_FETCH,
                        "snapshotFetch",
                        buildJsonObject {
                            put("namespace", msg.namespace)
                            if (msg.atHex.isNotEmpty()) put("atHex", msg.atHex)
                            if (msg.after.isNotEmpty()) put("after", msg.after)
                            if (msg.maxBytes != 0) put("maxBytes", msg.maxBytes)
                        },
                    )
                is SyncV2Message.SnapshotPage ->
                    Triple(
                        SyncV2.SNAPSHOT_PAGE,
                        "snapshotPage",
                        buildJsonObject {
                            put("namespace", msg.namespace)
                            put("commitPayload", toBase64(CommitPushCodec.encodeCommits(listOf(msg.commit))))
                            put(
                                "docs",
                                JsonArray(
                                    msg.docs.map {
                                        buildJsonObject {
                                            put("id", it.docId)
                                            put("body", it.body)
                                        }
                                    },
                                ),
                            )
                            if (msg.next.isNotEmpty()) put("next", msg.next)
                            put("done", msg.done)
                            put("total", msg.total)
                            msg.attachmentTables?.takeIf { it.isNotEmpty() }?.let { put("attachmentTables", toBase64(it)) }
                        },
                    )
                is SyncV2Message.RefUpdateAck ->
                    Triple(
                        SyncV2.REF_UPDATE_ACK,
                        "refUpdateAck",
                        buildJsonObject {
                            put("namespace", msg.namespace)
                            put("kind", msg.kind)
                            put("ref", msg.ref)
                            put("outcome", msg.outcome)
                            put("headHex", msg.headHex)
                            put("stored", msg.stored)
                            msg.conflictBytes?.takeIf { it.isNotEmpty() }?.let { put("conflictBytes", toBase64(it)) }
                        },
                    )
            }
        val envelope =
            buildJsonObject {
                put("kind", kind)
                put(kind, body)
            }
        return frame(code, msg.correlationId, envelope)
    }

    public fun decode(frame: ByteArray): SyncV2Message {
        if (frame.size < FRAME_HEADER_SIZE + 1) throw SyncV2DecodeException("peer sync: frame too short")
        val total = readLe(frame, 0, 4)
        try {
            validateFrameLength(total, DEFAULT_MAX_FRAME_BYTES)
        } catch (e: Exception) {
            throw SyncV2DecodeException("peer sync: ${e.message}")
        }
        if (frame.size < total || total < FRAME_HEADER_SIZE + 1) {
            throw SyncV2DecodeException("peer sync: frame shorter than its declared length")
        }
        val code = readLe(frame, 4, 2)
        val cid = readLe(frame, 8, 4)
        val tag = frame[FRAME_HEADER_SIZE].toInt()
        // 0 (KDB_BINARY) carries the JSON envelope too, as kdb-wire's decoder treats it.
        if (tag != 0 && tag != 1) throw SyncV2DecodeException("peer sync: v2 frame with payload encoding tag $tag")
        val env =
            try {
                syncJson.parseToJsonElement(frame.decodeToString(FRAME_HEADER_SIZE + 1, total)) as? JsonObject
            } catch (e: Exception) {
                throw SyncV2DecodeException("peer sync: payload is not valid JSON: ${e.message}")
            } ?: throw SyncV2DecodeException("peer sync: payload envelope is not a JSON object")
        val kind = env.str("kind")
        val b = env[kind] as? JsonObject ?: throw SyncV2DecodeException("peer sync: missing $kind body")
        val expected =
            when (kind) {
                "peerError" -> SyncV2.PEER_ERROR
                "syncHello" -> SyncV2.SYNC_HELLO
                "syncHelloAck" -> SyncV2.SYNC_HELLO_ACK
                "refsRequest" -> SyncV2.REFS_REQUEST
                "refsResult" -> SyncV2.REFS_RESULT
                "fetchRequest" -> SyncV2.FETCH_REQUEST
                "packPage" -> SyncV2.PACK_PAGE
                "refUpdate" -> SyncV2.REF_UPDATE
                "refUpdateAck" -> SyncV2.REF_UPDATE_ACK
                "snapshotFetch" -> SyncV2.SNAPSHOT_FETCH
                "snapshotPage" -> SyncV2.SNAPSHOT_PAGE
                else -> throw SyncV2DecodeException("peer sync: unknown v2 message kind '$kind'")
            }
        if (expected != code) throw SyncV2DecodeException("peer sync: frame type 0x${code.toString(16)} carries '$kind'")
        return try {
            when (kind) {
                "peerError" -> SyncV2Message.PeerError(cid, b.str("namespace"), b.str("code"), b.str("message"))
                "syncHello" ->
                    SyncV2Message.Hello(
                        cid, b.str("nodeId"), b.int("protocol"), b.strings("capabilities"), b.strings("namespaces"),
                        b.strOrNull("user"), b.strOrNull("password"), b.strOrNull("token"),
                    )
                "syncHelloAck" ->
                    SyncV2Message.HelloAck(
                        cid, b.bool("accepted"), b.str("nodeId"), b.int("protocol"), b.strings("capabilities"),
                        b.objects("refs").map(::refsFrom), b.str("reason"),
                    )
                "refsRequest" -> SyncV2Message.RefsRequest(cid, b.strings("namespaces"))
                "refsResult" -> SyncV2Message.RefsResult(cid, b.objects("refs").map(::refsFrom))
                "fetchRequest" ->
                    SyncV2Message.FetchRequest(
                        cid, b.str("namespace"), b.strings("wants").map(KdbHash::fromHex),
                        b.strings("haves").map(KdbHash::fromHex), b.int("maxBytes"),
                    )
                "packPage" ->
                    SyncV2Message.PackPage(
                        cid, b.str("namespace"), commitsFrom(fromBase64(b.strOrNull("commitsPayload")), b.bool("commitsZstd")),
                        stubsFrom(b), b.bool("done"), b.bool("commitsZstd"),
                    )
                "refUpdate" ->
                    SyncV2Message.RefUpdate(
                        cid, b.str("namespace"), b.str("kind"), b.str("ref"), b.str("oldHex"), b.str("newHex"),
                        commitsFrom(fromBase64(b.strOrNull("commitsPayload")), b.bool("commitsZstd")), stubsFrom(b),
                        b.bool("more"), b.bool("commitsZstd"),
                    )
                "snapshotFetch" -> SyncV2Message.SnapshotFetch(cid, b.str("namespace"), b.str("atHex"), b.str("after"), b.int("maxBytes"))
                "snapshotPage" -> {
                    val commits = CommitPushCodec.decodeCommits(fromBase64(b.strOrNull("commitPayload")))
                    if (commits.size != 1) throw SyncV2DecodeException("peer sync: snapshotPage must carry exactly one commit")
                    SyncV2Message.SnapshotPage(
                        cid, b.str("namespace"), commits[0], b.objects("docs").map { SnapshotDoc(it.str("id"), it.str("body")) },
                        b.str("next"), b.bool("done"), b.int("total"), b.strOrNull("attachmentTables")?.let(::fromBase64),
                    )
                }
                else ->
                    SyncV2Message.RefUpdateAck(
                        cid, b.str("namespace"), b.str("kind"), b.str("ref"), b.str("outcome"), b.str("headHex"),
                        b.int("stored"), b.strOrNull("conflictBytes")?.let(::fromBase64),
                    )
            }
        } catch (e: SyncV2DecodeException) {
            throw e
        } catch (e: Exception) {
            throw SyncV2DecodeException("peer sync: bad $kind: ${e.message}")
        }
    }

    private fun frame(code: Int, correlationId: Int, envelope: JsonObject): ByteArray {
        val body = syncJson.encodeToString(JsonObject.serializer(), envelope).encodeToByteArray()
        val total = FRAME_HEADER_SIZE + 1 + body.size
        validateFrameLength(total, DEFAULT_MAX_FRAME_BYTES)
        val out = ByteArray(total)
        writeLe(out, 0, total, 4)
        writeLe(out, 4, code, 2)
        writeLe(out, 6, KDB_WIRE_PROTOCOL_VERSION, 2)
        writeLe(out, 8, correlationId, 4)
        out[FRAME_HEADER_SIZE] = ENCODING_JSON
        body.copyInto(out, FRAME_HEADER_SIZE + 1)
        return out
    }

    private fun commitsPayload(commits: List<KdbCommit>, zstd: Boolean): ByteArray {
        val raw = CommitPushCodec.encodeCommits(commits)
        // Level 1, as Go's packZstdLevel: the win is in bytes on the wire, not the last percent.
        return if (zstd) ZstdCompression.compress(raw, 1) else raw
    }

    private fun commitsFrom(payload: ByteArray, zstd: Boolean): List<KdbCommit> {
        val raw = if (zstd) ZstdCompression.decompress(payload, MAX_PACK_PAYLOAD) else payload
        return CommitPushCodec.decodeCommits(raw)
    }

    private fun refsJson(r: NamespaceRefs): JsonObject =
        buildJsonObject {
            put("namespace", r.namespace)
            put("branches", JsonObject(r.branches.mapValues { JsonPrimitive(it.value) }))
            if (r.tags.isNotEmpty()) put("tags", JsonObject(r.tags.mapValues { JsonPrimitive(it.value) }))
            if (r.historyFloorHex.isNotEmpty()) put("historyFloorHex", r.historyFloorHex)
            if (r.resolutionHash.isNotEmpty()) put("resolutionHash", r.resolutionHash)
            if (r.shallow.isNotEmpty()) put("shallow", strings(r.shallow))
            if (r.access.isNotEmpty()) put("access", r.access)
        }

    private fun refsFrom(o: JsonObject): NamespaceRefs =
        NamespaceRefs(
            namespace = o.str("namespace"),
            branches = o.stringMap("branches"),
            tags = o.stringMap("tags"),
            historyFloorHex = o.str("historyFloorHex"),
            shallow = o.strings("shallow"),
            resolutionHash = o.str("resolutionHash"),
            access = o.str("access"),
        )

    private fun JsonObjectBuilder.stubs(stubs: List<WireCommitStub>) {
        if (stubs.isEmpty()) return
        put(
            "stubs",
            JsonArray(
                stubs.map {
                    buildJsonObject {
                        put("originalHashHex", it.originalHashHex)
                        put("archiveLocation", it.archiveLocation)
                        put("stubbedAtMicros", it.stubbedAtMicros)
                    }
                },
            ),
        )
    }

    private fun stubsFrom(o: JsonObject): List<WireCommitStub> =
        o.objects("stubs").map {
            WireCommitStub(it.str("originalHashHex"), it.str("archiveLocation"), it.long("stubbedAtMicros"))
        }

    private fun strings(values: List<String>): JsonArray = JsonArray(values.map { JsonPrimitive(it) })
}

private fun JsonObject.prim(key: String): JsonPrimitive? = (this[key] as? JsonPrimitive)?.takeUnless { it is JsonNull }

private fun JsonObject.str(key: String): String = prim(key)?.content ?: ""

private fun JsonObject.strOrNull(key: String): String? = prim(key)?.content

private fun JsonObject.int(key: String): Int = prim(key)?.intOrNull ?: 0

private fun JsonObject.long(key: String): Long = prim(key)?.content?.toLongOrNull() ?: 0L

private fun JsonObject.bool(key: String): Boolean = prim(key)?.booleanOrNull ?: false

private fun JsonObject.strings(key: String): List<String> =
    (this[key] as? JsonArray)?.map { (it as JsonElement).jsonPrimitive.content } ?: emptyList()

private fun JsonObject.objects(key: String): List<JsonObject> = (this[key] as? JsonArray)?.filterIsInstance<JsonObject>() ?: emptyList()

private fun JsonObject.stringMap(key: String): Map<String, String> =
    (this[key] as? JsonObject)?.mapValues { it.value.jsonPrimitive.content } ?: emptyMap()

/** Go's encoding/json form of `[]byte`: standard base64 with padding. */
@OptIn(ExperimentalEncodingApi::class)
internal fun toBase64(b: ByteArray): String = Base64.Default.encode(b)

@OptIn(ExperimentalEncodingApi::class)
internal fun fromBase64(s: String?): ByteArray = if (s.isNullOrEmpty()) ByteArray(0) else Base64.Default.decode(s)

private fun writeLe(b: ByteArray, off: Int, v: Int, n: Int) {
    for (i in 0 until n) b[off + i] = (v shr (8 * i)).toByte()
}

private fun readLe(b: ByteArray, off: Int, n: Int): Int {
    var v = 0
    for (i in 0 until n) v = v or ((b[off + i].toInt() and 0xff) shl (8 * i))
    return v
}
