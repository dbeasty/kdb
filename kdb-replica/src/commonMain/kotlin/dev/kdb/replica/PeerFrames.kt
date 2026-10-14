package dev.kdb.replica

import dev.kdb.wire.DEFAULT_MAX_FRAME_BYTES
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.WireMessage
import dev.kdb.wire.defaultWireCodec
import dev.kdb.wire.validateFrameLength
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject

/**
 * Peer sync v2 message codes (go/kdb/wire/types.go), the ones a client replica sends or receives.
 * Codes 0x50-0x57 are reserved for the replica (plan §6).
 */
public object SyncMsg {
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
    public const val PROJECT_FETCH: Int = 0x30
    public const val PROJECT_PAGE: Int = 0x31
    public const val PROJECT_WRITE: Int = 0x32
    public const val PROJECT_WRITE_RESULT: Int = 0x33
    public const val TREE_NODES: Int = 0x34
    public const val TREE_NODES_RESULT: Int = 0x35
    public const val OBJECT_FETCH: Int = 0x36
    public const val OBJECT_FETCH_RESULT: Int = 0x37
    public const val SQUASH_HEAD: Int = 0x40
    public const val SQUASH_HEAD_RESULT: Int = 0x41

    /** Attachments (docs/kdb-attachments-plan.md): always binary bodies (kdb-wire's codec). */
    public const val BLOB_FETCH: Int = 0x42
    public const val BLOB_FETCH_RESULT: Int = 0x43
    public const val ATTACHMENT_UPLOAD: Int = 0x44
    public const val ATTACHMENT_UPLOAD_RESULT: Int = 0x45
    public const val REFS_WATCH: Int = 0x50
    public const val REFS_CHANGED: Int = 0x51

    /** The envelope kind of each request this client sends. */
    internal val requestKinds: Map<Int, String> =
        mapOf(
            SYNC_HELLO to "syncHello",
            REFS_REQUEST to "refsRequest",
            FETCH_REQUEST to "fetchRequest",
            REF_UPDATE to "refUpdate",
            SNAPSHOT_FETCH to "snapshotFetch",
            TREE_NODES to "treeNodes",
            OBJECT_FETCH to "objectFetch",
            SQUASH_HEAD to "squashHead",
            PROJECT_FETCH to "projectFetch",
            PROJECT_WRITE to "projectWrite",
            REFS_WATCH to "refsWatch",
        )
}

private const val FRAME_HEADER_SIZE = 12
private const val ENCODING_JSON: Byte = 1

/**
 * A decoded v2 frame: its header fields and its JSON envelope `{"kind": .., "<kind>": {..}}`, or,
 * for the attachment messages, which are always binary bodies, the message kdb-wire decoded.
 */
internal class SyncFrame(val code: Int, val correlationId: Int, val envelope: JsonObject, val binary: WireMessage? = null) {
    val kind: String get() = if (binary != null) "binary" else envelope.str("kind")
}

/** kdb-wire's codec, for the binary attachment bodies (tag 2, or 3 when zstd-compressed). */
internal val attachmentCodec = defaultWireCodec()

internal val wireJson = Json { ignoreUnknownKeys = true }

/**
 * Peer sync v2 framing: the 12-byte little-endian frame header every KDB wire message has
 * (`i32 frameLength, u16 type, i16 protocolVersion, i32 correlationId`), then encoding tag 1 and
 * the JSON envelope. Validation and the constants are kdb-wire's; the codec itself is not, because
 * kdb-wire's WireCodec types frames by its WireMessageType enum, which has no v2 codes (v2 was
 * Go-only until the replicas), and this module may not change kdb-wire. Moving the v2 messages
 * into kdb-wire is the plan's eventual shape (§3.3).
 */
internal fun encodeSyncFrame(code: Int, correlationId: Int, envelope: JsonObject): ByteArray {
    val body = wireJson.encodeToString(JsonObject.serializer(), envelope).encodeToByteArray()
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

internal fun decodeSyncFrame(frame: ByteArray): SyncFrame {
    if (frame.size < FRAME_HEADER_SIZE + 1) throw ProtocolException("kdb replica: frame too short")
    val total = readLe(frame, 0, 4)
    validateFrameLength(total, DEFAULT_MAX_FRAME_BYTES)
    if (frame.size < total || total < FRAME_HEADER_SIZE + 1) {
        throw ProtocolException("kdb replica: frame shorter than its declared length")
    }
    val code = readLe(frame, 4, 2)
    val correlationId = readLe(frame, 8, 4)
    val tag = frame[FRAME_HEADER_SIZE].toInt()
    if (tag == 2 || tag == 3) {
        val msg =
            try {
                attachmentCodec.decode(frame)
            } catch (e: Exception) {
                throw ProtocolException("kdb replica: undecodable binary frame 0x${code.toString(16)}: ${e.message}", e)
            }
        return SyncFrame(code, correlationId, JsonObject(emptyMap()), msg)
    }
    // 0 (KDB_BINARY) carries the JSON envelope too, as kdb-wire's decoder treats it.
    if (tag != 0 && tag != 1) throw ProtocolException("kdb replica: unsupported payload encoding tag $tag")
    val text = frame.decodeToString(FRAME_HEADER_SIZE + 1, total)
    val env =
        try {
            wireJson.parseToJsonElement(text) as? JsonObject
        } catch (e: Exception) {
            throw ProtocolException("kdb replica: payload is not valid JSON: ${e.message}")
        } ?: throw ProtocolException("kdb replica: payload envelope is not a JSON object")
    return SyncFrame(code, correlationId, env)
}

internal fun envelope(kind: String, body: JsonObject): JsonObject =
    buildJsonObject {
        put("kind", kotlinx.serialization.json.JsonPrimitive(kind))
        put(kind, body)
    }

private fun writeLe(b: ByteArray, off: Int, v: Int, n: Int) {
    for (i in 0 until n) b[off + i] = (v shr (8 * i)).toByte()
}

private fun readLe(b: ByteArray, off: Int, n: Int): Int {
    var v = 0
    for (i in 0 until n) v = v or ((b[off + i].toInt() and 0xff) shl (8 * i))
    return v
}
