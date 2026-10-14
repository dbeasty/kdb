package dev.kdb.wire

/**
 * Binary frame bodies (encoding tag 2), matching go/kdb/wire/binary.go and binary_messages.go.
 *
 * A binary body is the message's fields in the protobuf wire format: each field is a key -
 * uvarint(fieldNumber shl 3 or wireType) - then a uvarint (wire type 0) or a uvarint length and
 * that many raw bytes (wire type 2). Strings are UTF-8 and never escaped, byte fields are raw, and
 * unknown fields are skipped, so the format grows additively, as the JSON envelope did.
 *
 * Conventions shared with the Go and TS codecs: an absent field is the zero value ("", 0, false,
 * empty bytes, empty list); an optional (nullable) field is present exactly when it is non-null,
 * even if ""; integers are varints of their two's-complement 64-bit value.
 *
 * Fields the Go messages carry and these Kotlin messages do not (SqlExec's pageSize and
 * continuation, DocumentGet's minCommit, Upsert's sessionId, SqlResult's continuation) are skipped
 * on decode and never written.
 *
 * Neither side sends tag 2 until the handshake ack lists [WIRE_FEATURE_BINARY]. Tag 3 (zstd) is
 * not offered: common code has no zstd.
 */

public const val WIRE_FEATURE_BINARY: String = "binary-frames"

/** The rest of the client set in binary - here, Search and SearchResult. Separate from
 * [WIRE_FEATURE_BINARY] because peers that shipped with that alone cannot decode these bodies. */
public const val WIRE_FEATURE_BINARY_2: String = "binary-frames-2"

/** Tag-3 frames: a zstd frame of a tag-2 body, used for bodies of 8 KiB or more. */
public const val WIRE_FEATURE_ZSTD: String = "zstd-frames"

/** Every feature this build implements, in the order a client offers them. */
public val SUPPORTED_WIRE_FEATURES: List<String> = listOf(WIRE_FEATURE_BINARY, WIRE_FEATURE_BINARY_2, WIRE_FEATURE_ZSTD)

/** The features in [offered] this build supports; what a server acks. The others extend or
 * compress binary frames, so without [WIRE_FEATURE_BINARY] none is agreed. */
public fun negotiateWireFeatures(offered: List<String>): List<String> {
    val agreed = SUPPORTED_WIRE_FEATURES.filter { it in offered }
    return if (WIRE_FEATURE_BINARY in agreed) agreed else emptyList()
}

/** Whether [type] is one of the load-path messages [WIRE_FEATURE_BINARY] covers. */
internal fun coreBinaryForm(type: WireMessageType): Boolean =
    when (type) {
        WireMessageType.SESSION_BEGIN, WireMessageType.SESSION_BEGIN_ACK, WireMessageType.SQL_EXEC,
        WireMessageType.SQL_RESULT, WireMessageType.TX_COMMIT, WireMessageType.TX_ROLLBACK,
        WireMessageType.CONFLICT_REPORT, WireMessageType.DOCUMENT_GET, WireMessageType.DOCUMENT_GET_RESULT,
        WireMessageType.UPSERT, WireMessageType.UPSERT_RESULT,
        -> true
        else -> false
    }

/** Whether [type] is one of the messages [WIRE_FEATURE_BINARY_2] adds that Kotlin has. */
internal fun moreBinaryForm(type: WireMessageType): Boolean =
    type == WireMessageType.SEARCH || type == WireMessageType.SEARCH_RESULT

internal const val TAG_BINARY_ZSTD: Byte = 3

/** The smallest binary body worth compressing (Go's zstdFrameThreshold). */
internal const val ZSTD_FRAME_THRESHOLD: Int = 8 shl 10

internal const val TAG_BINARY: Byte = 2

private const val WT_VARINT = 0
private const val WT_FIXED64 = 1
private const val WT_BYTES = 2
private const val WT_FIXED32 = 5

private class BinWriter {
    var buf = ByteArray(256)
    var len = 0

    private fun reserve(n: Int) {
        if (len + n <= buf.size) return
        var size = buf.size * 2
        while (size < len + n) size *= 2
        buf = buf.copyOf(size)
    }

    fun uvarint(value: Long) {
        reserve(10)
        var v = value
        while ((v and 0x7FL.inv()) != 0L) {
            buf[len++] = ((v and 0x7F) or 0x80).toByte()
            v = v ushr 7
        }
        buf[len++] = v.toByte()
    }

    fun key(field: Int, wireType: Int) = uvarint((field.toLong() shl 3) or wireType.toLong())

    fun raw(bytes: ByteArray) {
        reserve(bytes.size)
        bytes.copyInto(buf, len)
        len += bytes.size
    }

    fun bytesField(field: Int, bytes: ByteArray) {
        key(field, WT_BYTES)
        uvarint(bytes.size.toLong())
        raw(bytes)
    }

    fun string(field: Int, s: String) {
        if (s.isNotEmpty()) bytesField(field, s.encodeToByteArray())
    }

    fun optString(field: Int, s: String?) {
        if (s != null) bytesField(field, s.encodeToByteArray())
    }

    fun int(field: Int, v: Long) {
        if (v == 0L) return
        key(field, WT_VARINT)
        uvarint(v)
    }

    fun optInt(field: Int, v: Int?) {
        if (v == null) return
        key(field, WT_VARINT)
        uvarint(v.toLong())
    }

    fun bool(field: Int, v: Boolean) {
        if (v) int(field, 1)
    }

    fun bytes(field: Int, v: ByteArray) {
        if (v.isNotEmpty()) bytesField(field, v)
    }

    fun rawDouble(field: Int, v: Double) {
        key(field, WT_FIXED64)
        reserve(8)
        val bits = v.toRawBits()
        for (i in 0 until 8) buf[len++] = (bits ushr (8 * i)).toByte()
    }

    fun double(field: Int, v: Double) {
        if (v != 0.0) rawDouble(field, v)
    }

    fun optDouble(field: Int, v: Double?) {
        if (v != null) rawDouble(field, v)
    }

    /** A nested message: written to its own writer, then as one length-delimited field. */
    fun nested(field: Int, body: BinWriter.() -> Unit) {
        val inner = BinWriter()
        inner.body()
        key(field, WT_BYTES)
        uvarint(inner.len.toLong())
        reserve(inner.len)
        inner.buf.copyInto(buf, len, 0, inner.len)
        len += inner.len
    }

    fun finish(): ByteArray = buf.copyOf(len)
}

private class BinReader(val b: ByteArray, var off: Int, val end: Int) {
    val done: Boolean get() = off >= end

    fun uvarint(): Long {
        var result = 0L
        var shift = 0
        while (shift < 70) {
            if (off >= end) throw WireDecodeException("malformed binary body: truncated varint")
            val byte = b[off++].toInt() and 0xFF
            result = result or ((byte and 0x7F).toLong() shl shift)
            if (byte < 0x80) return result
            shift += 7
        }
        throw WireDecodeException("malformed binary body: varint longer than 10 bytes")
    }

    /** Returns the next field's number and wire type packed as (field shl 3 or wireType). */
    fun next(): Long {
        val key = uvarint()
        if (key ushr 3 == 0L || key ushr 3 > (1L shl 29)) {
            throw WireDecodeException("malformed binary body: invalid field number")
        }
        return key
    }

    /** Reads a length-delimited value; returns its start offset, leaving its length in [lastLen]. */
    var lastLen = 0

    fun span(wireType: Int): Int {
        if (wireType != WT_BYTES) throw WireDecodeException("malformed binary body: wrong wire type")
        val n = uvarint()
        if (n < 0 || n > (end - off).toLong()) {
            throw WireDecodeException("malformed binary body: field runs past the end of the body")
        }
        val start = off
        lastLen = n.toInt()
        off += lastLen
        return start
    }

    fun string(wireType: Int): String {
        val start = span(wireType)
        return b.decodeToString(start, start + lastLen)
    }

    fun bytes(wireType: Int): ByteArray {
        val start = span(wireType)
        return b.copyOfRange(start, start + lastLen)
    }

    fun long(wireType: Int): Long {
        if (wireType != WT_VARINT) throw WireDecodeException("malformed binary body: wrong wire type")
        return uvarint()
    }

    fun int(wireType: Int): Int = long(wireType).toInt()

    fun double(wireType: Int): Double {
        if (wireType != WT_FIXED64) throw WireDecodeException("malformed binary body: wrong wire type")
        if (end - off < 8) throw WireDecodeException("malformed binary body: field runs past the end of the body")
        var bits = 0L
        for (i in 0 until 8) bits = bits or ((b[off + i].toLong() and 0xFF) shl (8 * i))
        off += 8
        return Double.fromBits(bits)
    }

    /** The length-delimited field at the cursor, as a reader of its own. */
    fun nested(wireType: Int): BinReader {
        val start = span(wireType)
        return BinReader(b, start, start + lastLen)
    }

    fun skip(wireType: Int) {
        when (wireType) {
            WT_VARINT -> uvarint()
            WT_BYTES -> span(wireType)
            WT_FIXED64, WT_FIXED32 -> {
                val n = if (wireType == WT_FIXED64) 8 else 4
                if (end - off < n) throw WireDecodeException("malformed binary body: field runs past the end of the body")
                off += n
            }
            else -> throw WireDecodeException("malformed binary body: unknown wire type $wireType")
        }
    }

    inline fun forEachField(action: (field: Int, wireType: Int) -> Boolean) {
        while (!done) {
            val key = next()
            val field = (key ushr 3).toInt()
            val wireType = (key and 7).toInt()
            if (!action(field, wireType)) skip(wireType)
        }
    }
}

/** The binary body of [message], or null when its type has no binary form. */
internal fun encodeBinaryBody(message: WireMessage): ByteArray? {
    val w = BinWriter()
    when (message) {
        is WireMessage.SessionBegin -> {
            w.string(1, message.namespace)
            w.optString(2, message.sessionId)
            w.string(3, message.readConsistency)
            w.optString(4, message.baseVersionHex)
        }
        is WireMessage.SessionBeginAck -> {
            w.string(1, message.namespace)
            w.string(2, message.sessionId)
            w.string(3, message.headHex)
            w.string(4, message.readConsistency)
            w.optString(5, message.error)
        }
        is WireMessage.SqlExec -> {
            w.string(1, message.namespace)
            w.string(2, message.sessionId)
            w.string(3, message.sql)
            w.optString(4, message.parametersJson)
        }
        is WireMessage.SqlResult -> {
            w.string(1, message.namespace)
            w.string(2, message.sessionId)
            for (c in message.columns) w.optString(3, c)
            // Field 4 is one row: its cells back to back, each a uvarint length and its bytes.
            for (row in message.rows) {
                val cells = row.map { it.encodeToByteArray() }
                var size = 0L
                for (c in cells) size += uvarintLen(c.size.toLong()) + c.size
                w.key(4, WT_BYTES)
                w.uvarint(size)
                for (c in cells) {
                    w.uvarint(c.size.toLong())
                    w.raw(c)
                }
            }
            w.int(5, message.rowsAffected.toLong())
            w.string(6, message.resolvedCommitHex)
            w.bool(7, message.readOnly)
            w.optString(8, message.error)
            for (id in message.generatedIds) w.optString(9, id)
            w.optString(10, message.errorCode)
            w.optInt(11, message.retryAfterMs)
            // Field 13: every cell's kind, one byte each - the digits of cellKinds.
            message.cellKinds?.takeIf { it.isNotEmpty() }?.let { kinds ->
                w.bytesField(13, ByteArray(kinds.length) { (kinds[it] - '0').toByte() })
            }
        }
        is WireMessage.Search -> {
            w.string(1, message.namespace)
            w.string(2, message.sessionId ?: "")
            message.text?.let { t ->
                w.nested(3) {
                    string(1, t.index)
                    string(2, t.query)
                    int(3, (t.depth ?: 0).toLong())
                    optDouble(4, t.minScore?.toDouble())
                    optDouble(5, t.weight)
                }
            }
            message.vector?.let { v ->
                w.nested(4) {
                    string(1, v.index)
                    for (x in v.vector) rawDouble(2, x)
                    int(3, (v.depth ?: 0).toLong())
                    optDouble(4, v.minScore?.toDouble())
                    optDouble(5, v.weight)
                }
            }
            w.string(5, message.fusion ?: "")
            w.int(6, message.limit.toLong())
            w.bool(7, message.includeJson)
            w.string(8, message.atCommitHex ?: "")
        }
        is WireMessage.SearchResult -> {
            w.string(1, message.namespace)
            for (h in message.hits) {
                w.nested(2) {
                    string(1, h.docId)
                    double(2, h.score.toDouble())
                    optString(3, h.json)
                }
            }
            w.string(3, message.resolvedCommitHex)
            w.optString(4, message.error)
            w.optString(5, message.errorCode)
            w.optInt(6, message.retryAfterMs)
        }
        is WireMessage.TxCommit -> {
            w.string(1, message.namespace)
            w.string(2, message.sessionId)
            w.bytes(3, message.transactionBytes)
        }
        is WireMessage.TxRollback -> {
            w.string(1, message.namespace)
            w.string(2, message.sessionId)
        }
        is WireMessage.ConflictReport -> {
            w.string(1, message.namespace)
            w.bytes(2, message.reportBytes)
            w.optString(3, message.errorCode)
            w.optInt(4, message.retryAfterMs)
        }
        is WireMessage.DocumentGet -> {
            w.string(1, message.namespace)
            w.string(2, message.docId)
        }
        is WireMessage.DocumentGetResult -> {
            w.string(1, message.namespace)
            w.string(2, message.docId)
            w.optString(3, message.json)
            w.string(4, message.commitHex)
            w.optString(5, message.error)
            w.optString(6, message.errorCode)
            w.optInt(7, message.retryAfterMs)
        }
        is WireMessage.Upsert -> {
            w.string(1, message.namespace)
            w.string(2, message.docId)
            w.string(3, message.json)
        }
        is WireMessage.UpsertResult -> {
            w.string(1, message.namespace)
            w.string(2, message.commitHex)
            w.optString(3, message.error)
            w.optString(4, message.errorCode)
            w.optInt(5, message.retryAfterMs)
        }
        else -> return null
    }
    return w.finish()
}

private fun uvarintLen(value: Long): Int {
    var v = value
    var n = 1
    while ((v and 0x7FL.inv()) != 0L) {
        v = v ushr 7
        n++
    }
    return n
}

/** Decodes the tag-2 body in frame[start, end) of the message type named by [header]. */
internal fun decodeBinaryBody(
    header: WireHeader,
    frame: ByteArray,
    start: Int,
    end: Int,
): WireMessage {
    val r = BinReader(frame, start, end)
    return when (header.messageType) {
        WireMessageType.SESSION_BEGIN -> {
            var namespace = ""
            var sessionId: String? = null
            var readConsistency = ""
            var baseVersionHex: String? = null
            r.forEachField { f, wt ->
                when (f) {
                    1 -> namespace = r.string(wt)
                    2 -> sessionId = r.string(wt)
                    3 -> readConsistency = r.string(wt)
                    4 -> baseVersionHex = r.string(wt)
                    else -> return@forEachField false
                }
                true
            }
            WireMessage.SessionBegin(header, namespace, sessionId, readConsistency, baseVersionHex)
        }
        WireMessageType.SESSION_BEGIN_ACK -> {
            var namespace = ""
            var sessionId = ""
            var headHex = ""
            var readConsistency = ""
            var error: String? = null
            r.forEachField { f, wt ->
                when (f) {
                    1 -> namespace = r.string(wt)
                    2 -> sessionId = r.string(wt)
                    3 -> headHex = r.string(wt)
                    4 -> readConsistency = r.string(wt)
                    5 -> error = r.string(wt)
                    else -> return@forEachField false
                }
                true
            }
            WireMessage.SessionBeginAck(header, namespace, sessionId, headHex, readConsistency, error)
        }
        WireMessageType.SQL_EXEC -> {
            var namespace = ""
            var sessionId = ""
            var sql = ""
            var parametersJson: String? = null
            r.forEachField { f, wt ->
                when (f) {
                    1 -> namespace = r.string(wt)
                    2 -> sessionId = r.string(wt)
                    3 -> sql = r.string(wt)
                    4 -> parametersJson = r.string(wt)
                    else -> return@forEachField false
                }
                true
            }
            WireMessage.SqlExec(header, namespace, sessionId, sql, parametersJson)
        }
        WireMessageType.SQL_RESULT -> {
            var namespace = ""
            var sessionId = ""
            val columns = ArrayList<String>()
            val rows = ArrayList<List<String>>()
            var rowsAffected = 0
            var resolvedCommitHex = ""
            var readOnly = false
            var error: String? = null
            val generatedIds = ArrayList<String>()
            var errorCode: String? = null
            var retryAfterMs: Int? = null
            var cellKinds: String? = null
            r.forEachField { f, wt ->
                when (f) {
                    1 -> namespace = r.string(wt)
                    2 -> sessionId = r.string(wt)
                    3 -> columns.add(r.string(wt))
                    4 -> {
                        val rowStart = r.span(wt)
                        val row = BinReader(frame, rowStart, rowStart + r.lastLen)
                        val cells = ArrayList<String>()
                        while (!row.done) cells.add(row.string(WT_BYTES))
                        rows.add(cells)
                    }
                    5 -> rowsAffected = r.int(wt)
                    6 -> resolvedCommitHex = r.string(wt)
                    7 -> readOnly = r.long(wt) != 0L
                    8 -> error = r.string(wt)
                    9 -> generatedIds.add(r.string(wt))
                    10 -> errorCode = r.string(wt)
                    11 -> retryAfterMs = r.int(wt)
                    13 -> {
                        val kinds = r.bytes(wt)
                        cellKinds = CharArray(kinds.size) { '0' + (kinds[it].toInt() and 0xFF).coerceAtMost(9) }.concatToString()
                    }
                    else -> return@forEachField false
                }
                true
            }
            WireMessage.SqlResult(
                header, namespace, sessionId, columns, rows, rowsAffected, resolvedCommitHex, readOnly,
                error, generatedIds, errorCode, retryAfterMs, cellKinds,
            )
        }
        WireMessageType.SEARCH -> decodeSearch(header, r)
        WireMessageType.SEARCH_RESULT -> {
            var namespace = ""
            val hits = ArrayList<WireMessage.SearchHit>()
            var resolvedCommitHex = ""
            var error: String? = null
            var errorCode: String? = null
            var retryAfterMs: Int? = null
            r.forEachField { f, wt ->
                when (f) {
                    1 -> namespace = r.string(wt)
                    2 -> {
                        val h = r.nested(wt)
                        var docId = ""
                        var score = 0.0
                        var json: String? = null
                        h.forEachField { hf, hwt ->
                            when (hf) {
                                1 -> docId = h.string(hwt)
                                2 -> score = h.double(hwt)
                                3 -> json = h.string(hwt)
                                else -> return@forEachField false
                            }
                            true
                        }
                        hits.add(WireMessage.SearchHit(docId, score.toFloat(), json))
                    }
                    3 -> resolvedCommitHex = r.string(wt)
                    4 -> error = r.string(wt)
                    5 -> errorCode = r.string(wt)
                    6 -> retryAfterMs = r.int(wt)
                    else -> return@forEachField false
                }
                true
            }
            WireMessage.SearchResult(header, namespace, hits, resolvedCommitHex, error, errorCode, retryAfterMs)
        }
        WireMessageType.TX_COMMIT -> {
            var namespace = ""
            var sessionId = ""
            var transactionBytes = ByteArray(0)
            r.forEachField { f, wt ->
                when (f) {
                    1 -> namespace = r.string(wt)
                    2 -> sessionId = r.string(wt)
                    3 -> transactionBytes = r.bytes(wt)
                    else -> return@forEachField false
                }
                true
            }
            WireMessage.TxCommit(header, namespace, sessionId, transactionBytes)
        }
        WireMessageType.TX_ROLLBACK -> {
            var namespace = ""
            var sessionId = ""
            r.forEachField { f, wt ->
                when (f) {
                    1 -> namespace = r.string(wt)
                    2 -> sessionId = r.string(wt)
                    else -> return@forEachField false
                }
                true
            }
            WireMessage.TxRollback(header, namespace, sessionId)
        }
        WireMessageType.CONFLICT_REPORT -> {
            var namespace = ""
            var reportBytes = ByteArray(0)
            var errorCode: String? = null
            var retryAfterMs: Int? = null
            r.forEachField { f, wt ->
                when (f) {
                    1 -> namespace = r.string(wt)
                    2 -> reportBytes = r.bytes(wt)
                    3 -> errorCode = r.string(wt)
                    4 -> retryAfterMs = r.int(wt)
                    else -> return@forEachField false
                }
                true
            }
            WireMessage.ConflictReport(header, namespace, reportBytes, errorCode, retryAfterMs)
        }
        WireMessageType.DOCUMENT_GET -> {
            var namespace = ""
            var docId = ""
            r.forEachField { f, wt ->
                when (f) {
                    1 -> namespace = r.string(wt)
                    2 -> docId = r.string(wt)
                    else -> return@forEachField false
                }
                true
            }
            WireMessage.DocumentGet(header, namespace, docId)
        }
        WireMessageType.DOCUMENT_GET_RESULT -> {
            var namespace = ""
            var docId = ""
            var json: String? = null
            var commitHex = ""
            var error: String? = null
            var errorCode: String? = null
            var retryAfterMs: Int? = null
            r.forEachField { f, wt ->
                when (f) {
                    1 -> namespace = r.string(wt)
                    2 -> docId = r.string(wt)
                    3 -> json = r.string(wt)
                    4 -> commitHex = r.string(wt)
                    5 -> error = r.string(wt)
                    6 -> errorCode = r.string(wt)
                    7 -> retryAfterMs = r.int(wt)
                    else -> return@forEachField false
                }
                true
            }
            WireMessage.DocumentGetResult(header, namespace, docId, json, commitHex, error, errorCode, retryAfterMs)
        }
        WireMessageType.UPSERT -> {
            var namespace = ""
            var docId = ""
            var json = ""
            r.forEachField { f, wt ->
                when (f) {
                    1 -> namespace = r.string(wt)
                    2 -> docId = r.string(wt)
                    3 -> json = r.string(wt)
                    else -> return@forEachField false
                }
                true
            }
            WireMessage.Upsert(header, namespace, docId, json)
        }
        WireMessageType.UPSERT_RESULT -> {
            var namespace = ""
            var commitHex = ""
            var error: String? = null
            var errorCode: String? = null
            var retryAfterMs: Int? = null
            r.forEachField { f, wt ->
                when (f) {
                    1 -> namespace = r.string(wt)
                    2 -> commitHex = r.string(wt)
                    3 -> error = r.string(wt)
                    4 -> errorCode = r.string(wt)
                    5 -> retryAfterMs = r.int(wt)
                    else -> return@forEachField false
                }
                true
            }
            WireMessage.UpsertResult(header, namespace, commitHex, error, errorCode, retryAfterMs)
        }
        else -> throw WireDecodeException("message type ${header.messageType} has no binary form")
    }
}

private fun decodeSearch(header: WireHeader, r: BinReader): WireMessage.Search {
    var namespace = ""
    var sessionId: String? = null
    var text: WireMessage.SearchTextArm? = null
    var vector: WireMessage.SearchVectorArm? = null
    var fusion: String? = null
    var limit = 0
    var includeJson = false
    var atCommitHex: String? = null
    r.forEachField { f, wt ->
        when (f) {
            1 -> namespace = r.string(wt)
            2 -> sessionId = r.string(wt)
            3, 4 -> {
                val a = r.nested(wt)
                var index = ""
                var query = ""
                val values = ArrayList<Double>()
                var depth: Int? = null
                var minScore: Double? = null
                var weight: Double? = null
                a.forEachField { af, awt ->
                    when (af) {
                        1 -> index = a.string(awt)
                        2 -> if (f == 3) query = a.string(awt) else values.add(a.double(awt))
                        3 -> depth = a.int(awt)
                        4 -> minScore = a.double(awt)
                        5 -> weight = a.double(awt)
                        else -> return@forEachField false
                    }
                    true
                }
                if (f == 3) {
                    text = WireMessage.SearchTextArm(index, query, depth, minScore?.toFloat(), weight)
                } else {
                    vector = WireMessage.SearchVectorArm(index, values, depth, minScore?.toFloat(), weight)
                }
            }
            5 -> fusion = r.string(wt)
            6 -> limit = r.int(wt)
            7 -> includeJson = r.long(wt) != 0L
            8 -> atCommitHex = r.string(wt)
            else -> return@forEachField false
        }
        true
    }
    return WireMessage.Search(header, namespace, sessionId, text, vector, fusion, limit, includeJson, atCommitHex)
}
