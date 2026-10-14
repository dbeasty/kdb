package dev.kdb.wire

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid

/*
 * Binary bodies of the attachment messages, matching go/kdb/wire/attachment_ops.go
 * (appendAttachmentBody / decodeAttachmentBody) field for field. These messages have no JSON form:
 * every codec writes them as tag-2 bodies whatever was negotiated, and only sends them to a peer
 * that agreed WIRE_FEATURE_ATTACHMENTS (or the peer capability, for BLOB_FETCH).
 *
 * As in Go, an empty string, zero integer, false, empty bytes, zero UUID and zero hash are omitted.
 */

/** The upload chunk / data page bound (Go's MaxAttachmentChunkBytes). Clients send 1 MiB chunks. */
public const val MAX_ATTACHMENT_CHUNK_BYTES: Int = 4 shl 20

/** Whether [type] is one of the always-binary attachment messages. */
internal fun attachmentBinaryForm(type: WireMessageType): Boolean =
    when (type) {
        WireMessageType.ATTACHMENT_UPLOAD, WireMessageType.ATTACHMENT_UPLOAD_RESULT,
        WireMessageType.ATTACHMENT_GET, WireMessageType.ATTACHMENT_DATA,
        WireMessageType.ATTACHMENT_LIST, WireMessageType.ATTACHMENT_LIST_RESULT,
        WireMessageType.BLOB_FETCH, WireMessageType.BLOB_FETCH_RESULT,
        -> true
        else -> false
    }

private fun BinWriter.uuid(field: Int, id: KdbUuid?) {
    if (id == null || (id.msb == 0L && id.lsb == 0L)) return
    val out = ByteArray(16)
    for (i in 0 until 8) {
        out[i] = (id.msb ushr (56 - 8 * i)).toByte()
        out[8 + i] = (id.lsb ushr (56 - 8 * i)).toByte()
    }
    bytesField(field, out)
}

private fun BinWriter.hash(field: Int, h: KdbHash?) {
    if (h == null || h.bytes.all { it == 0.toByte() }) return
    bytesField(field, h.bytes)
}

private fun BinWriter.err(field: Int, error: String?, code: String?) {
    string(field, error ?: "")
    string(field + 1, code ?: "")
}

private fun BinWriter.info(field: Int, info: WireAttachmentInfo) {
    nested(field) {
        uuid(1, info.id)
        string(2, info.name)
        hash(3, info.hash)
        int(4, info.size)
        string(5, info.mime)
    }
}

private fun BinReader.uuid(wireType: Int): KdbUuid {
    val p = bytes(wireType)
    if (p.size != 16) throw WireDecodeException("malformed binary body: uuid is not 16 bytes")
    var msb = 0L
    var lsb = 0L
    for (i in 0 until 8) {
        msb = (msb shl 8) or (p[i].toLong() and 0xFF)
        lsb = (lsb shl 8) or (p[8 + i].toLong() and 0xFF)
    }
    return KdbUuid(msb, lsb)
}

private fun BinReader.hash(wireType: Int): KdbHash {
    val p = bytes(wireType)
    if (p.size != 32) throw WireDecodeException("malformed binary body: hash is not 32 bytes")
    return KdbHash.fromBytes(p)
}

private fun BinReader.bool(wireType: Int): Boolean = long(wireType) != 0L

private fun BinReader.info(wireType: Int): WireAttachmentInfo {
    val r = nested(wireType)
    var id: KdbUuid? = null
    var name = ""
    var hash: KdbHash? = null
    var size = 0L
    var mime = ""
    r.forEachField { f, wt ->
        when (f) {
            1 -> id = r.uuid(wt)
            2 -> name = r.string(wt)
            3 -> hash = r.hash(wt)
            4 -> size = r.long(wt)
            5 -> mime = r.string(wt)
            else -> return@forEachField false
        }
        true
    }
    return WireAttachmentInfo(id, name, hash, size, mime)
}

/** Writes [message]'s body to [w] if it is an attachment message; false otherwise. */
internal fun encodeAttachmentBody(w: BinWriter, message: WireMessage): Boolean {
    when (val m = message) {
        is WireMessage.AttachmentUpload -> {
            w.string(1, m.namespace)
            w.string(2, m.sessionId)
            w.string(3, m.uploadId)
            w.int(4, m.offset)
            w.bytes(5, m.data)
            w.bool(6, m.final)
            w.hash(7, m.expect)
        }
        is WireMessage.AttachmentUploadResult -> {
            w.string(1, m.namespace)
            w.string(2, m.uploadId)
            w.int(3, m.received)
            w.hash(4, m.hash)
            w.int(5, m.size)
            w.err(6, m.error, m.errorCode)
        }
        is WireMessage.AttachmentGet -> {
            w.string(1, m.namespace)
            w.string(2, m.sessionId)
            w.uuid(3, m.docId)
            w.uuid(4, m.attachmentId)
            w.string(5, m.name)
            w.string(6, m.asOfHex)
            w.int(7, m.offset)
            w.int(8, m.maxBytes)
            w.string(9, m.readId)
            w.bool(10, m.close)
        }
        is WireMessage.AttachmentData -> {
            w.string(1, m.namespace)
            w.string(2, m.readId)
            w.uuid(3, m.docId)
            w.info(4, m.info)
            w.string(5, m.commitHex)
            w.int(6, m.offset)
            w.bytes(7, m.data)
            w.bool(8, m.eof)
            w.bool(9, m.superseded)
            w.err(10, m.error, m.errorCode)
        }
        is WireMessage.AttachmentList -> {
            w.string(1, m.namespace)
            w.string(2, m.sessionId)
            w.uuid(3, m.docId)
            w.string(4, m.asOfHex)
        }
        is WireMessage.AttachmentListResult -> {
            w.string(1, m.namespace)
            w.string(2, m.commitHex)
            for (item in m.items) w.info(3, item)
            w.err(4, m.error, m.errorCode)
        }
        is WireMessage.BlobFetch -> {
            w.string(1, m.namespace)
            for (e in m.entries) {
                w.nested(2) {
                    uuid(1, e.docId)
                    uuid(2, e.attachmentId)
                    string(3, e.commitHex)
                    hash(4, e.hash)
                    int(5, e.offset)
                    int(6, e.maxBytes)
                }
            }
            w.int(3, m.maxBytes)
        }
        is WireMessage.BlobFetchResult -> {
            w.string(1, m.namespace)
            for (p in m.parts) {
                w.nested(2) {
                    int(1, p.index.toLong())
                    hash(2, p.hash)
                    int(3, p.size)
                    hash(4, p.tree)
                    int(5, p.offset)
                    bytes(6, p.data)
                    for (c in p.chunks) {
                        nested(7) {
                            int(1, c.leaf)
                            // Proof hashes are list elements: written whatever their value.
                            for (h in c.path) bytesField(2, h.bytes)
                        }
                    }
                    bool(8, p.eof)
                    bool(9, p.superseded)
                    err(10, p.error, p.errorCode)
                }
            }
            w.bool(3, m.more)
            w.err(4, m.error, m.errorCode)
        }
        else -> return false
    }
    return true
}

/** Decodes an attachment message's body; null if [header] names some other message type. */
internal fun decodeAttachmentBody(header: WireHeader, r: BinReader): WireMessage? =
    when (header.messageType) {
        WireMessageType.ATTACHMENT_UPLOAD -> {
            var namespace = ""
            var sessionId = ""
            var uploadId = ""
            var offset = 0L
            var data = ByteArray(0)
            var final = false
            var expect: KdbHash? = null
            r.forEachField { f, wt ->
                when (f) {
                    1 -> namespace = r.string(wt)
                    2 -> sessionId = r.string(wt)
                    3 -> uploadId = r.string(wt)
                    4 -> offset = r.long(wt)
                    5 -> data = r.bytes(wt)
                    6 -> final = r.bool(wt)
                    7 -> expect = r.hash(wt)
                    else -> return@forEachField false
                }
                true
            }
            WireMessage.AttachmentUpload(header, namespace, sessionId, uploadId, offset, data, final, expect)
        }
        WireMessageType.ATTACHMENT_UPLOAD_RESULT -> {
            var namespace = ""
            var uploadId = ""
            var received = 0L
            var hash: KdbHash? = null
            var size = 0L
            var error: String? = null
            var errorCode: String? = null
            r.forEachField { f, wt ->
                when (f) {
                    1 -> namespace = r.string(wt)
                    2 -> uploadId = r.string(wt)
                    3 -> received = r.long(wt)
                    4 -> hash = r.hash(wt)
                    5 -> size = r.long(wt)
                    6 -> error = r.string(wt)
                    7 -> errorCode = r.string(wt)
                    else -> return@forEachField false
                }
                true
            }
            WireMessage.AttachmentUploadResult(header, namespace, uploadId, received, hash, size, error, errorCode)
        }
        WireMessageType.ATTACHMENT_GET -> {
            var namespace = ""
            var sessionId = ""
            var docId: KdbUuid? = null
            var attachmentId: KdbUuid? = null
            var name = ""
            var asOfHex = ""
            var offset = 0L
            var maxBytes = 0L
            var readId = ""
            var close = false
            r.forEachField { f, wt ->
                when (f) {
                    1 -> namespace = r.string(wt)
                    2 -> sessionId = r.string(wt)
                    3 -> docId = r.uuid(wt)
                    4 -> attachmentId = r.uuid(wt)
                    5 -> name = r.string(wt)
                    6 -> asOfHex = r.string(wt)
                    7 -> offset = r.long(wt)
                    8 -> maxBytes = r.long(wt)
                    9 -> readId = r.string(wt)
                    10 -> close = r.bool(wt)
                    else -> return@forEachField false
                }
                true
            }
            WireMessage.AttachmentGet(
                header, namespace, sessionId, docId, attachmentId, name, asOfHex, offset, maxBytes, readId, close,
            )
        }
        WireMessageType.ATTACHMENT_DATA -> {
            var namespace = ""
            var readId = ""
            var docId: KdbUuid? = null
            var info = WireAttachmentInfo()
            var commitHex = ""
            var offset = 0L
            var data = ByteArray(0)
            var eof = false
            var superseded = false
            var error: String? = null
            var errorCode: String? = null
            r.forEachField { f, wt ->
                when (f) {
                    1 -> namespace = r.string(wt)
                    2 -> readId = r.string(wt)
                    3 -> docId = r.uuid(wt)
                    4 -> info = r.info(wt)
                    5 -> commitHex = r.string(wt)
                    6 -> offset = r.long(wt)
                    7 -> data = r.bytes(wt)
                    8 -> eof = r.bool(wt)
                    9 -> superseded = r.bool(wt)
                    10 -> error = r.string(wt)
                    11 -> errorCode = r.string(wt)
                    else -> return@forEachField false
                }
                true
            }
            WireMessage.AttachmentData(
                header, namespace, readId, docId, info, commitHex, offset, data, eof, superseded, error, errorCode,
            )
        }
        WireMessageType.ATTACHMENT_LIST -> {
            var namespace = ""
            var sessionId = ""
            var docId: KdbUuid? = null
            var asOfHex = ""
            r.forEachField { f, wt ->
                when (f) {
                    1 -> namespace = r.string(wt)
                    2 -> sessionId = r.string(wt)
                    3 -> docId = r.uuid(wt)
                    4 -> asOfHex = r.string(wt)
                    else -> return@forEachField false
                }
                true
            }
            WireMessage.AttachmentList(header, namespace, sessionId, docId, asOfHex)
        }
        WireMessageType.ATTACHMENT_LIST_RESULT -> {
            var namespace = ""
            var commitHex = ""
            val items = ArrayList<WireAttachmentInfo>()
            var error: String? = null
            var errorCode: String? = null
            r.forEachField { f, wt ->
                when (f) {
                    1 -> namespace = r.string(wt)
                    2 -> commitHex = r.string(wt)
                    3 -> items.add(r.info(wt))
                    4 -> error = r.string(wt)
                    5 -> errorCode = r.string(wt)
                    else -> return@forEachField false
                }
                true
            }
            WireMessage.AttachmentListResult(header, namespace, commitHex, items, error, errorCode)
        }
        WireMessageType.BLOB_FETCH -> {
            var namespace = ""
            val entries = ArrayList<WireBlobFetchEntry>()
            var maxBytes = 0L
            r.forEachField { f, wt ->
                when (f) {
                    1 -> namespace = r.string(wt)
                    2 -> entries.add(decodeBlobFetchEntry(r.nested(wt)))
                    3 -> maxBytes = r.long(wt)
                    else -> return@forEachField false
                }
                true
            }
            WireMessage.BlobFetch(header, namespace, entries, maxBytes)
        }
        WireMessageType.BLOB_FETCH_RESULT -> {
            var namespace = ""
            val parts = ArrayList<WireBlobFetchPart>()
            var more = false
            var error: String? = null
            var errorCode: String? = null
            r.forEachField { f, wt ->
                when (f) {
                    1 -> namespace = r.string(wt)
                    2 -> parts.add(decodeBlobPart(r.nested(wt)))
                    3 -> more = r.bool(wt)
                    4 -> error = r.string(wt)
                    5 -> errorCode = r.string(wt)
                    else -> return@forEachField false
                }
                true
            }
            WireMessage.BlobFetchResult(header, namespace, parts, more, error, errorCode)
        }
        else -> null
    }

private fun decodeBlobFetchEntry(r: BinReader): WireBlobFetchEntry {
    var docId: KdbUuid? = null
    var attachmentId: KdbUuid? = null
    var commitHex = ""
    var hash: KdbHash? = null
    var offset = 0L
    var maxBytes = 0L
    r.forEachField { f, wt ->
        when (f) {
            1 -> docId = r.uuid(wt)
            2 -> attachmentId = r.uuid(wt)
            3 -> commitHex = r.string(wt)
            4 -> hash = r.hash(wt)
            5 -> offset = r.long(wt)
            6 -> maxBytes = r.long(wt)
            else -> return@forEachField false
        }
        true
    }
    return WireBlobFetchEntry(docId, attachmentId, commitHex, hash, offset, maxBytes)
}

private fun decodeBlobPart(r: BinReader): WireBlobFetchPart {
    var index = 0
    var hash: KdbHash? = null
    var size = 0L
    var tree: KdbHash? = null
    var offset = 0L
    var data = ByteArray(0)
    val chunks = ArrayList<WireBlobChunkProof>()
    var eof = false
    var superseded = false
    var error: String? = null
    var errorCode: String? = null
    r.forEachField { f, wt ->
        when (f) {
            1 -> index = r.int(wt)
            2 -> hash = r.hash(wt)
            3 -> size = r.long(wt)
            4 -> tree = r.hash(wt)
            5 -> offset = r.long(wt)
            6 -> data = r.bytes(wt)
            7 -> {
                val c = r.nested(wt)
                var leaf = 0L
                val path = ArrayList<KdbHash>()
                c.forEachField { cf, cwt ->
                    when (cf) {
                        1 -> leaf = c.long(cwt)
                        2 -> path.add(c.hash(cwt))
                        else -> return@forEachField false
                    }
                    true
                }
                chunks.add(WireBlobChunkProof(leaf, path))
            }
            8 -> eof = r.bool(wt)
            9 -> superseded = r.bool(wt)
            10 -> error = r.string(wt)
            11 -> errorCode = r.string(wt)
            else -> return@forEachField false
        }
        true
    }
    return WireBlobFetchPart(index, hash, size, tree, offset, data, chunks, eof, superseded, error, errorCode)
}
