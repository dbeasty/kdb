package dev.kdb.jdbc

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.ATTACHMENT_CHUNK_BYTES
import dev.kdb.document.ATTACHMENT_HASH_SCHEME_CHUNKED_1
import dev.kdb.document.attachmentChunkCount
import dev.kdb.document.attachmentIdentity
import dev.kdb.document.chunkLeafHash
import dev.kdb.document.verifyMerkleInclusion
import dev.kdb.wire.MAX_ATTACHMENT_CHUNK_BYTES
import dev.kdb.wire.WireBlobChunkProof
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import kotlinx.coroutines.runBlocking
import java.io.IOException
import java.io.InputStream
import java.sql.SQLException

/**
 * A verified range of one attachment ([KdbAttachments.openRange]; Go's ranged AttachmentReader).
 * Pages are whole 1 MiB chunks from a chunk-aligned offset (ATTACHMENT_GET with proofs), each chunk
 * checked against the content hash by its RFC 6962 inclusion proof before any of the page is handed
 * out; only the asked range of them is returned.
 *
 * [info] and [commitHex] describe the version the read pinned; [superseded], once the range has
 * been read (or the stream closed), says a newer version exists or the attachment was removed. A
 * bad page fails a read with an IOException whose cause is a [KdbAttachmentIntegrityException].
 */
public class KdbAttachmentRangeInputStream internal constructor(
    private val host: AttachmentHost,
    private val doc: KdbUuid,
    private val selector: KdbAttachmentSelector,
    private val asOfHex: String,
    private val offset: Long,
    private val length: Long,
) : InputStream() {
    public lateinit var info: KdbAttachmentInfo
        private set
    public var commitHex: String = ""
        private set
    public var superseded: Boolean = false
        private set

    private var readId = ""
    private var tree: KdbHash? = null
    private var end = -1L // the range's end; -1 until the first page gives the size
    private var pageOff = offset / ATTACHMENT_CHUNK_BYTES * ATTACHMENT_CHUNK_BYTES // the next page's offset
    private var next = offset // the next byte handed out
    private var page = ByteArray(0)
    private var pos = 0
    private var limit = 0
    private var eof = false
    private var closed = false

    private companion object {
        const val CLOSE_TIMEOUT_MS = 2_000L
    }

    internal fun start() {
        val maxBytes =
            if (length >= 0) minOf(maxOf(length, 1L), MAX_ATTACHMENT_CHUNK_BYTES.toLong()) else MAX_ATTACHMENT_CHUNK_BYTES.toLong()
        val byId = selector as? KdbAttachmentSelector.ById
        val res =
            try {
                runBlocking {
                    request(
                        WireMessage.AttachmentGet(
                            KdbAttachments.hdr(WireMessageType.ATTACHMENT_GET), host.namespace, host.sessionId, doc,
                            attachmentId = byId?.id?.toKdb(), name = (selector as? KdbAttachmentSelector.ByName)?.name ?: "",
                            asOfHex = asOfHex, offset = offset, maxBytes = maxBytes, proofs = true,
                        ),
                    )
                }
            } catch (e: IOException) {
                throw KdbAttachments.unwrapRead(e)
            }
        if (res.error != null && res.info.id != null && offset > res.info.size) {
            throw KdbAttachmentRangeException(offset, res.info.size)
        }
        try {
            take(res)
        } catch (e: SQLException) {
            if (readId.isNotEmpty()) runCatching { release() }
            closed = true
            throw e
        }
    }

    private suspend fun request(message: WireMessage.AttachmentGet): WireMessage.AttachmentData {
        val reply =
            try {
                host.request(message)
            } catch (e: Exception) {
                if (e is kotlinx.coroutines.CancellationException && e !is kotlinx.coroutines.TimeoutCancellationException) throw e
                throw IOException("attachment read interrupted: ${e.message}", e)
            }
        return reply as? WireMessage.AttachmentData
            ?: throw IOException("unexpected attachment reply: ${reply.header.messageType}")
    }

    /** Checks a page - every chunk against the content hash by its proof - and keeps the part of it
     * inside the range. Throws before anything of a bad page is kept. */
    private fun take(res: WireMessage.AttachmentData) {
        if (res.error != null) {
            if (res.errorCode == "INTEGRITY") throw KdbAttachmentIntegrityException(res.error!!)
            throw KdbAttachmentException(res.error!!, res.errorCode)
        }
        if (end < 0) {
            readId = res.readId
            info = res.info.toInfo()
            commitHex = res.commitHex
            tree = res.tree
            end = info.size
            if (offset > end) throw KdbAttachmentRangeException(offset, end)
            if (length >= 0 && length < end - offset) end = offset + length
            if (res.scheme != ATTACHMENT_HASH_SCHEME_CHUNKED_1) {
                throw KdbAttachmentException("attachment content hash scheme ${res.scheme} is not supported", "UNSUPPORTED")
            }
            val t = tree ?: throw KdbAttachmentIntegrityException("attachment ${info.id}: the server sent no Merkle tree hash")
            if (attachmentIdentity(t, info.size).toHex() != info.hashHex) {
                throw KdbAttachmentIntegrityException(
                    "attachment ${info.id}: tree ${t.toHex()} and size ${info.size} do not hash to its content hash ${info.hashHex}",
                )
            }
        }
        if (res.offset != pageOff || res.tree != tree) {
            throw KdbAttachmentException("attachment page at offset ${res.offset}, expected $pageOff")
        }
        verifyChunks(res.data, res.offset, info.size, res.chunks, tree!!)
        pageOff += res.data.size
        if (res.data.isEmpty() && !res.eof) throw KdbAttachmentException("empty attachment page at ${res.offset} before the end")
        if (res.eof && pageOff != info.size) {
            throw KdbAttachmentIntegrityException("attachment ${info.id} ended at byte $pageOff of ${info.size}")
        }
        val from = next - res.offset
        val to = minOf(end, pageOff) - res.offset
        page = res.data
        pos = from.toInt()
        limit = if (from < to) to.toInt() else pos
        eof = res.eof
        superseded = res.superseded
    }

    /** That [data], read from [pageOffset] (a chunk boundary) of an attachment of [size] bytes, is
     * whole chunks each proved by [chunks] against [tree], and nothing more. */
    private fun verifyChunks(
        data: ByteArray,
        pageOffset: Long,
        size: Long,
        chunks: List<WireBlobChunkProof>,
        tree: KdbHash,
    ) {
        if (pageOffset % ATTACHMENT_CHUNK_BYTES != 0L) throw KdbAttachmentException("attachment page at $pageOffset is not chunk-aligned")
        val leaves = attachmentChunkCount(size)
        val first = pageOffset / ATTACHMENT_CHUNK_BYTES
        var at = 0
        for ((i, proof) in chunks.withIndex()) {
            val idx = first + i
            if (proof.leaf != idx) throw KdbAttachmentException("attachment proof for chunk ${proof.leaf} where $idx was expected")
            val n = minOf(ATTACHMENT_CHUNK_BYTES.toLong(), size - idx * ATTACHMENT_CHUNK_BYTES)
            if (n < 0 || data.size - at < n) throw KdbAttachmentIntegrityException("attachment ${info.id}: chunk $idx is short")
            if (!verifyMerkleInclusion(chunkLeafHash(data, at, n.toInt()), idx, leaves, proof.path, tree)) {
                throw KdbAttachmentIntegrityException("attachment ${info.id}: chunk $idx does not match its content hash")
            }
            at += n.toInt()
        }
        if (at != data.size) {
            throw KdbAttachmentIntegrityException("attachment ${info.id}: ${data.size - at} bytes at ${pageOffset + at} no proof covers")
        }
    }

    private fun fill(): Boolean {
        while (pos >= limit) {
            if (closed || eof) return false
            if (pageOff >= end) {
                // The range is read, short of the attachment's end: release the pinned read, which
                // also says whether a newer version exists.
                release()
                return false
            }
            try {
                val res =
                    runBlocking {
                        request(
                            WireMessage.AttachmentGet(
                                KdbAttachments.hdr(WireMessageType.ATTACHMENT_GET), host.namespace, host.sessionId, doc,
                                offset = pageOff, maxBytes = minOf(end - pageOff, MAX_ATTACHMENT_CHUNK_BYTES.toLong()),
                                readId = readId, proofs = true,
                            ),
                        )
                    }
                take(res)
            } catch (e: SQLException) {
                throw IOException(e.message, e)
            }
        }
        return true
    }

    override fun read(): Int =
        if (fill()) {
            next++
            page[pos++].toInt() and 0xFF
        } else {
            -1
        }

    override fun read(
        b: ByteArray,
        off: Int,
        len: Int,
    ): Int {
        if (len == 0) return 0
        if (!fill()) return -1
        val n = minOf(len, limit - pos)
        System.arraycopy(page, pos, b, off, n)
        pos += n
        next += n
        return n
    }

    override fun available(): Int = limit - pos

    /** Sends the close for a read the server still pins, taking superseded from its reply. */
    private fun release() {
        if (closed) return
        closed = true
        if (eof || readId.isEmpty()) return
        val reply =
            runCatching {
                runBlocking {
                    // Briefly: the pin also lapses on its own (the server's read lease).
                    kotlinx.coroutines.withTimeoutOrNull(CLOSE_TIMEOUT_MS) {
                        host.request(
                            WireMessage.AttachmentGet(
                                KdbAttachments.hdr(WireMessageType.ATTACHMENT_GET), host.namespace, host.sessionId, doc,
                                readId = readId, close = true,
                            ),
                        )
                    }
                }
            }.getOrNull()
        if (reply is WireMessage.AttachmentData && reply.error == null) superseded = reply.superseded
    }

    override fun close() {
        release()
    }
}

/**
 * One attachment as a read-only java.sql.Blob ([KdbAttachments.blob]), pinned to one version: the
 * attachment by ID, as of the commit it was looked up at. Positions are 1-based, as JDBC has them.
 * [getBytes] and [getBinaryStream] with a position read verified ranges (they need the server's
 * `attachment-proofs` wire feature); the whole-blob [getBinaryStream] is verified against the
 * content hash. Writes, [position] and [truncate] are not supported.
 */
public class KdbAttachmentBlob internal constructor(
    private val attachments: KdbAttachments,
    private val docId: String,
    /** The attachment this blob reads. */
    public val info: KdbAttachmentInfo,
    /** The commit it reads the attachment at. */
    public val commitHex: String,
) : java.sql.Blob {
    private var freed = false

    private val selector get() = KdbAttachmentSelector.ById(info.id)

    private fun checkLive() {
        if (freed) throw SQLException("the Blob has been freed")
    }

    override fun length(): Long {
        checkLive()
        return info.size
    }

    /**
     * Up to [length] bytes from 1-based [pos]: fewer when the blob ends first, none from just past its
     * last byte. SQLException when [pos] < 1, [length] < 0 or [pos] is further past the end.
     */
    override fun getBytes(
        pos: Long,
        length: Int,
    ): ByteArray {
        checkLive()
        if (pos < 1) throw SQLException("Blob position $pos is less than 1", "22011")
        if (length < 0) throw SQLException("Blob length $length is negative", "22011")
        if (pos - 1 > info.size) throw SQLException("Blob position $pos is past its ${info.size} bytes", "22011")
        return attachments.getRange(docId, selector, commitHex.ifEmpty { null }, pos - 1, length)
    }

    /** The whole blob, verified against its content hash before its last bytes are handed out. */
    override fun getBinaryStream(): InputStream {
        checkLive()
        return attachments.open(docId, selector, commitHex.ifEmpty { null })
    }

    /**
     * [length] bytes from 1-based [pos], each chunk verified before its bytes are handed out.
     * SQLException, per JDBC, when [pos] < 1, [pos] is past the blob's last byte, [length] < 0, or
     * the range runs past the end.
     */
    override fun getBinaryStream(
        pos: Long,
        length: Long,
    ): InputStream {
        checkLive()
        if (pos < 1) throw SQLException("Blob position $pos is less than 1", "22011")
        if (pos > info.size) throw SQLException("Blob position $pos is past its ${info.size} bytes", "22011")
        if (length < 0) throw SQLException("Blob length $length is negative", "22011")
        if (pos - 1 + length > info.size) {
            throw SQLException("Blob range of $length bytes from $pos runs past its ${info.size} bytes", "22011")
        }
        return attachments.openRange(docId, selector, commitHex.ifEmpty { null }, pos - 1, length)
    }

    override fun position(
        pattern: ByteArray?,
        start: Long,
    ): Long = throw java.sql.SQLFeatureNotSupportedException("Blob.position is not supported for attachments")

    override fun position(
        pattern: java.sql.Blob?,
        start: Long,
    ): Long = throw java.sql.SQLFeatureNotSupportedException("Blob.position is not supported for attachments")

    override fun setBytes(
        pos: Long,
        bytes: ByteArray?,
    ): Int = throw readOnly()

    override fun setBytes(
        pos: Long,
        bytes: ByteArray?,
        offset: Int,
        len: Int,
    ): Int = throw readOnly()

    override fun setBinaryStream(pos: Long): java.io.OutputStream = throw readOnly()

    override fun truncate(len: Long): Unit = throw readOnly()

    override fun free() {
        freed = true
    }

    private fun readOnly() =
        java.sql.SQLFeatureNotSupportedException("an attachment Blob is read-only: write attachments with KdbAttachments.put or SQL")
}
