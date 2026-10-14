package dev.kdb.jdbc

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.TransactionWireCodec
import dev.kdb.wire.WireAttachmentInfo
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import kotlinx.coroutines.runBlocking
import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.InputStream
import java.io.PushbackInputStream
import java.sql.SQLException
import java.util.UUID

/** One attachment of a document. [hashHex] is its scheme-1 content identity. */
public data class KdbAttachmentInfo(
    val id: UUID,
    val name: String,
    val hashHex: String,
    val size: Long,
    val mime: String,
)

/** An uploaded blob: what an attachment put, or a `$attachment` SQL parameter, references. */
public data class KdbBlobRef(
    val hashHex: String,
    val size: Long,
)

/** Where an interrupted upload stands: pass it to [KdbAttachments.upload] to resume. */
public data class KdbUploadState(
    val uploadId: String,
    val received: Long,
)

/** The result of a committed attachment change. */
public data class KdbAttachmentCommit(
    val info: KdbAttachmentInfo,
    val commitHex: String,
)

/** Names one attachment of a document, by ID or by name. */
public sealed class KdbAttachmentSelector {
    public data class ById(val id: UUID) : KdbAttachmentSelector()

    public data class ByName(val name: String) : KdbAttachmentSelector()

    internal fun find(items: List<KdbAttachmentInfo>): KdbAttachmentInfo? =
        when (this) {
            is ById -> items.firstOrNull { it.id == id }
            is ByName -> items.firstOrNull { it.name == name }
        }

    public companion object {
        @JvmStatic
        public fun byId(id: UUID): KdbAttachmentSelector = ById(id)

        @JvmStatic
        public fun byName(name: String): KdbAttachmentSelector = ByName(name)
    }
}

/**
 * How a put picks the attachment it writes - Go's embed PutAttachmentOptions: with an [id], that
 * attachment is created or replaced (keeping its name unless [name] is given); with only a [name],
 * the attachment of that name is replaced, or one is created with a fresh ID; with neither, one is
 * created with a fresh ID, named after it. [mime] is the media type stored with the bytes.
 */
public data class KdbPutAttachmentOptions
    @JvmOverloads
    constructor(
        val id: UUID? = null,
        val name: String? = null,
        val mime: String = "",
    )

/**
 * A refusal from the attachment API: the server's error and its typed code (NOT_FOUND, CONFLICT,
 * UNAUTHORIZED, UNSUPPORTED, INTEGRITY, ...). For an interrupted upload, [uploadState] says where to
 * resume. INTEGRITY is always the subclass [KdbAttachmentCorruptException].
 */
public open class KdbAttachmentException(
    message: String,
    public val errorCode: String? = null,
    public val uploadState: KdbUploadState? = null,
    cause: Throwable? = null,
) : SQLException(message, sqlStateFor(errorCode), cause)

/**
 * Attachment bytes do not match their content hash, so the copy they came from is unusable: the
 * server refused to serve its stored copy (wire code INTEGRITY - from a read's first page, a later
 * page, or SQL `ATTACHMENT()`), or a download failed this driver's own verification. Retrying the
 * same server cannot help; read from another replica, or have this one repaired. SQLState XX001
 * (data_corrupted). Go's `client.ErrAttachmentCorrupt`, TS's `KdbAttachmentCorruptError`.
 */
public class KdbAttachmentCorruptException(
    message: String,
    cause: Throwable? = null,
) : KdbAttachmentException(message, INTEGRITY, cause = cause) {
    internal companion object {
        const val INTEGRITY = "INTEGRITY"
    }
}

/** The exception a server's (error, errorCode) reply turns into. */
internal fun attachmentException(
    message: String,
    code: String?,
): KdbAttachmentException =
    if (code == KdbAttachmentCorruptException.INTEGRITY) KdbAttachmentCorruptException(message) else KdbAttachmentException(message, code)

/**
 * A ranged read starting past the attachment's end (Go's AttachmentRangeError). [size] is the
 * attachment's length, or -1 when the offset (negative) was refused before the server was asked.
 */
public class KdbAttachmentRangeException(
    public val offset: Long,
    public val size: Long,
) : KdbAttachmentException(
        if (size < 0) "attachment offset $offset is negative" else "attachment offset $offset is past its $size bytes",
    ) {
    /** SQLSTATE 22011, substring error. */
    override fun getSQLState(): String = "22011"
}

private fun sqlStateFor(code: String?): String? =
    when (code) {
        "CONFLICT" -> "40001"
        "NOT_FOUND" -> "02000"
        "UNAUTHORIZED" -> "42501"
        "UNSUPPORTED" -> "0A000"
        "INTEGRITY" -> "XX001"
        else -> null
    }

/** What [KdbAttachments] needs from its connection. */
internal interface AttachmentHost {
    val namespace: String
    val sessionId: String
    val attachmentsAgreed: Boolean

    /** Whether the server agreed `attachment-proofs` too: verified ranged reads. */
    val attachmentProofsAgreed: Boolean
    val inTransaction: Boolean
    val authorNodeId: KdbUuid

    fun checkWritable()

    suspend fun request(message: WireMessage): WireMessage
}

/**
 * The attachment API of a network JDBC connection (`connection.unwrap(KdbAttachments::class.java)`),
 * speaking ATTACHMENT_UPLOAD / GET / LIST (wire feature `attachments`) to a server that agreed it.
 *
 * - Uploads stream in 1 MiB chunks and are verified: the client hashes what it sends and checks the
 *   server's content hash against it. An interrupted upload throws a [KdbAttachmentException] whose
 *   [KdbAttachmentException.uploadState] resumes it.
 * - Downloads ([open]) stream page by page from a read that pins the version it started on, and a
 *   whole-attachment read is verified against the attachment's content hash before its last bytes
 *   are handed out.
 * - Ranged reads ([openRange], [readAt], [getRange], and [blob]'s positioned reads; wire feature
 *   `attachment-proofs`) fetch whole chunks with their Merkle inclusion proofs and verify each chunk
 *   before handing out any of its bytes, without downloading the rest of the attachment.
 * - [put], [rename] and [delete] commit at once, as their own transaction (an AttachmentPut or
 *   AttachmentDelete op through TX_COMMIT); inside a manual JDBC transaction use SQL instead
 *   (`UPDATE .. SET ATTACHMENT 'name' = STREAM ?`), which commits with it.
 *
 * Document IDs are the documents' `_id` UUID strings.
 */
public class KdbAttachments internal constructor(
    private val host: AttachmentHost,
) {
    /** Download page size asked of the server (it caps pages at 4 MiB). */
    public var pageBytes: Int = DEFAULT_PAGE_BYTES

    /** Stores [input]'s bytes in the namespace's attachment store and returns their content hash
     * and size. Nothing references the blob until a put (or a statement binding it) commits. With
     * [resume], continues an interrupted upload: [input] must again start at byte 0 - the bytes the
     * server already has are read and hashed locally, not resent. */
    @JvmOverloads
    public fun upload(
        input: InputStream,
        resume: KdbUploadState? = null,
    ): KdbBlobRef = runBlocking { uploadSuspend(input, -1, resume) }

    public fun upload(bytes: ByteArray): KdbBlobRef = upload(ByteArrayInputStream(bytes))

    /** Uploads [input] and commits it as one of [docId]'s attachments (see [KdbPutAttachmentOptions]). */
    @JvmOverloads
    public fun put(
        docId: String,
        input: InputStream,
        options: KdbPutAttachmentOptions = KdbPutAttachmentOptions(),
    ): KdbAttachmentCommit =
        runBlocking {
            checkPutAllowed()
            putRefSuspend(docId, uploadSuspend(input, -1, null), options)
        }

    @JvmOverloads
    public fun put(
        docId: String,
        bytes: ByteArray,
        options: KdbPutAttachmentOptions = KdbPutAttachmentOptions(),
    ): KdbAttachmentCommit = put(docId, ByteArrayInputStream(bytes), options)

    /** Commits an already uploaded blob as one of [docId]'s attachments. */
    @JvmOverloads
    public fun putRef(
        docId: String,
        blob: KdbBlobRef,
        options: KdbPutAttachmentOptions = KdbPutAttachmentOptions(),
    ): KdbAttachmentCommit = runBlocking { putRefSuspend(docId, blob, options) }

    /** [docId]'s attachments, sorted by name, as of [asOfHex] (null: the read head). */
    @JvmOverloads
    public fun list(
        docId: String,
        asOfHex: String? = null,
    ): List<KdbAttachmentInfo> = runBlocking { listSuspend(parseDoc(docId), asOfHex ?: "").first }

    @JvmOverloads
    public fun count(
        docId: String,
        asOfHex: String? = null,
    ): Int = list(docId, asOfHex).size

    /** One attachment's metadata, or null when [docId] has none matching [selector]. */
    @JvmOverloads
    public fun info(
        docId: String,
        selector: KdbAttachmentSelector,
        asOfHex: String? = null,
    ): KdbAttachmentInfo? = selector.find(list(docId, asOfHex))

    /**
     * Opens one attachment for reading, from [offset]. The stream pins the version it opened: it
     * keeps reading those bytes even if a newer version commits, and says so afterwards
     * ([KdbAttachmentInputStream.superseded]). A read from offset 0 is verified against the content
     * hash; a ranged read only against the size. Close it to release the server's pin early.
     */
    @JvmOverloads
    public fun open(
        docId: String,
        selector: KdbAttachmentSelector,
        asOfHex: String? = null,
        offset: Long = 0,
    ): KdbAttachmentInputStream {
        requireAgreed()
        return KdbAttachmentInputStream(host, parseDoc(docId), selector, asOfHex ?: "", offset, pageBytes).also {
            it.start()
        }
    }

    /**
     * Opens [length] bytes of one attachment from [offset] ([length] -1: to the end; a range running
     * past the end stops there), as of [asOfHex] (null: the read head) - Go's OpenAttachmentRange.
     * Every chunk the range touches is checked against the content hash with its RFC 6962 inclusion
     * proof before any of its bytes is handed out, so a mismatch is a [KdbAttachmentCorruptException]
     * with no bad byte returned; the rest of the attachment is never downloaded. The stream pins its
     * version like [open], and reports [KdbAttachmentRangeInputStream.superseded] once the range has
     * been read.
     *
     * An offset past the end is a [KdbAttachmentRangeException]; the server must have agreed the
     * `attachment-proofs` wire feature (else SQLFeatureNotSupportedException).
     */
    public fun openRange(
        docId: String,
        selector: KdbAttachmentSelector,
        asOfHex: String?,
        offset: Long,
        length: Long,
    ): KdbAttachmentRangeInputStream {
        requireAgreed()
        if (!host.attachmentProofsAgreed) throw java.sql.SQLFeatureNotSupportedException(PROOFS_NOT_AGREED)
        if (offset < 0) throw KdbAttachmentRangeException(offset, -1)
        if (length < -1) throw SQLException("invalid attachment range length $length")
        val doc = parseDoc(docId)
        return KdbAttachmentRangeInputStream(host, doc, selector, asOfHex ?: "", offset, length).also { it.start() }
    }

    /**
     * Reads up to [length] bytes of one attachment from [offset], verified (see [openRange]): fewer
     * only when the attachment ends first, none at its end.
     */
    public fun getRange(
        docId: String,
        selector: KdbAttachmentSelector,
        asOfHex: String?,
        offset: Long,
        length: Int,
    ): ByteArray {
        if (length < 0) throw SQLException("invalid attachment range length $length")
        return openRange(docId, selector, asOfHex, offset, length.toLong()).use { s ->
            try {
                s.readAllBytes()
            } catch (e: IOException) {
                throw unwrapRead(e)
            }
        }
    }

    /**
     * Reads into [buffer] (all of it) from [position] of one attachment, as of [asOfHex] (null: the
     * head) - Go's ReadAttachmentAt, like a positional read: returns how many bytes were read, fewer
     * than buffer.size only when the attachment ends first (0 at its end). Each call is its own
     * read, so to read one version across calls pass a commit as [asOfHex] (a stream's
     * [KdbAttachmentRangeInputStream.commitHex], say). A position past the end is a
     * [KdbAttachmentRangeException].
     */
    public fun readAt(
        docId: String,
        selector: KdbAttachmentSelector,
        asOfHex: String?,
        buffer: ByteArray,
        position: Long,
    ): Int =
        openRange(docId, selector, asOfHex, position, buffer.size.toLong()).use { s ->
            try {
                readFully(s, buffer, buffer.size)
            } catch (e: IOException) {
                throw unwrapRead(e)
            }
        }

    /**
     * One attachment as a read-only java.sql.Blob, pinned to the version current at [asOfHex] (null:
     * the read head) as of this call: every read of it sees those bytes. [java.sql.Blob.getBytes] and
     * [java.sql.Blob.getBinaryStream] with a position read verified ranges ([openRange]); the
     * whole-blob stream is verified against the content hash ([open]). Null when [docId] has no
     * attachment matching [selector].
     */
    @JvmOverloads
    public fun blob(
        docId: String,
        selector: KdbAttachmentSelector,
        asOfHex: String? = null,
    ): KdbAttachmentBlob? {
        val (items, commit) = runBlocking { listSuspend(parseDoc(docId), asOfHex ?: "") }
        val info = selector.find(items) ?: return null
        return KdbAttachmentBlob(this, docId, info, commit.ifEmpty { asOfHex ?: "" })
    }

    /** One attachment's bytes, whole and verified. Large attachments are better read with [open]. */
    @JvmOverloads
    public fun get(
        docId: String,
        selector: KdbAttachmentSelector,
        asOfHex: String? = null,
    ): ByteArray = open(docId, selector, asOfHex).use { it.readAllBytes() }

    /** Gives one attachment a new name, keeping its ID and bytes. */
    public fun rename(
        docId: String,
        selector: KdbAttachmentSelector,
        newName: String,
    ): KdbAttachmentCommit =
        runBlocking {
            checkPutAllowed()
            validateName(newName)
            val doc = parseDoc(docId)
            val (items, head) = listSuspend(doc, "")
            val a = selector.find(items) ?: throw KdbAttachmentException("no attachment $selector on $docId", "NOT_FOUND")
            val renamed = a.copy(name = newName)
            KdbAttachmentCommit(renamed, commit(head, listOf(putOp(doc, renamed))))
        }

    /** Removes one attachment; false (and no commit) when [docId] has none matching [selector]. */
    public fun delete(
        docId: String,
        selector: KdbAttachmentSelector,
    ): Boolean =
        runBlocking {
            checkPutAllowed()
            val doc = parseDoc(docId)
            val (items, head) = listSuspend(doc, "")
            val a = selector.find(items) ?: return@runBlocking false
            commit(head, listOf(KdbOp.AttachmentDelete(doc, a.id.toKdb())))
            true
        }

    internal suspend fun uploadSuspend(
        input: InputStream,
        length: Long,
        resume: KdbUploadState?,
    ): KdbBlobRef {
        requireAgreed()
        val hash = AttachmentContentHash()
        val source = PushbackInputStream(input, 1)
        var uploadId = resume?.uploadId ?: ""
        var received = 0L
        val buf = ByteArray(AttachmentContentHash.CHUNK_BYTES)
        if (resume != null && resume.received > 0) {
            // The server has these: hash them locally so the result can still be verified.
            var left = resume.received
            while (left > 0) {
                val n = source.read(buf, 0, minOf(left, buf.size.toLong()).toInt())
                if (n < 0) throw KdbAttachmentException("resumed upload: the input ended before byte ${resume.received}")
                hash.update(buf, 0, n)
                left -= n
            }
            received = resume.received
        }
        while (true) {
            val want = if (length >= 0) minOf(buf.size.toLong(), length - received).toInt() else buf.size
            val n = readFully(source, buf, want)
            hash.update(buf, 0, n)
            val final =
                when {
                    length >= 0 && received + n == length -> true
                    n < want -> {
                        if (length >= 0) throw KdbAttachmentException("the input ended at byte ${received + n} of $length")
                        true
                    }
                    else -> {
                        val b = source.read()
                        if (b >= 0) source.unread(b)
                        b < 0
                    }
                }
            var data = buf.copyOf(n)
            while (true) {
                val reply = sendChunk(uploadId, received, data, final)
                if (reply.error != null) {
                    val r = reply.received
                    // A chunk whose reply was lost, partly or wholly stored: send only what is missing.
                    if (reply.errorCode == "CONFLICT" && uploadId.isNotEmpty() && r > received && r <= received + data.size) {
                        data = data.copyOfRange((r - received).toInt(), data.size)
                        received = r
                        if (data.isEmpty() && !final) break
                        continue
                    }
                    throw KdbAttachmentException(
                        "upload failed: ${reply.error}", reply.errorCode,
                        KdbUploadState(uploadId, if (uploadId.isNotEmpty()) maxOf(r, 0) else 0),
                    )
                }
                if (reply.uploadId.isNotEmpty()) uploadId = reply.uploadId
                if (reply.received != received + data.size) {
                    throw KdbAttachmentException(
                        "upload out of step: the server has ${reply.received} bytes, the client sent ${received + data.size}",
                        "CONFLICT", KdbUploadState(uploadId, reply.received),
                    )
                }
                received = reply.received
                if (final) {
                    val local = hash.finishHex()
                    val remote = reply.hash?.toHex() ?: ""
                    if (remote != local || reply.size != hash.size) {
                        throw KdbAttachmentException(
                            "upload verification failed: the server stored $remote (${reply.size} bytes), " +
                                "the client sent $local (${hash.size} bytes)",
                        )
                    }
                    return KdbBlobRef(local, hash.size)
                }
                break
            }
        }
    }

    private suspend fun sendChunk(
        uploadId: String,
        offset: Long,
        data: ByteArray,
        final: Boolean,
    ): WireMessage.AttachmentUploadResult {
        val reply =
            try {
                host.request(
                    WireMessage.AttachmentUpload(
                        hdr(WireMessageType.ATTACHMENT_UPLOAD), host.namespace, host.sessionId, uploadId, offset, data, final,
                    ),
                )
            } catch (e: kotlinx.coroutines.CancellationException) {
                if (e is kotlinx.coroutines.TimeoutCancellationException) {
                    throw KdbAttachmentException("upload interrupted: ${e.message}", null, KdbUploadState(uploadId, offset), e)
                }
                throw e
            } catch (e: Exception) {
                throw KdbAttachmentException("upload interrupted: ${e.message}", null, KdbUploadState(uploadId, offset), e)
            }
        return reply as? WireMessage.AttachmentUploadResult
            ?: throw KdbAttachmentException("unexpected upload reply: ${reply.header.messageType}")
    }

    private suspend fun putRefSuspend(
        docId: String,
        blob: KdbBlobRef,
        options: KdbPutAttachmentOptions,
    ): KdbAttachmentCommit {
        checkPutAllowed()
        options.name?.let { validateName(it) }
        val doc = parseDoc(docId)
        val (items, head) = listSuspend(doc, "")
        val existing =
            when {
                options.id != null -> items.firstOrNull { it.id == options.id }
                options.name != null -> items.firstOrNull { it.name == options.name }
                else -> null
            }
        val id = existing?.id ?: options.id ?: UUID.randomUUID()
        val name = options.name ?: existing?.name ?: id.toString()
        val info = KdbAttachmentInfo(id, name, blob.hashHex, blob.size, options.mime)
        return KdbAttachmentCommit(info, commit(head, listOf(putOp(doc, info))))
    }

    internal suspend fun listSuspend(
        doc: KdbUuid,
        asOfHex: String,
    ): Pair<List<KdbAttachmentInfo>, String> {
        requireAgreed()
        val reply =
            host.request(WireMessage.AttachmentList(hdr(WireMessageType.ATTACHMENT_LIST), host.namespace, host.sessionId, doc, asOfHex))
        val r = reply as? WireMessage.AttachmentListResult
            ?: throw KdbAttachmentException("unexpected list reply: ${reply.header.messageType}")
        if (r.error != null) throw attachmentException(r.error!!, r.errorCode)
        return r.items.map { it.toInfo() } to r.commitHex
    }

    private suspend fun commit(
        headHex: String,
        ops: List<KdbOp>,
    ): String {
        val base = if (headHex.isEmpty()) KdbHash(ByteArray(32)) else KdbHash.fromHex(headHex)
        val tx = KdbTransaction(KdbUuid.random(), base, ops, KdbTimestamp.now(), host.authorNodeId)
        val reply =
            host.request(
                WireMessage.TxCommit(hdr(WireMessageType.TX_COMMIT), host.namespace, host.sessionId, TransactionWireCodec.encode(tx)),
            )
        return when (reply) {
            is WireMessage.SqlResult -> {
                if (reply.error != null) throw attachmentException(reply.error!!, reply.errorCode)
                reply.resolvedCommitHex
            }
            is WireMessage.ConflictReport -> throw KdbAttachmentException("transaction conflict", "CONFLICT")
            else -> throw KdbAttachmentException("unexpected commit reply: ${reply.header.messageType}")
        }
    }

    private fun putOp(
        doc: KdbUuid,
        info: KdbAttachmentInfo,
    ): KdbOp.AttachmentPut =
        KdbOp.AttachmentPut(
            doc, info.id.toKdb(), info.name, AttachmentContentHash.SCHEME, KdbHash.fromHex(info.hashHex), info.size, info.mime,
        )

    private fun checkPutAllowed() {
        requireAgreed()
        host.checkWritable()
        if (host.inTransaction) {
            throw SQLException(
                "attachment API writes commit on their own; inside a transaction use SQL " +
                    "(UPDATE .. SET ATTACHMENT 'name' = STREAM ?), which commits with it",
            )
        }
    }

    private fun requireAgreed() {
        if (!host.attachmentsAgreed) throw java.sql.SQLFeatureNotSupportedException(ATTACHMENTS_NOT_AGREED)
    }

    internal companion object {
        const val DEFAULT_PAGE_BYTES: Int = 1 shl 20
        const val ATTACHMENTS_NOT_AGREED: String =
            "the server did not agree the 'attachments' wire feature: it cannot store or serve attachments"
        const val PROOFS_NOT_AGREED: String =
            "the server did not agree the 'attachment-proofs' wire feature: it cannot serve verified ranged reads"

        /** A stream read's IOException as the typed SQLException it wraps, when it wraps one. */
        fun unwrapRead(e: IOException): SQLException =
            e.cause as? SQLException ?: KdbAttachmentException(e.message ?: "attachment read failed", cause = e)

        fun hdr(type: WireMessageType): WireHeader = WireHeader(type, KDB_WIRE_PROTOCOL_VERSION, 0, 0)

        fun parseDoc(docId: String): KdbUuid =
            try {
                KdbUuid.fromString(docId)
            } catch (e: IllegalArgumentException) {
                throw SQLException("invalid document id '$docId': ${e.message}")
            }

        fun validateName(name: String) {
            if (name.isEmpty()) throw SQLException("attachment name is empty")
            if (name.encodeToByteArray().size > 255) throw SQLException("attachment name is over 255 bytes")
            if (name.any { it.code < 0x20 || it.code == 0x7f }) throw SQLException("attachment name contains a control character")
        }

        fun readFully(
            input: InputStream,
            buf: ByteArray,
            want: Int,
        ): Int {
            var n = 0
            while (n < want) {
                val r = input.read(buf, n, want - n)
                if (r < 0) break
                n += r
            }
            return n
        }
    }
}

internal fun UUID.toKdb(): KdbUuid = KdbUuid(mostSignificantBits, leastSignificantBits)

internal fun KdbUuid.toJava(): UUID = UUID(msb, lsb)

internal fun WireAttachmentInfo.toInfo(): KdbAttachmentInfo =
    KdbAttachmentInfo((id ?: KdbUuid(0, 0)).toJava(), name, hash?.toHex() ?: "", size, mime)

/**
 * One attachment, read page by page (ATTACHMENT_GET / ATTACHMENT_DATA). [info] and [commitHex]
 * describe the version the read pinned; [superseded] is set once the last page says a newer version
 * exists (or the attachment was removed) since the read began.
 */
public class KdbAttachmentInputStream internal constructor(
    private val host: AttachmentHost,
    private val doc: KdbUuid,
    private val selector: KdbAttachmentSelector,
    private val asOfHex: String,
    private val startOffset: Long,
    private val pageBytes: Int,
) : InputStream() {
    public lateinit var info: KdbAttachmentInfo
        private set
    public var commitHex: String = ""
        private set
    public var superseded: Boolean = false
        private set

    private var readId = ""
    private var page = ByteArray(0)
    private var pos = 0
    private var next = startOffset
    private var eof = false
    private var closed = false
    private val hash = if (startOffset == 0L) AttachmentContentHash() else null

    private companion object {
        const val CLOSE_TIMEOUT_MS = 2_000L
    }

    internal fun start() {
        val first =
            try {
                runBlocking { fetch() }
            } catch (e: IOException) {
                (e.cause as? SQLException)?.let { throw it }
                throw KdbAttachmentException(e.message ?: "attachment read failed", cause = e)
            }
        accept(first)
    }

    private suspend fun fetch(): WireMessage.AttachmentData {
        val byId = selector as? KdbAttachmentSelector.ById
        val reply =
            try {
                host.request(
                    WireMessage.AttachmentGet(
                        KdbAttachments.hdr(WireMessageType.ATTACHMENT_GET), host.namespace, host.sessionId, doc,
                        attachmentId = byId?.id?.toKdb() ?: if (readId.isNotEmpty()) info.id.toKdb() else null,
                        name = (selector as? KdbAttachmentSelector.ByName)?.name ?: "",
                        asOfHex = asOfHex, offset = next, maxBytes = pageBytes.toLong(), readId = readId,
                    ),
                )
            } catch (e: Exception) {
                if (e is kotlinx.coroutines.CancellationException && e !is kotlinx.coroutines.TimeoutCancellationException) throw e
                throw IOException("attachment read interrupted: ${e.message}", e)
            }
        val data = reply as? WireMessage.AttachmentData
            ?: throw IOException("unexpected attachment reply: ${reply.header.messageType}")
        if (data.error != null) {
            throw IOException(data.error, attachmentException(data.error!!, data.errorCode))
        }
        return data
    }

    private fun accept(data: WireMessage.AttachmentData) {
        if (readId.isEmpty()) {
            info = data.info.toInfo()
            commitHex = data.commitHex
        }
        if (data.readId.isNotEmpty()) readId = data.readId
        if (data.offset != next) throw IOException("attachment page at offset ${data.offset}, expected $next")
        if (!data.eof && data.data.isEmpty()) throw IOException("empty attachment page before the end")
        next += data.data.size
        hash?.update(data.data)
        if (data.eof) {
            eof = true
            superseded = data.superseded
            verify()
        }
        page = data.data
        pos = 0
    }

    /** Runs before the last page's bytes are handed out, so a bad download never reads as complete. */
    private fun verify() {
        if (next != info.size) {
            throw IOException("attachment ${info.id} ended at byte $next of ${info.size}")
        }
        if (hash != null) {
            val got = hash.finishHex()
            if (got != info.hashHex) {
                val msg = "attachment ${info.id} failed verification: content hash $got, expected ${info.hashHex}"
                throw IOException(msg, KdbAttachmentCorruptException(msg))
            }
        }
    }

    private fun fill(): Boolean {
        while (pos >= page.size) {
            if (eof || closed) return false
            accept(runBlocking { fetch() })
        }
        return true
    }

    override fun read(): Int = if (fill()) page[pos++].toInt() and 0xFF else -1

    override fun read(
        b: ByteArray,
        off: Int,
        len: Int,
    ): Int {
        if (len == 0) return 0
        if (!fill()) return -1
        val n = minOf(len, page.size - pos)
        System.arraycopy(page, pos, b, off, n)
        pos += n
        return n
    }

    override fun available(): Int = page.size - pos

    override fun close() {
        if (closed) return
        closed = true
        if (!eof && readId.isNotEmpty()) {
            runCatching {
                runBlocking {
                    // Best effort, briefly: the pin also lapses on its own (the server's read lease).
                    kotlinx.coroutines.withTimeoutOrNull(CLOSE_TIMEOUT_MS) {
                        host.request(
                            WireMessage.AttachmentGet(
                                KdbAttachments.hdr(WireMessageType.ATTACHMENT_GET), host.namespace, host.sessionId, doc,
                                readId = readId, close = true,
                            ),
                        )
                    }
                }
            }
        }
    }
}
