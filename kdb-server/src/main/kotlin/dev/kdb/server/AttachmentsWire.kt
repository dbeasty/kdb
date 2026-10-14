package dev.kdb.server

import dev.kdb.auth.AuthAction
import dev.kdb.auth.Principal
import dev.kdb.codec.KdbHash
import dev.kdb.document.ATTACHMENT_CHUNK_BYTES
import dev.kdb.document.ATTACHMENT_HASH_SCHEME_CHUNKED_1
import dev.kdb.document.MerkleTree
import dev.kdb.document.attachmentChunkCount
import dev.kdb.document.attachmentIdentity
import dev.kdb.embed.AttachmentBlobStore
import dev.kdb.embed.AttachmentCorruptException
import dev.kdb.embed.AttachmentInfo
import dev.kdb.embed.AttachmentNotRetainedException
import dev.kdb.embed.AttachmentReader
import dev.kdb.embed.AttachmentSelector
import dev.kdb.embed.AttachmentTooLargeException
import dev.kdb.embed.AttachmentUpload
import dev.kdb.embed.NoSuchAttachmentException
import dev.kdb.embed.NoSuchDocumentException
import dev.kdb.embed.attachmentInfoAt
import dev.kdb.embed.attachmentStore
import dev.kdb.embed.getAttachment
import dev.kdb.embed.listAttachments
import dev.kdb.wire.MAX_ATTACHMENT_CHUNK_BYTES
import dev.kdb.wire.WireAttachmentInfo
import dev.kdb.wire.WireBlobChunkProof
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.security.SecureRandom

/*
 * The attachment client messages (docs/kdb-attachments-plan.md) - go/kdb/server/attachments_wire.go,
 * ported: uploads, paged reads pinned to the version they started on, and listings.
 *
 * Uploads belong to the server, not to a connection, so a client that loses its connection resumes
 * from the last result's received on a new one. An upload idle for [ATTACHMENT_UPLOAD_IDLE_MS] is
 * dropped (its partial bytes are collected with the store's unfinished segments). Reads belong to
 * the connection: each pins its attachment's version until its last page, a close, or the
 * connection ending - and the store's read lease bounds even that.
 */

internal const val ATTACHMENT_UPLOAD_IDLE_MS: Long = 60L * 60 * 1000
private const val ATTACHMENT_PAGE_DEFAULT: Long = 1L shl 20
private const val ATTACHMENT_READS_PER_CONN = 64
private const val ATTACHMENT_UPLOADS_PER_RUNTIME = 1024

internal class ServerAttachmentUpload(
    val ns: String,
    val principal: String,
    var up: AttachmentUpload?,
    var last: Long,
) {
    val mutex = Mutex()
    var received = 0L
    var done: Pair<KdbHash, Long>? = null
}

/** A runtime's in-flight uploads ([KdbServerRuntime.attachmentUploads]). */
public class ServerAttachmentUploads {
    internal val mutex = Mutex()
    internal val uploads = HashMap<String, ServerAttachmentUpload>()

    internal suspend fun sweepLocked(now: Long) {
        val it = uploads.values.iterator()
        while (it.hasNext()) {
            val up = it.next()
            if (now - up.last > ATTACHMENT_UPLOAD_IDLE_MS) {
                up.up?.abort()
                it.remove()
            }
        }
    }
}

private class OpenAttachmentRead(
    val reader: AttachmentReader,
    val ns: String,
    val doc: dev.kdb.codec.KdbUuid,
    val info: WireAttachmentInfo,
    val commit: KdbHash,
) {
    private var tree: MerkleTree? = null
    private var treeError: Throwable? = null
    private val treeMutex = Mutex()

    /** The blob's Merkle tree, built from the store's leaf hashes on the first page that asks for
     * proofs and kept for the rest of the read. It must hash, with the size, to the content hash. */
    suspend fun merkleTree(): MerkleTree =
        treeMutex.withLock {
            tree?.let { return@withLock it }
            treeError?.let { throw it }
            try {
                val leaves = (0 until attachmentChunkCount(info.size)).map { reader.chunkLeaf(it) }
                val t = MerkleTree(leaves)
                if (attachmentIdentity(t.root(), info.size) != info.hash) {
                    throw AttachmentCorruptException("attachment ${info.hash?.toHex()}: stored chunk hashes do not build its content hash")
                }
                tree = t
                t
            } catch (e: Throwable) {
                if (e is CancellationException) throw e
                treeError = e
                throw e
            }
        }
}

/** Busy: retry later (Go's BusyError). */
private class AttachmentBusyException(
    message: String,
) : RuntimeException(message)

private class AttachmentUnauthorizedException(
    message: String,
) : RuntimeException(message)

/**
 * One connection's attachment frames. [principal] authenticates the connection (null: refused,
 * with the message it carries); [readHead] is the commit a read without asOf sees for a session.
 */
internal class AttachmentWireHandler(
    private val server: KdbServerRuntime,
    private val defaultNamespace: String,
    private val authenticate: suspend () -> Principal,
    private val authorize: suspend (Principal, AuthAction) -> Unit,
    private val authFailureMessage: (Throwable) -> String,
    private val readHead: suspend (sessionId: String) -> KdbHash?,
) {
    private val readsMutex = Mutex()
    private val reads = HashMap<String, OpenAttachmentRead>()

    private fun header(
        corr: Int,
        type: WireMessageType,
    ) = WireHeader(type, KDB_WIRE_PROTOCOL_VERSION, corr, 0)

    private fun ns(requested: String) = requested.ifEmpty { defaultNamespace }

    private fun codeFor(e: Throwable): String =
        when (e) {
            is NoSuchAttachmentException, is NoSuchDocumentException, is AttachmentNotRetainedException -> "NOT_FOUND"
            is dev.kdb.embed.AttachmentBlobNotFoundException -> "NOT_FOUND"
            is AttachmentTooLargeException -> "RESOURCE_EXHAUSTED"
            is AttachmentCorruptException -> "INTEGRITY"
            is AttachmentBusyException -> "BUSY"
            is AttachmentUnauthorizedException -> "UNAUTHORIZED"
            else -> "INTERNAL"
        }

    private suspend fun authorizeOn(action: AuthAction) {
        val principal =
            try {
                authenticate()
            } catch (e: Throwable) {
                if (e is CancellationException) throw e
                throw AttachmentUnauthorizedException(authFailureMessage(e))
            }
        try {
            authorize(principal, action)
        } catch (e: Throwable) {
            if (e is CancellationException) throw e
            throw AttachmentUnauthorizedException(authFailureMessage(e))
        }
    }

    private suspend fun principalId(): String = runCatching { authenticate().id }.getOrDefault("")

    /** Releases every read this connection holds - its teardown. */
    suspend fun closeAll() {
        val open = readsMutex.withLock { reads.values.toList().also { reads.clear() } }
        for (rd in open) rd.reader.close()
    }

    suspend fun handleUpload(msg: WireMessage.AttachmentUpload): WireMessage.AttachmentUploadResult {
        val corr = msg.header.correlationId

        fun reply(
            up: ServerAttachmentUpload?,
            id: String,
            err: Throwable? = null,
        ): WireMessage.AttachmentUploadResult =
            WireMessage.AttachmentUploadResult(
                header(corr, WireMessageType.ATTACHMENT_UPLOAD_RESULT), msg.namespace, uploadId = id,
                received = up?.received ?: 0, hash = up?.done?.first, size = up?.done?.second ?: 0,
                error = err?.let { it.message ?: it.toString() }, errorCode = err?.let { codeFor(it) },
            )
        val ns = ns(msg.namespace)
        try {
            if (server.readOnly) throw IllegalStateException("namespace $ns is read-only on this server")
            // Uploading is the first half of committing: it takes the commit permission.
            authorizeOn(AuthAction.TxCommit(ns))
            if (msg.data.size > MAX_ATTACHMENT_CHUNK_BYTES) {
                throw IllegalArgumentException("chunk of ${msg.data.size} bytes is over the $MAX_ATTACHMENT_CHUNK_BYTES limit")
            }
        } catch (e: Throwable) {
            if (e is CancellationException) throw e
            return reply(null, msg.uploadId, e)
        }
        val store = server.runtime.attachmentStore(ns)
        val ups = server.attachmentUploads
        val now = server.nowMillis()
        val who = principalId()
        var id = msg.uploadId
        if (id.isEmpty() && msg.expect != null) {
            heldBlob(store, msg.expect!!)?.let { size ->
                return reply(ServerAttachmentUpload(ns, who, null, now).also { it.received = size; it.done = msg.expect!! to size }, "")
            }
        }
        val up: ServerAttachmentUpload =
            ups.mutex.withLock {
                ups.sweepLocked(now)
                if (id.isEmpty()) {
                    if (ups.uploads.size >= ATTACHMENT_UPLOADS_PER_RUNTIME) {
                        return reply(null, "", AttachmentBusyException("too many uploads in progress"))
                    }
                    id = newToken()
                    ServerAttachmentUpload(ns, who, store.begin(), now).also { ups.uploads[id] = it }
                } else {
                    ups.uploads[id]
                        ?: return reply(null, id, NoSuchAttachmentException("no such attachment: upload $id (expired or never started)"))
                }
            }
        return up.mutex.withLock {
            if (up.ns != ns || up.principal != who) {
                return@withLock reply(null, id, AttachmentUnauthorizedException("upload belongs to another namespace or principal"))
            }
            up.last = now
            if (up.done != null) return@withLock reply(up, id) // a retried final chunk: already finished
            if (msg.offset != up.received) {
                return@withLock reply(
                    up, id,
                    IllegalStateException("chunk at offset ${msg.offset} does not follow the ${up.received} bytes received: resume from there"),
                )
            }
            try {
                up.up!!.write(msg.data)
                up.received += msg.data.size
                if (msg.final) {
                    val info = up.up!!.finish()
                    up.done = info.hash to info.size
                    up.up = null
                }
                reply(up, id)
            } catch (e: Throwable) {
                if (e is CancellationException) throw e
                up.up?.abort()
                up.up = null
                ups.mutex.withLock { ups.uploads.remove(id) }
                reply(up, id, e)
            }
        }
    }

    /** The commit a read sees: [asOfHex], else the session's read head, else the namespace's head. */
    private suspend fun readHeadFor(
        sessionId: String,
        asOfHex: String,
    ): KdbHash {
        if (asOfHex.isNotEmpty()) return KdbHash.fromHex(asOfHex)
        if (sessionId.isNotEmpty()) readHead(sessionId)?.let { return it }
        return server.runtime.dag.head()
    }

    private fun AttachmentInfo.toWire() = WireAttachmentInfo(id, name, contentHash, size, mime)

    suspend fun handleList(msg: WireMessage.AttachmentList): WireMessage.AttachmentListResult {
        val corr = msg.header.correlationId
        val ns = ns(msg.namespace)
        return try {
            val doc = msg.docId ?: throw NoSuchDocumentException("no such document: no docId")
            authorizeOn(AuthAction.DocumentRead(ns, doc.toString()))
            val at = readHeadFor(msg.sessionId, msg.asOfHex)
            val items = server.runtime.listAttachments(ns, doc, at)
            WireMessage.AttachmentListResult(
                header(corr, WireMessageType.ATTACHMENT_LIST_RESULT), msg.namespace, at.toHex(), items.map { it.toWire() },
            )
        } catch (e: Throwable) {
            if (e is CancellationException) throw e
            WireMessage.AttachmentListResult(
                header(corr, WireMessageType.ATTACHMENT_LIST_RESULT), msg.namespace, error = e.message ?: e.toString(), errorCode = codeFor(e),
            )
        }
    }

    suspend fun handleGet(msg: WireMessage.AttachmentGet): WireMessage.AttachmentData {
        val corr = msg.header.correlationId
        var readId = msg.readId
        var rd: OpenAttachmentRead? = null

        fun out(
            data: ByteArray = ByteArray(0),
            offset: Long = 0,
            eof: Boolean = false,
            superseded: Boolean = false,
            error: Throwable? = null,
            errorCode: String? = null,
            tree: KdbHash? = null,
            chunks: List<WireBlobChunkProof> = emptyList(),
            scheme: Int = 0,
        ) = WireMessage.AttachmentData(
            header(corr, WireMessageType.ATTACHMENT_DATA), msg.namespace, readId, msg.docId,
            rd?.info ?: WireAttachmentInfo(), rd?.commit?.toHex() ?: "", offset, data, eof, superseded,
            error?.let { it.message ?: it.toString() }, errorCode ?: error?.let { codeFor(it) }, tree, chunks, scheme,
        )

        fun fail(e: Throwable): WireMessage.AttachmentData {
            // A damaged stored copy goes out as INTEGRITY, and to the log: the client can read
            // another replica, but only an operator can repair this one.
            if (e is AttachmentCorruptException) {
                System.err.println(
                    "kdb: stored attachment bytes are corrupt: namespace=${rd?.ns ?: msg.namespace} doc=${msg.docId} " +
                        "hash=${rd?.info?.hash?.toHex()} offset=${msg.offset}: ${e.message}",
                )
            }
            return out(error = e)
        }
        if (msg.readId.isNotEmpty()) {
            rd =
                readsMutex.withLock {
                    reads[msg.readId]?.also { if (msg.close) reads.remove(msg.readId) }
                }
            if (rd == null) return fail(NoSuchAttachmentException("no such attachment: read ${msg.readId} (finished, closed or expired)"))
            if (msg.close) {
                rd.reader.close()
                return out(eof = true, superseded = superseded(rd))
            }
        } else {
            val ns = ns(msg.namespace)
            try {
                val doc = msg.docId ?: throw NoSuchDocumentException("no such document: no docId")
                authorizeOn(AuthAction.DocumentRead(ns, doc.toString()))
                val at = readHeadFor(msg.sessionId, msg.asOfHex)
                val sel = msg.attachmentId?.let { AttachmentSelector.ById(it) } ?: AttachmentSelector.ByName(msg.name)
                val read = server.runtime.getAttachment(ns, doc, sel, at)
                val open = OpenAttachmentRead(read.reader, ns, doc, read.info.toWire(), at)
                val full = readsMutex.withLock {
                    if (reads.size >= ATTACHMENT_READS_PER_CONN) {
                        true
                    } else {
                        readId = newToken()
                        reads[readId] = open
                        false
                    }
                }
                if (full) {
                    read.reader.close()
                    return fail(AttachmentBusyException("too many open attachment reads on this connection"))
                }
                rd = open
            } catch (e: Throwable) {
                if (e is CancellationException) throw e
                return fail(e)
            }
        }
        val read = rd!!
        var page = msg.maxBytes
        if (page <= 0) page = ATTACHMENT_PAGE_DEFAULT
        if (page > MAX_ATTACHMENT_CHUNK_BYTES) page = MAX_ATTACHMENT_CHUNK_BYTES.toLong()
        var off = msg.offset
        val size = read.info.size
        if (off < 0 || off > size) {
            // Info stays on the error, so the client learns the size it overran.
            closeRead(readId)
            readId = ""
            return out(
                error = IllegalArgumentException("offset $off outside the attachment's $size bytes"),
                errorCode = "SCHEMA_VIOLATION",
            )
        }
        var tree: KdbHash? = null
        var chunks: List<WireBlobChunkProof> = emptyList()
        var scheme = 0
        try {
            if (msg.proofs) {
                // Whole chunks, so the client can check every one before handing out any of its bytes.
                val chunk = ATTACHMENT_CHUNK_BYTES.toLong()
                var end = (off + page + chunk - 1) / chunk * chunk
                off = off / chunk * chunk
                end = minOf(size, maxOf(end, off + chunk), off + MAX_ATTACHMENT_CHUNK_BYTES)
                page = end - off
                val t =
                    try {
                        read.merkleTree()
                    } catch (e: Throwable) {
                        if (e is CancellationException) throw e
                        closeRead(readId)
                        readId = ""
                        return out(error = e, errorCode = "INTEGRITY")
                    }
                tree = t.root()
                scheme = ATTACHMENT_HASH_SCHEME_CHUNKED_1
                val proofs = ArrayList<WireBlobChunkProof>()
                var i = off / chunk
                while (i < attachmentChunkCount(size) && (i * chunk < end || size == 0L)) {
                    proofs += WireBlobChunkProof(i, t.proof(i.toInt()))
                    i++
                }
                chunks = proofs
            }
            val n = minOf(page, size - off).toInt()
            val buf = ByteArray(n)
            var got = 0
            while (got < n) {
                val r = read.reader.readAt(off + got, buf, got, n - got)
                if (r <= 0) break
                got += r
            }
            val data = if (got == n) buf else buf.copyOf(got)
            var eof = false
            var sup = false
            if (off + got >= size) {
                eof = true
                closeRead(readId)
                sup = superseded(read)
            }
            return out(data, off, eof, sup, tree = tree, chunks = chunks, scheme = scheme)
        } catch (e: Throwable) {
            if (e is CancellationException) throw e
            closeRead(readId)
            return fail(e)
        }
    }

    private suspend fun closeRead(id: String) {
        if (id.isEmpty()) return
        val rd = readsMutex.withLock { reads.remove(id) }
        rd?.reader?.close()
    }

    /** Whether the attachment a finished read served is no longer what the namespace's head has. */
    private suspend fun superseded(rd: OpenAttachmentRead): Boolean {
        val head = runCatching { server.runtime.dag.head() }.getOrNull() ?: return false
        if (head == rd.commit) return false
        val id = rd.info.id ?: return false
        return try {
            server.runtime.attachmentInfoAt(rd.ns, rd.doc, AttachmentSelector.ById(id), head).contentHash != rd.info.hash
        } catch (e: Throwable) {
            if (e is CancellationException) throw e
            true // removed, or the document is gone
        }
    }

    /** The size of [h] when the store holds it - and holds it for the commit the client sends next. */
    private suspend fun heldBlob(
        store: AttachmentBlobStore,
        h: KdbHash,
    ): Long? {
        if (!store.has(h)) return null
        val size =
            try {
                val r = store.open(h)
                try {
                    r.size
                } finally {
                    r.close()
                }
            } catch (e: Throwable) {
                if (e is CancellationException) throw e
                return null
            }
        store.hold(h) // the client skips the bytes and commits a reference next
        return size
    }

    private companion object {
        val random = SecureRandom()

        fun newToken(): String {
            val b = ByteArray(16)
            random.nextBytes(b)
            return b.joinToString("") { (it.toInt() and 0xff).toString(16).padStart(2, '0') }
        }
    }
}
