package dev.kdb.peersync

import dev.kdb.codec.KdbHash
import dev.kdb.dag.CommitDag
import dev.kdb.document.ATTACHMENT_CHUNK_BYTES
import dev.kdb.document.AttachmentTable
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.document.MerkleTree
import dev.kdb.document.attachmentChunkCount
import dev.kdb.document.attachmentIdentity
import dev.kdb.document.chunkLeafHash
import dev.kdb.document.verifyMerkleInclusion
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.MAX_ATTACHMENT_CHUNK_BYTES
import dev.kdb.wire.WireBlobChunkProof
import dev.kdb.wire.WireBlobFetchEntry
import dev.kdb.wire.WireBlobFetchPart
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.supervisorScope

// Attachment blobs in peer sync - a port of go/kdb/peersync/attachments.go.
//
// Commits carry attachment refs (content hashes); the blobs travel separately:
//   - pulling, a node fetches the blobs a page of commits attaches, and that it lacks, with
//     BLOB_FETCH before it stores the page, so no commit it adopts names bytes it does not hold.
//     Each chunk arrives with its RFC 6962 audit path and is checked before it is written;
//   - pushing, a node uploads them over the sync session (ATTACHMENT_UPLOAD with Expect, so a blob
//     the host already holds costs one round trip) before the REF_UPDATE that names them;
//   - a host serves BLOB_FETCH for an attachment the requested commit's table references with that
//     hash - the session's pull grant covers the namespace, and the reference proves the bytes
//     belong to it.
//
// A version the host no longer retains answers NOT_FOUND for that entry; the puller goes on without
// it, and a read of that version on the puller then fails the way it would on the host.

/** A BLOB_FETCH_RESULT page's byte budget (Go's blobFetchPageBytes). */
internal const val BLOB_FETCH_PAGE_BYTES: Long = 8L shl 20

/** Entries per BLOB_FETCH (Go's blobFetchBatch). */
internal const val BLOB_FETCH_BATCH: Int = 64

private fun header(type: WireMessageType, correlationId: Int) = WireHeader(type, KDB_WIRE_PROTOCOL_VERSION, correlationId, 0)

/**
 * A blob arriving in pieces, stored as it arrives: [PeerBlobStore.put] runs in [scope] reading from
 * a channel the pieces are written to. [finish] ends it and returns what the store made of it;
 * [abort] drops it, storing nothing. [scope] must be a supervisor's - a failed store is reported by
 * [finish] and [write], not by cancelling the caller.
 */
internal class BlobSink(
    store: PeerBlobStore,
    scope: CoroutineScope,
) {
    private val pieces = Channel<ByteArray>(Channel.UNLIMITED)
    private var current: ByteArray? = null
    private var pos = 0
    private val job: Deferred<PeerStoredBlob> = scope.async { store.put(::read) }

    private suspend fun read(buffer: ByteArray, offset: Int, length: Int): Int {
        while (true) {
            val p = current
            if (p != null && pos < p.size) {
                val n = minOf(length, p.size - pos)
                p.copyInto(buffer, offset, pos, pos + n)
                pos += n
                return n
            }
            current = pieces.receiveCatching().getOrNull() ?: return -1
            pos = 0
        }
    }

    suspend fun write(data: ByteArray) {
        if (job.isCompleted) job.await() // a store that failed says so now, not at finish
        if (data.isNotEmpty()) pieces.send(data)
    }

    suspend fun finish(): PeerStoredBlob {
        pieces.close()
        return job.await()
    }

    fun abort() {
        pieces.close()
        job.cancel()
    }
}

// --- host --------------------------------------------------------------------------------

/**
 * One sync session's attachment frames, host side: BLOB_FETCH for a puller and ATTACHMENT_UPLOAD
 * for a pusher. [scope] runs the uploads' stores; [close] aborts what is unfinished.
 */
internal class BlobHostSession(
    private val dag: CommitDag,
    private val store: PeerBlobStore,
    private val scope: CoroutineScope,
) {
    private class Upload(val namespace: String, val sink: BlobSink) {
        var received: Long = 0
    }

    private val uploads = HashMap<String, Upload>()
    private var uploadSeq = 0

    suspend fun blobFetch(m: WireMessage.BlobFetch): WireMessage.BlobFetchResult {
        var budget = m.maxBytes
        if (budget <= 0 || budget > BLOB_FETCH_PAGE_BYTES) budget = BLOB_FETCH_PAGE_BYTES
        val head: AttachmentTable? = runCatching { dag.attachmentTableAt(dag.head()) }.getOrNull()
        val parts = ArrayList<WireBlobFetchPart>()
        var more = false
        for ((i, e) in m.entries.withIndex()) {
            if (budget <= 0) {
                more = true
                break
            }
            val (part, used) = serveBlobEntry(i, e, budget, head)
            budget -= used
            parts += part
        }
        return WireMessage.BlobFetchResult(header(WireMessageType.BLOB_FETCH_RESULT, m.header.correlationId), m.namespace, parts, more)
    }

    private suspend fun serveBlobEntry(
        index: Int,
        e: WireBlobFetchEntry,
        budget: Long,
        head: AttachmentTable?,
    ): Pair<WireBlobFetchPart, Long> {
        fun fail(code: String, message: String) =
            WireBlobFetchPart(index = index, hash = e.hash, offset = e.offset, error = message, errorCode = code) to 0L
        val hash = e.hash ?: return fail(PeerErrorCodes.SCHEMA_VIOLATION, "no blob hash")
        val commit =
            try {
                KdbHash.fromHex(e.commitHex)
            } catch (x: Exception) {
                return fail(PeerErrorCodes.SCHEMA_VIOLATION, "invalid commit: ${x.message}")
            }
        val table =
            try {
                dag.attachmentTableAt(commit)
            } catch (x: Exception) {
                return fail(PeerErrorCodes.NOT_FOUND, "commit ${e.commitHex}: ${x.message}")
            }
        val doc = e.docId ?: return fail(PeerErrorCodes.SCHEMA_VIOLATION, "no document id")
        val id = e.attachmentId ?: return fail(PeerErrorCodes.SCHEMA_VIOLATION, "no attachment id")
        val a = table.get(doc, id)
        if (a == null || a.contentHash != hash) {
            return fail(PeerErrorCodes.NOT_FOUND, "attachment $id of $doc at ${e.commitHex} does not reference ${hash.toHex()}")
        }
        val chunk = ATTACHMENT_CHUNK_BYTES.toLong()
        if (e.offset < 0 || e.offset % chunk != 0L || e.offset > a.size) {
            return fail(PeerErrorCodes.SCHEMA_VIOLATION, "offset ${e.offset} is not a chunk boundary within ${a.size} bytes")
        }
        val r =
            try {
                store.open(hash)
            } catch (x: PeerBlobCorruptException) {
                return fail(PeerErrorCodes.INTEGRITY, "blob ${hash.toHex()}: ${x.message}")
            } ?: return fail(PeerErrorCodes.NOT_FOUND, "blob ${hash.toHex()} is not retained here")
        try {
            if (r.size != a.size) return fail(PeerErrorCodes.INTEGRITY, "blob ${hash.toHex()} holds ${r.size} bytes, its ref says ${a.size}")
            val tree: MerkleTree
            val data: ByteArray
            try {
                val count = attachmentChunkCount(a.size)
                tree = MerkleTree(List(count.toInt()) { r.chunkLeaf(it.toLong()) })
                var want = e.maxBytes
                if (want <= 0 || want > budget) want = budget
                want = (want + chunk - 1) / chunk * chunk // whole chunks, at least one
                val end = minOf(a.size, e.offset + want)
                data = ByteArray((end - e.offset).toInt())
                var got = 0
                while (got < data.size) {
                    val n = r.readAt(e.offset + got, data, got, data.size - got)
                    if (n <= 0) throw PeerBlobCorruptException("blob ${hash.toHex()} ends at ${e.offset + got} of ${a.size} bytes")
                    got += n
                }
            } catch (x: PeerBlobCorruptException) {
                return fail(PeerErrorCodes.INTEGRITY, "blob ${hash.toHex()}: ${x.message}")
            }
            val end = e.offset + data.size
            val proofs = ArrayList<WireBlobChunkProof>()
            if (a.size == 0L) {
                proofs += WireBlobChunkProof(0, tree.proof(0))
            } else {
                var off = e.offset
                while (off < end) {
                    val idx = (off / chunk).toInt()
                    proofs += WireBlobChunkProof(idx.toLong(), tree.proof(idx))
                    off += chunk
                }
            }
            val eof = end == a.size
            var superseded = false
            if (eof && head != null) {
                val now = head.get(doc, id)
                superseded = now == null || now.contentHash != hash
            }
            val part =
                WireBlobFetchPart(
                    index = index, hash = hash, size = a.size, tree = tree.root(), offset = e.offset, data = data,
                    chunks = proofs, eof = eof, superseded = superseded,
                )
            return part to data.size.toLong()
        } finally {
            r.close()
        }
    }

    suspend fun upload(m: WireMessage.AttachmentUpload): WireMessage.AttachmentUploadResult {
        val reply = WireMessage.AttachmentUploadResult(header(WireMessageType.ATTACHMENT_UPLOAD_RESULT, m.header.correlationId), m.namespace, m.uploadId)
        val expect = m.expect
        if (m.uploadId.isEmpty() && expect != null && store.has(expect)) {
            val size =
                store.open(expect)?.let { r ->
                    try {
                        r.size
                    } finally {
                        r.close()
                    }
                }
            // The REF_UPDATE referencing it comes next: hold the blob until it does.
            if (size != null && store.hold(expect)) {
                return reply.copy(hash = expect, size = size, received = size)
            }
        }
        if (m.data.size > MAX_ATTACHMENT_CHUNK_BYTES) throw PeerSyncException("peer sync: upload chunk of ${m.data.size} bytes")
        var id = m.uploadId
        val up: Upload?
        if (id.isEmpty()) {
            id = "s${++uploadSeq}"
            up = Upload(m.namespace, BlobSink(store, scope))
            uploads[id] = up
        } else {
            up = uploads[id]
        }
        if (up == null || up.namespace != m.namespace) throw PeerSyncException("peer sync: unknown upload \"${m.uploadId}\"")
        if (m.offset != up.received) {
            return reply.copy(
                uploadId = id, received = up.received,
                error = "chunk at ${m.offset} does not follow the ${up.received} bytes received", errorCode = PeerErrorCodes.CONFLICT,
            )
        }
        try {
            up.sink.write(m.data)
        } catch (x: Throwable) {
            drop(id)
            throw x
        }
        up.received += m.data.size
        if (!m.final) return reply.copy(uploadId = id, received = up.received)
        uploads.remove(id)
        val stored = up.sink.finish()
        return reply.copy(uploadId = id, received = up.received, hash = stored.hash, size = stored.size)
    }

    private fun drop(id: String) {
        uploads.remove(id)?.sink?.abort()
    }

    /** Aborts the session's unfinished uploads. */
    fun close() {
        for (u in uploads.values) u.sink.abort()
        uploads.clear()
    }
}

// --- client ------------------------------------------------------------------------------

/** A blob a page of commits attaches. */
private class BlobNeed(
    var entry: WireBlobFetchEntry,
) {
    var sink: BlobSink? = null
    var done: Boolean = false
}

/** The blobs [commits] attach that [store] lacks, once each. */
private suspend fun attachedBlobs(store: PeerBlobStore, commits: List<KdbCommit>): List<BlobNeed> {
    val seen = HashSet<KdbHash>()
    val out = ArrayList<BlobNeed>()
    for (c in commits) {
        for (op in c.operations) {
            if (op !is KdbOp.AttachmentPut || !seen.add(op.contentHash)) continue
            if (!store.has(op.contentHash)) {
                out += BlobNeed(WireBlobFetchEntry(op.docId, op.attachmentId, c.hash.toHex(), op.contentHash, 0, 0))
            }
        }
    }
    return out
}

/** Whether [commits] carry anything an attachment-unaware peer could not decode: the first such
 * commit, or null. */
internal fun firstCommitWithAttachments(commits: List<KdbCommit>): KdbCommit? =
    commits.firstOrNull { c -> c.attachmentTreeHash != null || c.operations.any { it is KdbOp.AttachmentPut || it is KdbOp.AttachmentDelete } }

/**
 * Takes in, from the peer, the blobs [commits] attach that [store] lacks - before the commits are
 * stored, so none names bytes this node does not hold. Every chunk is checked against its audit
 * path; a chunk that fails stores nothing and fails the fetch ([BlobIntegrityException]), as does
 * an entry the peer answers INTEGRITY for. NOT_FOUND (not retained there) is skipped.
 */
internal suspend fun fetchBlobs(
    conn: V2Conn,
    store: PeerBlobStore,
    namespace: String,
    commits: List<KdbCommit>,
) {
    val needs = attachedBlobs(store, commits)
    if (needs.isEmpty()) return
    supervisorScope {
        try {
            while (true) {
                val batch = needs.filter { !it.done }.take(BLOB_FETCH_BATCH)
                if (batch.isEmpty()) return@supervisorScope
                val req =
                    WireMessage.BlobFetch(
                        header(WireMessageType.BLOB_FETCH, conn.next()), namespace, batch.map { it.entry }, BLOB_FETCH_PAGE_BYTES,
                    )
                val res =
                    conn.requestWire(req) as? WireMessage.BlobFetchResult
                        ?: throw PeerSyncException("peer sync: expected BLOB_FETCH_RESULT")
                if (res.error != null) throw RemotePeerException(res.errorCode ?: PeerErrorCodes.INTERNAL, res.error!!)
                var progressed = false
                for (p in res.parts) {
                    if (p.index < 0 || p.index >= batch.size) throw PeerSyncException("peer sync: BLOB_FETCH_RESULT names an entry that was not asked for")
                    val n = batch[p.index]
                    if (p.error != null) {
                        if (p.errorCode == PeerErrorCodes.NOT_FOUND) {
                            n.done = true // not retained there: go on without it
                            progressed = true
                            continue
                        }
                        throw RemotePeerException(p.errorCode ?: PeerErrorCodes.INTERNAL, p.error!!)
                    }
                    take(n, store, this, p)
                    progressed = true
                }
                if (!progressed) throw PeerSyncException("peer sync: BLOB_FETCH made no progress")
            }
        } finally {
            for (n in needs) if (!n.done) n.sink?.abort()
        }
    }
}

/** Checks one part's chunks against the blob's tree and writes them. */
private suspend fun take(
    n: BlobNeed,
    store: PeerBlobStore,
    scope: CoroutineScope,
    p: WireBlobFetchPart,
) {
    val chunk = ATTACHMENT_CHUNK_BYTES.toLong()
    val want = n.entry.hash!!
    if (p.offset != n.entry.offset || p.hash != want) {
        throw PeerSyncException("peer sync: BLOB_FETCH part for ${p.hash?.toHex()} at ${p.offset}, asked at ${n.entry.offset}")
    }
    val tree = p.tree ?: throw BlobIntegrityException("peer sync: BLOB_FETCH part for ${want.toHex()} has no tree hash")
    if (attachmentIdentity(tree, p.size) != want) {
        throw BlobIntegrityException("peer sync: BLOB_FETCH tree and size do not hash to the blob asked for (${want.toHex()})")
    }
    val leaves = attachmentChunkCount(p.size)
    var data = 0 // read position in p.data
    val first = n.entry.offset / chunk
    for ((i, proof) in p.chunks.withIndex()) {
        val idx = first + i
        if (proof.leaf != idx) throw BlobIntegrityException("peer sync: BLOB_FETCH proofs out of order")
        val size = minOf(chunk, p.size - idx * chunk).toInt()
        if (size < 0 || p.data.size - data < size) throw BlobIntegrityException("peer sync: BLOB_FETCH part is shorter than its chunks")
        if (!verifyMerkleInclusion(chunkLeafHash(p.data, data, size), idx, leaves, proof.path, tree)) {
            throw BlobIntegrityException("peer sync: BLOB_FETCH chunk $idx of ${want.toHex()} fails its proof")
        }
        val sink = n.sink ?: BlobSink(store, scope).also { n.sink = it }
        sink.write(p.data.copyOfRange(data, data + size))
        data += size
        n.entry = n.entry.copy(offset = n.entry.offset + size)
    }
    if (data != p.data.size) throw BlobIntegrityException("peer sync: BLOB_FETCH part carries bytes no proof covers")
    if (n.entry.offset < p.size) return
    val sink = n.sink ?: BlobSink(store, scope).also { n.sink = it }
    n.done = true
    val stored = sink.finish()
    if (stored.hash != want) throw BlobIntegrityException("peer sync: fetched blob does not hash to what was asked for")
}

/** Sends the peer the blobs [commits] attach, before the commits - a blob it holds already costs
 * one round trip (Expect). A blob this node does not hold either (not retained) is skipped. */
internal suspend fun uploadBlobs(
    conn: V2Conn,
    store: PeerBlobStore,
    namespace: String,
    commits: List<KdbCommit>,
) {
    val seen = HashSet<KdbHash>()
    for (c in commits) {
        for (op in c.operations) {
            if (op is KdbOp.AttachmentPut && seen.add(op.contentHash)) uploadBlob(conn, store, namespace, op.contentHash)
        }
    }
}

private suspend fun uploadBlob(
    conn: V2Conn,
    store: PeerBlobStore,
    namespace: String,
    hash: KdbHash,
) {
    val r = store.open(hash) ?: return
    try {
        val buf = ByteArray(ATTACHMENT_CHUNK_BYTES)
        var id = ""
        var off = 0L
        var first = true
        while (true) {
            var n = 0
            while (n < buf.size && off + n < r.size) {
                val got = r.readAt(off + n, buf, n, minOf(buf.size - n.toLong(), r.size - off - n).toInt())
                if (got <= 0) break
                n += got
            }
            val final = off + n >= r.size
            val msg =
                WireMessage.AttachmentUpload(
                    header(WireMessageType.ATTACHMENT_UPLOAD, conn.next()), namespace, uploadId = id, offset = off,
                    data = buf.copyOf(n), final = final, expect = if (first) hash else null,
                )
            val res =
                conn.requestWire(msg) as? WireMessage.AttachmentUploadResult
                    ?: throw PeerSyncException("peer sync: expected ATTACHMENT_UPLOAD_RESULT")
            if (res.error != null) throw RemotePeerException(res.errorCode ?: PeerErrorCodes.INTERNAL, res.error!!)
            if (first && res.hash == hash && res.uploadId.isEmpty()) return // the host holds it already
            first = false
            id = res.uploadId
            off += n
            if (final) {
                if (res.hash != hash) throw PeerSyncException("peer sync: the host hashed an uploaded blob differently")
                return
            }
        }
    } finally {
        r.close()
    }
}
