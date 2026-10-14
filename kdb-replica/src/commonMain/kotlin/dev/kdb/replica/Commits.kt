package dev.kdb.replica

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.computeContentHash

/**
 * A commit as the replica holds it: kdb-document's [KdbCommit] (the canonical
 * `CommitPayload` codec, golden-pinned to Go) plus the exact bytes it arrived as.
 *
 * A received commit keeps its bytes, and its hash is SHA-256 of those bytes, never of a
 * re-encoding. That is how Go treats a pack (`FromPayloadBytes`), and it means a field the decoder
 * might not model cannot change a hash. Encoding happens only for commits the replica authors.
 */
public class ReplicaCommit internal constructor(
    public val commit: KdbCommit,
    public val payload: ByteArray,
) {
    /** Hex of the commit hash. */
    public val hash: String = commit.hash.toHex()
    public val parents: List<String> = commit.parentHashes.map { it.toHex() }
    public val treeHash: String = commit.documentTreeHash.toHex()
    public val timestampMicros: Long get() = commit.timestamp.toEpochMicros()
    public val ops: List<KdbOp> get() = commit.operations

    override fun toString(): String = "ReplicaCommit($hash)"

    public companion object {
        /** Decodes a commit from its canonical payload bytes; its hash is SHA-256 of exactly those bytes. */
        public fun decode(payload: ByteArray): ReplicaCommit = ReplicaCommit(KdbCommit.fromPayloadBytes(payload), payload)

        /** Builds a commit the replica authors: encodes it and derives its hash. */
        public fun build(
            parents: List<String>,
            namespace: String,
            transactionId: KdbUuid,
            timestampMicros: Long,
            authorNodeId: KdbUuid,
            ops: List<KdbOp>,
            treeHash: String,
            message: String,
        ): ReplicaCommit {
            val c =
                KdbCommit.build(
                    parentHashes = parents.map { KdbHash.fromBytes(hex32(it)) },
                    namespaceId = namespace,
                    transactionId = transactionId,
                    timestamp = microsToTimestamp(timestampMicros),
                    authorNodeId = authorNodeId,
                    operations = ops,
                    documentTreeHash = KdbHash.fromBytes(hex32(treeHash)),
                    schemaHash = null,
                    message = message,
                )
            return ReplicaCommit(c, c.toPayloadBytes())
        }
    }
}

internal fun microsToTimestamp(micros: Long): KdbTimestamp =
    KdbTimestamp(micros.floorDiv(1000L), micros.mod(1000L).toInt())

/**
 * Splits a pack's `commitsPayload` (go/kdb/wire/commit_push_codec.go): `u32le count`, then per
 * commit `u32le len ++ payload`. kdb-wire's CommitPushCodec has the same format, but it keeps only
 * decoded commits, and the replica needs each commit's bytes.
 */
public fun decodeCommitList(bytes: ByteArray): List<ReplicaCommit> {
    if (bytes.size < 4) return emptyList() // as Go's DecodeCommits: an empty pack
    val count = readU32Le(bytes, 0)
    if (count > (bytes.size - 4L) / 4) {
        throw CommitListException("commit list declares $count commits, too many for its size")
    }
    val out = ArrayList<ReplicaCommit>(count.toInt())
    var off = 4
    repeat(count.toInt()) {
        if (off + 4 > bytes.size) throw CommitListException("truncated commit list")
        val len = readU32Le(bytes, off)
        off += 4
        if (off + len > bytes.size) throw CommitListException("truncated commit in list")
        out.add(ReplicaCommit.decode(bytes.copyOfRange(off, off + len.toInt())))
        off += len.toInt()
    }
    if (off != bytes.size) throw CommitListException("trailing bytes after commit list")
    return out
}

public fun encodeCommitList(commits: List<ReplicaCommit>): ByteArray {
    var n = 4
    for (c in commits) n += 4 + c.payload.size
    val out = ByteArray(n)
    writeU32Le(out, 0, commits.size)
    var off = 4
    for (c in commits) {
        writeU32Le(out, off, c.payload.size)
        c.payload.copyInto(out, off + 4)
        off += 4 + c.payload.size
    }
    return out
}

public class CommitListException(message: String) : RuntimeException("kdb replica: $message")

private fun readU32Le(b: ByteArray, off: Int): Long =
    ((b[off].toLong() and 0xff) or
        ((b[off + 1].toLong() and 0xff) shl 8) or
        ((b[off + 2].toLong() and 0xff) shl 16) or
        ((b[off + 3].toLong() and 0xff) shl 24))

private fun writeU32Le(b: ByteArray, off: Int, v: Int) {
    b[off] = v.toByte()
    b[off + 1] = (v shr 8).toByte()
    b[off + 2] = (v shr 16).toByte()
    b[off + 3] = (v shr 24).toByte()
}

private val GENESIS_TX = KdbUuid.fromString("00000000-0000-4000-8000-000000000001")
private val GENESIS_AUTHOR = KdbUuid.fromString("00000000-0000-4000-8000-000000000002")

/**
 * The commit every namespace starts at, on every node (Go's dag.NewInMemoryCommitDag, Kotlin's
 * InMemoryCommitDag): fixed transaction and author ids, timestamp zero, no ops, the empty tree.
 * A replica starts there too, so what it writes before it ever syncs still shares a root with the
 * host's history.
 */
public fun genesisCommit(namespace: String): ReplicaCommit =
    ReplicaCommit.build(
        parents = emptyList(),
        namespace = namespace,
        transactionId = GENESIS_TX,
        timestampMicros = 0,
        authorNodeId = GENESIS_AUTHOR,
        ops = emptyList(),
        treeHash = DocumentTree.EMPTY.treeHash.toHex(),
        message = "genesis",
    )

/** Content hash of a document: SHA-256 of `DocumentBody{1: uuid id, 2: string json}` (kdb-document). */
public fun contentHash(docId: String, json: String): String =
    computeContentHash(KdbDocument(parseUuid(docId), json)).toHex()
