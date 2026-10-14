package dev.kdb.peersync

import dev.kdb.codec.KdbHash

/**
 * The attachment blob store peer sync moves bytes in and out of - one namespace's. kdb-embed's
 * AttachmentBlobStore is the implementation (kdb-embed depends on this module, so it adapts its
 * store to this interface rather than the other way round).
 *
 * Content is addressed by its scheme-1 content hash (docs/kdb-attachments-plan.md): a store holds a
 * blob or it does not, and what it holds always hashes to its name.
 */
public interface PeerBlobStore {
    /** Whether the blob is held whole. */
    public suspend fun has(hash: KdbHash): Boolean

    /** Opens a held blob for reading, pinned until closed; null when it is not held (never stored,
     * or not retained). A blob whose stored bytes are damaged throws [PeerBlobCorruptException],
     * here or on a read. */
    public suspend fun open(hash: KdbHash): PeerBlobReader?

    /** Stores the bytes [source] yields (it returns how many it wrote into the buffer, -1 at the
     * end), returning their content hash and size. The blob is visible to [has] only once whole.
     * A failure - of the source or the store - stores nothing. */
    public suspend fun put(source: suspend (buffer: ByteArray, offset: Int, length: Int) -> Int): PeerStoredBlob

    /** Keeps a held blob from collection until a commit references it (a pushing peer's Expect:
     * its REF_UPDATE comes next). False when the blob is not held. */
    public suspend fun hold(hash: KdbHash): Boolean
}

/** A pinned read of one blob. */
public interface PeerBlobReader {
    public val size: Long

    /** The leaf hash of scheme-1 chunk [index] (SHA-256(0x00 || chunk)), for audit paths. */
    public suspend fun chunkLeaf(index: Long): KdbHash

    /** Reads up to [length] bytes at [position] into [buffer]; returns how many (fewer only at the
     * end of the blob). */
    public suspend fun readAt(position: Long, buffer: ByteArray, offset: Int, length: Int): Int

    public suspend fun close()
}

public data class PeerStoredBlob(
    val hash: KdbHash,
    val size: Long,
)

/** This node's copy of a blob is damaged: served as INTEGRITY, so the asker tries another peer. */
public class PeerBlobCorruptException(
    message: String,
    cause: Throwable? = null,
) : PeerSyncException(message, cause)

/**
 * Snapshot bootstrap for one namespace (go/kdb/peersync/snapshot.go): a fresh node takes a peer's
 * state at a commit instead of its history, and a node serves its own state at its head. kdb-embed
 * implements it over the runtime (installSnapshot, which a file runtime makes durable as Go's
 * PersistSnapshot does).
 */
public interface PeerSnapshots {
    /** Whether the namespace is fresh - main at its genesis, nothing else - so a snapshot may be installed. */
    public suspend fun canInstall(): Boolean

    /** Installs [root]'s state: [documents] must hash to its tree, and [attachmentTables] (packed)
     * hold its attachment table when it has one. Durable before it returns. */
    public suspend fun install(
        root: dev.kdb.document.KdbCommit,
        documents: List<dev.kdb.document.KdbDocument>,
        attachmentTables: ByteArray?,
    )

    /** Commits held without their parents (roots of installed snapshots), advertised as shallow. */
    public suspend fun shallowRoots(): List<KdbHash>

    /** The live tree at main's head, [head]: document id to content hash. */
    public suspend fun headTree(head: dev.kdb.document.KdbCommit): dev.kdb.document.DocumentTree

    /** The documents of [ids] as they are at main's head (null for one not there). */
    public suspend fun documentsAtHead(ids: List<dev.kdb.codec.KdbUuid>): List<dev.kdb.document.KdbDocument?>
}
