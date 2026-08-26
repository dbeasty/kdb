package dev.kdb.storage.io

public object SegmentNameBuilder {
    public fun delta(namespaceId: String, segmentId: String): String =
        path(namespaceId, SegmentKind.DELTA, segmentId)

    /**
     * File-name component (no namespace/kind prefix) a sequenced delta
     * segment uses: a 20-digit zero-padded decimal sequence number plus
     * ".seg". Zero-padded so lexicographic sort - what listSegments and
     * every platform I/O implementation give back for free - equals
     * commit order. Matches the Go side's
     * io.SegmentNameBuilder.DeltaSequencedFileName exactly, byte for
     * byte, so a mixed Go/Kotlin deployment's segments interleave
     * correctly (kdb-spec-layer13 Component 47 §11 Kotlin parity).
     */
    public fun deltaSequencedFileName(seq: Long): String = seq.toString().padStart(20, '0') + ".seg"

    /**
     * Parses a file-name component produced by [deltaSequencedFileName]
     * back into its sequence number, or null for anything else -
     * including pre-Layer-13 random-UUID segment names. Callers use that
     * to detect a legacy data directory and refuse to guess at its order.
     */
    public fun parseDeltaSequencedFileName(fileName: String): Long? {
        val suffix = ".seg"
        if (!fileName.endsWith(suffix)) return null
        val digits = fileName.removeSuffix(suffix)
        if (digits.length != 20 || digits.any { it < '0' || it > '9' }) return null
        return digits.toLongOrNull()
    }

    /**
     * Full segment path for a sequenced delta segment - the naming
     * scheme Component 47 replaces random-UUID delta segment names with,
     * so segment file order is commit order by construction (see
     * kdb-spec-layer13-resource-governance.md §4.1).
     */
    public fun deltaSequenced(namespaceId: String, seq: Long): String =
        path(namespaceId, SegmentKind.DELTA, deltaSequencedFileName(seq))

    public fun wal(namespaceId: String, walId: String): String =
        path(namespaceId, SegmentKind.WAL, walId)

    public fun sstable(namespaceId: String, level: Int, fileId: String): String =
        "ns/$namespaceId/${SegmentKind.SSTABLE.name.lowercase()}/L$level/$fileId"

    public fun namespacePrefix(namespaceId: String): String = "ns/$namespaceId/"

    private fun path(namespaceId: String, kind: SegmentKind, segmentId: String): String =
        "ns/$namespaceId/${kind.name.lowercase()}/$segmentId"
}

public object SnapshotKeyBuilder {
    public fun enlistment(enlistmentId: String): String = "kdb:snap:$enlistmentId"
}
