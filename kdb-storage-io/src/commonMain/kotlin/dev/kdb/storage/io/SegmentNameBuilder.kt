package dev.kdb.storage.io

public object SegmentNameBuilder {
    public fun delta(namespaceId: String, segmentId: String): String =
        path(namespaceId, SegmentKind.DELTA, segmentId)

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
