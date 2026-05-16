package dev.kdb.storage

/** Eviction state of a realized store per enlistment. */
public enum class EnlistmentEvictionState {
    FULL,
    DOC_EVICTED,
    EVICTED,
    RELEASED,
}

/** Controls whether the index store participates in LRU eviction. */
public enum class IndexRetention {
    PINNED,
    EVICTABLE,
}

public enum class RebuildBlockingPolicy {
    WAIT,
    PARTIAL_OK,
}

public enum class EnlistmentPushState {
    IDLE,
    PUSHING,
    REJECTED,
    RESOLVING,
}

public enum class SnapshotFailureReason {
    NOT_FOUND,
    INTEGRITY_CHECK_FAILED,
    DESERIALIZATION_ERROR,
    ANCHOR_COMPACTED_AWAY,
}

public enum class CompressionCodec {
    NONE,
    ZSTD,
}
