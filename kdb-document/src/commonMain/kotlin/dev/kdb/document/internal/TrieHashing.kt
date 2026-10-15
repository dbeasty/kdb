package dev.kdb.document.internal

/**
 * SHA-256 of an internal trie node with a single occupied slot: 0x01, then zeros, with [child] in
 * slot [nib]. A compressed leaf's spine is one of these per level, so it is most of what building
 * or inserting into a document tree hashes. A platform may start from a precomputed state for the
 * constant prefix (Go's onlyChildMidstate); the result is the same bytes either way.
 */
internal expect fun sha256OnlyChild(
    nib: Int,
    child: ByteArray,
): ByteArray

/** [sha256OnlyChild] hashing the whole preimage. */
internal fun sha256OnlyChildPortable(
    nib: Int,
    child: ByteArray,
): ByteArray {
    val buf = ByteArray(1 + 16 * 32)
    buf[0] = 0x01
    child.copyInto(buf, 1 + nib * 32)
    return sha256Digest(buf)
}

/**
 * [task] for 0 until [n], in order. A platform with threads may run them concurrently, so a task
 * must only read what the others share; elsewhere they run one after another.
 */
internal expect fun <T> runEach(
    n: Int,
    task: (Int) -> T,
): List<T>
