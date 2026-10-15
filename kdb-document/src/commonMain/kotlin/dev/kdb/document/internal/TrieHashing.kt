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

/**
 * [sha256OnlyChild] on the portable SHA-256, resumed from the state after nib's constant prefix's
 * whole blocks (0 to 7 of the nine, 3.5 on average): what JS and native use.
 */
internal fun sha256OnlyChildMidstate(
    nib: Int,
    child: ByteArray,
): ByteArray {
    val skip = ONLY_CHILD_SKIP[nib]
    val buf = ByteArray(1 + 16 * 32)
    if (skip == 0) buf[0] = 0x01
    child.copyInto(buf, 1 + nib * 32)
    return sha256Resume(ONLY_CHILD_MIDSTATE[nib].copyOf(), skip.toLong(), buf, skip)
}

private val ONLY_CHILD_SKIP: IntArray = IntArray(16) { (1 + it * 32) / SHA256_BLOCK * SHA256_BLOCK }

private val ONLY_CHILD_MIDSTATE: Array<IntArray> =
    Array(16) { nib ->
        val state = SHA256_H0.copyOf()
        val prefix = ByteArray(ONLY_CHILD_SKIP[nib])
        if (prefix.isNotEmpty()) prefix[0] = 0x01
        val w = IntArray(64)
        var off = 0
        while (off < prefix.size) {
            sha256Compress(state, prefix, off, w)
            off += SHA256_BLOCK
        }
        state
    }

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
