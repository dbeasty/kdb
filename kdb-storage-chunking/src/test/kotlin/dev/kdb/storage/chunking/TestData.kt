package dev.kdb.storage.chunking

import java.util.Random

internal fun randomBytes(
    size: Int,
    seed: Long,
): ByteArray {
    val out = ByteArray(size)
    Random(seed).nextBytes(out)
    return out
}

/** [original] with [insertedLength] fresh random bytes spliced in at [atOffset] — simulates a small edit. */
internal fun withInsertion(
    original: ByteArray,
    atOffset: Int,
    insertedLength: Int,
    seed: Long,
): ByteArray {
    val insertion = randomBytes(insertedLength, seed)
    return original.copyOfRange(0, atOffset) + insertion + original.copyOfRange(atOffset, original.size)
}
