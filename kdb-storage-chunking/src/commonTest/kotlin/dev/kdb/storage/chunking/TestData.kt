package dev.kdb.storage.chunking

/** Deterministic, dependency-free PRNG (xorshift64*) so tests behave identically on every target. */
private class Xorshift64Star(seed: Long) {
    private var state: Long = if (seed == 0L) 0xdeadbeefL else seed

    fun nextByte(): Byte {
        state = state xor (state ushr 12)
        state = state xor (state shl 25)
        state = state xor (state ushr 27)
        val result = state * -0x61c8864680b583ebL
        return (result ushr 56).toByte()
    }
}

internal fun randomBytes(
    size: Int,
    seed: Long,
): ByteArray {
    val rng = Xorshift64Star(seed)
    return ByteArray(size) { rng.nextByte() }
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
