package dev.kdb.document.internal

import java.security.MessageDigest
import java.util.stream.Collectors
import java.util.stream.IntStream

private const val PREIMAGE = 1 + 16 * 32
private const val BLOCK = 64

// How many leading bytes of nib's preimage are whole blocks of the constant prefix (0x01, zeros).
private val onlyChildSkip = IntArray(16) { (1 + it * 32) / BLOCK * BLOCK }

/** Per thread: a digest primed with each nibble's constant prefix, and a buffer for the rest. */
private class OnlyChildHasher {
    val primed =
        Array(16) { nib ->
            MessageDigest.getInstance("SHA-256").also { d ->
                val skip = onlyChildSkip[nib]
                if (skip > 0) d.update(ByteArray(skip).also { it[0] = 0x01 })
            }
        }
    val tail = ByteArray(PREIMAGE)
}

private val onlyChildHashers = ThreadLocal.withInitial { OnlyChildHasher() }

/**
 * Resumes from the primed state instead of rehashing the constant prefix: a clone and the
 * remaining blocks, up to 7 of a preimage's 9 fewer for the high nibbles.
 */
internal actual fun sha256OnlyChild(
    nib: Int,
    child: ByteArray,
): ByteArray {
    val h = onlyChildHashers.get()
    val skip = onlyChildSkip[nib]
    val len = PREIMAGE - skip
    val tail = h.tail
    tail.fill(0, 0, len)
    if (skip == 0) tail[0] = 0x01
    child.copyInto(tail, 1 + nib * 32 - skip)
    val d = h.primed[nib].clone() as MessageDigest
    d.update(tail, 0, len)
    return d.digest()
}

/** On the common fork-join pool, as a parallel stream; the calling thread takes part. */
internal actual fun <T> runEach(
    n: Int,
    task: (Int) -> T,
): List<T> {
    if (n <= 1) return List(n, task)
    return IntStream.range(0, n).parallel().mapToObj { task(it) }.collect(Collectors.toList())
}
