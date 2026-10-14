package dev.kdb.compression

import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFailsWith
import kotlin.test.fail

/**
 * Damaged input must fail with [ZstdFormatException] - never another exception type (an index
 * error, an allocation failure, a hang) and never wrong bytes. Runs on every target against the
 * pure-Kotlin decoder, which is what Kotlin/JS and Kotlin/Native read zstd with.
 *
 * "Never wrong bytes" can only be promised for frames that carry the XXH64 content checksum - in a
 * frame without one, a flipped bit inside a raw literal is undetectable by any zstd decoder. Go and
 * the pure-Kotlin encoder always write it; KDB's formats also CRC the compressed bodies they store.
 */
open class ZstdCorruptionTest {
    /** Kept modest for Kotlin/JS (see the mocha timeout in the root build.gradle.kts); ZstdCorruptionJvmTest runs far more. */
    protected open val mutationsPerFrame: Int = 150

    private val original = ZstdTestCorpus.json(20_000)

    /** Decodes [frame], accepting a typed failure or the exact original; anything else fails. */
    private fun decodeOrReject(frame: ByteArray, what: String, checksummed: Boolean) {
        val got =
            try {
                ZstdDecoder.decompress(frame, original.size * 2)
            } catch (_: ZstdFormatException) {
                return
            } catch (e: Throwable) {
                fail("$what: threw ${e::class.simpleName} (${e.message}) instead of ZstdFormatException")
            }
        if (checksummed) assertContentEquals(original, got, "$what: a checksummed frame decoded to wrong bytes")
    }

    private fun mutations(frame: ByteArray, seed: Int, count: Int): Sequence<Pair<String, ByteArray>> =
        sequence {
            val rnd = Random(seed)
            // Not 0: zero bytes is how Go encodes an empty payload, so it decodes to empty output.
            // Callers record the uncompressed length next to every body and check it.
            for (cut in listOf(1, 3, 4, 5, 6, 8, frame.size / 2, frame.size - 5, frame.size - 1)) {
                yield("truncated to $cut" to frame.copyOf(cut))
            }
            yield("trailing byte" to frame + byteArrayOf(0))
            repeat(count) {
                val m = frame.copyOf()
                val kind = rnd.nextInt(3)
                val at = rnd.nextInt(m.size)
                when (kind) {
                    0 -> m[at] = (m[at].toInt() xor (1 shl rnd.nextInt(8))).toByte()
                    1 -> m[at] = rnd.nextInt().toByte()
                    else -> for (k in at until minOf(m.size, at + 1 + rnd.nextInt(16))) m[k] = 0
                }
                yield("mutation $it (kind $kind at $at)" to m)
            }
        }

    @Test
    fun damagedPureKotlinFramesFailTyped() {
        val frame = ZstdEncoder.compress(original)
        assertContentEquals(original, ZstdDecoder.decompress(frame, original.size))
        for ((what, m) in mutations(frame, seed = 1, count = mutationsPerFrame * 4)) decodeOrReject(m, what, checksummed = true)
    }

    @Test
    fun damagedGoldenFramesFailTyped() {
        // Real frames from libzstd and klauspost, including Huffman literals and FSE tables the
        // pure-Kotlin encoder never writes.
        for ((name, frame, checksummed) in ZstdGoldenCorpus.frames().filter { it.payload.contentEquals(original) }.map {
            Triple(it.name, it.frame, it.checksummed)
        }) {
            for ((what, m) in mutations(frame, seed = name.hashCode(), count = mutationsPerFrame)) {
                decodeOrReject(m, "$name $what", checksummed)
            }
        }
    }

    @Test
    fun randomGarbageFailsTyped() {
        val rnd = Random(99)
        repeat(500) {
            val junk = rnd.nextBytes(rnd.nextInt(1, 200))
            if (it % 2 == 0) {
                // Valid magic and header, garbage after: reaches the block and entropy decoders.
                junk[0] = 0x28; if (junk.size > 3) { junk[1] = 0xB5.toByte(); junk[2] = 0x2F; junk[3] = 0xFD.toByte() }
            }
            decodeOrReject(junk, "garbage $it", checksummed = false)
        }
    }

    @Test
    fun dictionaryFramesAreRejected() {
        // Frame header with a 1-byte dictionary id (7): KDB never uses dictionaries.
        val frame = byteArrayOf(0x28, 0xB5.toByte(), 0x2F, 0xFD.toByte(), 0x21, 0x07, 0x03, 0x19, 0x00, 0x00, 0x6B, 0x64, 0x62)
        assertFailsWith<ZstdFormatException> { ZstdDecoder.decompress(frame, 100) }
    }
}
