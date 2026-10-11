package dev.kdb.compression

import com.github.luben.zstd.Zstd
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.fail

/**
 * [ZstdCorruptionTest] at a depth only the JVM runs in reasonable time, plus libzstd frames of
 * every level (which carry no checksum, so only "typed failure or some output" is checkable).
 */
class ZstdCorruptionJvmTest : ZstdCorruptionTest() {
    override val mutationsPerFrame: Int = 4000

    @Test
    fun damagedLibzstdFramesFailTyped() {
        val payload = ZstdTestCorpus.json(30_000)
        val rnd = Random(5)
        for (level in listOf(-3, 1, 3, 9, 19)) {
            val frame = Zstd.compress(payload, level)
            repeat(3000) {
                val m = frame.copyOf()
                m[rnd.nextInt(m.size)] = rnd.nextInt().toByte()
                try {
                    ZstdDecoder.decompress(m, payload.size * 2)
                } catch (_: ZstdFormatException) {
                } catch (e: Throwable) {
                    fail("level $level mutation $it: ${e::class.simpleName} (${e.message})")
                }
            }
        }
    }
}
