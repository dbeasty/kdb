package dev.kdb.compression

import dev.kdb.golden.PhysicalGoldenFixtures
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Roadmap R1: every target decodes the zstd frames Go and the JVM write. On JS and Native,
 * [ZstdCompression] is the pure-Kotlin decoder; on the JVM it is zstd-jni, and the pure-Kotlin
 * decoder is exercised directly as well, so the JVM run checks the same code JS and Native ship.
 */
class ZstdGoldenCorpusTest {
    // Pinned literally, as in Go's TestZstdCorpusPayloadIsPinned: generator drift in either
    // language fails here rather than as a confusing mismatch below.
    @Test
    fun corpusPayloadIsPinned() {
        val want = "{\"id\":0,\"name\":\"user-19\",\"tags\":[\"t15\",\"t1\"],\"score\":19292,\"active\":true}\n"
        assertEquals(want, ZstdTestCorpus.json(want.length).decodeToString())
        assertEquals(140_000, ZstdTestCorpus.json(140_000).size)
    }

    @Test
    fun everyCorpusFrameDecodes() {
        val frames = ZstdGoldenCorpus.frames()
        val names = frames.map { it.name }
        for (r in ZstdGoldenCorpus.required) assertTrue(r in names, "golden fixture $r is missing (make test-physical-golden)")
        for (f in frames) {
            assertContentEquals(f.payload, ZstdCompression.decompress(f.frame, f.payload.size), "${f.name} via ZstdCompression")
            assertContentEquals(f.payload, ZstdDecoder.decompress(f.frame, f.payload.size), "${f.name} via the pure-Kotlin decoder")
        }
    }

    @Test
    fun goFramesCarryChecksumsAndJvmFramesDoNot() {
        // Pins which shapes the corpus covers: klauspost always writes the XXH64 checksum and
        // zstd-jni's one-shot frames never do, so both decoder paths are exercised.
        for (f in ZstdGoldenCorpus.frames()) {
            when {
                f.name.startsWith("go/") -> assertTrue(f.checksummed, "${f.name} should carry a checksum")
                f.name.endsWith("_purekotlin.hex") || f.name.endsWith("_stream.hex") -> assertTrue(f.checksummed, f.name)
                else -> assertEquals(false, f.checksummed, f.name)
            }
        }
    }

    @Test
    fun originalZstdBodyFixturesDecode() {
        val want = "kdb zstd interop fixture payload, long enough to actually compress".encodeToByteArray()
        for (side in listOf("go", "kotlin")) {
            val body = PhysicalGoldenFixtures.read(side, "zstd_body.hex")!!
            assertContentEquals(want, ZstdCompression.decompress(body, 1 shl 20), "$side/zstd_body.hex")
            assertContentEquals(want, ZstdDecoder.decompress(body, 1 shl 20), "$side/zstd_body.hex, pure-Kotlin")
            // Go writes zero bytes for an empty payload, zstd-jni a 9-byte empty frame.
            val empty = PhysicalGoldenFixtures.read(side, "zstd_empty_body.hex")!!
            assertEquals(0, ZstdCompression.decompress(empty, 1 shl 20).size, "$side/zstd_empty_body.hex")
            assertEquals(0, ZstdDecoder.decompress(empty, 1 shl 20).size, "$side/zstd_empty_body.hex, pure-Kotlin")
        }
    }

    @Test
    fun pureKotlinEncoderIsDeterministicAndMatchesItsFixtures() {
        for (f in ZstdGoldenCorpus.frames().filter { it.name.endsWith("_purekotlin.hex") }) {
            assertContentEquals(f.frame, ZstdEncoder.compress(f.payload), "${f.name}: the encoder's output changed")
        }
    }
}
