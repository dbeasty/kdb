package dev.kdb.compression

import com.github.luben.zstd.Zstd
import com.github.luben.zstd.ZstdOutputStream
import java.io.ByteArrayOutputStream
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.fail

/**
 * The pure-Kotlin codec that Kotlin/JS and Kotlin/Native run, checked against libzstd (zstd-jni)
 * on the JVM, where both are available: every frame libzstd writes - all levels, with and without
 * checksum and content size, multi-block, concatenated - must decode to the same bytes, and every
 * frame [ZstdEncoder] writes must decode in libzstd. Go is covered by the golden fixtures
 * (ZstdGoldenCorpusTest, go/kdb/interop/delta_physical_golden_test.go).
 */
class PureKotlinZstdDifferentialTest {
    private fun inputs(): List<Pair<String, ByteArray>> {
        val rnd = Random(42)
        return listOf(
            "empty" to ByteArray(0),
            "one byte" to byteArrayOf(7),
            "short text" to "kdb".encodeToByteArray(),
            "repetitive" to "kdb compression test ".repeat(500).encodeToByteArray(),
            "noise" to rnd.nextBytes(5000),
            "run" to ByteArray(300_000) { 0x41 },
            "runs and noise" to ByteArray(200_000) { if ((it / 997) % 3 == 0) rnd.nextInt().toByte() else (it / 5000).toByte() },
            "json corpus" to ZstdTestCorpus.json(400_000),
            "small json" to ZstdTestCorpus.json(700),
            "skewed bytes" to ByteArray(100_000) { (rnd.nextInt(64).let { v -> if (v < 50) 'a'.code + v % 3 else v }).toByte() },
            "all byte values" to ByteArray(70_000) { (it * 31 + it / 256).toByte() },
            // Every byte value, geometrically skewed: Huffman literals with FSE-compressed weights.
            "skewed binary" to ByteArray(150_000) { var v = 0; while (v < 255 && rnd.nextInt(5) != 0) v++; (v * 37).toByte() },
            "skewed binary, small" to ByteArray(300) { var v = 0; while (v < 255 && rnd.nextInt(3) != 0) v++; (255 - v).toByte() },
        )
    }

    private fun streamed(input: ByteArray, level: Int, checksum: Boolean): ByteArray =
        ByteArrayOutputStream().use { buf ->
            ZstdOutputStream(buf, level).use {
                it.setChecksum(checksum)
                // Small writes force libzstd to emit frames without a content size, block by block.
                var p = 0
                while (p < input.size) {
                    val n = minOf(10_000, input.size - p)
                    it.write(input, p, n)
                    it.flush()
                    p += n
                }
            }
            buf.toByteArray()
        }

    @Test
    fun decodesEveryLibzstdFrame() {
        for ((name, input) in inputs()) {
            for (level in listOf(-5, -1, 1, 3, 6, 12, 19, 22)) {
                val frames =
                    listOf(
                        "one-shot" to Zstd.compress(input, level),
                        "streamed" to streamed(input, level, checksum = false),
                        "streamed+checksum" to streamed(input, level, checksum = true),
                    )
                for ((shape, frame) in frames) {
                    val got =
                        try {
                            ZstdDecoder.decompress(frame, input.size)
                        } catch (e: Exception) {
                            fail("$name level $level $shape: ${e.message}", e)
                        }
                    assertContentEquals(input, got, "$name level $level $shape")
                }
            }
        }
    }

    @Test
    fun libzstdDecodesEveryPureKotlinFrame() {
        for ((name, input) in inputs()) {
            val frame = ZstdEncoder.compress(input)
            val viaLibzstd = ByteArray(input.size)
            assertEquals(input.size.toLong(), Zstd.decompressedSize(frame), "$name: declared content size")
            assertEquals(input.size.toLong(), Zstd.decompress(viaLibzstd, frame), "$name: libzstd output size")
            assertContentEquals(input, viaLibzstd, "$name: libzstd decoding the pure-Kotlin frame")
            assertContentEquals(input, ZstdDecoder.decompress(frame, input.size), "$name: pure-Kotlin round trip")
        }
    }

    @Test
    fun pureKotlinEncoderActuallyCompresses() {
        val json = ZstdTestCorpus.json(400_000)
        val frame = ZstdEncoder.compress(json)
        val libzstd = Zstd.compress(json, 3)
        println("json corpus ${json.size} B: pure-Kotlin ${frame.size} B, zstd-jni level 3 ${libzstd.size} B")
        assert(frame.size < json.size / 3) { "pure-Kotlin frame is ${frame.size} bytes for ${json.size}" }
    }

    /** Offset of the first block's literals section in a pure-Kotlin (single-segment) frame. */
    private fun literalsAt(frame: ByteArray): Int {
        val fcs = when ((frame[4].toInt() and 0xFF) ushr 6) { 0 -> 1; 1 -> 2; 2 -> 4; else -> 8 }
        val block = 5 + fcs
        assertEquals(2, (frame[block].toInt() ushr 1) and 3, "the first block is compressed")
        return block + 3
    }

    /** The first block's literals type (0 raw, 1 RLE, 2 Huffman). */
    private fun firstLiteralsType(frame: ByteArray): Int = frame[literalsAt(frame)].toInt() and 3

    /** The first block's Huffman tree header byte: >= 128 direct weights, < 128 FSE-compressed. */
    private fun treeHeader(frame: ByteArray): Int {
        val at = literalsAt(frame)
        val headerSize = when ((frame[at].toInt() ushr 2) and 3) { 0, 1 -> 3; 2 -> 4; else -> 5 }
        return frame[at + headerSize].toInt() and 0xFF
    }

    @Test
    fun encoderUsesHuffmanLiteralsWithBothWeightEncodings() {
        // Bytes 0-3, equally likely: every weight is the same, which FSE cannot code, so the
        // table must use direct 4-bit weights (tree header >= 128).
        val rnd = Random(3)
        val four = ByteArray(20_000) { rnd.nextInt(4).toByte() }
        val fourFrame = ZstdEncoder.compress(four)
        assertEquals(2, firstLiteralsType(fourFrame))
        assert(treeHeader(fourFrame) >= 128) { "expected direct weights, tree header ${treeHeader(fourFrame)}" }
        assertContentEquals(four, ZstdDecoder.decompress(fourFrame, four.size))
        assertContentEquals(four, ByteArray(four.size).also { Zstd.decompress(it, fourFrame) })
        // JSON: mostly-zero weights, where FSE-compressed weights are smaller.
        val json = ZstdEncoder.compress(ZstdTestCorpus.json(20_000))
        assertEquals(2, firstLiteralsType(json))
        assert(treeHeader(json) < 128) { "expected FSE-compressed weights, tree header ${treeHeader(json)}" }
        // Bytes above 128 can only use FSE-compressed weights (tree header < 128).
        val binary = ByteArray(20_000) { var v = 0; while (v < 255 && rnd.nextInt(4) != 0) v++; (255 - v).toByte() }
        val frame = ZstdEncoder.compress(binary)
        assertEquals(2, firstLiteralsType(frame))
        assert(treeHeader(frame) < 128) { "expected FSE-compressed weights, tree header ${treeHeader(frame)}" }
        assertContentEquals(binary, ZstdDecoder.decompress(frame, binary.size))
        val back = ByteArray(binary.size)
        Zstd.decompress(back, frame)
        assertContentEquals(binary, back)
    }

    @Test
    fun randomisedRoundTrips() {
        val rnd = Random(1234)
        repeat(300) { i ->
            val size = if (i % 10 == 0) rnd.nextInt(300_000) else rnd.nextInt(3000)
            val alphabet = 1 + rnd.nextInt(255)
            val input = ByteArray(size)
            var p = 0
            while (p < size) {
                if (p > 8 && rnd.nextInt(4) == 0) {
                    // copy from earlier: exercises offsets, overlap and repeat offsets
                    val off = 1 + rnd.nextInt(minOf(p, if (rnd.nextBoolean()) 8 else 70_000))
                    val len = minOf(size - p, 3 + rnd.nextInt(if (rnd.nextBoolean()) 20 else 2000))
                    repeat(len) { input[p] = input[p - off]; p++ }
                } else {
                    input[p++] = rnd.nextInt(alphabet).toByte()
                }
            }
            val level = listOf(-3, 1, 3, 9, 19)[i % 5]
            assertContentEquals(input, ZstdDecoder.decompress(Zstd.compress(input, level), size), "case $i level $level")
            val own = ZstdEncoder.compress(input)
            val back = ByteArray(size)
            Zstd.decompress(back, own)
            assertContentEquals(input, back, "case $i: libzstd reading the pure-Kotlin frame")
        }
    }

    @Test
    fun concatenatedAndSkippableFrames() {
        val a = "first frame ".repeat(50).encodeToByteArray()
        val b = ZstdTestCorpus.json(5000)
        val skippable = byteArrayOf(0x5A, 0x2A, 0x4D, 0x18, 3, 0, 0, 0, 9, 9, 9)
        val joined = Zstd.compress(a, 3) + skippable + ZstdEncoder.compress(b) + streamed(a, 5, checksum = true)
        assertContentEquals(a + b + a, ZstdDecoder.decompress(joined, 1 shl 20))
    }

    @Test
    fun outputLargerThanMaxIsRejected() {
        val input = ZstdTestCorpus.json(10_000)
        for (frame in listOf(Zstd.compress(input, 3), streamed(input, 3, checksum = false), ZstdEncoder.compress(input))) {
            assertFailsWith<ZstdFormatException> { ZstdDecoder.decompress(frame, input.size - 1) }
            assertContentEquals(input, ZstdDecoder.decompress(frame, input.size))
        }
    }

    // Published XXH64 vectors for short inputs; inputs of 32+ bytes (the striped path) are covered
    // by decodesEveryLibzstdFrame, whose checksummed frames carry libzstd's own XXH64.
    @Test
    fun xxHash64KnownVectors() {
        assertEquals("ef46db3751d8e999", XxHash64.hash(ByteArray(0)).toULong().toString(16))
        assertEquals("d24ec4f1a98c6e5b", XxHash64.hash("a".encodeToByteArray()).toULong().toString(16))
        assertEquals("44bc2cf5ad770999", XxHash64.hash("abc".encodeToByteArray()).toULong().toString(16))
    }
}
