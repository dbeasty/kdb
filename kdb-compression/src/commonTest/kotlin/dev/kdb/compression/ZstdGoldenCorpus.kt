package dev.kdb.compression

import dev.kdb.golden.PhysicalGoldenFixtures
import kotlin.test.assertContentEquals
import kotlin.test.assertTrue

/**
 * The zstd golden corpus: `go/testdata/golden/physical/<side>/zstd_corpus_<payload>_<variant>.hex`.
 * Go writes its side with klauspost/compress at several levels (TestExportPhysicalGoldenZstd);
 * the JVM writes its side with zstd-jni and with the pure-Kotlin [ZstdEncoder]
 * (ZstdGoldenExportTest). `<payload>` names a [ZstdTestCorpus] payload both languages generate.
 */
internal object ZstdGoldenCorpus {
    class Frame(val name: String, val frame: ByteArray, val payload: ByteArray) {
        /** Whether the frame carries the XXH64 content checksum (frame header descriptor bit 2). */
        val checksummed: Boolean get() = frame.size > 4 && frame[4].toInt() and 4 != 0
    }

    private val payloads = mutableMapOf<String, ByteArray>()

    fun payload(spec: String): ByteArray =
        payloads.getOrPut(spec) {
            when {
                spec == "empty" -> ByteArray(0)
                spec.startsWith("json") && spec.endsWith("k") -> ZstdTestCorpus.json(spec.drop(4).dropLast(1).toInt() * 1000)
                else -> error("unknown zstd corpus payload '$spec'")
            }
        }

    fun frames(): List<Frame> =
        listOf("go", "kotlin").flatMap { side ->
            PhysicalGoldenFixtures.names(side).filter { it.startsWith("zstd_corpus_") }.map { name ->
                val spec = name.removePrefix("zstd_corpus_").substringBefore('_')
                Frame("$side/$name", PhysicalGoldenFixtures.read(side, name)!!, payload(spec))
            }
        }

    /** Fixtures that must exist, so a missing export cannot turn the corpus tests into no-ops. */
    val required: List<String> =
        listOf(
            "go/zstd_corpus_json20k_l1.hex", "go/zstd_corpus_json20k_l3.hex", "go/zstd_corpus_json20k_l7.hex",
            "go/zstd_corpus_json20k_l19.hex", "go/zstd_corpus_json140k_l3.hex",
            "kotlin/zstd_corpus_json20k_l1.hex", "kotlin/zstd_corpus_json20k_l3.hex", "kotlin/zstd_corpus_json20k_l19.hex",
            "kotlin/zstd_corpus_json20k_stream.hex", "kotlin/zstd_corpus_json140k_l3.hex",
            "kotlin/zstd_corpus_json20k_purekotlin.hex", "kotlin/zstd_corpus_json140k_purekotlin.hex",
            "kotlin/zstd_corpus_empty_purekotlin.hex",
        )

    /**
     * On a target whose [ZstdCompression.compress] is the pure-Kotlin encoder (JS, Native): what it
     * writes is byte-for-byte the committed `*_purekotlin` fixture, which Go decodes in
     * TestKotlinZstdCorpusCrossDecodes - so Go reads the frames this target writes.
     */
    fun assertPlatformCompressWritesCommittedFixtures() {
        val pure = frames().filter { it.name.endsWith("_purekotlin.hex") }
        assertTrue(pure.size >= 3, "pure-Kotlin fixtures missing: ${pure.map { it.name }}")
        for (f in pure) assertContentEquals(f.frame, ZstdCompression.compress(f.payload), "${f.name}: this target's output")
    }
}
