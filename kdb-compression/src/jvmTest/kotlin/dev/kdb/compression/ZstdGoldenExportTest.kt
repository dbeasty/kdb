package dev.kdb.compression

import com.github.luben.zstd.Zstd
import com.github.luben.zstd.ZstdOutputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.nio.file.Paths
import kotlin.io.path.isDirectory
import kotlin.test.Test

/**
 * Exports the Kotlin side of the zstd golden corpus (see [ZstdGoldenCorpus]); run by
 * `make test-physical-golden`. zstd-jni frames are what the JVM writes; `_purekotlin` frames are
 * what Kotlin/JS and Native write, and their jsTest/nativeTest assert exactly that.
 */
class ZstdGoldenExportTest {
    @Test
    fun exportZstdCorpus() {
        for (level in listOf(1, 3, 19)) writeHex("zstd_corpus_json20k_l$level.hex", Zstd.compress(ZstdGoldenCorpus.payload("json20k"), level))
        writeHex("zstd_corpus_json140k_l3.hex", Zstd.compress(ZstdGoldenCorpus.payload("json140k"), 3))
        // A streamed frame: no content size, a checksum, one block per flush.
        val streamed =
            ByteArrayOutputStream().use { buf ->
                ZstdOutputStream(buf, 3).use { out ->
                    out.setChecksum(true)
                    ZstdGoldenCorpus.payload("json20k").asList().chunked(7000).forEach {
                        out.write(it.toByteArray())
                        out.flush()
                    }
                }
                buf.toByteArray()
            }
        writeHex("zstd_corpus_json20k_stream.hex", streamed)
        for (spec in listOf("json20k", "json140k", "empty")) {
            writeHex("zstd_corpus_${spec}_purekotlin.hex", ZstdEncoder.compress(ZstdGoldenCorpus.payload(spec)))
        }
    }

    private fun writeHex(name: String, bytes: ByteArray) {
        val root = Paths.get(System.getProperty("user.dir"))
        val repo = generateSequence(root) { it.parent }.firstOrNull { it.resolve("go").isDirectory() } ?: root
        val dir = repo.resolve("go/testdata/golden/physical/kotlin").toFile().also { it.mkdirs() }
        File(dir, name).writeText(bytes.joinToString("") { "%02x".format(it) } + "\n")
    }
}
