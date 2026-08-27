package dev.kdb.compression

import com.github.luben.zstd.ZstdOutputStream
import java.io.ByteArrayOutputStream
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFailsWith

class ZstdCompressionJvmTest {
    private val payload = ByteArray(4096) { (it % 251).toByte() }

    @Test
    fun roundTripOwnEncoder() {
        val compressed = ZstdCompression.compress(payload, 3)
        assertContentEquals(payload, ZstdCompression.decompress(compressed, payload.size))
    }

    @Test
    fun decompressesFrameWithoutDeclaredContentSize() {
        // Streaming encoders (and Go's klauspost EncodeAll - the other KDB implementation)
        // legally emit frames with no Frame_Content_Size field. Requiring one made every
        // Go-written delta segment look corrupt on the JVM, and the replayer then opened the
        // whole directory as silently empty (found by the differential-CLI e2e scenario).
        val sizeless = ByteArrayOutputStream().use { buf ->
            ZstdOutputStream(buf).use { it.write(payload) }
            buf.toByteArray()
        }
        assertContentEquals(payload, ZstdCompression.decompress(sizeless, payload.size))
    }

    @Test
    fun declaredSizeLargerThanMaxIsRejected() {
        val compressed = ZstdCompression.compress(payload, 3)
        assertFailsWith<IllegalArgumentException> {
            ZstdCompression.decompress(compressed, payload.size - 1)
        }
    }

    @Test
    fun incompressibleDataRoundTrips() {
        val noise = Random(7).nextBytes(2048)
        val compressed = ZstdCompression.compress(noise, 3)
        assertContentEquals(noise, ZstdCompression.decompress(compressed, noise.size))
    }
}
