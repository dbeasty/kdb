package dev.kdb.compression

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFailsWith

class ZstdCompressionTest {
    /**
     * A real zstd frame (as the JVM and Go write it) for "kdb", with its content size declared.
     * Stub targets used to read its first four bytes as a length prefix and return garbage or
     * fail with a confusing size error; they must now refuse with [ZstdUnavailableException].
     */
    private val realFrame: ByteArray =
        byteArrayOf(0x28, 0xB5.toByte(), 0x2F, 0xFD.toByte(), 0x20, 0x03, 0x19, 0x00, 0x00, 0x6B, 0x64, 0x62)

    @Test
    fun roundTripsWhereAvailableAndRefusesWhereNot() {
        val payload = "kdb zstd round trip payload, compressible compressible".encodeToByteArray()
        if (ZstdCompression.isAvailable) {
            assertContentEquals(payload, ZstdCompression.decompress(ZstdCompression.compress(payload), payload.size))
            assertContentEquals("kdb".encodeToByteArray(), ZstdCompression.decompress(realFrame, 3))
        } else {
            assertFailsWith<ZstdUnavailableException> { ZstdCompression.compress(payload) }
            assertFailsWith<ZstdUnavailableException> { ZstdCompression.decompress(realFrame, 3) }
        }
    }
}
