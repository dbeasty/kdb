package dev.kdb.compression

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertTrue

class ZstdCompressionTest {
    /**
     * A real zstd frame (as the JVM and Go write it) for "kdb", with its content size declared.
     * JS and Native used to refuse it with ZstdUnavailableException (and before that misread its
     * first four bytes as a length prefix); every target must now decode it.
     */
    private val realFrame: ByteArray =
        byteArrayOf(0x28, 0xB5.toByte(), 0x2F, 0xFD.toByte(), 0x20, 0x03, 0x19, 0x00, 0x00, 0x6B, 0x64, 0x62)

    @Test
    fun everyTargetReadsAndWritesZstd() {
        assertTrue(ZstdCompression.canDecompress, "roadmap R1: every target decodes zstd")
        assertTrue(ZstdCompression.canCompress, "roadmap R1: every target encodes zstd")
        assertContentEquals("kdb".encodeToByteArray(), ZstdCompression.decompress(realFrame, 3))
        val payload = "kdb zstd round trip payload, compressible compressible".encodeToByteArray()
        val frame = ZstdCompression.compress(payload)
        assertContentEquals(byteArrayOf(0x28, 0xB5.toByte(), 0x2F, 0xFD.toByte()), frame.copyOf(4), "a real zstd frame")
        assertContentEquals(payload, ZstdCompression.decompress(frame, payload.size))
    }
}
