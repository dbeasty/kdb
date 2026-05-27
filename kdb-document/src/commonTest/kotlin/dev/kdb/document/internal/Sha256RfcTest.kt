package dev.kdb.document.internal

import kotlin.test.Test
import kotlin.test.assertEquals

private val hexDigits = "0123456789abcdef".toCharArray()

private fun hexByte(b: Byte): String {
    val v = b.toInt() and 0xFF
    return "${hexDigits[v shr 4]}${hexDigits[v and 0xF]}"
}

private fun ByteArray.toHexLower(): String = joinToString("") { hexByte(it) }

class Sha256RfcTest {
    @Test
    fun matchesKnownRfcVectors() {
        assertEquals(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            sha256Digest(byteArrayOf()).toHexLower(),
        )
        assertEquals(
            "6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d",
            sha256Digest(byteArrayOf(0)).toHexLower(),
        )
        assertEquals(
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            sha256Digest("abc".encodeToByteArray()).toHexLower(),
        )
    }
}
