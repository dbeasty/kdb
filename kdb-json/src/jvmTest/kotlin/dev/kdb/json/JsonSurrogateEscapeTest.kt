package dev.kdb.json

import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Mirrors go/kdb/json/parser_escapes_test.go. A \uXXXX escape carries one UTF-16 code unit, so
 * a character outside the BMP - every emoji, and a great deal of CJK - can only be written as a
 * surrogate pair.
 *
 * Kotlin gets this right for free: a Kotlin String is UTF-16, so appending the two halves to a
 * StringBuilder reassembles the character. Go's parser encoded each half to UTF-8 separately,
 * which substituted U+FFFD for each lone surrogate and lost the character - the two
 * implementations disagreed on the same input until that was fixed. Pinned here so this side
 * cannot quietly regress to Go's old behaviour.
 */
class JsonSurrogateEscapeTest {
    /** Builds \uXXXX escape text from raw UTF-16 code units, keeping this source pure ASCII. */
    private fun esc(vararg codeUnits: Int): String =
        codeUnits.joinToString("") { "\\u" + it.toString(16).padStart(4, '0').uppercase() }

    private fun escapeFor(codePoint: Int): String {
        if (codePoint <= 0xFFFF) return esc(codePoint)
        val v = codePoint - 0x10000
        return esc(0xD800 + (v shr 10), 0xDC00 + (v and 0x3FF))
    }

    private fun stringAt(doc: String, path: String): String {
        val v = kdbJsonGet(doc, path)
        return (v as JsonValue.JString).value
    }

    @Test
    fun surrogatePairsBecomeOneCharacter() {
        val codePoints =
            listOf(
                0x1F600, // GRINNING FACE
                0x1D11E, // MUSICAL SYMBOL G CLEF
                0x2000B, // CJK ext B ideograph
                0x10000, // lowest supplementary
                0x10FFFF, // highest code point
            )
        for (cp in codePoints) {
            val doc = """{"v":"${escapeFor(cp)}"}"""
            val got = stringAt(doc, "$.v")
            assertEquals(
                String(Character.toChars(cp)),
                got,
                "escape ${escapeFor(cp)} did not reassemble into one character",
            )
            assertEquals(1, got.codePointCount(0, got.length), "expected exactly one code point")
        }
    }

    @Test
    fun bmpEscapesDecode() {
        assertEquals("A", stringAt("""{"v":"${esc(0x0041)}"}""", "$.v"))
        assertEquals("\u00e9", stringAt("""{"v":"${esc(0x00E9)}"}""", "$.v"))
        assertEquals("\u4e2d", stringAt("""{"v":"${esc(0x4E2D)}"}""", "$.v"))
    }

    @Test
    fun rawUtf8PassesThroughUntouched() {
        for (cp in listOf(0x00E9, 0x4E2D, 0x1F600)) {
            val raw = String(Character.toChars(cp))
            assertEquals(raw, stringAt("""{"v":"$raw"}""", "$.v"))
        }
    }
}
