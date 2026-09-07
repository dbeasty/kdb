package dev.kdb.index.fulltext

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Parity gate for the §6.1 analyzer and its Porter stemmer: the Kotlin analyzer must produce
 * exactly the token lists and stems the Go tree pinned in the shared fixtures.
 */
class AnalyzerParityTest {

    @Test
    fun tokensMatchAnalyzerVectors() {
        val name = "analyzer_vectors.json"
        val fixture = GoldenFixtures.json(name)
        if (fixture == null) {
            println(GoldenFixtures.missing(name))
            return
        }
        val vectors = fixture.arr()
        assertTrue(vectors.isNotEmpty(), "$name holds no vectors")
        for (vector in vectors) {
            val text = vector.field("text")?.str() ?: ""
            val expected = vector.field("tokens")?.arr()?.map { it.str() } ?: emptyList()
            assertEquals(expected, FullTextAnalyzer.analyze(text), "analyze(${quote(text)})")
        }
    }

    @Test
    fun stemsMatchPorterVectors() {
        val name = "porter_vectors.txt"
        val text = GoldenFixtures.text(name)
        if (text == null) {
            println(GoldenFixtures.missing(name))
            return
        }
        var checked = 0
        for (line in text.lineSequence()) {
            val trimmed = line.trim()
            if (trimmed.isEmpty() || trimmed.startsWith("#")) continue
            val parts = trimmed.split(' ').filter { it.isNotEmpty() }
            assertEquals(2, parts.size, "malformed line in $name: $line")
            assertEquals(parts[1], PorterStemmer.stem(parts[0]), "stem(${parts[0]})")
            checked++
        }
        assertTrue(checked >= 200, "$name should hold at least 200 pairs, found $checked")
    }

    private fun quote(s: String) = "\"$s\""
}
