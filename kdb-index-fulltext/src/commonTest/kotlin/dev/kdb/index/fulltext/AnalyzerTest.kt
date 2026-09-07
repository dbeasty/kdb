package dev.kdb.index.fulltext

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class AnalyzerTest {
    /** Guards §6.1 step 1: every non letter-or-digit code point is a token boundary. */
    @Test
    fun splitsOnEveryNonLetterOrDigit() {
        assertEquals(
            listOf("deploy", "staging", "v2", "now"),
            FullTextAnalyzer.splitTokens("deploy/staging — v2 (now)!"),
        )
    }

    /** Guards §6.1 step 2: tokens are lowercased before every later step. */
    @Test
    fun lowercasesBeforeStopwordsAndStemming() {
        // "Deploying" → step1ab strips "ing" → "deploy" → step1c turns the y after a vowel into i.
        assertEquals(listOf("deploi"), FullTextAnalyzer.analyze("THE Deploying"))
    }

    /** Guards §6.1 step 3: a token longer than 64 code points is dropped, 64 is kept. */
    @Test
    fun dropsTokensLongerThan64CodePoints() {
        val exactly64 = "x".repeat(64)
        val tooLong = "y".repeat(65)
        assertEquals(listOf(exactly64), FullTextAnalyzer.analyze("$exactly64 $tooLong"))
    }

    /** Guards §6.2: exactly the 33 listed stopwords, and only after lowercasing. */
    @Test
    fun dropsExactlyTheThirtyThreeStopwords() {
        assertEquals(33, FullTextAnalyzer.STOPWORDS.size)
        assertEquals(
            listOf(
                "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is",
                "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there",
                "these", "they", "this", "to", "was", "will", "with",
            ).sorted(),
            FullTextAnalyzer.STOPWORDS.sorted(),
        )
        assertTrue(FullTextAnalyzer.analyze("THE and OF").isEmpty())
    }

    /** Guards §6.1 step 6: positions are assigned after stopword removal, so phrases skip them. */
    @Test
    fun positionsAreAssignedAfterStopwordRemoval() {
        // "the" and "of" are dropped, so "deploi" is 0 and "stage" is 1 — adjacent for phrases.
        assertEquals(listOf("deploi", "stage"), FullTextAnalyzer.analyze("the deploy of staging"))
    }

    /** Guards §6.1 step 5: a token holding any non-ASCII letter is left unstemmed. */
    @Test
    fun leavesNonAsciiTokensUnstemmed() {
        assertEquals(listOf("naïve"), FullTextAnalyzer.analyze("naïve"))
        // The ASCII neighbour of the same shape still stems.
        assertEquals(listOf("caress"), FullTextAnalyzer.analyze("caresses"))
    }

    /** Guards digits surviving analysis: they are letters-or-digits, never dropped. */
    @Test
    fun keepsDigitTokens() {
        assertEquals(listOf("v2", "2026"), FullTextAnalyzer.analyze("v2 2026"))
    }
}
