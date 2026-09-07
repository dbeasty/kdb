package dev.kdb.index.fulltext

import kotlin.test.Test
import kotlin.test.assertEquals

class PorterStemmerTest {
    /** Guards the classic Porter test cases the spec lists for `porter_vectors.txt` (§6.1). */
    @Test
    fun stemsThePaperExamples() {
        val cases =
            mapOf(
                "caresses" to "caress", "ponies" to "poni", "ties" to "ti", "caress" to "caress",
                "cats" to "cat", "feed" to "feed", "agreed" to "agre", "plastered" to "plaster",
                "motoring" to "motor", "sing" to "sing", "conflated" to "conflat", "troubled" to "troubl",
                "sized" to "size", "hopping" to "hop", "tanned" to "tan", "falling" to "fall",
                "hissing" to "hiss", "fizzed" to "fizz", "failing" to "fail", "filing" to "file",
                "happy" to "happi", "sky" to "sky", "relational" to "relat", "conditional" to "condit",
                "rational" to "ration", "valenci" to "valenc", "hesitanci" to "hesit",
                "digitizer" to "digit", "conformabli" to "conform", "radicalli" to "radic",
                "differentli" to "differ", "vileli" to "vile", "analogousli" to "analog",
                "vietnamization" to "vietnam", "predication" to "predic", "operator" to "oper",
                "feudalism" to "feudal", "decisiveness" to "decis", "hopefulness" to "hope",
                "callousness" to "callous", "formaliti" to "formal", "sensitiviti" to "sensit",
                "sensibiliti" to "sensibl", "triplicate" to "triplic", "formative" to "form",
                "formalize" to "formal", "electriciti" to "electr", "electrical" to "electr",
                "hopeful" to "hope", "goodness" to "good", "revival" to "reviv", "allowance" to "allow",
                "inference" to "infer", "airliner" to "airlin", "gyroscopic" to "gyroscop",
                "adjustable" to "adjust", "defensible" to "defens", "irritant" to "irrit",
                "replacement" to "replac", "adjustment" to "adjust", "dependent" to "depend",
                "adoption" to "adopt", "homologou" to "homolog", "communism" to "commun",
                "activate" to "activ", "angulariti" to "angular", "homologous" to "homolog",
                "effective" to "effect", "bowdlerize" to "bowdler", "probate" to "probat",
                "rate" to "rate", "cease" to "ceas", "controll" to "control", "roll" to "roll",
            )
        for ((word, expected) in cases) {
            assertEquals(expected, PorterStemmer.stem(word), "stem($word)")
        }
    }

    /** Guards the reference implementation's short-word rule: two letters or fewer are untouched. */
    @Test
    fun leavesWordsOfTwoLettersOrFewerAlone() {
        assertEquals("as", PorterStemmer.stem("as"))
        assertEquals("i", PorterStemmer.stem("i"))
    }

    /** Guards the paper's step-2 table, which Go implements: `abli → able`, and no `logi` rule. */
    @Test
    fun usesThePapersStepTwoTable() {
        // conformabli → (abli→able) conformable → (step 4, able, m>1) conform
        assertEquals("conform", PorterStemmer.stem("conformabli"))
        // No logi → log rule: apologi keeps its stem shape rather than becoming "apolog".
        assertEquals("apologi", PorterStemmer.stem("apologi"))
    }

    /** Guards non-ASCII tokens being returned unchanged rather than mangled byte-wise. */
    @Test
    fun leavesNonAsciiUnchanged() {
        assertEquals("café", PorterStemmer.stem("café"))
        assertEquals("naïveté", PorterStemmer.stem("naïveté"))
    }
}
