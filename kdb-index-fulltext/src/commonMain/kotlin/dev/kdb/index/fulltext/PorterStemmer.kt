package dev.kdb.index.fulltext

/**
 * Porter's 1980 stemmer ("An algorithm for suffix stripping", Program 14(3)): the reference
 * implementation's control flow with the *paper's* rule tables — step 2 keeps `abli → able` and
 * has no `logi → log` rule (those are later Tartarus departures). This is the variant Go
 * implements and `porter_vectors.txt` pins.
 *
 * Tokens of two characters or fewer are returned unchanged, as is any token holding a non-ASCII
 * character (the algorithm is defined over ASCII only, so such a token is left alone rather than
 * mangled). A token of ASCII letters *and digits* is stemmed — digits behave as consonants —
 * which is what Go's byte-wise check does.
 */
public object PorterStemmer {
    public fun stem(word: String): String {
        if (word.length <= 2) return word
        for (ch in word) if (ch.code >= 0x80) return word
        return Stemmer(word.toCharArray()).run()
    }

    private class Stemmer(private var b: CharArray) {
        private var k = b.size - 1
        private var j = 0

        fun run(): String {
            if (k <= 1) return b.concatToString(0, k + 1)
            step1ab()
            step1c()
            step2()
            step3()
            step4()
            step5()
            return b.concatToString(0, k + 1)
        }

        private fun cons(i: Int): Boolean =
            when (b[i]) {
                'a', 'e', 'i', 'o', 'u' -> false
                'y' -> if (i == 0) true else !cons(i - 1)
                else -> true
            }

        /** Number of consonant sequences between 0 and j. */
        private fun m(): Int {
            var n = 0
            var i = 0
            while (true) {
                if (i > j) return n
                if (!cons(i)) break
                i++
            }
            i++
            while (true) {
                while (true) {
                    if (i > j) return n
                    if (cons(i)) break
                    i++
                }
                i++
                n++
                while (true) {
                    if (i > j) return n
                    if (!cons(i)) break
                    i++
                }
                i++
            }
        }

        private fun vowelInStem(): Boolean {
            for (i in 0..j) if (!cons(i)) return true
            return false
        }

        private fun doubleC(j: Int): Boolean {
            if (j < 1) return false
            if (b[j] != b[j - 1]) return false
            return cons(j)
        }

        private fun cvc(i: Int): Boolean {
            if (i < 2 || !cons(i) || cons(i - 1) || !cons(i - 2)) return false
            val ch = b[i]
            return !(ch == 'w' || ch == 'x' || ch == 'y')
        }

        private fun ends(s: String): Boolean {
            val l = s.length
            val o = k - l + 1
            if (o < 0) return false
            for (i in 0 until l) if (b[o + i] != s[i]) return false
            j = k - l
            return true
        }

        private fun setTo(s: String) {
            val l = s.length
            val o = j + 1
            if (o + l > b.size) b = b.copyOf(o + l)
            for (i in 0 until l) b[o + i] = s[i]
            k = j + l
        }

        private fun r(s: String) {
            if (m() > 0) setTo(s)
        }

        private fun step1ab() {
            if (b[k] == 's') {
                when {
                    ends("sses") -> k -= 2
                    ends("ies") -> setTo("i")
                    b[k - 1] != 's' -> k--
                }
            }
            if (ends("eed")) {
                if (m() > 0) k--
            } else if ((ends("ed") || ends("ing")) && vowelInStem()) {
                k = j
                when {
                    ends("at") -> setTo("ate")
                    ends("bl") -> setTo("ble")
                    ends("iz") -> setTo("ize")
                    doubleC(k) -> {
                        k--
                        val ch = b[k]
                        if (ch == 'l' || ch == 's' || ch == 'z') k++
                    }
                    m() == 1 && cvc(k) -> setTo("e")
                }
            }
        }

        private fun step1c() {
            if (ends("y") && vowelInStem()) b[k] = 'i'
        }

        private fun step2() {
            if (k == 0) return
            when (b[k - 1]) {
                'a' -> {
                    if (ends("ational")) { r("ate"); return }
                    if (ends("tional")) { r("tion"); return }
                }
                'c' -> {
                    if (ends("enci")) { r("ence"); return }
                    if (ends("anci")) { r("ance"); return }
                }
                'e' -> if (ends("izer")) { r("ize"); return }
                'l' -> {
                    if (ends("abli")) { r("able"); return }
                    if (ends("alli")) { r("al"); return }
                    if (ends("entli")) { r("ent"); return }
                    if (ends("eli")) { r("e"); return }
                    if (ends("ousli")) { r("ous"); return }
                }
                'o' -> {
                    if (ends("ization")) { r("ize"); return }
                    if (ends("ation")) { r("ate"); return }
                    if (ends("ator")) { r("ate"); return }
                }
                's' -> {
                    if (ends("alism")) { r("al"); return }
                    if (ends("iveness")) { r("ive"); return }
                    if (ends("fulness")) { r("ful"); return }
                    if (ends("ousness")) { r("ous"); return }
                }
                't' -> {
                    if (ends("aliti")) { r("al"); return }
                    if (ends("iviti")) { r("ive"); return }
                    if (ends("biliti")) { r("ble"); return }
                }
            }
        }

        private fun step3() {
            when (b[k]) {
                'e' -> {
                    if (ends("icate")) { r("ic"); return }
                    if (ends("ative")) { r(""); return }
                    if (ends("alize")) { r("al"); return }
                }
                'i' -> if (ends("iciti")) { r("ic"); return }
                'l' -> {
                    if (ends("ical")) { r("ic"); return }
                    if (ends("ful")) { r(""); return }
                }
                's' -> if (ends("ness")) { r(""); return }
            }
        }

        private fun step4() {
            if (k == 0) return
            val matched =
                when (b[k - 1]) {
                    'a' -> ends("al")
                    'c' -> ends("ance") || ends("ence")
                    'e' -> ends("er")
                    'i' -> ends("ic")
                    'l' -> ends("able") || ends("ible")
                    'n' -> ends("ant") || ends("ement") || ends("ment") || ends("ent")
                    'o' -> (ends("ion") && j >= 0 && (b[j] == 's' || b[j] == 't')) || ends("ou")
                    's' -> ends("ism")
                    't' -> ends("ate") || ends("iti")
                    'u' -> ends("ous")
                    'v' -> ends("ive")
                    'z' -> ends("ize")
                    else -> false
                }
            if (matched && m() > 1) k = j
        }

        private fun step5() {
            j = k
            if (b[k] == 'e') {
                val a = m()
                if (a > 1 || (a == 1 && !cvc(k - 1))) k--
            }
            if (b[k] == 'l' && doubleC(k) && m() > 1) k--
        }
    }
}
