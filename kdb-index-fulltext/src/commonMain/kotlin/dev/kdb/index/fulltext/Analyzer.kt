package dev.kdb.index.fulltext

/**
 * The full-text analyzer (Layer 16 §6.1), identical in both trees:
 *
 * 1. split on every code point that is not a Unicode letter or digit;
 * 2. lowercase (simple, per-code-point case mapping — what Go's `strings.ToLower` does);
 * 3. drop tokens longer than 64 code points;
 * 4. drop the 33 stopwords of §6.2;
 * 5. Porter-stem each remaining token ([PorterStemmer.stem] leaves non-ASCII tokens alone);
 * 6. positions are 0-based indexes into the returned list, i.e. assigned *after* stopword removal.
 */
public object FullTextAnalyzer {
    public const val MAX_TOKEN_CODE_POINTS: Int = 64

    /** Exactly the classic Lucene/Snowball English stopword set (§6.2). */
    public val STOPWORDS: Set<String> =
        setOf(
            "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is",
            "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there",
            "these", "they", "this", "to", "was", "will", "with",
        )

    /** Analyzed terms; the index of each term is its position. */
    public fun analyze(text: String): List<String> {
        val out = mutableListOf<String>()
        for (raw in splitTokens(text)) {
            val lowered = lowercaseSimple(raw)
            if (codePointCount(lowered) > MAX_TOKEN_CODE_POINTS) continue
            if (lowered in STOPWORDS) continue
            out += PorterStemmer.stem(lowered)
        }
        return out
    }

    /** Step 1 only: raw tokens (letters/digits runs), before lowercasing. */
    public fun splitTokens(text: String): List<String> {
        val out = mutableListOf<String>()
        val sb = StringBuilder()
        var i = 0
        while (i < text.length) {
            val ch = text[i]
            if (ch.isHighSurrogate() && i + 1 < text.length && text[i + 1].isLowSurrogate()) {
                val low = text[i + 1]
                if (supplementaryIsLetterOrDigit(ch, low)) {
                    sb.append(ch).append(low)
                } else if (sb.isNotEmpty()) {
                    out += sb.toString()
                    sb.clear()
                }
                i += 2
                continue
            }
            if (ch.isLetterOrDigit()) {
                sb.append(ch)
            } else if (sb.isNotEmpty()) {
                out += sb.toString()
                sb.clear()
            }
            i++
        }
        if (sb.isNotEmpty()) out += sb.toString()
        return out
    }

    private fun lowercaseSimple(s: String): String {
        val sb = StringBuilder(s.length)
        for (ch in s) sb.append(if (ch.isSurrogate()) ch else ch.lowercaseChar())
        return sb.toString()
    }

    private fun codePointCount(s: String): Int {
        var n = 0
        var i = 0
        while (i < s.length) {
            if (s[i].isHighSurrogate() && i + 1 < s.length && s[i + 1].isLowSurrogate()) i += 2 else i++
            n++
        }
        return n
    }
}

/** Whether the supplementary code point formed by a surrogate pair is a Unicode letter or digit. */
internal expect fun supplementaryIsLetterOrDigit(
    high: Char,
    low: Char,
): Boolean
