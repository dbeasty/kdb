package dev.kdb.index.fulltext

/**
 * Kotlin/Native has no code-point classification API; supplementary-plane characters (outside
 * the BMP) are treated as token boundaries here. JVM and JS classify them exactly.
 */
internal actual fun supplementaryIsLetterOrDigit(
    high: Char,
    low: Char,
): Boolean = false
