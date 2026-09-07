package dev.kdb.index.fulltext

private val letterOrDigit = Regex("^[\\p{L}\\p{Nd}]$")

internal actual fun supplementaryIsLetterOrDigit(
    high: Char,
    low: Char,
): Boolean = letterOrDigit.matches(charArrayOf(high, low).concatToString())
