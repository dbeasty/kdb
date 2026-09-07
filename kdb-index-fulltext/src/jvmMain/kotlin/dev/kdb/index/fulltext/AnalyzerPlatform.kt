package dev.kdb.index.fulltext

internal actual fun supplementaryIsLetterOrDigit(
    high: Char,
    low: Char,
): Boolean = Character.isLetterOrDigit(Character.toCodePoint(high, low))
