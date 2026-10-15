package dev.kdb.document.internal

internal actual fun sha256OnlyChild(
    nib: Int,
    child: ByteArray,
): ByteArray = sha256OnlyChildMidstate(nib, child)

internal actual fun <T> runEach(
    n: Int,
    task: (Int) -> T,
): List<T> = List(n, task)
