@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package dev.kdb.codec.internal

import kotlinx.cinterop.convert
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.usePinned
import platform.posix.fclose
import platform.posix.fopen
import platform.posix.fread

internal actual fun secureRandomBytes(count: Int): ByteArray {
    require(count >= 0)
    val out = ByteArray(count)
    out.usePinned { pin ->
        val f = fopen("/dev/urandom", "rb") ?: error("failed to open /dev/urandom")
        try {
            val read = fread(pin.addressOf(0), 1u, count.convert(), f)
            if (read.toInt() != count) {
                error("short read from /dev/urandom")
            }
        } finally {
            fclose(f)
        }
    }
    return out
}
