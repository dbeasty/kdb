package dev.kdb.document.internal

internal actual fun sha256Digest(message: ByteArray): ByteArray = sha256DigestPortable(message)
