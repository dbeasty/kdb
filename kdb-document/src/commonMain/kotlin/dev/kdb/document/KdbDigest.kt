package dev.kdb.document

import dev.kdb.document.internal.sha256Digest

/** Multiplatform SHA-256 digest used by schema hashing and DAG verification ([Layer 1]). */
public fun kdbSha256(message: ByteArray): ByteArray = sha256Digest(message)
