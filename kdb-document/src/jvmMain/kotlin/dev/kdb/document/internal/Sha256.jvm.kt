package dev.kdb.document.internal

import java.security.MessageDigest

// One digest per thread: getInstance looks the provider up on every call, and a MessageDigest
// is not safe to share.
private val digests = ThreadLocal.withInitial { MessageDigest.getInstance("SHA-256") }

/**
 * The JDK's SHA-256, which HotSpot runs on the CPU's SHA instructions (x86 SHA-NI, ARMv8 SHA2):
 * about ten times the portable implementation on a 513-byte trie node.
 */
internal actual fun sha256Digest(message: ByteArray): ByteArray = digests.get().digest(message)
