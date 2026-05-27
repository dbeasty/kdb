package document

import "crypto/sha256"

// SHA256Digest returns the SHA-256 digest of message (RFC 6234), matching JVM
// `java.security.MessageDigest` and Kotlin `dev.kdb.document.internal.sha256Digest`.
func SHA256Digest(message []byte) []byte {
	sum := sha256.Sum256(message)
	return sum[:]
}
