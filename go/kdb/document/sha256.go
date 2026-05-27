package document

import "crypto/sha256"

// SHA256Digest returns the SHA-256 digest of message (RFC 6234).
func SHA256Digest(message []byte) []byte {
	sum := sha256.Sum256(message)
	return sum[:]
}
