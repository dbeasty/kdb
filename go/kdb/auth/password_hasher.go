package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"

	"golang.org/x/crypto/pbkdf2"
)

// Password hashing parameters. Deliberately identical to the Kotlin reference
// (kdb-auth-store's PasswordHasher.kt: PBKDF2WithHmacSHA256, 120k iterations, 256-bit key,
// 16-byte salt) so a hash produced by one implementation verifies against the other - see
// component 38 spec §5's hash-portability contract and §7 test 5.
const (
	passwordHashIterations   = 120_000
	passwordHashKeyLenBytes  = 32 // 256 bits
	passwordHashSaltLenBytes = 16
)

// HashPassword derives a PBKDF2-HMAC-SHA256 hash for password with a fresh random salt,
// returning both as lowercase hex - the same encoding VerifyPassword and the Kotlin
// PasswordHasher expect.
func HashPassword(password string) (hashHex, saltHex string, err error) {
	salt := make([]byte, passwordHashSaltLenBytes)
	if _, err := rand.Read(salt); err != nil {
		return "", "", fmt.Errorf("kdb auth: generating password salt: %w", err)
	}
	digest := deriveKey(password, salt)
	return hex.EncodeToString(digest), hex.EncodeToString(salt), nil
}

// VerifyPassword reports whether password matches the given hex-encoded hash/salt, using a
// constant-time comparison. Malformed hex decodes to a non-match rather than an error, matching
// the Kotlin reference's fail-closed behavior.
func VerifyPassword(password, expectedHashHex, saltHex string) bool {
	salt, err := hex.DecodeString(saltHex)
	if err != nil {
		return false
	}
	expected, err := hex.DecodeString(expectedHashHex)
	if err != nil {
		return false
	}
	digest := deriveKey(password, salt)
	return subtle.ConstantTimeCompare(digest, expected) == 1
}

func deriveKey(password string, salt []byte) []byte {
	return pbkdf2.Key([]byte(password), salt, passwordHashIterations, passwordHashKeyLenBytes, sha256.New)
}
