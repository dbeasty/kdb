package auth

import "testing"

// TestVerifyPasswordAgainstKotlinReference is component 38 spec §7 test 5: a hash produced by
// the Kotlin PasswordHasher must verify successfully via this package's VerifyPassword, proving
// PBKDF2 parameter compatibility rather than assuming it. The expected hash below was computed
// independently via the JDK's own javax.crypto (PBKDF2WithHmacSHA256, 120k iterations, 256-bit
// key) for password "correct horse battery staple" and salt bytes 0x00..0x0f - the exact
// algorithm/parameters kdb-auth-store's PasswordHasher.kt uses, run outside this Go package so
// this is a genuine cross-implementation check, not a self-consistency one.
func TestVerifyPasswordAgainstKotlinReference(t *testing.T) {
	const password = "correct horse battery staple"
	const saltHex = "000102030405060708090a0b0c0d0e0f"
	const kotlinHashHex = "e962ebd8267bc839386d4608bbc3c8ac36bfb215faa8544e3a4e2cbccec84806"

	if !VerifyPassword(password, kotlinHashHex, saltHex) {
		t.Fatal("Go VerifyPassword rejected a hash produced by the JDK reference implementation")
	}
	if VerifyPassword("wrong password", kotlinHashHex, saltHex) {
		t.Fatal("VerifyPassword accepted the wrong password")
	}
}

func TestHashPasswordRoundTrip(t *testing.T) {
	hashHex, saltHex, err := HashPassword("s3cret!")
	if err != nil {
		t.Fatal(err)
	}
	if len(hashHex) != passwordHashKeyLenBytes*2 {
		t.Fatalf("hash length: got %d hex chars, want %d", len(hashHex), passwordHashKeyLenBytes*2)
	}
	if len(saltHex) != passwordHashSaltLenBytes*2 {
		t.Fatalf("salt length: got %d hex chars, want %d", len(saltHex), passwordHashSaltLenBytes*2)
	}
	if !VerifyPassword("s3cret!", hashHex, saltHex) {
		t.Fatal("VerifyPassword rejected its own HashPassword output")
	}
	if VerifyPassword("not-the-secret", hashHex, saltHex) {
		t.Fatal("VerifyPassword accepted the wrong password")
	}
}

func TestHashPasswordSaltsAreRandom(t *testing.T) {
	_, saltA, err := HashPassword("same-password")
	if err != nil {
		t.Fatal(err)
	}
	_, saltB, err := HashPassword("same-password")
	if err != nil {
		t.Fatal(err)
	}
	if saltA == saltB {
		t.Fatal("expected two independent HashPassword calls to use different random salts")
	}
}

func TestVerifyPasswordRejectsMalformedHex(t *testing.T) {
	if VerifyPassword("x", "not-hex", "also-not-hex") {
		t.Fatal("expected malformed hex to fail closed")
	}
}
