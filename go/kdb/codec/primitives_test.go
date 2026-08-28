package codec

import (
	"strings"
	"testing"
)

// UUID, Hash and Timestamp are the identity and ordering types every layer above this one is
// built on - a commit's hash, a document's id, a write's instant - and none of them had a test.

func TestUUIDStringRoundTrip(t *testing.T) {
	for _, s := range []string{
		"00000000-0000-0000-0000-000000000000",
		"11111111-2222-4333-8444-555555555555",
		"ffffffff-ffff-ffff-ffff-ffffffffffff",
		"7fffffff-ffff-ffff-ffff-ffffffffffff",
		"80000000-0000-0000-0000-000000000000", // MSB's sign bit set: the int64 field goes negative
	} {
		u, err := UUIDFromString(s)
		if err != nil {
			t.Fatalf("%s: %v", s, err)
		}
		if got := u.String(); got != s {
			t.Errorf("%s round-tripped as %s", s, got)
		}
	}
}

func TestUUIDBytesRoundTrip(t *testing.T) {
	b := []byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x46, 0x77,
		0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}
	u, err := UUIDFromBytes(b)
	if err != nil {
		t.Fatal(err)
	}
	got := u.Bytes()
	if len(got) != 16 {
		t.Fatalf("Bytes() returned %d bytes", len(got))
	}
	for i := range b {
		if got[i] != b[i] {
			t.Fatalf("byte %d: got %#x, want %#x", i, got[i], b[i])
		}
	}
	// Bytes() must hand back a copy - a caller mutating it must not corrupt the UUID, which is
	// used as a map key and a hash input all over the storage layer.
	got[0] = 0xee
	if u.Bytes()[0] != 0x00 {
		t.Fatal("Bytes() aliases the UUID's own storage")
	}
}

func TestUUIDFromBytesRejectsWrongLength(t *testing.T) {
	for _, n := range []int{0, 15, 17, 32} {
		if _, err := UUIDFromBytes(make([]byte, n)); err == nil {
			t.Errorf("%d bytes was accepted as a uuid", n)
		}
	}
}

func TestUUIDFromStringRejectsMalformed(t *testing.T) {
	for _, s := range []string{
		"",
		"not-a-uuid",
		"11111111-2222-4333-8444-55555555555",   // one hex digit short
		"11111111-2222-4333-8444-5555555555555", // one too many
		"gggggggg-2222-4333-8444-555555555555",  // right length, not hex
	} {
		if _, err := UUIDFromString(s); err == nil {
			t.Errorf("%q was accepted as a uuid", s)
		}
	}
}

// UUIDFromString and ParseUUID are separate entry points with separate validation; ParseUUID
// additionally trims surrounding whitespace, which is what makes it the one to use on
// user-supplied text.
func TestParseUUIDMatchesUUIDFromStringAndTrims(t *testing.T) {
	const s = "11111111-2222-4333-8444-555555555555"
	strict, err := UUIDFromString(s)
	if err != nil {
		t.Fatal(err)
	}
	lenient, err := ParseUUID("  " + s + "\n")
	if err != nil {
		t.Fatalf("ParseUUID did not trim surrounding whitespace: %v", err)
	}
	if lenient != strict {
		t.Fatalf("ParseUUID gave %v, UUIDFromString gave %v", lenient, strict)
	}
	if _, err := ParseUUID("nope"); err == nil {
		t.Error("ParseUUID accepted a non-uuid")
	}
}

func TestUUIDAcceptsUppercaseAndNormalizesToLower(t *testing.T) {
	upper, err := UUIDFromString("AABBCCDD-EEFF-4011-8022-334455667788")
	if err != nil {
		t.Fatal(err)
	}
	lower, err := UUIDFromString("aabbccdd-eeff-4011-8022-334455667788")
	if err != nil {
		t.Fatal(err)
	}
	if upper != lower {
		t.Fatal("case changed the parsed value")
	}
	if got := upper.String(); got != "aabbccdd-eeff-4011-8022-334455667788" {
		t.Fatalf("String() is not canonical lowercase: %s", got)
	}
}

func TestRandomUUIDIsVersion4AndDistinct(t *testing.T) {
	seen := make(map[UUID]bool, 64)
	for i := 0; i < 64; i++ {
		u, err := RandomUUID()
		if err != nil {
			t.Fatal(err)
		}
		if seen[u] {
			t.Fatalf("RandomUUID repeated %s within %d draws", u, i+1)
		}
		seen[u] = true
		b := u.Bytes()
		if got := b[6] >> 4; got != 4 {
			t.Fatalf("version nibble is %d, want 4 (%s)", got, u)
		}
		if got := b[8] >> 6; got != 0b10 {
			t.Fatalf("variant bits are %02b, want 10 (%s)", got, u)
		}
	}
}

func TestHashHexRoundTrip(t *testing.T) {
	hex64 := strings.Repeat("ab", 32)
	h, err := HashFromHex(hex64)
	if err != nil {
		t.Fatal(err)
	}
	if got := h.Hex(); got != hex64 {
		t.Fatalf("hex round trip: got %s", got)
	}
	fromBytes, err := HashFromBytes(h.Bytes[:])
	if err != nil {
		t.Fatal(err)
	}
	if fromBytes != h {
		t.Fatal("HashFromBytes disagrees with HashFromHex")
	}
}

func TestHashRejectsMalformed(t *testing.T) {
	for _, s := range []string{
		"",
		strings.Repeat("ab", 31),       // 62 chars
		strings.Repeat("ab", 32) + "c", // 65 chars
		strings.Repeat("zz", 32),       // right length, not hex
	} {
		if _, err := HashFromHex(s); err == nil {
			t.Errorf("%d-char %q was accepted as a hash", len(s), s)
		}
	}
	for _, n := range []int{0, 31, 33} {
		if _, err := HashFromBytes(make([]byte, n)); err == nil {
			t.Errorf("%d bytes was accepted as a hash", n)
		}
	}
}

func TestHashUppercaseHexParsesToSameValue(t *testing.T) {
	lower, err := HashFromHex(strings.Repeat("ab", 32))
	if err != nil {
		t.Fatal(err)
	}
	upper, err := HashFromHex(strings.Repeat("AB", 32))
	if err != nil {
		t.Fatal(err)
	}
	if lower != upper {
		t.Fatal("case changed the parsed hash")
	}
}

// TimestampFromEpochMicros splits micros into (millis, remainder). The negative case is the one
// worth pinning: Go's % keeps the sign of the dividend, so the naive split puts a pre-epoch
// instant in the wrong millisecond with a negative remainder.
func TestTimestampEpochMicrosRoundTrip(t *testing.T) {
	for _, micros := range []int64{
		0,
		1,
		999,
		1000,
		1_700_000_000_123_456,
		-1,
		-999,
		-1000,
		-1001,
		-1_700_000_000_123_456,
	} {
		ts := TimestampFromEpochMicros(micros)
		if got := ts.EpochMicros(); got != micros {
			t.Errorf("%d micros round-tripped as %d (millis=%d remainder=%d)",
				micros, got, ts.EpochMillis, ts.MicroRemainder)
		}
		if ts.MicroRemainder < 0 || ts.MicroRemainder > 999 {
			t.Errorf("%d micros gave an out-of-range remainder %d", micros, ts.MicroRemainder)
		}
	}
}

func TestTimestampFromISO8601(t *testing.T) {
	ts, err := TimestampFromISO8601("2026-08-27T12:34:56.123456Z")
	if err != nil {
		t.Fatal(err)
	}
	if ts.MicroRemainder != 456 {
		t.Fatalf("micro remainder: got %d, want 456", ts.MicroRemainder)
	}
	// Second-resolution input (no fractional part) must also parse - the RFC3339Nano attempt
	// fails on it and the plain RFC3339 fallback is what catches it.
	plain, err := TimestampFromISO8601("2026-08-27T12:34:56Z")
	if err != nil {
		t.Fatalf("second-resolution timestamp: %v", err)
	}
	if plain.MicroRemainder != 0 {
		t.Fatalf("second-resolution remainder: %d", plain.MicroRemainder)
	}
	if ts.EpochMillis-plain.EpochMillis != 123 {
		t.Fatalf("fractional part is %d ms off", ts.EpochMillis-plain.EpochMillis)
	}
	if _, err := TimestampFromISO8601("27 August 2026"); err == nil {
		t.Error("a non-ISO-8601 string was accepted")
	}
}

func TestTimestampNowIsWithinPlausibleRange(t *testing.T) {
	ts := TimestampNow()
	// Somewhere between 2020 and 2100 - enough to catch a unit mix-up (nanos vs micros vs
	// millis), which is the only realistic failure here.
	const y2020Micros = 1_577_836_800_000_000
	const y2100Micros = 4_102_444_800_000_000
	if m := ts.EpochMicros(); m < y2020Micros || m > y2100Micros {
		t.Fatalf("TimestampNow() = %d micros, outside 2020-2100 - unit mix-up?", m)
	}
	if ts.MicroRemainder < 0 || ts.MicroRemainder > 999 {
		t.Fatalf("remainder out of range: %d", ts.MicroRemainder)
	}
}
