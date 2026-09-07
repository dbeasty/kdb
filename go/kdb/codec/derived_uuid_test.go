package codec_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

// TestDocIDNamespaceIsTheSpecConstant pins KDB_DOC_ID_NAMESPACE's raw bytes to the UUID the spec
// names, so a typo in the byte literal cannot silently change every derived id.
func TestDocIDNamespaceIsTheSpecConstant(t *testing.T) {
	if got := codec.DocIDNamespace.String(); got != "6f5b9a1c-2d3e-4f70-8a9b-1c2d3e4f5a6b" {
		t.Fatalf("DocIDNamespace = %s", got)
	}
}

// TestDerivedUUIDHandChecked is the one vector worked by hand (kdb-spec-layer16 §9.4), so the
// fixture file is anchored to something other than this implementation's own output:
//
//	input  = namespace bytes ‖ utf8("order-1")
//	       = 6f5b9a1c2d3e4f708a9b1c2d3e4f5a6b 6f726465722d31
//	sha256 = 54d100dbb8d01b388755264670b3fc47 b9dbcc75a42f513d8d84c01460bc2e9d
//	first 16 bytes: 54d100db b8d0 1b38 8755 264670b3fc47
//	byte 6 (0x1b) -> (0x1b & 0x0f) | 0x80 = 0x8b   (version nibble 8)
//	byte 8 (0x87) -> (0x87 & 0x3f) | 0x80 = 0x87   (variant 10, already set)
//	uuid   = 54d100db-b8d0-8b38-8755-264670b3fc47
func TestDerivedUUIDHandChecked(t *testing.T) {
	if got := codec.DerivedUUID("order-1").String(); got != "54d100db-b8d0-8b38-8755-264670b3fc47" {
		t.Fatalf("DerivedUUID(order-1) = %s", got)
	}
}

// TestDerivedUUIDMatchesGoldenVectors reads the shared parity fixture both trees must agree on.
func TestDerivedUUIDMatchesGoldenVectors(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "golden", "search", "derived_id_vectors.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Vectors []struct {
			ID   string `json:"id"`
			UUID string `json:"uuid"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Vectors) < 10 {
		t.Fatalf("fixture has %d vectors, want at least 10", len(fixture.Vectors))
	}
	for _, v := range fixture.Vectors {
		if got := codec.DerivedUUID(v.ID).String(); got != v.UUID {
			t.Errorf("DerivedUUID(%q) = %s, want %s", v.ID, got, v.UUID)
		}
	}
}

// TestDerivedUUIDIsVersion8Variant10 checks the version/variant bits are forced on every input,
// including ones whose digest happens to already carry other bits there.
func TestDerivedUUIDIsVersion8Variant10(t *testing.T) {
	for _, s := range []string{"", "a", "b", "c", "order-1", "日本語", "\x00\xff"} {
		b := codec.DerivedUUID(s).Bytes()
		if b[6]>>4 != 8 {
			t.Errorf("%q: version nibble %x, want 8", s, b[6]>>4)
		}
		if b[8]>>6 != 2 {
			t.Errorf("%q: variant bits %b, want 10", s, b[8]>>6)
		}
	}
}

// TestDerivedUUIDIsDeterministicAndDistinct: same input, same id; different input, different id.
func TestDerivedUUIDIsDeterministicAndDistinct(t *testing.T) {
	if codec.DerivedUUID("k") != codec.DerivedUUID("k") {
		t.Fatal("not deterministic")
	}
	if codec.DerivedUUID("k") == codec.DerivedUUID("K") {
		t.Fatal("case-different inputs collided")
	}
}
