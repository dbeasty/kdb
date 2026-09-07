package interop

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

// Physical-layer conformance fixtures - docs/kdb-physical-layer-compat-test-plan.md.
//
// Fixtures live under go/testdata/golden/physical/{kotlin,go}/ as lowercase hex with a trailing
// newline, matching the existing golden/codec convention: they diff readably and need no binary
// handling in git.
//
// Every value below is fixed - no clocks, no randomness. A fixture that varies between runs
// cannot be committed, and one that varies between languages proves nothing. These must stay in
// lockstep with the Kotlin side's companion objects (WalPhysicalGoldenTest, SsTablePhysicalGolden
// Test, DeltaPhysicalGoldenTest); a mismatch there shows up as a decode failure, not a silent pass.
const (
	fixtureNamespaceID = "fixture-ns"
	fixtureWalIDString = "00112233-4455-6677-8899-aabbccddeeff"
	// fixtureEpochMicros and fixtureNegEpochMicros: the second is pre-epoch, pinning sign
	// handling in the 8-byte epochMicros field where an unsigned reassembly on either side would
	// otherwise go unnoticed.
	fixtureEpochMicros    int64 = 1_700_000_000_123_456
	fixtureNegEpochMicros int64 = -987_654_321
)

// fixturePayload has high and low bytes plus a run, so a codec that drops, pads, or reorders
// shows up rather than round-tripping by luck.
var fixturePayload = []byte{0, 1, 2, 3, 0x7F, 0x80, 0xFF, 0x2A, 0x2A, 0x2A}

func fixtureHash(t *testing.T, seed byte) codec.Hash {
	t.Helper()
	b := make([]byte, 32)
	for i := range b {
		b[i] = seed + byte(i)
	}
	h, err := codec.HashFromBytes(b)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// physicalGoldenDir is go/testdata/golden/physical/<side>. side is "go" for fixtures this module
// produces and "kotlin" for the ones Gradle writes.
func physicalGoldenDir(t *testing.T, side string) string {
	t.Helper()
	for _, base := range []string{filepath.Join("..", ".."), "."} {
		d := filepath.Join(base, "testdata", "golden", "physical", side)
		if _, err := os.Stat(filepath.Join(base, "testdata")); err == nil {
			if err := os.MkdirAll(d, 0o755); err != nil {
				t.Fatal(err)
			}
			return d
		}
	}
	t.Fatal("cannot locate go/testdata from the test working directory")
	return ""
}

func writePhysicalGolden(t *testing.T, name string, b []byte) {
	t.Helper()
	p := filepath.Join(physicalGoldenDir(t, "go"), name)
	if err := os.WriteFile(p, []byte(hex.EncodeToString(b)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// readKotlinGolden returns nil (and skips the calling test) when Gradle has not yet exported the
// fixture. A missing fixture is a setup gap, not a compatibility failure: failing on it would
// make this suite unrunnable from a fresh clone without a JDK.
func readKotlinGolden(t *testing.T, name string) []byte {
	t.Helper()
	p := filepath.Join(physicalGoldenDir(t, "kotlin"), name)
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Skipf("kotlin golden %s missing (run the Gradle exporter - see the test plan §3): %v", name, err)
		return nil
	}
	b, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatalf("golden %s is not valid hex: %v", name, err)
	}
	return b
}
