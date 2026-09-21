package embed

import (
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

// Cross-language fixtures for the cross-namespace transaction state, read by the Kotlin
// reference's replay (kdb-jdbc TxnDecisionsGoldenTest). Deterministic, so writing them on every
// run leaves the checked-in copies unchanged unless the format itself changes - which is exactly
// when the Kotlin side must be told.
//
//   - txn_decision_file.hex: a decision log for epoch 7 recording two groups, then a torn record.
//   - txn_group_marker.txt: the commit message of a part of group 1 of that epoch.
func TestExportTxnGoldenFixtures(t *testing.T) {
	g1 := codec.UUID{MSB: 0x0102030405060708, LSB: 0x090a0b0c0d0e0f10}
	g2 := codec.UUID{MSB: 0x1112131415161718, LSB: 0x191a1b1c1d1e1f20}

	file := make([]byte, decisionHeaderSize, decisionHeaderSize+2*decisionRecordSize+7)
	copy(file, decisionHeaderMagic)
	binary.BigEndian.PutUint64(file[8:16], 7)
	for _, id := range []codec.UUID{g1, g2} {
		rec := make([]byte, decisionRecordSize)
		encodeDecisionRecord(rec, id)
		file = append(file, rec...)
	}
	file = append(file, 1, 2, 3, 4, 5, 6, 7) // a torn record: never acknowledged, ignored

	// The bytes must read back through the real reader before they are published.
	tmp := filepath.Join(t.TempDir(), "d.log")
	if err := os.WriteFile(tmp, file, 0o644); err != nil {
		t.Fatal(err)
	}
	set, err := readDecisions(tmp)
	if err != nil || len(set) != 2 {
		t.Fatalf("fixture reads back as %v, %v", set, err)
	}

	marker := GroupMarker{Host: "host-a", Epoch: 7, Group: g1, Parts: []string{"bank/accounts", "bank/ledger"}}.Message()
	if got, ok := ParseGroupMarker(marker); !ok || got.Group != g1 || got.Host != "host-a" {
		t.Fatalf("marker round trip: %+v %v", got, ok)
	}

	dir := goldenPhysicalDir(t)
	if err := os.WriteFile(filepath.Join(dir, "txn_decision_file.hex"), []byte(hex.EncodeToString(file)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "txn_group_marker.txt"), []byte(marker+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if g1.String() != "01020304-0506-0708-090a-0b0c0d0e0f10" {
		t.Fatalf("group id renders as %s; the Kotlin fixture test hardcodes the canonical form", g1)
	}
}

func goldenPhysicalDir(t *testing.T) string {
	t.Helper()
	for _, base := range []string{filepath.Join("..", ".."), "."} {
		if _, err := os.Stat(filepath.Join(base, "testdata")); err == nil {
			d := filepath.Join(base, "testdata", "golden", "physical", "go")
			if err := os.MkdirAll(d, 0o755); err != nil {
				t.Fatal(err)
			}
			return d
		}
	}
	t.Fatal("cannot locate go/testdata")
	return ""
}
