package transaction

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

// A conflict origin crosses the control plane and the durable conflict queue as JSON: ids as
// text, and it must read back to the same value.
func TestConflictOriginJSONRoundTrip(t *testing.T) {
	h, err := codec.HashFromHex(strings.Repeat("ab", 32))
	if err != nil {
		t.Fatal(err)
	}
	o := ConflictOrigin{NodeID: codec.DerivedUUID("node"), Commit: h, TimestampMicros: 42}
	b, err := json.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"nodeId":"` + o.NodeID.String() + `","commit":"` + h.Hex() + `","timestampMicros":42}`
	if string(b) != want {
		t.Fatalf("got %s, want %s", b, want)
	}
	var back ConflictOrigin
	if err := json.Unmarshal(b, &back); err != nil || back != o {
		t.Fatalf("round trip: %+v %v", back, err)
	}
	if b, _ := json.Marshal(ConflictOrigin{}); string(b) != `{}` {
		t.Fatalf("an unknown origin should be empty, got %s", b)
	}
}
