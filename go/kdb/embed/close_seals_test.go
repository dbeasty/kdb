package embed

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/schema"
)

// Regression test for kdb-spec-layer13 Component 47 §2.4: an orderly Close
// must flush and seal the active delta segment, not merely release the
// directory lock.
func TestClose_SealsActiveSegment(t *testing.T) {
	dir := t.TempDir()
	ns := "demo/users"

	rt, err := OpenFileRuntime(dir, "demo", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PutJSONDocument(rt, ns, `{"name":"a"}`); err != nil {
		t.Fatal(err)
	}
	rt.Close()

	segPath := filepath.Join(dir, "ns", ns, "delta", "00000000000000000000.seg")
	info, err := os.Stat(segPath)
	if err != nil {
		t.Fatalf("expected sealed segment file to exist: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("expected non-empty segment after Close")
	}
}
