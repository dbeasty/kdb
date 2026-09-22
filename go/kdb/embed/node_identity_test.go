package embed

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

func TestNodeIDPersistsAcrossOpen(t *testing.T) {
	root := t.TempDir()
	first, err := LoadOrCreateNodeID(root)
	if err != nil {
		t.Fatal(err)
	}
	second, err := LoadOrCreateNodeID(root)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("node id changed across opens: %s then %s", first, second)
	}
	if other, _ := LoadOrCreateNodeID(t.TempDir()); other == first {
		t.Fatal("two data roots got the same node id")
	}
}

// TestNodeIDAdoptsTxnHost: a data root that already wrote cross-namespace markers keeps the
// identity those markers name.
func TestNodeIDAdoptsTxnHost(t *testing.T) {
	root := t.TempDir()
	host, _ := codec.RandomUUID()
	if err := os.MkdirAll(txnDir(root), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeHostID(root, host.String()); err != nil {
		t.Fatal(err)
	}
	id, err := LoadOrCreateNodeID(root)
	if err != nil {
		t.Fatal(err)
	}
	if id != host {
		t.Fatalf("expected the txn host id %s, got %s", host, id)
	}
	if _, err := os.Stat(filepath.Join(root, nodeFileName)); err != nil {
		t.Fatalf("NODE not written: %v", err)
	}
}

func TestNodeIDFromEnvOverrides(t *testing.T) {
	want, _ := codec.RandomUUID()
	t.Setenv(NodeIDEnv, want.String())
	got, err := LoadOrCreateNodeID(t.TempDir())
	if err != nil || got != want {
		t.Fatalf("got %s, %v; want %s", got, err, want)
	}
}
