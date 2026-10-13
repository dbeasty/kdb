//go:build unix

package embed_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

// sstableSidecarFiles lists ns's SSTables and their index sidecars, relative to the namespace directory.
func sstableSidecarFiles(t *testing.T, dataRoot, ns string) (tables, sidecars []string) {
	t.Helper()
	nsDir := filepath.Join(dataRoot, "ns", filepath.FromSlash(ns))
	filepath.WalkDir(nsDir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(nsDir, p)
		rel = filepath.ToSlash(rel)
		switch {
		case strings.HasPrefix(rel, "sstable/"):
			tables = append(tables, rel)
		case strings.HasPrefix(rel, "sstidx/"):
			sidecars = append(sidecars, rel)
		}
		return nil
	})
	return tables, sidecars
}

// probeEveryTable looks up a key no table holds, which asks every table.
func probeEveryTable(t *testing.T, rt *embed.EmbeddedKdbRuntime) {
	t.Helper()
	eng, ok := rt.Storage.(*engine.ServerEngine)
	if !ok {
		t.Fatalf("storage is %T", rt.Storage)
	}
	if eng.BlobTableCount() == 0 {
		t.Fatal("no SSTables registered")
	}
	var absent codec.Hash
	absent.Bytes[0] = 0xAB
	_, _ = eng.ReadBlob(absent)
}

// The file runtime writes a sidecar beside every SSTable it flushes; a read-only runtime uses
// the writer's sidecars but never writes one; and the writer clears sidecars left without a table.
func TestSSTableSidecarsInTheFileRuntime(t *testing.T) {
	dataRoot := t.TempDir()
	const ns = "app/side"
	rt, err := embed.OpenFileRuntime(dataRoot, "demo", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		writeDoc(t, rt, `{"n":1}`)
	}
	rt.Close()
	tables, sidecars := sstableSidecarFiles(t, dataRoot, ns)
	if len(tables) == 0 {
		t.Fatal("closing wrote no SSTable")
	}
	want := map[string]bool{}
	for _, tb := range tables {
		want["sstidx/"+strings.TrimPrefix(tb, "sstable/")+".sidx"] = true
	}
	for _, sc := range sidecars {
		delete(want, sc)
	}
	if len(want) > 0 {
		t.Fatalf("tables without a sidecar: %v (tables %v, sidecars %v)", want, tables, sidecars)
	}

	// A read-only runtime with no sidecars to use serves the footers and writes nothing.
	if err := os.RemoveAll(filepath.Join(dataRoot, "ns", ns, "sstidx")); err != nil {
		t.Fatal(err)
	}
	ro, err := embed.OpenReadOnlyFileRuntime(dataRoot, "demo", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	probeEveryTable(t, ro)
	ro.Close()
	if _, sidecars := sstableSidecarFiles(t, dataRoot, ns); len(sidecars) != 0 {
		t.Fatalf("a read-only runtime wrote sidecars: %v", sidecars)
	}

	// The writer rebuilds missing sidecars on first use and removes orphans at open.
	orphan := filepath.Join(dataRoot, "ns", ns, "sstidx", "L0", "00000000-0000-4000-8000-000000000000.sidx")
	if err := os.MkdirAll(filepath.Dir(orphan), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(orphan, []byte("stray"), 0o644); err != nil {
		t.Fatal(err)
	}
	rw, err := embed.OpenFileRuntime(dataRoot, "demo", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan sidecar survived the writer's open: %v", err)
	}
	probeEveryTable(t, rw)
	rw.Close()
	tables, sidecars = sstableSidecarFiles(t, dataRoot, ns)
	if len(sidecars) != len(tables) {
		t.Fatalf("after the writer's probe: %d tables, %d sidecars", len(tables), len(sidecars))
	}
}
