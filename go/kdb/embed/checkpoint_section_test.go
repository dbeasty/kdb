package embed

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

func sectionTestShim(t *testing.T) (storage.PlatformIOShim, string) {
	t.Helper()
	root := t.TempDir()
	store, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		t.Fatal(err)
	}
	return storio.NewFileBackedPlatformIO(storio.PlatformIOConfig{RootDirectory: &root}, store), root
}

func sectionTestTree(t *testing.T, n int, salt byte) document.DocumentTree {
	t.Helper()
	entries := make(map[codec.UUID]codec.Hash, n)
	for i := 0; i < n; i++ {
		id, _ := codec.RandomUUID()
		var h codec.Hash
		h.Bytes[0], h.Bytes[1], h.Bytes[2], h.Bytes[31] = byte(i), byte(i>>8), byte(i>>16), salt
		entries[id] = h
	}
	tree, err := document.BuildTrieDocumentTree(entries)
	if err != nil {
		t.Fatal(err)
	}
	return tree
}

func snapFilesWith(t *testing.T, root, part string) []string {
	t.Helper()
	var out []string
	_ = filepath.Walk(filepath.Join(root, "snap"), func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.Contains(filepath.Base(p), part) {
			out = append(out, p)
		}
		return nil
	})
	return out
}

// Past the legacy bound, a store that streams snapshots gets version 6: a small record naming a
// section that holds the tree and commits. It reads back exactly; a rewrite retires the previous
// section; and a damaged, truncated or missing section makes the checkpoint unusable - so the open
// replays - rather than partly used.
func TestCheckpointV6SectionRoundTripsAndRetires(t *testing.T) {
	shim, root := sectionTestShim(t)
	tree := sectionTestTree(t, legacyCheckpointMaxEntries+5000, 1)
	commits := testCheckpointCommits(t, 300)
	packed, err := dag.PackCheckpointCommits(commits)
	if err != nil {
		t.Fatal(err)
	}
	cp := namespaceCheckpoint{NamespaceID: "app/ns", ThroughSequence: 7, FloorSequence: 2, LiveTree: tree,
		State:     dag.CheckpointState{PackedCommits: packed},
		GraphRefs: []dag.GraphSegmentRef{{Seq: 3, CRC: 9, Records: 11}}}
	if err := writeCheckpoint(shim, cp); err != nil {
		t.Fatal(err)
	}
	raw, err := shim.ReadSnapshot(checkpointKey("app/ns"))
	if err != nil {
		t.Fatal(err)
	}
	if v := checkpointVersionOf(t, raw); v != checkpointSectionFormatVersion {
		t.Fatalf("written as version %d, want %d", v, checkpointSectionFormatVersion)
	}
	if len(raw) > 64<<10 {
		t.Fatalf("the version 6 record is %d bytes; the tree and commits belong in the section", len(raw))
	}
	sections := snapFilesWith(t, root, "checkpoint-section")
	if len(sections) != 1 {
		t.Fatalf("%d section files, want 1: %v", len(sections), sections)
	}

	got, ok := readCheckpoint(shim, "app/ns")
	if !ok {
		t.Fatal("the version 6 checkpoint did not read back")
	}
	if got.LiveTree.TreeHash != tree.TreeHash || got.LiveTree.Size() != tree.Size() ||
		string(got.State.PackedCommits) != string(packed) || got.ThroughSequence != 7 || got.FloorSequence != 2 ||
		len(got.GraphRefs) != 1 || got.GraphRefs[0] != cp.GraphRefs[0] {
		t.Fatalf("version 6 did not round-trip: tree %v/%d, %d commit bytes, through %d floor %d, refs %v",
			got.LiveTree.TreeHash == tree.TreeHash, got.LiveTree.Size(), len(got.State.PackedCommits), got.ThroughSequence, got.FloorSequence, got.GraphRefs)
	}

	// A rewrite with another tree names a new section and deletes the old one.
	cp2 := cp
	cp2.LiveTree = sectionTestTree(t, legacyCheckpointMaxEntries+10, 2)
	if err := writeCheckpoint(shim, cp2); err != nil {
		t.Fatal(err)
	}
	after := snapFilesWith(t, root, "checkpoint-section")
	if len(after) != 1 || after[0] == sections[0] {
		t.Fatalf("after a rewrite: %v (was %v), want exactly the new section", after, sections)
	}
	if got, ok := readCheckpoint(shim, "app/ns"); !ok || got.LiveTree.TreeHash != cp2.LiveTree.TreeHash {
		t.Fatal("the rewritten checkpoint did not read back")
	}

	// Damage: a flipped byte, a short file, no file. Each is unusable, never half-used.
	section := after[0]
	good, err := os.ReadFile(section)
	if err != nil {
		t.Fatal(err)
	}
	for name, bad := range map[string][]byte{
		"flipped": func() []byte { b := append([]byte(nil), good...); b[len(b)/2] ^= 0x40; return b }(),
		"short":   good[:len(good)-100],
	} {
		if err := os.WriteFile(section, bad, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, ok := readCheckpoint(shim, "app/ns"); ok {
			t.Fatalf("a %s section was used", name)
		}
	}
	if err := os.Remove(section); err != nil {
		t.Fatal(err)
	}
	if _, ok := readCheckpoint(shim, "app/ns"); ok {
		t.Fatal("a checkpoint whose section is gone was used")
	}
	// Without a store to read the section from, a version 6 record is refused, not misread.
	if _, err := decodeCheckpoint("app/ns", raw); err == nil {
		t.Fatal("decodeCheckpoint accepted version 6 without a section")
	}
}

// Through a real runtime, with and without a tree file: a namespace past the legacy bound closes
// to a version 6 checkpoint and reopens from it with every document intact - with a tree file,
// by mapping the tree segment the record's digest names.
func TestRuntimeReopensFromCheckpointV6(t *testing.T) {
	for _, treeFile := range []bool{false, true} {
		t.Run(map[bool]string{false: "heap-tree", true: "tree-file"}[treeFile], func(t *testing.T) {
			if treeFile {
				skipIfNoMmap(t)
			}
			root := t.TempDir()
			const ns = "test/v6"
			opts := FileRuntimeOptions{}
			opts.Storage.TreeFile = treeFile
			rt, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), opts)
			if err != nil {
				t.Fatal(err)
			}
			n := legacyCheckpointMaxEntries + 2000
			batch := make([]string, 0, 500)
			for i := 0; i < n; i++ {
				b, _ := json.Marshal(map[string]any{"id": treeDocID(i), "i": i})
				batch = append(batch, string(b))
				if len(batch) == cap(batch) || i == n-1 {
					if err := putJSONBatch(rt, ns, batch); err != nil {
						t.Fatal(err)
					}
					batch = batch[:0]
				}
			}
			want := rt.Storage.(*engine.ServerEngine).LiveTree().TreeHash
			rt.Close()
			raw, err := os.ReadFile(filepath.Join(root, "snap", storio.SnapFileName(checkpointKey(ns))))
			if err != nil {
				t.Fatal(err)
			}
			if v := checkpointVersionOf(t, raw); v != checkpointSectionFormatVersion {
				t.Fatalf("closed to checkpoint version %d, want %d", v, checkpointSectionFormatVersion)
			}
			rt2, err := OpenFileRuntimeWithOptions(root, "test", ns, schema.None(), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer rt2.Close()
			live := rt2.Storage.(*engine.ServerEngine).LiveTree()
			if live.TreeHash != want || live.Size() != n {
				t.Fatalf("reopened tree differs (size %d, want %d)", live.Size(), n)
			}
			if treeFile {
				if _, mapped := live.Paged(); !mapped {
					t.Fatal("with a tree file the reopened tree is not mapped from its segment")
				}
			}
			for _, i := range []int{0, n / 2, n - 1} {
				id, _ := codec.ParseUUID(treeDocID(i))
				d, err := rt2.Storage.GetDocument(ns, id, live.TreeHash)
				if err != nil || d == nil {
					t.Fatalf("document %d missing after reopen (%v)", i, err)
				}
			}
		})
	}
}

// A data dir checkpointed by an older build as version 4 reads as before, and the next checkpoint
// rewrites it as version 6: migration is the ordinary checkpoint, nothing more.
func TestCheckpointV4MigratesToV6(t *testing.T) {
	shim, _ := sectionTestShim(t)
	tree := sectionTestTree(t, legacyCheckpointMaxEntries+100, 3)
	cp := namespaceCheckpoint{NamespaceID: "app/old", ThroughSequence: 1, LiveTree: tree}
	raw, err := encodeCheckpoint(cp) // what an older build wrote
	if err != nil {
		t.Fatal(err)
	}
	if v := checkpointVersionOf(t, raw); v != checkpointPackedFormatVersion {
		t.Fatalf("setup: version %d", v)
	}
	if err := shim.WriteSnapshot(checkpointKey("app/old"), raw); err != nil {
		t.Fatal(err)
	}
	got, ok := readCheckpoint(shim, "app/old")
	if !ok || got.LiveTree.TreeHash != tree.TreeHash {
		t.Fatal("a version 4 checkpoint no longer reads")
	}
	if err := writeCheckpoint(shim, got); err != nil {
		t.Fatal(err)
	}
	raw, _ = shim.ReadSnapshot(checkpointKey("app/old"))
	if v := checkpointVersionOf(t, raw); v != checkpointSectionFormatVersion {
		t.Fatalf("the next checkpoint wrote version %d, want %d", v, checkpointSectionFormatVersion)
	}
	if again, ok := readCheckpoint(shim, "app/old"); !ok || again.LiveTree.TreeHash != tree.TreeHash {
		t.Fatal("the migrated checkpoint does not read back")
	}
}
