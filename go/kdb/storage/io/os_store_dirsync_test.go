package io

import (
	"path/filepath"
	"sync"
	"testing"
)

// recordDirSyncs replaces syncDir for the test, returning the directories synced so far.
func recordDirSyncs(t *testing.T) func() map[string]bool {
	t.Helper()
	var mu sync.Mutex
	synced := map[string]bool{}
	prev := syncDir
	syncDir = func(dir string) error {
		mu.Lock()
		synced[dir] = true
		mu.Unlock()
		return prev(dir)
	}
	t.Cleanup(func() { syncDir = prev })
	return func() map[string]bool {
		mu.Lock()
		defer mu.Unlock()
		out := map[string]bool{}
		for d := range synced {
			out[d] = true
		}
		return out
	}
}

// Fsyncing a newly created segment makes its bytes durable but not its name: the directory
// entry needs a sync of its own, as do the entries of any directories created to hold it. Before
// this, no segment's directory was ever synced, so a power loss after a commit was acknowledged
// could drop the new delta segment holding it - file and all.
func TestFlushSyncsTheDirectoriesOfANewSegment(t *testing.T) {
	synced := recordDirSyncs(t)
	root := t.TempDir()
	store, err := NewOSByteStore(PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	name := SegmentNameBuilder.DeltaSequenced("app/data", 1)
	if _, err := store.Append(name, []byte("frame")); err != nil {
		t.Fatal(err)
	}
	if err := store.Flush(name, true); err != nil {
		t.Fatal(err)
	}
	file := store.pathFor(name)
	want := []string{filepath.Dir(file), root}
	// Every directory between root and the file was created for it; each one's parent changed.
	for d := filepath.Dir(file); d != root; d = filepath.Dir(d) {
		want = append(want, filepath.Dir(d))
	}
	got := synced()
	for _, d := range want {
		if !got[d] {
			t.Errorf("directory %s was not synced by the first fsync flush (synced: %v)", d, got)
		}
	}

	// Once synced, they are no longer pending.
	if _, err := store.Append(name, []byte("frame")); err != nil {
		t.Fatal(err)
	}
	if err := store.Flush(name, true); err != nil {
		t.Fatal(err)
	}
	if len(store.unsyncedDirs) != 0 {
		t.Fatalf("%d directories still pending after a flush", len(store.unsyncedDirs))
	}

	// A flush without fsync promises nothing, and syncs nothing.
	name2 := SegmentNameBuilder.DeltaSequenced("app/other", 1)
	if _, err := store.Append(name2, []byte("frame")); err != nil {
		t.Fatal(err)
	}
	if err := store.Flush(name2, false); err != nil {
		t.Fatal(err)
	}
	if synced()[filepath.Dir(store.pathFor(name2))] {
		t.Fatal("a flush without fsync synced a directory")
	}
	// The pending directory survives it, for the next fsync flush - of a different segment if
	// need be, since sealing name2 may come later.
	if err := store.Flush(name, true); err != nil {
		t.Fatal(err)
	}
	if !synced()[filepath.Dir(store.pathFor(name2))] {
		t.Fatal("a pending directory was not synced by the next fsync flush")
	}
}
