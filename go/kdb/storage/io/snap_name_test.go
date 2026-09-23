package io

import (
	"os"
	"path/filepath"
	"testing"
)

func osStore(t *testing.T) (*OSByteStore, string) {
	t.Helper()
	root := t.TempDir()
	cfg := DefaultPlatformIOConfig()
	cfg.RootDirectory = &root
	cfg.FsyncOnFlush = false
	store, err := NewOSByteStore(cfg)
	if err != nil {
		t.Fatalf("open os byte store: %v", err)
	}
	return store, root
}

// A snapshot's name has to stay one path component. While it did not, a namespace id's slash
// became a real directory, so an id that is a prefix of another id wrote a file exactly where
// the other one needed a directory.
func TestSnapFileNameIsFlat(t *testing.T) {
	for _, c := range []struct{ key, want string }{
		{"kdb:checkpoint:users", "kdb_checkpoint_users"},
		{"kdb:checkpoint:repro/account", "kdb_checkpoint_repro%2Faccount"},
		{"kdb:checkpoint:repro/account/ro", "kdb_checkpoint_repro%2Faccount%2Fro"},
		{"kdb:snap:a/b", "kdb_snap_a%2Fb"},
	} {
		if got := SnapFileName(c.key); got != c.want {
			t.Errorf("SnapFileName(%q) = %q, want %q", c.key, got, c.want)
		}
	}
	// Two ids may never flatten onto one name. "_" is legal inside a segment, so substituting
	// the slash with one would make these two collide.
	if SnapFileName("kdb:checkpoint:a_b/c") == SnapFileName("kdb:checkpoint:a/b_c") {
		t.Fatal("two different namespace ids flattened to the same snapshot file name")
	}
}

// The store writes one file per key directly under snap/, never a directory tree.
func TestSnapshotWritesOneFlatFile(t *testing.T) {
	store, root := osStore(t)
	if err := store.WriteSnapshot("kdb:checkpoint:repro/account", []byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "snap"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("snap/ holds %d entries, want 1", len(entries))
	}
	if entries[0].IsDir() {
		t.Fatalf("snap/%s is a directory; the id's slash was kept as a path separator", entries[0].Name())
	}
	if entries[0].Name() != "kdb_checkpoint_repro%2Faccount" {
		t.Fatalf("snap/ holds %q", entries[0].Name())
	}
}

// A checkpoint written by a build that nested the slash must still be found. For a namespace
// bootstrapped from a peer's snapshot the checkpoint is the only record of its state and open
// refuses to proceed without it, so not finding it would strand the namespace rather than cost
// a slow start.
func TestSnapshotReadsAPreFlatteningFile(t *testing.T) {
	store, root := osStore(t)
	const key = "kdb:checkpoint:repro/account"

	legacy := filepath.Join(root, "snap", "kdb_checkpoint_repro", "account")
	if err := os.MkdirAll(filepath.Dir(legacy), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(legacy, []byte("from an older build"), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := store.ReadSnapshot(key)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "from an older build" {
		t.Fatalf("read back %q, want the legacy file's contents", got)
	}

	// Writing retires it, so a later read cannot fall back to a checkpoint describing a log that
	// is no longer underneath it - which is what promotion, moving only the new name, would leave.
	if err := store.WriteSnapshot(key, []byte("current")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Fatalf("the pre-flattening file survived a write (stat err %v)", err)
	}
	got, err = store.ReadSnapshot(key)
	if err != nil || string(got) != "current" {
		t.Fatalf("read back %q (err %v), want %q", got, err, "current")
	}
}

// Deleting a key removes both names: a stale legacy file would otherwise reappear through the
// read fallback once the current one is gone.
func TestDeleteSnapshotRemovesThePreFlatteningFileToo(t *testing.T) {
	store, root := osStore(t)
	const key = "kdb:checkpoint:repro/account"

	legacy := filepath.Join(root, "snap", "kdb_checkpoint_repro", "account")
	if err := os.MkdirAll(filepath.Dir(legacy), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(legacy, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteSnapshot(key); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got, err := store.ReadSnapshot(key)
	if err != nil {
		t.Fatalf("read after delete: %v", err)
	}
	if got != nil {
		t.Fatalf("a deleted snapshot read back as %q", got)
	}
}
