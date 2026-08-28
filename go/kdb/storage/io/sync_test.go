package io

import (
	"os"
	"path/filepath"
	"testing"
)

// TestSyncFileBothModes exercises the platform syncFile in both modes against
// a real file: the fast path must succeed (or fall back to a full sync) on
// every supported platform, since a sync that silently fails is a durability
// hole no benchmark would ever notice.
func TestSyncFileBothModes(t *testing.T) {
	for _, mode := range []SyncMode{SyncModeFull, SyncModeFast} {
		f, err := os.Create(filepath.Join(t.TempDir(), "seg"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte("payload")); err != nil {
			t.Fatal(err)
		}
		if err := syncFile(f, mode); err != nil {
			t.Errorf("syncFile(mode=%d): %v", mode, err)
		}
		_ = f.Close()
	}
}

// TestOSByteStoreFlushHonorsSyncMode: a store constructed with SyncModeFast
// must still make Flush a real operation - bytes appended before the flush are
// readable afterward, on both the cached-handle path and the no-handle path.
func TestOSByteStoreFlushHonorsSyncMode(t *testing.T) {
	root := t.TempDir()
	s, err := NewOSByteStore(PlatformIOConfig{RootDirectory: &root, SyncMode: SyncModeFast})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	name := "ns/test/wal/seg1"
	if _, err := s.Append(name, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(name, true); err != nil {
		t.Fatalf("Flush with open handle: %v", err)
	}
	// The no-open-handle fallback path.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(name, true); err != nil {
		t.Fatalf("Flush without open handle: %v", err)
	}
	b, err := s.Read(name, 0, 16)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "hello" {
		t.Fatalf("read back %q, want hello", b)
	}
}
