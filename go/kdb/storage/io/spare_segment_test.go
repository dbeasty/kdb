package io

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

const testSpareBytes = 1 << 20

func openPreallocShim(t *testing.T, prealloc int64) (*FileBackedPlatformIO, *OSByteStore, string) {
	t.Helper()
	root := t.TempDir()
	cfg := DefaultPlatformIOConfig()
	cfg.RootDirectory = &root
	cfg.FsyncOnFlush = true
	cfg.PreallocateBytes = prealloc
	store, err := NewOSByteStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return NewFileBackedPlatformIO(cfg, store), store, root
}

// waitSpare waits until the spare for segmentName has finished preparing and reports whether it
// is ready to be linked.
func waitSpare(t *testing.T, s *OSByteStore, segmentName string) bool {
	t.Helper()
	s.mu.Lock()
	sp := s.spares[segmentName]
	s.mu.Unlock()
	if sp == nil {
		t.Fatalf("no spare for %s", segmentName)
	}
	<-sp.ready
	s.mu.Lock()
	defer s.mu.Unlock()
	return sp.file != nil
}

func openFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skip("no /proc/self/fd")
	}
	return len(entries)
}

// The next delta segment is prepared once the active one is half written, without appearing in
// the directory, and becomes - at rotation - a segment identical to one created on the spot:
// same name, same size, the data where it was written and zeros after it.
func TestSpareSegmentBecomesTheNextSegment(t *testing.T) {
	if !unnamedFilesSupported {
		t.Skip("no unnamed files on this platform")
	}
	shim, store, root := openPreallocShim(t, testSpareBytes)
	first := SegmentNameBuilder.DeltaSequenced("app/data", 4)
	second := SegmentNameBuilder.DeltaSequenced("app/data", 5)

	half := bytes.Repeat([]byte{0x11}, testSpareBytes/2-1)
	if _, err := shim.AppendToSegment(first, half); err != nil {
		t.Fatal(err)
	}
	if n := store.spareCount(); n != 0 {
		t.Fatalf("%d spares before the segment is half written, want 0", n)
	}
	if _, err := shim.AppendToSegment(first, []byte{0x22}); err != nil {
		t.Fatal(err)
	}
	if n := store.spareCount(); n != 1 {
		t.Fatalf("%d spares once the segment is half written, want 1", n)
	}
	if !waitSpare(t, store, second) {
		t.Skip("this filesystem has no O_TMPFILE")
	}
	// Prepared, and invisible: nothing lists it and it has no name.
	names, err := shim.ListSegments("app/data")
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != first {
		t.Fatalf("listed %v while the spare is prepared, want only %s", names, first)
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(second))); !os.IsNotExist(err) {
		t.Fatalf("the spare has a name before rotation: %v", err)
	}
	// More appends to the same segment start nothing new.
	if _, err := shim.AppendToSegment(first, []byte{0x33}); err != nil {
		t.Fatal(err)
	}
	if n := store.spareCount(); n != 1 {
		t.Fatalf("%d spares after another append, want still 1", n)
	}

	store.mu.Lock()
	spareInfo, err := store.spares[second].file.Stat()
	store.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := shim.SealSegment(first); err != nil {
		t.Fatal(err)
	}
	data := []byte("first frame of the next segment")
	if size, err := shim.AppendToSegment(second, data); err != nil || size != int64(len(data)) {
		t.Fatalf("append to the next segment: size %d, %v; want %d", size, err, len(data))
	}
	if n := store.spareCount(); n != 0 {
		t.Fatalf("%d spares after rotation, want 0 (taken)", n)
	}
	if err := shim.FlushSegment(second); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(root, filepath.FromSlash(second)))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(info, spareInfo) {
		t.Fatal("the next segment is not the spare prepared for it")
	}
	if info.Size() != testSpareBytes {
		t.Fatalf("next segment is %d bytes on disk, want the preallocated %d", info.Size(), testSpareBytes)
	}
	firstInfo, err := os.Stat(filepath.Join(root, filepath.FromSlash(first)))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode() != firstInfo.Mode() {
		t.Fatalf("next segment mode %v, want %v like the segment created by name", info.Mode(), firstInfo.Mode())
	}
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(second)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw[:len(data)], data) || !bytes.Equal(raw[len(data):], make([]byte, testSpareBytes-len(data))) {
		t.Fatal("next segment does not hold the data followed by zeros")
	}
	// The data reads back through the store, and a second rotation finds its own spare.
	got, err := shim.ReadFromSegment(second, 0, len(data))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read back %q, %v", got, err)
	}
	if _, err := shim.AppendToSegment(second, bytes.Repeat([]byte{0x44}, testSpareBytes/2)); err != nil {
		t.Fatal(err)
	}
	if !waitSpare(t, store, SegmentNameBuilder.DeltaSequenced("app/data", 6)) {
		t.Fatal("second spare failed to prepare")
	}
}

// A segment name that already exists when the writer reaches it is reopened, never replaced by
// the spare - the same answer as creating it by name gives.
func TestSpareSegmentNeverReplacesAnExistingFile(t *testing.T) {
	if !unnamedFilesSupported {
		t.Skip("no unnamed files on this platform")
	}
	shim, store, root := openPreallocShim(t, testSpareBytes)
	first := SegmentNameBuilder.DeltaSequenced("app/data", 1)
	second := SegmentNameBuilder.DeltaSequenced("app/data", 2)
	if _, err := shim.AppendToSegment(first, bytes.Repeat([]byte{1}, testSpareBytes/2)); err != nil {
		t.Fatal(err)
	}
	if !waitSpare(t, store, second) {
		t.Skip("this filesystem has no O_TMPFILE")
	}
	if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(second)), []byte("xyz"), 0o644); err != nil {
		t.Fatal(err)
	}
	before := openFDs(t)
	size, err := shim.AppendToSegment(second, []byte("abc"))
	if err != nil {
		t.Fatal(err)
	}
	if size != 6 {
		t.Fatalf("appended to an existing 3-byte segment and got size %d, want 6", size)
	}
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(second)))
	if err != nil || string(raw) != "xyzabc" {
		t.Fatalf("existing segment holds %q, %v; want xyzabc", raw, err)
	}
	// The spare's descriptor was closed (the reopened segment holds one in its place).
	if after := openFDs(t); after != before {
		t.Fatalf("%d descriptors open after the spare was refused, want %d", after, before)
	}
}

// Spares do not outlive the namespace that prepared them, or the store: their descriptors (and
// so their disk space) are released by ReleaseHandles and Close.
func TestSpareSegmentsAreReleased(t *testing.T) {
	if !unnamedFilesSupported {
		t.Skip("no unnamed files on this platform")
	}
	shim, store, _ := openPreallocShim(t, testSpareBytes)
	before := openFDs(t)
	for _, ns := range []string{"a/one", "b/two"} {
		if _, err := shim.AppendToSegment(SegmentNameBuilder.DeltaSequenced(ns, 0), bytes.Repeat([]byte{1}, testSpareBytes/2)); err != nil {
			t.Fatal(err)
		}
		if !waitSpare(t, store, SegmentNameBuilder.DeltaSequenced(ns, 1)) {
			t.Skip("this filesystem has no O_TMPFILE")
		}
	}
	if err := shim.ReleaseHandles(SegmentNameBuilder.NamespacePrefix("a/one")); err != nil {
		t.Fatal(err)
	}
	if n := store.spareCount(); n != 1 {
		t.Fatalf("%d spares after releasing one namespace, want 1", n)
	}
	// One write handle and one spare left, for b/two.
	if got := openFDs(t); got != before+2 {
		t.Fatalf("%d descriptors after releasing a/one, want %d", got, before+2)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if n := store.spareCount(); n != 0 {
		t.Fatalf("%d spares after Close, want 0", n)
	}
	if got := openFDs(t); got != before {
		t.Fatalf("%d descriptors after Close, want %d", got, before)
	}
}

// Closing while a spare is still being prepared waits for it and leaves no descriptor behind.
func TestCloseWaitsForASpareBeingPrepared(t *testing.T) {
	if !unnamedFilesSupported {
		t.Skip("no unnamed files on this platform")
	}
	const big = 32 << 20
	shim, store, _ := openPreallocShim(t, big)
	before := openFDs(t)
	if _, err := shim.AppendToSegment(SegmentNameBuilder.DeltaSequenced("a/one", 0), make([]byte, big/2)); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if got := openFDs(t); got != before {
		t.Fatalf("%d descriptors after Close, want %d", got, before)
	}
}

// Without unnamed files (off Linux), or for anything but a preallocated delta segment, nothing is
// prepared.
func TestNoSpareWithoutPreallocationOrOffLinux(t *testing.T) {
	shim, store, _ := openPreallocShim(t, 0)
	if _, err := shim.AppendToSegment(SegmentNameBuilder.DeltaSequenced("a/one", 0), make([]byte, 4<<20)); err != nil {
		t.Fatal(err)
	}
	if n := store.spareCount(); n != 0 {
		t.Fatalf("%d spares without preallocation, want 0", n)
	}
	shim, store, _ = openPreallocShim(t, testSpareBytes)
	if _, err := shim.AppendToSegment(SegmentNameBuilder.WAL("a/one", "w1"), make([]byte, testSpareBytes)); err != nil {
		t.Fatal(err)
	}
	if n := store.spareCount(); n != 0 {
		t.Fatalf("%d spares for a WAL segment, want 0", n)
	}
	if _, err := shim.AppendToSegment(SegmentNameBuilder.DeltaSequenced("a/one", 0), make([]byte, testSpareBytes/2)); err != nil {
		t.Fatal(err)
	}
	want := 0
	if unnamedFilesSupported {
		want = 1
	}
	if n := store.spareCount(); n != want {
		t.Fatalf("%d spares for a half-written delta segment, want %d", n, want)
	}
}

func TestNextDeltaSegmentName(t *testing.T) {
	next, ok := nextDeltaSegmentName(SegmentNameBuilder.DeltaSequenced("a/b", 41))
	if !ok || next != SegmentNameBuilder.DeltaSequenced("a/b", 42) {
		t.Fatalf("next = %q, %v", next, ok)
	}
	if _, ok := nextDeltaSegmentName(SegmentNameBuilder.WAL("a/b", "x")); ok {
		t.Fatal("a WAL segment has a next delta segment")
	}
}
