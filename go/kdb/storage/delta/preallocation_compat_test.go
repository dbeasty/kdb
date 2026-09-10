package delta

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

const testPreallocBytes = 1 << 20 // 1MiB - large enough to dwarf the frames, small enough to zero-fill fast

// shimAt builds a file-backed shim over an explicit root, so two shims with
// different preallocation settings can be pointed at the *same* directory -
// which is the whole point of these tests.
func shimAt(t *testing.T, root string, preallocBytes int64) storage.PlatformIOShim {
	t.Helper()
	cfg := storio.PlatformIOConfig{
		RootDirectory:    &root,
		FsyncOnFlush:     true,
		PreallocateBytes: preallocBytes,
	}
	store, err := storio.NewOSByteStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return storio.NewFileBackedPlatformIO(cfg, store)
}

// writeCommits appends n chained commits through a writer built on shim and
// seals the segment, returning the hashes in write order.
func writeCommits(t *testing.T, shim storage.PlatformIOShim, n int) []codec.Hash {
	t.Helper()
	cfg := newConfig(shim, storage.CompressionNone)
	w, err := Factory{Config: cfg}.OpenWriter(testNS)
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	var hashes []codec.Hash
	var parent *codec.Hash
	for i := 0; i < n; i++ {
		c := buildCommit(t, parent)
		if _, err := w.Append(record(t, c)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		h := c.Hash
		hashes = append(hashes, h)
		parent = &h
	}
	if _, err := w.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return hashes
}

// readAllCommits lists every segment through shim and returns each record's
// commit hash, in segment then frame order.
func readAllCommits(t *testing.T, shim storage.PlatformIOShim) []codec.Hash {
	t.Helper()
	r := Factory{Config: newConfig(shim, storage.CompressionNone)}.OpenReader(testNS)
	segs, err := r.ListSegments()
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	var got []codec.Hash
	for _, seg := range segs {
		recs, err := r.ReadAll(seg)
		if err != nil {
			t.Fatalf("ReadAll(%v): %v", seg.SequenceNumber, err)
		}
		for _, rec := range recs {
			got = append(got, rec.CommitHash)
		}
	}
	return got
}

func assertSameHashes(t *testing.T, got, want []codec.Hash) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("read %d commits, wrote %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("commit %d = %v, want %v", i, got[i], want[i])
		}
	}
}

// A segment written with preallocation on must be fully readable by a process
// that has it off. This is the direction that can actually break: the file is
// physically 1MiB of which only a few hundred bytes are frames, and the reader
// has no idea that is the case. It works because the trailing zeros have no
// frame magic, so ScanSegmentBytes' torn-tail handling stops there - the same
// mechanism that tolerates a partially written tail after a crash.
func TestPreallocatedSegmentReadableWithoutPreallocation(t *testing.T) {
	root := t.TempDir()
	want := writeCommits(t, shimAt(t, root, testPreallocBytes), 5)

	// A reader that knows nothing about preallocation.
	got := readAllCommits(t, shimAt(t, root, 0))
	assertSameHashes(t, got, want)
}

// And the reverse: a segment written the old way stays readable by a process
// with preallocation switched on. Nothing special should happen here - the
// setting only affects segments this process *creates* - but it is the other
// half of the compatibility claim and cheap to pin.
func TestNonPreallocatedSegmentReadableWithPreallocation(t *testing.T) {
	root := t.TempDir()
	want := writeCommits(t, shimAt(t, root, 0), 5)

	got := readAllCommits(t, shimAt(t, root, testPreallocBytes))
	assertSameHashes(t, got, want)
}

// Both settings writing into one namespace, in sequence, as happens across a
// rollback or a staged rollout: each process creates its own segment
// (OpenWriter never resumes one), so the namespace ends up holding a
// preallocated segment and a non-preallocated one, and every commit in both
// must still read back in order.
func TestMixedPreallocationInOneNamespace(t *testing.T) {
	root := t.TempDir()

	want := writeCommits(t, shimAt(t, root, testPreallocBytes), 3)
	want = append(want, writeCommits(t, shimAt(t, root, 0), 3)...)
	want = append(want, writeCommits(t, shimAt(t, root, testPreallocBytes), 3)...)

	for _, tc := range []struct {
		name     string
		prealloc int64
	}{
		{"reader with preallocation off", 0},
		{"reader with preallocation on", testPreallocBytes},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertSameHashes(t, readAllCommits(t, shimAt(t, root, tc.prealloc)), want)
		})
	}
}

// The mechanism itself: a preallocated segment must be physically its full
// size on disk while reporting only its frames logically. If the file is
// short, preallocation silently did nothing and every sync is still paying the
// metadata commit this exists to remove - a failure the compatibility tests
// above would not notice, since they pass just as well with the feature off.
func TestPreallocatedSegmentIsPhysicallyFullSize(t *testing.T) {
	root := t.TempDir()
	writeCommits(t, shimAt(t, root, testPreallocBytes), 3)

	segPath := filepath.Join(root, filepath.FromSlash(storio.SegmentNameBuilder.DeltaSequenced(testNS, 0)))
	info, err := os.Stat(segPath)
	if err != nil {
		t.Fatalf("stat segment: %v", err)
	}
	if info.Size() != testPreallocBytes {
		t.Fatalf("segment is %d bytes on disk, want %d - preallocation did not take effect",
			info.Size(), testPreallocBytes)
	}

	// The segment ref must report the *logical* size - the end of the frames -
	// not the file size. Everything downstream sizes its reads from this, so if
	// it reported the padded size, every ReadAll of a 64MiB segment holding a
	// few KiB of commits would allocate 64MiB, reintroducing exactly the
	// over-allocation that readFullSegment's windowed scan was written to fix.
	r := Factory{Config: newConfig(shimAt(t, root, testPreallocBytes), storage.CompressionNone)}.OpenReader(testNS)
	segs, err := r.ListSegments()
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	if len(segs) != 1 {
		t.Fatalf("got %d segments, want 1", len(segs))
	}
	if segs[0].SizeBytes >= testPreallocBytes {
		t.Fatalf("segment ref SizeBytes = %d, want the logical frame end (well under %d)",
			segs[0].SizeBytes, testPreallocBytes)
	}

	// And the same namespace written without it must not be padded, or the
	// flag is not actually a flag.
	rootOff := t.TempDir()
	writeCommits(t, shimAt(t, rootOff, 0), 3)
	offPath := filepath.Join(rootOff, filepath.FromSlash(storio.SegmentNameBuilder.DeltaSequenced(testNS, 0)))
	offInfo, err := os.Stat(offPath)
	if err != nil {
		t.Fatalf("stat unpreallocated segment: %v", err)
	}
	if offInfo.Size() >= testPreallocBytes {
		t.Fatalf("segment with preallocation off is %d bytes, expected far smaller", offInfo.Size())
	}
}
