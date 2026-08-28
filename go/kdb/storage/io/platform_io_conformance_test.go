package io_test

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// The whole PlatformIOShim surface - append, read, seal, list, delete, snapshots - had no test,
// on either implementation. The in-memory shim stands in for the file-backed one throughout the
// rest of the suite, which only works if the two behave the same way: a double that is more
// permissive than the real thing lets a bug pass every test and fail in production. So the same
// suite runs against both.

type shimFactory struct {
	name string
	open func(t *testing.T) storage.PlatformIOShim
}

func shimFactories() []shimFactory {
	return []shimFactory{
		{
			name: "in-memory",
			open: func(t *testing.T) storage.PlatformIOShim {
				return storio.NewInMemoryPlatformIO()
			},
		},
		{
			name: "file-backed",
			open: func(t *testing.T) storage.PlatformIOShim {
				t.Helper()
				root := t.TempDir()
				cfg := storio.DefaultPlatformIOConfig()
				cfg.RootDirectory = &root
				// Real fsync on every flush makes this suite noticeably slower without testing
				// anything it covers; durability under fsync is the storage engine's own tests.
				cfg.FsyncOnFlush = false
				store, err := storio.NewOSByteStore(cfg)
				if err != nil {
					t.Fatalf("open os byte store: %v", err)
				}
				return storio.NewFileBackedPlatformIO(cfg, store)
			},
		},
	}
}

// forEachShim runs body against every implementation as a subtest.
func forEachShim(t *testing.T, body func(t *testing.T, shim storage.PlatformIOShim)) {
	t.Helper()
	for _, f := range shimFactories() {
		t.Run(f.name, func(t *testing.T) {
			body(t, f.open(t))
		})
	}
}

const testNS = "app/data"

func deltaSegment(seq int64) string {
	return storio.SegmentNameBuilder.DeltaSequenced(testNS, seq)
}

func TestShimAppendThenReadBack(t *testing.T) {
	forEachShim(t, func(t *testing.T, shim storage.PlatformIOShim) {
		name := deltaSegment(1)
		size, err := shim.AppendToSegment(name, []byte("hello "))
		if err != nil {
			t.Fatalf("first append: %v", err)
		}
		if size != 6 {
			t.Fatalf("first append reported size %d, want 6", size)
		}
		size, err = shim.AppendToSegment(name, []byte("world"))
		if err != nil {
			t.Fatalf("second append: %v", err)
		}
		// The reported size is the segment's total, not the appended chunk's - callers use it to
		// track where the next record starts.
		if size != 11 {
			t.Fatalf("second append reported size %d, want 11", size)
		}

		got, err := shim.ReadFromSegment(name, 0, 11)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(got) != "hello world" {
			t.Fatalf("read back %q", got)
		}

		// A read from an offset returns the tail from there.
		got, err = shim.ReadFromSegment(name, 6, 5)
		if err != nil {
			t.Fatalf("offset read: %v", err)
		}
		if string(got) != "world" {
			t.Fatalf("offset read gave %q, want %q", got, "world")
		}
	})
}

// Asking for more than is there returns what is there, rather than erroring or padding - a
// reader that does not know a segment's length depends on this.
func TestShimReadPastTheEndReturnsWhatExists(t *testing.T) {
	forEachShim(t, func(t *testing.T, shim storage.PlatformIOShim) {
		name := deltaSegment(1)
		if _, err := shim.AppendToSegment(name, []byte("abc")); err != nil {
			t.Fatal(err)
		}
		got, err := shim.ReadFromSegment(name, 0, 1000)
		if err != nil {
			t.Fatalf("over-long read: %v", err)
		}
		if string(got) != "abc" {
			t.Fatalf("got %q, want %q", got, "abc")
		}
		// Reading exactly at the end is empty, not an error.
		got, err = shim.ReadFromSegment(name, 3, 10)
		if err != nil {
			t.Fatalf("read at end: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("read at end gave %q", got)
		}
	})
}

func TestShimRejectsNegativeOffsetAndLength(t *testing.T) {
	forEachShim(t, func(t *testing.T, shim storage.PlatformIOShim) {
		name := deltaSegment(1)
		if _, err := shim.AppendToSegment(name, []byte("abc")); err != nil {
			t.Fatal(err)
		}
		if _, err := shim.ReadFromSegment(name, -1, 1); err == nil {
			t.Error("a negative offset was accepted")
		}
		if _, err := shim.ReadFromSegment(name, 0, -1); err == nil {
			t.Error("a negative length was accepted")
		}
	})
}

func TestShimReadOfAnUnknownSegmentFails(t *testing.T) {
	forEachShim(t, func(t *testing.T, shim storage.PlatformIOShim) {
		if _, err := shim.ReadFromSegment(deltaSegment(99), 0, 1); err == nil {
			t.Error("reading a segment that was never written succeeded")
		}
	})
}

// Sealing is what says "this segment is finished". A write after it is a real bug, and both
// implementations have to catch it - the in-memory shim used to accept it silently, so such a
// bug passed every test that used the double and failed only against a real disk.
func TestShimSealedSegmentRejectsFurtherAppends(t *testing.T) {
	forEachShim(t, func(t *testing.T, shim storage.PlatformIOShim) {
		name := deltaSegment(1)
		if _, err := shim.AppendToSegment(name, []byte("before")); err != nil {
			t.Fatal(err)
		}
		if err := shim.SealSegment(name); err != nil {
			t.Fatalf("seal: %v", err)
		}
		if _, err := shim.AppendToSegment(name, []byte("after")); err == nil {
			t.Fatal("an append to a sealed segment was accepted")
		}
		// Sealing does not make the segment unreadable - readers still need it.
		got, err := shim.ReadFromSegment(name, 0, 6)
		if err != nil {
			t.Fatalf("read after seal: %v", err)
		}
		if string(got) != "before" {
			t.Fatalf("read after seal gave %q", got)
		}
	})
}

func TestShimSealIsIdempotent(t *testing.T) {
	forEachShim(t, func(t *testing.T, shim storage.PlatformIOShim) {
		name := deltaSegment(1)
		if _, err := shim.AppendToSegment(name, []byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := shim.SealSegment(name); err != nil {
			t.Fatalf("first seal: %v", err)
		}
		if err := shim.SealSegment(name); err != nil {
			t.Fatalf("second seal: %v", err)
		}
	})
}

// Segment file names are zero-padded so that lexicographic order is sequence order, and readers
// rely on it: delta replay identifies the *most recently written* segment as the last one and
// only tolerates a torn tail there. An arbitrary order means a corrupt middle segment could
// land last and be forgiven while a genuine torn tail is treated as fatal.
func TestShimListSegmentsIsSortedBySequence(t *testing.T) {
	forEachShim(t, func(t *testing.T, shim storage.PlatformIOShim) {
		// Written out of order, and spanning a digit-count boundary so a non-padded sort would
		// disagree with a numeric one.
		for _, seq := range []int64{11, 2, 1, 100, 20, 3} {
			if _, err := shim.AppendToSegment(deltaSegment(seq), []byte("x")); err != nil {
				t.Fatal(err)
			}
		}
		got, err := shim.ListSegments(testNS)
		if err != nil {
			t.Fatal(err)
		}
		want := []string{
			deltaSegment(1), deltaSegment(2), deltaSegment(3),
			deltaSegment(11), deltaSegment(20), deltaSegment(100),
		}
		if len(got) != len(want) {
			t.Fatalf("listed %d segments, want %d: %v", len(got), len(want), got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("segment %d is %q, want %q\n got %v\nwant %v", i, got[i], want[i], got, want)
			}
		}
	})
}

func TestShimListSegmentsIsScopedToItsNamespace(t *testing.T) {
	forEachShim(t, func(t *testing.T, shim storage.PlatformIOShim) {
		if _, err := shim.AppendToSegment(storio.SegmentNameBuilder.DeltaSequenced("app/one", 1), []byte("x")); err != nil {
			t.Fatal(err)
		}
		if _, err := shim.AppendToSegment(storio.SegmentNameBuilder.DeltaSequenced("app/two", 1), []byte("y")); err != nil {
			t.Fatal(err)
		}
		got, err := shim.ListSegments("app/one")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 {
			t.Fatalf("listing app/one gave %v", got)
		}
		// A namespace nothing was written to lists nothing, rather than everything.
		got, err = shim.ListSegments("app/three")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Fatalf("listing an unused namespace gave %v", got)
		}
	})
}

func TestShimDeleteRemovesTheSegment(t *testing.T) {
	forEachShim(t, func(t *testing.T, shim storage.PlatformIOShim) {
		name := deltaSegment(1)
		if _, err := shim.AppendToSegment(name, []byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := shim.DeleteSegment(name); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if got, err := shim.ListSegments(testNS); err != nil || len(got) != 0 {
			t.Fatalf("after delete: %v, %v", got, err)
		}
		if _, err := shim.ReadFromSegment(name, 0, 1); err == nil {
			t.Error("a deleted segment was still readable")
		}
	})
}

// Deleting frees the name: a fresh segment written under it starts empty rather than inheriting
// the old contents or staying sealed.
func TestShimDeletedNameCanBeReused(t *testing.T) {
	forEachShim(t, func(t *testing.T, shim storage.PlatformIOShim) {
		name := deltaSegment(1)
		if _, err := shim.AppendToSegment(name, []byte("old")); err != nil {
			t.Fatal(err)
		}
		if err := shim.SealSegment(name); err != nil {
			t.Fatal(err)
		}
		if err := shim.DeleteSegment(name); err != nil {
			t.Fatal(err)
		}

		size, err := shim.AppendToSegment(name, []byte("new"))
		if err != nil {
			t.Fatalf("append after delete: %v - the seal outlived the segment", err)
		}
		if size != 3 {
			t.Fatalf("reused segment reports size %d, want 3 - old bytes survived", size)
		}
		got, err := shim.ReadFromSegment(name, 0, 3)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "new" {
			t.Fatalf("reused segment reads %q", got)
		}
	})
}

func TestShimFlushIsSafeOnAnyState(t *testing.T) {
	forEachShim(t, func(t *testing.T, shim storage.PlatformIOShim) {
		name := deltaSegment(1)
		if _, err := shim.AppendToSegment(name, []byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := shim.FlushSegment(name); err != nil {
			t.Fatalf("flush: %v", err)
		}
		// Flushing twice, and flushing after a seal, are both fine.
		if err := shim.FlushSegment(name); err != nil {
			t.Fatalf("second flush: %v", err)
		}
		if err := shim.SealSegment(name); err != nil {
			t.Fatal(err)
		}
		if err := shim.FlushSegment(name); err != nil {
			t.Fatalf("flush after seal: %v", err)
		}
	})
}

func TestShimSnapshotRoundTrip(t *testing.T) {
	forEachShim(t, func(t *testing.T, shim storage.PlatformIOShim) {
		const key = "app/data/index/status"
		body := []byte{0x00, 0x01, 0xff, 0x7f}

		// A key that was never written reads as absent rather than as an error.
		got, err := shim.ReadSnapshot(key)
		if err != nil {
			t.Fatalf("read of an absent snapshot: %v", err)
		}
		if got != nil {
			t.Fatalf("an absent snapshot read back as %v", got)
		}

		if err := shim.WriteSnapshot(key, body); err != nil {
			t.Fatalf("write: %v", err)
		}
		got, err = shim.ReadSnapshot(key)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(got) != string(body) {
			t.Fatalf("read back %v, want %v", got, body)
		}

		// Writing again replaces rather than appends.
		if err := shim.WriteSnapshot(key, []byte{0x09}); err != nil {
			t.Fatal(err)
		}
		got, _ = shim.ReadSnapshot(key)
		if len(got) != 1 || got[0] != 0x09 {
			t.Fatalf("overwrite left %v", got)
		}

		if err := shim.DeleteSnapshot(key); err != nil {
			t.Fatalf("delete: %v", err)
		}
		got, err = shim.ReadSnapshot(key)
		if err != nil {
			t.Fatalf("read after delete: %v", err)
		}
		if got != nil {
			t.Fatalf("a deleted snapshot read back as %v", got)
		}
	})
}

// Returned buffers must be copies: a caller that mutates what it read must not corrupt the
// stored bytes, which for the in-memory shim would be the live map value.
func TestShimReturnsCopiesNotAliases(t *testing.T) {
	forEachShim(t, func(t *testing.T, shim storage.PlatformIOShim) {
		name := deltaSegment(1)
		if _, err := shim.AppendToSegment(name, []byte("abc")); err != nil {
			t.Fatal(err)
		}
		first, err := shim.ReadFromSegment(name, 0, 3)
		if err != nil {
			t.Fatal(err)
		}
		first[0] = 'z'
		second, err := shim.ReadFromSegment(name, 0, 3)
		if err != nil {
			t.Fatal(err)
		}
		if string(second) != "abc" {
			t.Fatalf("mutating a read buffer changed the segment: now %q", second)
		}

		const key = "app/data/snap"
		body := []byte{1, 2, 3}
		if err := shim.WriteSnapshot(key, body); err != nil {
			t.Fatal(err)
		}
		// Mutating the caller's own slice after writing must not change what was stored.
		body[0] = 9
		got, _ := shim.ReadSnapshot(key)
		if len(got) != 3 || got[0] != 1 {
			t.Fatalf("the stored snapshot aliased the caller's slice: %v", got)
		}
	})
}

// AvailableBytes is declared on the shim but has no consumer anywhere in the tree, and the OS
// store says as much and returns a 0 sentinel. So this only pins that it answers without
// error - asserting a positive value would be inventing a contract nobody relies on. If a disk
// guard ever starts consuming it, that sentinel has to be replaced with a real statfs, and this
// test is where to tighten.
func TestShimAvailableBytesAnswersWithoutError(t *testing.T) {
	forEachShim(t, func(t *testing.T, shim storage.PlatformIOShim) {
		if _, err := shim.AvailableBytes(); err != nil {
			t.Fatalf("available bytes: %v", err)
		}
	})
}

func TestShimAppendsAreDurableAcrossManySegments(t *testing.T) {
	forEachShim(t, func(t *testing.T, shim storage.PlatformIOShim) {
		const n = 25
		for i := int64(0); i < n; i++ {
			if _, err := shim.AppendToSegment(deltaSegment(i), []byte(fmt.Sprintf("segment-%d", i))); err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
		}
		listed, err := shim.ListSegments(testNS)
		if err != nil {
			t.Fatal(err)
		}
		if len(listed) != n {
			t.Fatalf("listed %d segments, want %d", len(listed), n)
		}
		for i := int64(0); i < n; i++ {
			want := fmt.Sprintf("segment-%d", i)
			got, err := shim.ReadFromSegment(deltaSegment(i), 0, len(want))
			if err != nil {
				t.Fatalf("read %d: %v", i, err)
			}
			if string(got) != want {
				t.Fatalf("segment %d holds %q, want %q", i, got, want)
			}
		}
	})
}
