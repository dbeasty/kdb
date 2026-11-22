package sstable

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// footerless returns h's blocks without its footer: what a writer that died between its last block
// and its footer left (zolik's ns/zolik/sessions/sstable/L4/60d3f762-..., written by v0.7.x: 544
// whole blocks ending exactly at EOF, no footer magic anywhere, the "trailer" being the last
// block's zstd checksum).
func footerless(t *testing.T, shim storage.PlatformIOShim, h Handle) []byte {
	t.Helper()
	data := readSegment(t, shim, h.SegmentName)
	idx, err := NewDefaultReader(shim, h).Index()
	if err != nil {
		t.Fatal(err)
	}
	var end int64
	for _, bh := range idx {
		if e := bh.Offset + blockHeaderSize + int64(bh.CompressedSize); !bh.Deleted && e > end {
			end = e
		}
	}
	return data[:end]
}

func putSegment(t *testing.T, shim storage.PlatformIOShim, name string, data []byte) {
	t.Helper()
	if _, err := shim.AppendToSegment(name, data); err != nil {
		t.Fatal(err)
	}
}

func TestUnfinishedTableRecognisesOnlyAMissingOrTornFooter(t *testing.T) {
	src := testShim(t)
	h := bigTable(t, src, "app/docs", 40, 64, 1, nil)
	whole := readSegment(t, src, h.SegmentName)
	blocks := footerless(t, src, h)

	torn := append([]byte(nil), whole[:len(blocks)+20]...)
	tornLastBlock := append([]byte(nil), blocks[:len(blocks)-3]...)
	badHeader := append([]byte(nil), blocks...)
	badHeader[blockHeaderSize+int(readInt(blocks, 4))+2] = 7 // the second block's reserved bytes
	badIndex := append([]byte(nil), whole...)
	badIndex[len(blocks)+10] ^= 0xFF // inside the index text: a whole footer, damaged

	for _, c := range []struct {
		name string
		data []byte
		want bool
	}{
		{"finished", whole, false},
		{"no footer", blocks, true},
		{"torn footer", torn, true},
		{"torn last block", tornLastBlock, false},
		{"not a block header", badHeader, false},
		{"whole footer, bad index", badIndex, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			shim := testShim(t)
			putSegment(t, shim, h.SegmentName, c.data)
			got, err := unfinishedTable(shim, h)
			if err != nil {
				t.Fatal(err)
			}
			if got != c.want {
				t.Fatalf("unfinishedTable = %v, want %v", got, c.want)
			}
		})
	}
}

// A footerless table's last four bytes can decode to a length that fits the file. Index must still
// call it corrupt - it used to parse whatever sat there as a footer.
func TestFooterWithoutMagicIsCorrupt(t *testing.T) {
	src := testShim(t)
	h := bigTable(t, src, "app/docs", 40, 64, 1, nil)
	data := footerless(t, src, h)
	copy(data[len(data)-4:], []byte{0, 0, 0, 8}) // a plausible 8-byte index
	shim := testShim(t)
	putSegment(t, shim, h.SegmentName, data)
	if _, err := NewDefaultReader(shim, h).Index(); !errors.Is(err, ErrCorruptTable) {
		t.Fatalf("Index error = %v, want ErrCorruptTable", err)
	}
}

// The zolik case: a namespace holding a table with no footer could never be merged again, by
// v0.7.1 or v0.8.0. The merge now sets it aside - kept, under sstable-unfinished/, where no open
// registers it - and merges the rest.
func TestMergeSetsAsideATableWithNoFooter(t *testing.T) {
	const ns = "zolik/sessions"
	shim := testShim(t)
	store := NewLsmBlobStore(shim, ns, nil)
	bigTable(t, shim, ns, 30, 64, 1, nil)
	bigTable(t, shim, ns, 30, 64, 2, nil)

	// The unfinished table: blocks of a third table under an sstable/L4 name, as v0.7.x left it.
	scratch := testShim(t)
	c := bigTable(t, scratch, ns, 30, 64, 3, nil)
	bad := storio.SegmentNameBuilder.SSTable(ns, 4, "60d3f762-1112-4271-8982-756435ac63d9")
	putSegment(t, shim, bad, footerless(t, scratch, c))
	if err := store.DiscoverTables(); err != nil {
		t.Fatal(err)
	}
	if n := len(store.Tables()); n != 3 {
		t.Fatalf("%d tables registered, want 3", n)
	}

	res, err := store.Compact(2, nil)
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}
	want := storio.SegmentNameBuilder.SSTableUnfinished(ns, "L4/60d3f762-1112-4271-8982-756435ac63d9")
	if len(res.SetAside) != 1 || res.SetAside[0] != want {
		t.Fatalf("SetAside = %v, want [%s]", res.SetAside, want)
	}
	if res.Merged != 2 || len(res.Outputs) == 0 {
		t.Fatalf("merge result %+v: want the two good tables merged", res)
	}
	names, _ := shim.ListSegments(ns)
	var kept bool
	for _, n := range names {
		if n == bad {
			t.Fatalf("%s still under sstable/", bad)
		}
		kept = kept || n == want
	}
	if !kept {
		t.Fatalf("set-aside table not kept at %s; segments %v", want, names)
	}
	for seed := 1; seed <= 2; seed++ {
		for i := 0; i < 30; i++ {
			if store.Get(keyFor([]byte(fmt.Sprintf("key-%d-%06d", seed, i)))) == nil {
				t.Fatalf("key %d/%d lost by the merge", seed, i)
			}
		}
	}

	// A later open does not register it again.
	again := NewLsmBlobStore(shim, ns, nil)
	if err := again.DiscoverTables(); err != nil {
		t.Fatal(err)
	}
	for _, h := range again.Tables() {
		if strings.Contains(h.SegmentName, "60d3f762") {
			t.Fatalf("reopen registered %s", h.SegmentName)
		}
	}
}

// Damage that is not a missing footer still stops the merge: it never completes around a table it
// cannot read, which would drop that table's keys.
func TestMergeStillStopsOnOtherCorruptTables(t *testing.T) {
	const ns = "app/docs"
	shim := testShim(t)
	store := NewLsmBlobStore(shim, ns, nil)
	bigTable(t, shim, ns, 30, 64, 1, nil)
	scratch := testShim(t)
	c := bigTable(t, scratch, ns, 30, 64, 2, nil)
	data := readSegment(t, scratch, c.SegmentName)
	copy(data[len(data)-4:], []byte{0x1B, 0x36, 0x43, 0x58}) // whole blocks and footer, bad trailer
	putSegment(t, shim, storio.SegmentNameBuilder.SSTable(ns, 4, "damaged"), data)
	if err := store.DiscoverTables(); err != nil {
		t.Fatal(err)
	}
	res, err := store.Compact(2, nil)
	if !errors.Is(err, ErrCorruptTable) {
		t.Fatalf("merge error = %v, want ErrCorruptTable", err)
	}
	if len(res.SetAside) != 0 || len(store.Tables()) != 2 {
		t.Fatalf("set aside %v, %d tables: want nothing moved", res.SetAside, len(store.Tables()))
	}
}

// failFooterShim fails the append that would write a table's footer, as a process killed between
// a table's blocks and its footer does.
type failFooterShim struct {
	*storio.InMemoryPlatformIO
	appends map[string]int
	killed  bool
}

func (s *failFooterShim) AppendToSegment(name string, data []byte) (int64, error) {
	s.appends[name]++
	if s.appends[name] == 2 && !s.killed {
		s.killed = true
		return 0, errors.New("killed")
	}
	return s.InMemoryPlatformIO.AppendToSegment(name, data)
}

// A flush that dies before its footer leaves nothing under sstable/: it writes a partial name and
// renames it only once the footer is down. It used to write its own name directly, which is how
// the zolik table came to be.
func TestFlushThatDiesBeforeItsFooterLeavesNoTable(t *testing.T) {
	const ns = "app/docs"
	shim := &failFooterShim{InMemoryPlatformIO: testShim(t), appends: map[string]int{}}
	store := NewLsmBlobStore(shim, ns, nil)
	w := store.NewWriter(0)
	for i := 0; i < 20; i++ {
		w.Put(keyFor([]byte(fmt.Sprintf("k%d", i))), []byte(fmt.Sprintf("value %d", i)))
	}
	if _, err := w.Finish(); err == nil {
		t.Fatal("Finish succeeded through a failed footer append")
	}
	reopened := NewLsmBlobStore(shim.InMemoryPlatformIO, ns, nil)
	if err := reopened.DiscoverTables(); err != nil {
		t.Fatal(err)
	}
	if n := len(reopened.Tables()); n != 0 {
		t.Fatalf("a dead flush left %d registered table(s): %v", n, reopened.Tables())
	}

	// And a flush that finishes ends up under its own name, readable.
	w = store.NewWriter(0)
	w.Put(keyFor([]byte("x")), []byte("y"))
	h, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(h.SegmentName, "/sstable/L0/") {
		t.Fatalf("flushed table named %s", h.SegmentName)
	}
	store.AddTable(h)
	if string(store.Get(keyFor([]byte("x")))) != "y" {
		t.Fatal("flushed value not readable")
	}
}

// A merge clears partial tables a dead merge left, and must not take a flush's live one for one.
func TestMergeLeavesALiveFlushPartialAlone(t *testing.T) {
	const ns = "app/docs"
	shim := testShim(t)
	store := NewLsmBlobStore(shim, ns, nil)
	live := storio.SegmentNameBuilder.SSTablePartial(ns, "flushing")
	dead := storio.SegmentNameBuilder.SSTablePartial(ns, "dead.0000")
	putSegment(t, shim, live, []byte("blocks so far"))
	putSegment(t, shim, dead, []byte("blocks so far"))
	store.startWriting(live)
	store.removePartialTables()
	names, _ := shim.ListSegments(ns)
	var haveLive, haveDead bool
	for _, n := range names {
		haveLive = haveLive || n == live
		haveDead = haveDead || n == dead
	}
	if !haveLive || haveDead {
		t.Fatalf("live kept %v (want true), dead kept %v (want false)", haveLive, haveDead)
	}
	store.doneWriting(live)
}
