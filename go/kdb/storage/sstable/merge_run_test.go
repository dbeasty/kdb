package sstable

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
	"github.com/limidus/kdb/go/kdb/storage/mmapfile"
)

// smallMergeTables shrinks a merge's output tables for the length of a test.
func smallMergeTables(t *testing.T, keys int, bytes int64) {
	t.Helper()
	k, b := mergeTableKeys, mergeTableBytes
	mergeTableKeys, mergeTableBytes = keys, bytes
	t.Cleanup(func() { mergeTableKeys, mergeTableBytes = k, b })
}

// tableEntries reads every key of table h with its value (nil for a tombstone), in footer order.
func tableEntries(t *testing.T, shim storage.PlatformIOShim, h Handle) ([]indexEntry, [][]byte) {
	t.Helper()
	r := NewDefaultReader(shim, h)
	size, err := r.segmentSize()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := r.indexEntries(size)
	if err != nil {
		t.Fatal(err)
	}
	values := make([][]byte, len(entries))
	for i, e := range entries {
		if e.handle.Deleted {
			continue
		}
		if values[i], err = r.ReadBlockAt(e.handle); err != nil {
			t.Fatal(err)
		}
	}
	return entries, values
}

// A merge larger than one output table writes a run: tables of at most mergeTableKeys keys, in
// ascending, non-overlapping key order, one run between them, holding every surviving key - and
// each table exactly the file Finish writes for its entries.
func TestMergeSplitsItsOutputIntoARun(t *testing.T) {
	smallMergeTables(t, 100, 1<<40)
	shim := testShim(t)
	ns := "app/docs"
	a := bigTable(t, shim, ns, 450, 200, 1, nil)
	b := bigTable(t, shim, ns, 400, 200, 2, []string{"key-1-000003", "key-1-000300"})
	out, err := Compact(shim, ns, 1, []Handle{a, b}, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := (450 + 400 + 0 + 99) / 100; len(out) != want {
		t.Fatalf("%d output tables, want %d", len(out), want)
	}
	if n := countRuns(out); n != 1 {
		t.Fatalf("the output is %d runs, want 1", n)
	}
	total := 0
	var last *codec.Hash
	for i, h := range out {
		entries, values := tableEntries(t, shim, h)
		if len(entries) > 100 {
			t.Fatalf("table %d holds %d keys, over the cap", i, len(entries))
		}
		total += len(entries)
		w := NewDefaultWriter(shim, ns, 1)
		for j, e := range entries {
			if last != nil && bytes.Compare(last.Bytes[:], e.key.Bytes[:]) >= 0 {
				t.Fatalf("table %d entry %d is not above the key before it", i, j)
			}
			k := e.key
			last = &k
			if e.handle.Deleted {
				w.Delete(e.key)
			} else {
				w.Put(e.key, values[j])
			}
		}
		want, err := w.Finish()
		if err != nil {
			t.Fatal(err)
		}
		if h.FileHash != want.FileHash || !bytes.Equal(readSegment(t, shim, h.SegmentName), readSegment(t, shim, want.SegmentName)) {
			t.Fatalf("table %d differs from what Finish writes for its entries", i)
		}
	}
	if total != 450+400 {
		t.Fatalf("the run holds %d keys, want %d (two of b's are tombstones for a's keys)", total, 450+400)
	}
	// The byte cap splits too.
	smallMergeTables(t, 1<<30, 20_000)
	out, err = Compact(shim, ns, 1, []Handle{a, b}, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) < 5 {
		t.Fatalf("a %d-byte cap wrote only %d tables", 20_000, len(out))
	}
}

// noLocalPaths hides the file-backed shim's local paths, so nothing it holds has a sidecar.
type noLocalPaths struct{ *storio.FileBackedPlatformIO }

func (noLocalPaths) LocalSegmentPath(string) (string, bool) { return "", false }

// A merge reads each input through its sidecar when it has one, and through its footer
// otherwise, and the two must produce the same run: same winner per key (newest input), same
// tombstones, same bytes.
func TestMergeFromSidecarsEqualsMergeFromFooters(t *testing.T) {
	shim, _ := osShim(t)
	ns := "app/docs"
	older := writeFileTable(t, shim, ns, map[string]string{"a": "old", "b": "keep", "c": "doomed"}, nil)
	newer := writeFileTable(t, shim, ns, map[string]string{"a": "new", "d": "4"}, []string{"c"})
	inputs := []Handle{older, newer}

	sources, err := openMergeSources(shim, inputs, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i, src := range sources {
		if _, ok := src.(*sidecarSource); !ok {
			t.Fatalf("input %d is read through %T, not its sidecar", i, src)
		}
	}
	closeMergeSources(sources)

	viaSidecars, err := Compact(shim, ns, 1, inputs, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	viaFooters, err := Compact(noLocalPaths{shim}, ns, 1, inputs, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(viaSidecars) != 1 || len(viaFooters) != 1 || viaSidecars[0].FileHash != viaFooters[0].FileHash {
		t.Fatalf("sidecar-fed merge %+v differs from footer-fed %+v", viaSidecars, viaFooters)
	}
	r := NewDefaultReader(shim, viaSidecars[0])
	for k, want := range map[string]string{"a": "new", "b": "keep", "d": "4"} {
		if got, _ := r.Get(keyFor([]byte(k))); string(got) != want {
			t.Fatalf("%s reads %q, want %q", k, got, want)
		}
	}
	if _, deleted, found, _ := r.Lookup(keyFor([]byte("c"))); !found || !deleted {
		t.Fatal("the newer input's tombstone did not win")
	}
}

func writeFileTable(t *testing.T, shim storage.PlatformIOShim, ns string, puts map[string]string, deletes []string) Handle {
	t.Helper()
	w := NewDefaultWriter(shim, ns, 0)
	for k, v := range puts {
		w.Put(keyFor([]byte(k)), []byte(v))
	}
	for _, k := range deletes {
		w.Delete(keyFor([]byte(k)))
	}
	h, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// The trigger counts runs, not tables: a store whose last merge wrote several tables does not
// merge again until enough new runs - flushes - have joined it.
func TestCompactionTriggerCountsRuns(t *testing.T) {
	smallMergeTables(t, 50, 1<<40)
	shim := testShim(t)
	ns := "app/docs"
	store := NewLsmBlobStore(shim, ns, NewBlockCache(1<<20))
	for i := 0; i < DefaultCompactionTrigger; i++ {
		store.AddTable(bigTable(t, shim, ns, 60, 100, int64(i), nil))
	}
	res, err := store.Compact(DefaultCompactionTrigger, nil)
	if err != nil || res.Merged != DefaultCompactionTrigger {
		t.Fatalf("first merge: %+v, %v", res, err)
	}
	if len(store.Tables()) < 4 || store.RunCount() != 1 {
		t.Fatalf("after the merge: %d tables in %d runs, want several tables in one run", len(store.Tables()), store.RunCount())
	}
	if res, err := store.Compact(DefaultCompactionTrigger, nil); err != nil || res.Merged != 0 {
		t.Fatalf("a store of one run merged again: %+v, %v", res, err)
	}
	// Three small flushes beside the one run are not yet a merge: tiering waits for four of a
	// size (tiering.go). The fourth makes a window, merged into a run of its own; the large run
	// is left alone.
	for i := 0; i < DefaultCompactionTrigger-1; i++ {
		store.AddTable(bigTable(t, shim, ns, 10, 100, int64(10+i), nil))
	}
	if res, err := store.Compact(DefaultCompactionTrigger, nil); err != nil || res.Merged != 0 {
		t.Fatalf("three small flushes beside a large run were merged: %+v, %v", res, err)
	}
	store.AddTable(bigTable(t, shim, ns, 10, 100, int64(10+DefaultCompactionTrigger-1), nil))
	res, err = store.Compact(DefaultCompactionTrigger, nil)
	if err != nil || res.Merged != DefaultCompactionTrigger || res.Full || store.RunCount() != 2 {
		t.Fatalf("merge of four small flushes beside a run: %+v, %v, %d runs after", res, err, store.RunCount())
	}
	if got := store.Get(keyFor([]byte("key-12-000009"))); len(got) != 100 {
		t.Fatalf("a key from the last flush reads %d bytes", len(got))
	}
	if got := store.Get(keyFor([]byte("key-0-000059"))); len(got) != 100 {
		t.Fatalf("a key from the first merge reads %d bytes", len(got))
	}
}

func TestRunOf(t *testing.T) {
	ns := "app/docs"
	flushed := storio.SegmentNameBuilder.SSTable(ns, 0, "9f956c9f-2a7c-4409-9ca8-74669fb17fe7")
	if runOf(flushed) != flushed {
		t.Fatalf("a flushed table's run is %q", runOf(flushed))
	}
	a := storio.SegmentNameBuilder.SSTable(ns, 2, runTableID("r1", 0))
	b := storio.SegmentNameBuilder.SSTable(ns, 2, runTableID("r1", 7))
	c := storio.SegmentNameBuilder.SSTable(ns, 2, runTableID("r2", 0))
	if runOf(a) != runOf(b) || runOf(a) == runOf(c) || runOf(a) == runOf(flushed) {
		t.Fatalf("runs: %q %q %q", runOf(a), runOf(b), runOf(c))
	}
	if n := countRuns([]Handle{{SegmentName: flushed}, {SegmentName: a}, {SegmentName: b}, {SegmentName: c}}); n != 3 {
		t.Fatalf("countRuns = %d, want 3", n)
	}
}

// Cancelled after several of its tables are complete - sidecars written - a merge still leaves
// nothing behind: no table, partial or renamed, and no sidecar.
func TestCancelledRunMergeLeavesNoTablesOrSidecars(t *testing.T) {
	smallMergeTables(t, 500, 1<<40)
	shim, root := osShim(t)
	ns := "app/docs"
	in := bigTable(t, shim, ns, 3*compactionCheckEvery, 100, 1, nil)
	files := func() []string {
		var out []string
		_ = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() && !strings.Contains(p, "/snap/") {
				out = append(out, p)
			}
			return nil
		})
		return out
	}
	before := files()
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	keep := func(codec.Hash) bool {
		if calls++; calls == compactionCheckEvery+1 {
			cancel()
		}
		return true
	}
	if _, err := CompactContext(ctx, shim, ns, 1, []Handle{in}, false, keep); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if after := files(); len(after) != len(before) {
		t.Fatalf("a cancelled merge left files behind:\nbefore %v\nafter  %v", before, after)
	}
}

// renameCrashShim dies on its crashAt'th rename: the process stops part way through a merge's
// renames, and nothing it would do afterwards reaches storage.
type renameCrashShim struct {
	*storio.InMemoryPlatformIO
	crashAt, renames int
	crashed          bool
}

func (s *renameCrashShim) RenameSegment(from, to string) error {
	if s.renames++; s.renames == s.crashAt {
		s.crashed = true
		panic("crash")
	}
	return s.InMemoryPlatformIO.RenameSegment(from, to)
}

func (s *renameCrashShim) DeleteSegment(name string) error {
	if s.crashed {
		return errors.New("process is dead")
	}
	return s.InMemoryPlatformIO.DeleteSegment(name)
}

// A crash between a run's renames leaves some of its tables registered beside the inputs at the
// next open. That is safe - they hold the inputs' own bytes - and the next merge takes them in.
func TestCrashDuringARunsRenamesIsHarmless(t *testing.T) {
	smallMergeTables(t, 100, 1<<40)
	shim := &renameCrashShim{InMemoryPlatformIO: testShim(t), crashAt: 3}
	ns := "app/docs"
	store := NewLsmBlobStore(shim, ns, NewBlockCache(1<<20))
	for i := 0; i < DefaultCompactionTrigger; i++ {
		store.AddTable(bigTable(t, shim, ns, 120, 100, int64(i), nil))
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("the merge did not reach the crash")
			}
		}()
		_, _ = store.Compact(DefaultCompactionTrigger, nil)
	}()

	shim.crashed, shim.crashAt = false, -1
	reopened := NewLsmBlobStore(shim, ns, NewBlockCache(1<<20))
	if err := reopened.DiscoverTables(); err != nil {
		t.Fatal(err)
	}
	if n := len(reopened.Tables()); n != DefaultCompactionTrigger+2 {
		t.Fatalf("reopen registered %d tables, want the %d inputs and the 2 renamed outputs", n, DefaultCompactionTrigger)
	}
	for i := 0; i < DefaultCompactionTrigger; i++ {
		k := keyFor([]byte(fmt.Sprintf("key-%d-%06d", i, 119)))
		if got := reopened.Get(k); len(got) != 100 {
			t.Fatalf("input %d's last key reads %d bytes after the crash", i, len(got))
		}
	}
	res, err := reopened.Compact(DefaultCompactionTrigger, nil)
	if err != nil || res.Merged != DefaultCompactionTrigger+2 || reopened.RunCount() != 1 {
		t.Fatalf("merge after the crash: %+v, %v, %d runs", res, err, reopened.RunCount())
	}
	names, _ := shim.ListSegments(ns)
	for _, name := range names {
		if strings.Contains(name, "sstable-partial/") {
			t.Fatalf("%s survived the merge after the crash", name)
		}
	}
	if len(names) != len(reopened.Tables()) {
		t.Fatalf("%d segments on disk for %d tables", len(names), len(reopened.Tables()))
	}
}

// A merge reading an input's sidecar drops the pages it has read from the process's resident set,
// so a merge of many inputs does not end with all their sidecars resident.
func TestSidecarSourceDoesNotLeaveItsPagesResident(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("DropPages is a no-op here")
	}
	if !mmapfile.Supported {
		t.Skip("sidecars need mmap")
	}
	const n = 1 << 20 // 48 MB of entries
	path := filepath.Join(t.TempDir(), "t.sidx")
	var fileHash codec.Hash
	func() {
		entries := make([]indexEntry, n)
		for i := range entries {
			binary.BigEndian.PutUint64(entries[i].key.Bytes[:], uint64(i))
			entries[i].handle = BlockHandle{Offset: int64(i) * 100, CompressedSize: 90}
		}
		if err := writeSidecarFile(path, entries, 12345, fileHash); err != nil {
			t.Fatal(err)
		}
	}()
	sc, err := openSidecar(path, 12345, fileHash)
	if err != nil {
		t.Fatal(err)
	}
	defer sc.close()
	runtime.GC()
	debug.FreeOSMemory()
	rss := func() int64 {
		out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
		if err != nil {
			t.Skipf("ps: %v", err)
		}
		kb, _ := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
		return kb << 10
	}
	// Opening verified the whole file (its checksum), which touched every page; drop them so the
	// read below starts from nothing resident.
	if err := sc.region.DropPages(0, len(sc.region.Bytes())); err != nil {
		t.Fatal(err)
	}
	before := rss()
	src := &sidecarSource{sc: sc}
	buf := make([]indexEntry, mergeBatch)
	read := 0
	for {
		got, err := src.fill(buf)
		if err != nil {
			t.Fatal(err)
		}
		if got == 0 {
			break
		}
		if want := uint64(read); binary.BigEndian.Uint64(buf[0].key.Bytes[:]) != want {
			t.Fatalf("entry %d has the wrong key", read)
		}
		read += got
	}
	after := rss()
	t.Logf("RSS %d MB before reading a %d MB sidecar, %d MB after", before>>20, n*sidecarEntrySize>>20, after>>20)
	if read != n {
		t.Fatalf("read %d entries, want %d", read, n)
	}
	if grown := after - before; grown > 16<<20 {
		t.Fatalf("RSS grew %d MB reading the sidecar: its pages were left resident", grown>>20)
	}
}
