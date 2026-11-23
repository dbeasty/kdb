package deltaindex

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// A miss caught up with the segment being written reads only the frames appended since the last
// refresh: it never rescans that segment, however often keys are looked up as it grows, and a
// key that is not in the log costs no scan at all. Once the segment seals, its records go to a
// sidecar without a rescan either.
func TestCatchesUpWithTheOpenSegmentWithoutRescanning(t *testing.T) {
	root := t.TempDir()
	cfg := storio.DefaultPlatformIOConfig()
	cfg.RootDirectory = &root
	cfg.FsyncOnFlush = false
	store, err := storio.NewOSByteStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	shim := storio.NewFileBackedPlatformIO(cfg, store)
	ecfg := storage.StorageEngineConfig{CompressionCodec: storage.CompressionNone, IOShim: shim}
	w, err := delta.Factory{Config: ecfg}.OpenWriter(testNS)
	if err != nil {
		t.Fatal(err)
	}
	r := &countingReader{DefaultReader: delta.NewDefaultReader(testNS, shim, ecfg)}
	x := newIndex(r, t.TempDir())
	defer x.Close()

	type at struct{ seq, off int64 }
	where := map[codec.Hash]at{}
	var parent []codec.Hash
	n := 0
	appendCommits := func(count int) []codec.Hash {
		t.Helper()
		var recs []storage.DeltaRecord
		var hashes []codec.Hash
		for i := 0; i < count; i++ {
			doc, _ := codec.RandomUUID()
			c := document.Commit{ParentHashes: parent, NamespaceID: testNS, Timestamp: codec.Timestamp{EpochMillis: int64(n)},
				Operations:       []document.Op{document.WriteOp{DocID: doc, Patch: fmt.Sprintf(`{"i":%d}`, n)}},
				DocumentTreeHash: document.EmptyDocumentTree().TreeHash}
			n++
			h, err := document.ComputeCommitHash(c)
			if err != nil {
				t.Fatal(err)
			}
			c.Hash = h
			payload, _ := c.ToPayloadBytes()
			recs = append(recs, storage.DeltaRecord{CommitHash: h, NamespaceID: testNS, CommitPayload: payload})
			hashes = append(hashes, h)
			parent = []codec.Hash{h}
		}
		offsets, err := w.AppendBatch(recs)
		if err != nil {
			t.Fatal(err)
		}
		for i, h := range hashes {
			where[h] = at{w.SequenceNumber(), offsets[i]}
		}
		return hashes
	}
	requireFound := func(hashes []codec.Hash) {
		t.Helper()
		for _, h := range hashes {
			loc, ok, err := x.Lookup(h)
			if err != nil || !ok {
				t.Fatalf("commit %s not found: ok=%v err=%v", h.Hex()[:8], ok, err)
			}
			if want := where[h]; loc.Segment.SequenceNumber != want.seq || loc.FrameOffset != want.off {
				t.Fatalf("commit found at (%d,%d), written at (%d,%d)", loc.Segment.SequenceNumber, loc.FrameOffset, want.seq, want.off)
			}
		}
	}

	all := appendCommits(50)
	requireFound(all)
	scans := r.scans.Load()
	if scans != 1 {
		t.Fatalf("%d scans for the first miss, want 1", scans)
	}
	for round := 0; round < 10; round++ {
		more := appendCommits(20)
		requireFound(more)
		all = append(all, more...)
	}
	if got := r.scans.Load(); got != scans {
		t.Fatalf("the open segment was rescanned %d times while it grew", got-scans)
	}
	if _, ok, err := x.Lookup(codec.Hash{Bytes: [32]byte{0xEE}}); ok || err != nil {
		t.Fatalf("absent key: ok=%v err=%v", ok, err)
	}
	if got := r.scans.Load(); got != scans {
		t.Fatal("a key absent from the log cost a scan")
	}
	requireFound(all)

	// Rotate: the old segment seals and a new one opens. The sealed segment's records move to a
	// sidecar as they are; only the new segment is scanned.
	if _, err := w.Seal(); err != nil {
		t.Fatal(err)
	}
	w, err = delta.Factory{Config: ecfg}.OpenWriter(testNS)
	if err != nil {
		t.Fatal(err)
	}
	more := appendCommits(30)
	requireFound(more)
	if got := r.scans.Load(); got != scans+1 {
		t.Fatalf("%d scans after rotating, want %d (the new segment only)", got, scans+1)
	}
	if st := x.Stats(); st.OffHeapSegs() != 1 || st.Segments != 2 {
		t.Fatalf("after rotating: %d segments, %d off the heap; want 2, 1", st.Segments, st.OffHeapSegs())
	}
	all = append(all, more...)
	for round := 0; round < 3; round++ {
		more := appendCommits(10)
		requireFound(more)
		all = append(all, more...)
	}
	if got := r.scans.Load(); got != scans+1 {
		t.Fatalf("the new open segment was rescanned (%d scans)", got)
	}
	requireFound(all)
}

func TestMergeRecordsNewerWins(t *testing.T) {
	rec := func(k byte, off uint64) []byte {
		b := make([]byte, recordSize)
		b[0] = k
		b[32] = byte(off)
		return b
	}
	cat := func(rs ...[]byte) []byte {
		var out []byte
		for _, r := range rs {
			out = append(out, r...)
		}
		return out
	}
	got := mergeRecords(cat(rec(1, 1), rec(3, 1), rec(5, 1)), cat(rec(2, 2), rec(3, 2), rec(6, 2)))
	want := cat(rec(1, 1), rec(2, 2), rec(3, 2), rec(5, 1), rec(6, 2))
	if string(got) != string(want) {
		t.Fatalf("merge = %x\nwant  %x", got, want)
	}
	if got := mergeRecords(nil, rec(1, 1)); string(got) != string(rec(1, 1)) {
		t.Fatal("merge into nothing")
	}
}
