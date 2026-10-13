package backup

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/integrity"
)

// referenceHeads is how Create worked the manifest's count and heads out before it streamed:
// every commit of the log in one map, then the ones no other commit names as a parent.
func referenceHeads(t *testing.T, all map[string]document.Commit) (int, map[string]string) {
	t.Helper()
	isParent := map[string]bool{}
	for _, c := range all {
		for _, p := range c.ParentHashes {
			isParent[p.Hex()] = true
		}
	}
	var tips []string
	for h := range all {
		if !isParent[h] {
			tips = append(tips, h)
		}
	}
	sort.Strings(tips)
	heads := map[string]string{}
	for i, tip := range tips {
		name := "main"
		if i > 0 {
			name = fmt.Sprintf("tip-%d", i)
		}
		heads[name] = tip
	}
	return len(all), heads
}

func withParents(t *testing.T, parents ...codec.Hash) document.Commit {
	t.Helper()
	c := buildCommit(t, nil)
	c.ParentHashes = parents
	h, err := document.ComputeCommitHash(c)
	if err != nil {
		t.Fatal(err)
	}
	c.Hash = h
	return c
}

// TestBackupHeadsMatchTheWholeLog: the streamed count and heads are exactly what holding the
// whole log gave, on a log with the shapes a real one has - branches, a merge, a commit logged
// twice (once after its child), history logged below a commit after it (a deepen), and a parent
// that never arrives (a shallow root's).
func TestBackupHeadsMatchTheWholeLog(t *testing.T) {
	shim := newTestShim(t)
	main := chain(t, 6)
	side := withParents(t, main[2].Hash)
	sideTip := withParents(t, side.Hash)
	merge := withParents(t, main[5].Hash, side.Hash)
	other := withParents(t, main[3].Hash) // a second head
	// A root whose history arrives later, below it: root's parent deep1, deep1's parent deep0.
	deep0 := withParents(t, codec.Hash{Bytes: [32]byte{0x77}}) // its parent never arrives
	deep1 := withParents(t, deep0.Hash)
	root := withParents(t, deep1.Hash)
	above := withParents(t, root.Hash)

	appendSegment(t, shim, 1, main[0], main[1], main[2], side, main[3])
	appendSegment(t, shim, 2, main[4], main[5], merge, main[2], other) // main[2] logged again
	appendSegment(t, shim, 3, root, above, sideTip)
	appendSegment(t, shim, 4, deep0, deep1, side) // history below root, logged after it; side again

	store := &DirStore{Root: t.TempDir()}
	m, err := Create(shim, ns, store, "")
	if err != nil {
		t.Fatal(err)
	}
	all, err := integrity.ScanVerifiedCommits(shim, ns)
	if err != nil {
		t.Fatal(err)
	}
	wantCount, wantHeads := referenceHeads(t, all)
	if m.CommitCount != wantCount {
		t.Fatalf("CommitCount %d, want %d", m.CommitCount, wantCount)
	}
	if fmt.Sprint(m.HeadHashes) != fmt.Sprint(wantHeads) {
		t.Fatalf("heads %v, want %v", m.HeadHashes, wantHeads)
	}
	if len(wantHeads) != 4 { // merge, other, above, sideTip
		t.Fatalf("fixture: want 4 heads, the reference found %d", len(wantHeads))
	}
	back, err := LoadManifest(store, ns, m.BackupID)
	if err != nil || back.CommitCount != m.CommitCount || fmt.Sprint(back.HeadHashes) != fmt.Sprint(m.HeadHashes) {
		t.Fatalf("the uploaded manifest differs: %v", err)
	}
}

// heapProbeStore measures the live heap at its first Put - after the classification pass, when
// Create used to hold every commit of the log.
type heapProbeStore struct {
	*DirStore
	probed bool
	live   uint64
}

func (s *heapProbeStore) Put(ctx context.Context, key string, data []byte) error {
	if !s.probed {
		s.probed = true
		s.live = liveHeapBytes()
	}
	return s.DirStore.Put(ctx, key, data)
}

func liveHeapBytes() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// TestBackupDoesNotHoldTheLog: what Create keeps live between classifying the log and uploading
// it is a few dozen bytes a commit (its hash), not the commits: the log here is ~2 KB a commit.
func TestBackupDoesNotHoldTheLog(t *testing.T) {
	shim := newTestShim(t)
	const perSegment, segments = 500, 8
	pad := strings.Repeat("p", 2_000)
	var parent *codec.Hash
	for seq := int64(1); seq <= segments; seq++ {
		batch := make([]document.Commit, 0, perSegment)
		for i := 0; i < perSegment; i++ {
			c := buildCommit(t, parent)
			c.Operations = []document.Op{document.WriteOp{DocID: c.TransactionID, Patch: fmt.Sprintf(`{"pad":%q}`, pad)}}
			h, err := document.ComputeCommitHash(c)
			if err != nil {
				t.Fatal(err)
			}
			c.Hash = h
			batch = append(batch, c)
			parent = &h
		}
		appendSegment(t, shim, seq, batch...)
	}
	n := perSegment * segments
	store := &heapProbeStore{DirStore: &DirStore{Root: t.TempDir()}}
	base := liveHeapBytes()
	m, err := Create(shim, ns, store, "")
	if err != nil {
		t.Fatal(err)
	}
	if m.CommitCount != n || len(m.HeadHashes) != 1 || m.HeadHashes["main"] != parent.Hex() {
		t.Fatalf("manifest: %d commits, heads %v", m.CommitCount, m.HeadHashes)
	}
	held := int64(store.live) - int64(base)
	var segment int64
	for _, e := range m.Segments {
		segment = max(segment, e.SizeBytes)
	}
	t.Logf("%d commits, ~%d MB of log in %d segments of %d KB: %d KB live after classifying (%d B/commit)",
		n, n*2_000>>20, segments, segment>>10, held>>10, held/int64(n))
	// A hash a commit (about 70 B in its map), plus at most a segment or two in flight - the
	// reading is taken mid-backup. Holding the log was ~2,800 B a commit here.
	if limit := int64(n)*100 + 2*segment; held > limit {
		t.Fatalf("%d KB live after classifying the log, limit %d KB: the commits are being held", held>>10, limit>>10)
	}
}
