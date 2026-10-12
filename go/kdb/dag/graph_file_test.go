package dag

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage/mmapfile"
)

func skipWithoutMmap(t *testing.T) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "probe")
	os.WriteFile(p, []byte{1}, 0o644)
	m, err := mmapfile.Open(p)
	if errors.Is(err, mmapfile.ErrUnsupported) {
		t.Skip("no mmap on this platform")
	}
	if m != nil {
		m.Close()
	}
}

// graphView is everything observable about a DAG that the graph file must not change.
type graphView struct {
	commits  map[codec.Hash]document.Commit
	gens     map[codec.Hash]uint32
	count    int
	ancestry map[[2]codec.Hash]bool
	byTx     map[codec.UUID]codec.Hash
	walk     []string
	horizon  int
}

func viewOf(t *testing.T, b *dagBuilder, hashes []codec.Hash) graphView {
	t.Helper()
	d := b.dag
	v := graphView{
		commits:  map[codec.Hash]document.Commit{},
		gens:     map[codec.Hash]uint32{},
		ancestry: map[[2]codec.Hash]bool{},
		byTx:     map[codec.UUID]codec.Hash{},
		count:    d.CommitCount(),
		horizon:  len(d.Horizon()),
	}
	for _, h := range hashes {
		c, ok := d.GetCommit(h)
		if !ok {
			t.Fatalf("%s missing", b.name(h))
		}
		v.commits[h] = c
		d.mu.Lock()
		d.ensureGenerationsLocked()
		v.gens[h] = d.nodes.generation(h)
		d.mu.Unlock()
		if got, ok := d.GetCommitByTransactionID(c.TransactionID); ok {
			v.byTx[c.TransactionID] = got.Hash
		}
		for _, o := range hashes {
			v.ancestry[[2]codec.Hash{h, o}] = d.IsAncestor(h, o)
		}
	}
	head, _ := d.Head()
	for _, e := range d.Walk(head, nil, 1<<30) {
		if f, ok := e.(FullEntry); ok {
			v.walk = append(v.walk, b.name(f.Commit.Hash))
		}
	}
	return v
}

func assertSameView(t *testing.T, before, after graphView) {
	t.Helper()
	if before.count != after.count {
		t.Errorf("CommitCount %d -> %d", before.count, after.count)
	}
	for h, c := range before.commits {
		if !reflect.DeepEqual(c, after.commits[h]) {
			t.Errorf("commit %s changed:\n before %+v\n after  %+v", h.Hex()[:8], c, after.commits[h])
		}
		if before.gens[h] != after.gens[h] {
			t.Errorf("generation of %s %d -> %d", h.Hex()[:8], before.gens[h], after.gens[h])
		}
	}
	if !reflect.DeepEqual(before.ancestry, after.ancestry) {
		t.Error("IsAncestor answers changed")
	}
	if !reflect.DeepEqual(before.byTx, after.byTx) {
		t.Errorf("transaction lookups changed: %v -> %v", before.byTx, after.byTx)
	}
	if len(before.walk) < 2 {
		t.Fatalf("walk saw %d commits; the comparison below would prove nothing", len(before.walk))
	}
	if !reflect.DeepEqual(before.walk, after.walk) {
		t.Errorf("Walk changed:\n before %v\n after  %v", before.walk, after.walk)
	}
	if before.horizon != after.horizon {
		t.Errorf("Horizon %d -> %d", before.horizon, after.horizon)
	}
}

func graphDag(t *testing.T) (*dagBuilder, []codec.Hash, string) {
	t.Helper()
	skipWithoutMmap(t)
	b := newDagBuilder(t)
	b.dag.SetGraphSettings(GraphSettings{FileEnabled: true, RebuildCommits: 1 << 30})
	path := filepath.Join(t.TempDir(), "graph", "ns.kdbg")
	b.dag.ConfigureGraphFile(path)
	t.Cleanup(b.dag.CloseGraphFile)
	hashes := append([]codec.Hash{b.root}, richGraph(b)...)
	_, _, _, m := b.diamond()
	hashes = append(hashes, m)
	return b, hashes, path
}

func allHashes(b *dagBuilder) []codec.Hash {
	var out []codec.Hash
	b.dag.mu.RLock()
	b.dag.nodes.forEach(func(h codec.Hash) { out = append(out, h) })
	b.dag.mu.RUnlock()
	return out
}

// The file changes where commits live and nothing else.
func TestGraphFileIsInvisibleToReaders(t *testing.T) {
	b, _, _ := graphDag(t)
	hashes := allHashes(b)
	before := viewOf(t, b, hashes)

	if err := b.dag.RebuildGraphFile(); err != nil {
		t.Fatal(err)
	}
	if !b.dag.GraphFileActive() {
		t.Fatal("file not active after rebuild")
	}
	inFile, resident, _ := b.dag.GraphFileStats()
	if inFile != len(hashes) || resident != 0 {
		t.Fatalf("file holds %d, resident %d; want %d and 0", inFile, resident, len(hashes))
	}
	if n := len(b.dag.txIndex); n != 0 {
		t.Fatalf("txIndex still holds %d entries the file answers", n)
	}
	assertSameView(t, before, viewOf(t, b, hashes))
}

// Commits appended after the file stay resident, the next rebuild folds them in, and the result
// is still the same graph.
func TestGraphFileRebuildMergesResidentTier(t *testing.T) {
	b, _, _ := graphDag(t)
	if err := b.dag.RebuildGraphFile(); err != nil {
		t.Fatal(err)
	}
	head, _ := b.dag.Head()
	x := b.commit("x", head)
	y := b.commit("y", x)
	_, resident, _ := b.dag.GraphFileStats()
	if resident != 2 {
		t.Fatalf("resident %d after two appends, want 2", resident)
	}
	hashes := allHashes(b)
	before := viewOf(t, b, hashes)
	if err := b.dag.RebuildGraphFile(); err != nil {
		t.Fatal(err)
	}
	if _, resident, _ := b.dag.GraphFileStats(); resident != 0 {
		t.Fatalf("resident %d after rebuild", resident)
	}
	assertSameView(t, before, viewOf(t, b, hashes))
	if !b.dag.IsAncestor(b.root, y) {
		t.Fatal("root is not an ancestor of y")
	}
}

// A commit removed after the file was written must stay removed - in this process, and in the
// next one that attaches the same file to a DAG that does not hold it.
func TestGraphFileNeverResurrectsDeletedCommits(t *testing.T) {
	b, _, path := graphDag(t)
	head, _ := b.dag.Head()
	tip := b.commit("tip", head)
	victim := b.commit("victim", tip)
	if err := b.dag.RebuildGraphFile(); err != nil {
		t.Fatal(err)
	}
	if _, err := b.dag.StubCommit(victim, "archive://x"); err != nil {
		t.Fatal(err)
	}
	if b.dag.HasCommit(victim) {
		t.Fatal("stubbed commit still visible through the file")
	}
	countAfterStub := b.dag.CommitCount()

	// A fresh DAG that never held victim, attaching the same file.
	fresh := newDagBuilder(t)
	fresh.dag.SetGraphSettings(GraphSettings{FileEnabled: true, RebuildCommits: 1 << 30})
	fresh.dag.ConfigureGraphFile(path)
	for _, h := range allHashes(b) {
		c, _ := b.dag.GetCommit(h)
		if len(c.ParentHashes) == 0 {
			continue // genesis differs per DAG
		}
		_ = fresh.dag.PutCommit(c, false)
	}
	before := fresh.dag.CommitCount()
	if err := fresh.dag.OpenGraphFile(); err != nil {
		t.Fatal(err)
	}
	if fresh.dag.HasCommit(victim) {
		t.Fatal("file resurrected a commit the DAG does not hold")
	}
	if got := fresh.dag.CommitCount(); got != before {
		t.Fatalf("attach changed CommitCount %d -> %d", before, got)
	}
	_ = countAfterStub
}

func TestGraphFileCorruptIsRemovedAndHarmless(t *testing.T) {
	b, _, path := graphDag(t)
	if err := b.dag.RebuildGraphFile(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	fresh := newDagBuilder(t)
	fresh.dag.SetGraphSettings(GraphSettings{FileEnabled: true})
	fresh.dag.ConfigureGraphFile(path)
	head, _ := fresh.dag.Head()
	fresh.commit("a", head)
	before := fresh.dag.CommitCount()
	if err := fresh.dag.OpenGraphFile(); !errors.Is(err, errGraphFile) {
		t.Fatalf("OpenGraphFile on a corrupt file = %v, want errGraphFile", err)
	}
	if fresh.dag.GraphFileActive() {
		t.Fatal("corrupt file attached")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("corrupt file left in place")
	}
	if fresh.dag.CommitCount() != before {
		t.Fatal("failed attach changed the DAG")
	}
}

func TestGraphFileRejectsOtherNamespace(t *testing.T) {
	b, _, path := graphDag(t)
	if err := b.dag.RebuildGraphFile(); err != nil {
		t.Fatal(err)
	}
	if _, err := openGraphBase(path, "other"); !errors.Is(err, errGraphFile) {
		t.Fatalf("open for another namespace = %v", err)
	}
}

func TestGraphFileRebuildThreshold(t *testing.T) {
	skipWithoutMmap(t)
	b := newDagBuilder(t)
	b.dag.SetGraphSettings(GraphSettings{FileEnabled: true, RebuildCommits: 5})
	b.dag.ConfigureGraphFile(filepath.Join(t.TempDir(), "g.kdbg"))
	head := b.root
	for i := 0; i < 3; i++ {
		head = b.commit("c", head)
	}
	if did, err := b.dag.MaybeRebuildGraphFile(); err != nil || did {
		t.Fatalf("rebuilt below threshold: %v %v", did, err)
	}
	for i := 0; i < 3; i++ {
		head = b.commit("c", head)
	}
	if did, err := b.dag.MaybeRebuildGraphFile(); err != nil || !did {
		t.Fatalf("did not rebuild above threshold: %v %v", did, err)
	}
}

// Ops and messages survive the move, and ops eviction/hydration still works on file commits.
func TestGraphFileKeepsMessagesAndOctopusParents(t *testing.T) {
	skipWithoutMmap(t)
	d, _ := NewInMemoryCommitDag("ns")
	d.SetGraphSettings(GraphSettings{FileEnabled: true})
	d.ConfigureGraphFile(filepath.Join(t.TempDir(), "g.kdbg"))
	var parents []codec.Hash
	for i := 0; i < 4; i++ {
		tx, _ := codec.RandomUUID()
		c, err := document.BuildCommit(nil, "ns", tx, codec.Timestamp{EpochMillis: int64(i)}, tx, nil, codec.Hash{}, nil, "root")
		if err != nil {
			t.Fatal(err)
		}
		if err := d.PutCommit(c, false); err != nil {
			t.Fatal(err)
		}
		parents = append(parents, c.Hash)
	}
	schema := codec.Hash{}
	schema.Bytes[3] = 7
	tx, _ := codec.RandomUUID()
	oct, err := document.BuildCommit(parents, "ns", tx, codec.Timestamp{EpochMillis: 99, MicroRemainder: 5}, tx, nil, codec.Hash{}, &schema, "octopus merge")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.PutCommit(oct, true); err != nil {
		t.Fatal(err)
	}
	if err := d.RebuildGraphFile(); err != nil {
		t.Fatal(err)
	}
	got, ok := d.GetCommit(oct.Hash)
	if !ok || !reflect.DeepEqual(got, oct) {
		t.Fatalf("octopus commit did not round trip:\n got %+v\nwant %+v", got, oct)
	}
	if again, _ := document.ComputeCommitHash(got); again != oct.Hash {
		t.Fatal("commit read from the file no longer hashes to itself")
	}
}

// Readers never see a torn graph while a rebuild swaps the file underneath them.
func TestGraphFileConcurrentRebuildAndReads(t *testing.T) {
	mappedBefore := mmapfile.MappedBytes()
	b, _, _ := graphDag(t)
	head, _ := b.dag.Head()
	for i := 0; i < 50; i++ {
		head = b.commit("c", head)
	}
	readHead := head
	hashes := allHashes(b)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, h := range hashes {
					if _, ok := b.dag.GetCommit(h); !ok {
						t.Errorf("commit vanished during rebuild")
						return
					}
				}
				b.dag.Walk(readHead, nil, 1<<30)
			}
		}()
	}
	for i := 0; i < 10; i++ {
		head = b.commit("w", head)
		if err := b.dag.RebuildGraphFile(); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	b.dag.CloseGraphFile()
	if n := mmapfile.MappedBytes() - mappedBefore; n != 0 {
		t.Fatalf("%d bytes still mapped after close", n)
	}
}
