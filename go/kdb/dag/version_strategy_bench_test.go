package dag

// Full-picture comparison of the three ways to resolve "which version am I reading" over a
// versioned document store: the original shared RWMutex, the RCU snapshot that shipped, and
// an MVCC pinned-snapshot handle. Unlike head_read_strategy_bench_test.go, which covers only
// the point-read path, this suite exercises reads, writes, transactions and a mixed workload,
// so the write side of the trade is visible too.
//
// All three strategies do exactly the same real work per operation - the same persistent-trie
// tree update on commit, the same map retention of every historical tree, the same document
// lookups - so any difference is attributable to the version-resolution mechanism alone.
//
// The transaction shape is modelled on transaction/default_engine.go's detectConflicts, which
// is the reason this suite exists. For every operation in a transaction it does two lookups:
//
//	baseDoc,     _ := store.GetDocument(ns, docID, baseTreeHash)    // the tx's BASE version
//	existingDoc, _ := store.GetDocument(ns, docID, targetTreeHash)  // the CURRENT head
//
// The base-version lookup is the interesting one. It names an older tree, so it misses the
// "is this the newest tree" fast path that the shipped RCU snapshot provides and falls back to
// the mutex-guarded map - once per operation. Pinning the base tree for the life of the
// transaction is exactly what MVCC buys, and this is where it should show up.
//
//	go test ./kdb/dag/ -run '^$' -bench BenchmarkVersionStrategy -benchtime 1s

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// docWrite is one staged document write, as a transaction would carry.
type docWrite struct {
	id   codec.UUID
	hash codec.Hash
}

// txView is a transaction's handle on the versions it reads. GetAtBase resolves against the
// version the transaction started from; GetAtHead against the newest committed version.
type txView interface {
	GetAtBase(docID codec.UUID) (codec.Hash, bool)
	GetAtHead(docID codec.UUID) (codec.Hash, bool)
}

// versionStore is the mechanism under test.
type versionStore interface {
	Get(docID codec.UUID) (codec.Hash, bool) // point read at the newest version
	Begin() txView                           // open a transaction view
	Commit(writes []docWrite)                // publish a new version
}

// ---------------------------------------------------------------------------
// Shared core: the parts every strategy has in common, so the comparison isolates the
// version-resolution mechanism rather than accidentally measuring different data structures.
// ---------------------------------------------------------------------------

type storeCore struct {
	mu    sync.RWMutex
	trees map[codec.Hash]document.DocumentTree // every version ever committed
	tree  document.DocumentTree                // newest
}

func newStoreCore() storeCore {
	empty := document.EmptyDocumentTree()
	return storeCore{
		trees: map[codec.Hash]document.DocumentTree{empty.TreeHash: empty},
		tree:  empty,
	}
}

// applyLocked performs the real commit work: a persistent-trie update per write, then
// retention of the resulting tree. Identical across all three strategies.
func (c *storeCore) applyLocked(writes []docWrite) document.DocumentTree {
	tree := c.tree
	for _, w := range writes {
		tree, _ = tree.With(w.id, w.hash)
	}
	c.trees[tree.TreeHash] = tree
	c.tree = tree
	return tree
}

// ---------------------------------------------------------------------------
// Strategy 1 — BASELINE: one shared RWMutex over the maps. What the code did before.
// Every version resolution, current or historical, takes RLock.
// ---------------------------------------------------------------------------

type baselineStore struct{ storeCore }

func newBaselineStore() *baselineStore { return &baselineStore{newStoreCore()} }

func (s *baselineStore) Get(docID codec.UUID) (codec.Hash, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tree.HashFor(docID)
}

func (s *baselineStore) Begin() txView {
	s.mu.RLock()
	base := s.tree.TreeHash
	s.mu.RUnlock()
	return &baselineView{s: s, base: base}
}

func (s *baselineStore) Commit(writes []docWrite) {
	s.mu.Lock()
	s.applyLocked(writes)
	s.mu.Unlock()
}

type baselineView struct {
	s    *baselineStore
	base codec.Hash
}

func (v *baselineView) GetAtBase(docID codec.UUID) (codec.Hash, bool) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	t, ok := v.s.trees[v.base]
	if !ok {
		return codec.Hash{}, false
	}
	return t.HashFor(docID)
}

func (v *baselineView) GetAtHead(docID codec.UUID) (codec.Hash, bool) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	return v.s.tree.HashFor(docID)
}

// ---------------------------------------------------------------------------
// Strategy 2 — RCU (what shipped): the newest tree is republished behind an atomic.Pointer,
// so reads of the CURRENT version are lock-free. Historical versions - which is what a
// transaction's base is - still fall back to the mutex-guarded map, once per lookup.
// ---------------------------------------------------------------------------

type rcuSnap struct {
	hash codec.Hash
	tree document.DocumentTree
}

type rcuStore struct {
	latest atomic.Pointer[rcuSnap]
	_      [128 - 8]byte
	storeCore
}

func newRCUStore() *rcuStore {
	s := &rcuStore{storeCore: newStoreCore()}
	s.latest.Store(&rcuSnap{hash: s.tree.TreeHash, tree: s.tree})
	return s
}

// treeAt mirrors ServerEngine.treeAt: lock-free for the newest version, locked otherwise.
func (s *rcuStore) treeAt(h codec.Hash) (document.DocumentTree, bool) {
	if snap := s.latest.Load(); snap != nil && snap.hash == h {
		return snap.tree, true
	}
	s.mu.RLock()
	t, ok := s.trees[h]
	s.mu.RUnlock()
	return t, ok
}

func (s *rcuStore) Get(docID codec.UUID) (codec.Hash, bool) {
	return s.latest.Load().tree.HashFor(docID)
}

func (s *rcuStore) Begin() txView {
	return &rcuView{s: s, base: s.latest.Load().hash}
}

func (s *rcuStore) Commit(writes []docWrite) {
	s.mu.Lock()
	tree := s.applyLocked(writes)
	s.latest.Store(&rcuSnap{hash: tree.TreeHash, tree: tree})
	s.mu.Unlock()
}

type rcuView struct {
	s    *rcuStore
	base codec.Hash
}

// GetAtBase re-resolves the base tree on EVERY call. Once a concurrent writer has advanced
// the head, base is no longer the newest version, so this misses the atomic fast path and
// takes the mutex - which is the cost MVCC removes.
func (v *rcuView) GetAtBase(docID codec.UUID) (codec.Hash, bool) {
	t, ok := v.s.treeAt(v.base)
	if !ok {
		return codec.Hash{}, false
	}
	return t.HashFor(docID)
}

func (v *rcuView) GetAtHead(docID codec.UUID) (codec.Hash, bool) {
	return v.s.latest.Load().tree.HashFor(docID)
}

// ---------------------------------------------------------------------------
// Strategy 3 — MVCC: Begin() resolves BOTH versions once and pins them for the life of the
// transaction. Every subsequent lookup is a pure trie walk with no atomics and no locks, and
// all reads in the transaction see one consistent instant (repeatable read).
//
// The cost is staleness by construction: a pinned view cannot observe a write committed after
// it was opened, so this is only correct where the caller wants a fixed snapshot - which a
// transaction's base version does, and a bare point read does not.
// ---------------------------------------------------------------------------

type mvccStore struct {
	latest atomic.Pointer[rcuSnap]
	_      [128 - 8]byte
	storeCore
}

func newMVCCStore() *mvccStore {
	s := &mvccStore{storeCore: newStoreCore()}
	s.latest.Store(&rcuSnap{hash: s.tree.TreeHash, tree: s.tree})
	return s
}

func (s *mvccStore) Get(docID codec.UUID) (codec.Hash, bool) {
	return s.latest.Load().tree.HashFor(docID)
}

// Begin pins the BASE version once, and only the base. The head deliberately stays live:
// conflict detection has to observe writes committed after this transaction started, or it
// would not detect the conflicts it exists to detect. So MVCC's advantage over RCU here is
// exactly one thing - the base lookup stops re-resolving a historical tree through the mutex
// on every operation - and nothing else.
func (s *mvccStore) Begin() txView {
	return &mvccView{s: s, base: s.latest.Load().tree, ok: true}
}

func (s *mvccStore) Commit(writes []docWrite) {
	s.mu.Lock()
	tree := s.applyLocked(writes)
	s.latest.Store(&rcuSnap{hash: tree.TreeHash, tree: tree})
	s.mu.Unlock()
}

type mvccView struct {
	s    *mvccStore
	base document.DocumentTree
	ok   bool
}

// GetAtBase reads the pinned base: a pure trie walk, no atomic and no lock, however many
// operations the transaction has.
func (v *mvccView) GetAtBase(docID codec.UUID) (codec.Hash, bool) {
	if !v.ok {
		return codec.Hash{}, false
	}
	return v.base.HashFor(docID)
}

// GetAtHead reads the live head, same as RCU - see Begin.
func (v *mvccView) GetAtHead(docID codec.UUID) (codec.Hash, bool) {
	return v.s.latest.Load().tree.HashFor(docID)
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

var versionStrategies = []struct {
	name string
	make func() versionStore
}{
	{"baseline-rwmutex", func() versionStore { return newBaselineStore() }},
	{"rcu-snapshot", func() versionStore { return newRCUStore() }},
	{"mvcc-pinned", func() versionStore { return newMVCCStore() }},
}

const versionPoolSize = 4096

func seedVersionStore(b *testing.B, s versionStore) []codec.UUID {
	b.Helper()
	ids := make([]codec.UUID, versionPoolSize)
	batch := make([]docWrite, 0, 64)
	for i := 0; i < versionPoolSize; i++ {
		id, err := codec.RandomUUID()
		if err != nil {
			b.Fatal(err)
		}
		ids[i] = id
		var h codec.Hash
		h.Bytes[0], h.Bytes[1] = byte(i), byte(i>>8)
		batch = append(batch, docWrite{id: id, hash: h})
		if len(batch) == cap(batch) {
			s.Commit(batch)
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		s.Commit(batch)
	}
	return ids
}

var versionSink atomic.Uint64

// BenchmarkVersionStrategyRead: the point-read path, for reference against the transaction
// numbers below. MVCC has no advantage here - one read cannot amortize its own acquisition.
func BenchmarkVersionStrategyRead(b *testing.B) {
	for _, st := range versionStrategies {
		s := st.make()
		ids := seedVersionStore(b, s)

		b.Run(st.name+"/single-user", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			var acc uint64
			for i := 0; i < b.N; i++ {
				h, _ := s.Get(ids[i%len(ids)])
				acc += uint64(h.Bytes[0])
			}
			b.StopTimer()
			versionSink.Add(acc)
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
		})
		b.Run(st.name+"/heavy-multi-user", func(b *testing.B) {
			b.ReportAllocs()
			b.SetParallelism(64)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var acc uint64
				i := 0
				for pb.Next() {
					h, _ := s.Get(ids[i%len(ids)])
					acc += uint64(h.Bytes[0])
					i++
				}
				versionSink.Add(acc)
			})
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
		})
	}
}

// BenchmarkVersionStrategyWrite: pure commit throughput - what publishing a snapshot costs
// the writer. RCU and MVCC each allocate one snapshot object and do one atomic store per
// commit that the baseline does not; this is where that shows up.
func BenchmarkVersionStrategyWrite(b *testing.B) {
	for _, st := range versionStrategies {
		b.Run(st.name+"/single-user", func(b *testing.B) {
			s := st.make()
			ids := seedVersionStore(b, s)
			w := make([]docWrite, 1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w[0] = docWrite{id: ids[i%len(ids)], hash: hashOf(i)}
				s.Commit(w)
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
		})
		b.Run(st.name+"/heavy-multi-user", func(b *testing.B) {
			s := st.make()
			ids := seedVersionStore(b, s)
			b.ReportAllocs()
			b.SetParallelism(64)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				w := make([]docWrite, 1)
				i := 0
				for pb.Next() {
					w[0] = docWrite{id: ids[i%len(ids)], hash: hashOf(i)}
					s.Commit(w)
					i++
				}
			})
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
		})
	}
}

// BenchmarkVersionStrategyTransaction is the case this suite was built for: a transaction of
// opsPerTx operations, each doing detectConflicts' two lookups (base version + current head),
// followed by the commit. Run with concurrent committers, so the head genuinely moves and a
// transaction's base is genuinely historical - which is what makes RCU's fast path miss.
func BenchmarkVersionStrategyTransaction(b *testing.B) {
	for _, opsPerTx := range []int{1, 4, 16} {
		for _, st := range versionStrategies {
			b.Run(fmt.Sprintf("%s/ops=%d/heavy-multi-user", st.name, opsPerTx), func(b *testing.B) {
				s := st.make()
				ids := seedVersionStore(b, s)
				b.ReportAllocs()
				b.SetParallelism(64)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					w := make([]docWrite, opsPerTx)
					var acc uint64
					i := 0
					for pb.Next() {
						view := s.Begin()
						for k := 0; k < opsPerTx; k++ {
							id := ids[(i+k)%len(ids)]
							bh, _ := view.GetAtBase(id) // historical version
							hh, _ := view.GetAtHead(id) // current version
							acc += uint64(bh.Bytes[0]) + uint64(hh.Bytes[0])
							w[k] = docWrite{id: id, hash: hashOf(i + k)}
						}
						s.Commit(w)
						i += opsPerTx
					}
					versionSink.Add(acc)
				})
				b.StopTimer()
				b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "txn/sec")
				b.ReportMetric(float64(b.N*opsPerTx)/b.Elapsed().Seconds(), "ops/sec")
			})
		}
	}
}

// BenchmarkVersionStrategyMixed: 80% point reads, 20% transactions - the shape a real server
// sees, and the one that decides whether a mechanism is worth adopting overall.
func BenchmarkVersionStrategyMixed(b *testing.B) {
	const opsPerTx = 4
	for _, st := range versionStrategies {
		b.Run(st.name+"/heavy-multi-user", func(b *testing.B) {
			s := st.make()
			ids := seedVersionStore(b, s)
			b.ReportAllocs()
			b.SetParallelism(64)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				w := make([]docWrite, opsPerTx)
				var acc uint64
				i := 0
				for pb.Next() {
					if i%5 == 4 {
						view := s.Begin()
						for k := 0; k < opsPerTx; k++ {
							id := ids[(i+k)%len(ids)]
							bh, _ := view.GetAtBase(id)
							hh, _ := view.GetAtHead(id)
							acc += uint64(bh.Bytes[0]) + uint64(hh.Bytes[0])
							w[k] = docWrite{id: id, hash: hashOf(i + k)}
						}
						s.Commit(w)
					} else {
						h, _ := s.Get(ids[i%len(ids)])
						acc += uint64(h.Bytes[0])
					}
					i++
				}
				versionSink.Add(acc)
			})
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
		})
	}
}

func hashOf(i int) codec.Hash {
	var h codec.Hash
	h.Bytes[0], h.Bytes[1], h.Bytes[2] = byte(i), byte(i>>8), byte(i>>16)
	return h
}
