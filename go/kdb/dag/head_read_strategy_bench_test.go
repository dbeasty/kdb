package dag

// Decision record for how InMemoryCommitDag serves the read path. Compares the candidate
// concurrency mechanisms for the read-mostly "head pointer + immutable commit store" access
// pattern that KdbServerRuntime.GetDocument hits on every read, against the shape the
// production code actually uses: Head() then GetCommit(head), with the real codec.Hash and
// document.Commit types so struct-copy costs are faithful.
//
// Variant C (RCU-style immutable snapshot behind an atomic.Pointer) is what
// InMemoryCommitDag.head and ServerEngine.latestTree implement. The others are kept
// runnable so the choice stays checkable rather than being an assertion in a comment -
// in particular variant B, which is the obvious "just cache the head hash" fix and which
// these numbers show recovers only about a third of the available win, because the second
// RLock in GetCommit is just as expensive as the first.
//
// Numbers and analysis: docs/benchmarks/workload-matrix.md, Finding 2.
//
//	go test ./kdb/dag/ -run '^$' -bench BenchmarkShootout -benchtime 1s

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// headReader is the operation under test: resolve the current head and its commit,
// returning the document tree hash (what GetDocument actually needs).
type headReader interface {
	Read(shard int) codec.Hash
	Advance(c document.Commit)
}

func makeCommit(i int) document.Commit {
	var h, tree codec.Hash
	h.Bytes[0] = byte(i)
	h.Bytes[1] = byte(i >> 8)
	tree.Bytes[0] = byte(i + 1)
	return document.Commit{
		Hash:             h,
		ParentHashes:     []codec.Hash{{}},
		NamespaceID:      "bench/shootout",
		DocumentTreeHash: tree,
		Message:          "shootout",
	}
}

// ---------------------------------------------------------------------------
// A. Baseline: single shared RWMutex over maps (what InMemoryCommitDag does today).
// Reader does 2x (RLock+RUnlock) = 4 atomic RMWs on ONE shared cache line.
// ---------------------------------------------------------------------------

type variantBaseline struct {
	mu      sync.RWMutex
	commits map[codec.Hash]document.Commit
	head    codec.Hash
}

func newBaseline(seed document.Commit) *variantBaseline {
	v := &variantBaseline{commits: map[codec.Hash]document.Commit{seed.Hash: seed}, head: seed.Hash}
	return v
}

func (v *variantBaseline) Read(int) codec.Hash {
	v.mu.RLock()
	h := v.head
	v.mu.RUnlock()

	v.mu.RLock()
	c := v.commits[h]
	v.mu.RUnlock()
	return c.DocumentTreeHash
}

func (v *variantBaseline) Advance(c document.Commit) {
	v.mu.Lock()
	v.commits[c.Hash] = c
	v.head = c.Hash
	v.mu.Unlock()
}

// ---------------------------------------------------------------------------
// B. Atomic head hash only (the "cache the head behind an atomic pointer" proposal
// as literally stated). Head() is lock-free, but GetCommit still takes the RWMutex,
// so the reader still does 2 atomic RMWs on the shared cache line.
// ---------------------------------------------------------------------------

type variantAtomicHead struct {
	head    atomic.Pointer[codec.Hash]
	mu      sync.RWMutex
	commits map[codec.Hash]document.Commit
}

func newAtomicHead(seed document.Commit) *variantAtomicHead {
	v := &variantAtomicHead{commits: map[codec.Hash]document.Commit{seed.Hash: seed}}
	h := seed.Hash
	v.head.Store(&h)
	return v
}

func (v *variantAtomicHead) Read(int) codec.Hash {
	h := *v.head.Load()
	v.mu.RLock()
	c := v.commits[h]
	v.mu.RUnlock()
	return c.DocumentTreeHash
}

func (v *variantAtomicHead) Advance(c document.Commit) {
	v.mu.Lock()
	v.commits[c.Hash] = c
	v.mu.Unlock()
	h := c.Hash
	v.head.Store(&h)
}

// ---------------------------------------------------------------------------
// C. RCU-style immutable snapshot publication (RocksDB SuperVersion / LMDB meta-page
// shape). Reader does ONE atomic pointer load and ZERO writes to shared memory, and
// skips the map lookup and the Commit struct copy entirely.
// ---------------------------------------------------------------------------

type headSnap struct {
	hash   codec.Hash
	commit document.Commit
}

type variantSnapshot struct {
	snap    atomic.Pointer[headSnap]
	mu      sync.RWMutex
	commits map[codec.Hash]document.Commit
}

func newSnapshot(seed document.Commit) *variantSnapshot {
	v := &variantSnapshot{commits: map[codec.Hash]document.Commit{seed.Hash: seed}}
	v.snap.Store(&headSnap{hash: seed.Hash, commit: seed})
	return v
}

func (v *variantSnapshot) Read(int) codec.Hash {
	s := v.snap.Load()
	return s.commit.DocumentTreeHash
}

func (v *variantSnapshot) Advance(c document.Commit) {
	v.mu.Lock()
	v.commits[c.Hash] = c
	v.mu.Unlock()
	v.snap.Store(&headSnap{hash: c.Hash, commit: c})
}

// D. Seqlock (Linux timekeeping shape): reader reads a sequence counter, reads the data,
// re-reads the counter, retries if it moved - no reader-side writes at all.
//
// DELIBERATELY NOT IMPLEMENTED HERE. A seqlock reads the protected data while a writer may
// be mutating it, which is a data race by Go's memory model no matter how the retry loop is
// written; the race detector flags every read, and the compiler is entitled to miscompile
// it. For a payload containing pointers - document.Commit has ParentHashes, Operations and
// two strings - it is worse than stale: a torn slice header (pointer from one version,
// length from another) is memory-unsafe. A prototype restricted to codec.Hash ([32]byte,
// pointer-free) measured 7.65M ops/sec parallel, below both E and F, and `go test -race`
// reported the expected DATA RACE between the reader's `h = v.head` and the writer's
// `v.head = c.Hash`. Ruled out on soundness, not just on speed; see workload-matrix.md.
//
// ---------------------------------------------------------------------------
// E. Sharded / striped RWMutex (drwmutex, Linux percpu-rwsem, RocksDB CoreLocalArray).
// Each reader locks only its own shard, so reader atomics land on different cache lines.
// Writers must take every shard: writer cost becomes O(shards).
//
// Shard index here is per-goroutine (passed in), which is an OPTIMISTIC bound - a real
// implementation shards by P via runtime.procPin and pays for P migration.
// ---------------------------------------------------------------------------

type paddedRW struct {
	mu sync.RWMutex
	_  [64 - 24%64]byte // keep each mutex on its own cache line
}

type variantSharded struct {
	shards  []paddedRW
	commits map[codec.Hash]document.Commit
	head    codec.Hash
}

func newSharded(seed document.Commit) *variantSharded {
	return &variantSharded{
		shards:  make([]paddedRW, runtime.GOMAXPROCS(0)),
		commits: map[codec.Hash]document.Commit{seed.Hash: seed},
		head:    seed.Hash,
	}
}

func (v *variantSharded) Read(shard int) codec.Hash {
	s := &v.shards[shard%len(v.shards)]
	s.mu.RLock()
	h := v.head
	c := v.commits[h]
	s.mu.RUnlock()
	return c.DocumentTreeHash
}

func (v *variantSharded) Advance(c document.Commit) {
	for i := range v.shards {
		v.shards[i].mu.Lock()
	}
	v.commits[c.Hash] = c
	v.head = c.Hash
	for i := len(v.shards) - 1; i >= 0; i-- {
		v.shards[i].mu.Unlock()
	}
}

// ---------------------------------------------------------------------------
// F. atomic head + sync.Map for commits (Go stdlib's own read-mostly structure:
// atomic.Pointer to a read-only map, with an amended dirty map behind a Mutex).
// ---------------------------------------------------------------------------

type variantSyncMap struct {
	head    atomic.Pointer[codec.Hash]
	commits sync.Map
}

func newSyncMap(seed document.Commit) *variantSyncMap {
	v := &variantSyncMap{}
	v.commits.Store(seed.Hash, seed)
	h := seed.Hash
	v.head.Store(&h)
	return v
}

func (v *variantSyncMap) Read(int) codec.Hash {
	h := *v.head.Load()
	raw, _ := v.commits.Load(h)
	c, _ := raw.(document.Commit)
	return c.DocumentTreeHash
}

func (v *variantSyncMap) Advance(c document.Commit) {
	v.commits.Store(c.Hash, c)
	h := c.Hash
	v.head.Store(&h)
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

var shootoutVariants = []struct {
	name string
	make func(document.Commit) headReader
}{
	{"A-baseline-rwmutex", func(c document.Commit) headReader { return newBaseline(c) }},
	{"B-atomic-head-only", func(c document.Commit) headReader { return newAtomicHead(c) }},
	{"C-rcu-snapshot", func(c document.Commit) headReader { return newSnapshot(c) }},
	{"E-sharded-rwmutex", func(c document.Commit) headReader { return newSharded(c) }},
	{"F-syncmap", func(c document.Commit) headReader { return newSyncMap(c) }},
	{"G-mvcc-snapshot", func(c document.Commit) headReader { return newMVCC(c) }},
}

var shootoutSink atomic.Uint64

// BenchmarkShootoutReadSingle: one goroutine, no concurrency - the ceiling each
// mechanism can reach with zero contention.
func BenchmarkShootoutReadSingle(b *testing.B) {
	for _, v := range shootoutVariants {
		b.Run(v.name, func(b *testing.B) {
			r := v.make(makeCommit(1))
			b.ReportAllocs()
			b.ResetTimer()
			var acc uint64
			for i := 0; i < b.N; i++ {
				acc += uint64(r.Read(0).Bytes[0])
			}
			b.StopTimer()
			shootoutSink.Add(acc)
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
		})
	}
}

// BenchmarkShootoutReadParallel: the real workload shape - RunParallel at the same
// parallelism 64 the workload matrix uses (64*GOMAXPROCS goroutines).
func BenchmarkShootoutReadParallel(b *testing.B) {
	for _, v := range shootoutVariants {
		b.Run(v.name, func(b *testing.B) {
			r := v.make(makeCommit(1))
			var seq atomic.Int64
			b.ReportAllocs()
			b.SetParallelism(64)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				shard := int(seq.Add(1) - 1)
				var acc uint64
				for pb.Next() {
					acc += uint64(r.Read(shard).Bytes[0])
				}
				shootoutSink.Add(acc)
			})
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
		})
	}
}

// ---------------------------------------------------------------------------
// G. MVCC snapshot handle. Distinct from RCU (C) in WHEN the version is resolved, not in
// how it is published: RCU resolves the latest version on EVERY operation, so each read is
// independently fresh but nothing ties two reads together. MVCC hands the caller a pinned
// version it holds ACROSS operations - so N reads cost one acquisition instead of N, and
// every read in the group sees the same instant (repeatable read / snapshot isolation).
//
// The two compose rather than compete: this is exactly LMDB's shape, where a lock-free
// atomically-published meta page (RCU) is what a read transaction pins (MVCC). The cost is
// that a pinned snapshot is deliberately stale - a read through it will not observe a write
// committed after acquisition, which breaks read-your-writes for a caller that expects
// GetDocument to see its own preceding Commit.
// ---------------------------------------------------------------------------

type variantMVCC struct {
	snap    atomic.Pointer[headSnap]
	mu      sync.RWMutex
	commits map[codec.Hash]document.Commit
}

func newMVCC(seed document.Commit) *variantMVCC {
	v := &variantMVCC{commits: map[codec.Hash]document.Commit{seed.Hash: seed}}
	v.snap.Store(&headSnap{hash: seed.Hash, commit: seed})
	return v
}

// Acquire pins a version. One atomic load, amortized over every read that follows.
func (v *variantMVCC) Acquire() *headSnap { return v.snap.Load() }

// ReadPinned is the per-operation cost once a version is pinned: no atomics at all.
func (v *variantMVCC) ReadPinned(s *headSnap) codec.Hash { return s.commit.DocumentTreeHash }

func (v *variantMVCC) Read(int) codec.Hash { return v.ReadPinned(v.Acquire()) }

func (v *variantMVCC) Advance(c document.Commit) {
	v.mu.Lock()
	v.commits[c.Hash] = c
	v.mu.Unlock()
	v.snap.Store(&headSnap{hash: c.Hash, commit: c})
}

// BenchmarkShootoutSnapshotAmortization isolates the one axis on which MVCC differs from
// RCU: how many reads share a single version acquisition. At 1 op per snapshot the two are
// the same thing; the gap that opens as the group grows is exactly what a pinned read
// transaction buys, and what it costs in staleness.
func BenchmarkShootoutSnapshotAmortization(b *testing.B) {
	for _, opsPerSnapshot := range []int{1, 8, 64} {
		rcu := newSnapshot(makeCommit(1))
		b.Run(fmt.Sprintf("rcu-load-per-op/ops=%d", opsPerSnapshot), func(b *testing.B) {
			b.SetParallelism(64)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var acc uint64
				for pb.Next() {
					// Fresh atomic load for every operation.
					for i := 0; i < opsPerSnapshot; i++ {
						acc += uint64(rcu.Read(0).Bytes[0])
					}
				}
				shootoutSink.Add(acc)
			})
			b.StopTimer()
			b.ReportMetric(float64(b.N*opsPerSnapshot)/b.Elapsed().Seconds(), "reads/sec")
		})

		mv := newMVCC(makeCommit(1))
		b.Run(fmt.Sprintf("mvcc-pinned-snapshot/ops=%d", opsPerSnapshot), func(b *testing.B) {
			b.SetParallelism(64)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var acc uint64
				for pb.Next() {
					// One acquisition, then opsPerSnapshot reads through it.
					s := mv.Acquire()
					for i := 0; i < opsPerSnapshot; i++ {
						acc += uint64(mv.ReadPinned(s).Bytes[0])
					}
				}
				shootoutSink.Add(acc)
			})
			b.StopTimer()
			b.ReportMetric(float64(b.N*opsPerSnapshot)/b.Elapsed().Seconds(), "reads/sec")
		})
	}
}

// BenchmarkShootoutReadParallelWithWriter: same, but with one writer advancing the head
// continuously. Exposes the mechanisms whose reads are cheap only while nothing writes
// (seqlock retries, sharded-RWMutex writer starvation).
func BenchmarkShootoutReadParallelWithWriter(b *testing.B) {
	for _, v := range shootoutVariants {
		b.Run(v.name, func(b *testing.B) {
			r := v.make(makeCommit(1))
			stop := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 2; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					r.Advance(makeCommit(i))
				}
			}()

			var seq atomic.Int64
			b.ReportAllocs()
			b.SetParallelism(64)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				shard := int(seq.Add(1) - 1)
				var acc uint64
				for pb.Next() {
					acc += uint64(r.Read(shard).Bytes[0])
				}
				shootoutSink.Add(acc)
			})
			b.StopTimer()
			close(stop)
			wg.Wait()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
		})
	}
}
