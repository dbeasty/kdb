package engine

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/metrics"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
	"github.com/limidus/kdb/go/kdb/storage/memtable"
	"github.com/limidus/kdb/go/kdb/storage/sstable"
	"github.com/limidus/kdb/go/kdb/storage/wal"
)

// ServerEngine is the server-side LSM storage engine with optional WAL.
type ServerEngine struct {
	namespaceID string
	config      storage.StorageEngineConfig
	wal         wal.WriteAheadLog
	groupCommit *wal.GroupCommitter

	// docsByHash holds every document version ever committed, keyed by
	// content hash (see doc_hash_shard.go) rather than guarded by one
	// namespace-wide mutex. PutDocument/DeleteDocument stage into pending
	// instead of writing here directly: writes are not visible via
	// GetDocument until CommitTree flushes them, matching the
	// pre-existing InMemoryStorageAdapter contract (see the "reintroduce
	// staging" note below) and letting a failed transaction's write phase
	// be rolled back via DiscardPending without ever having mutated
	// committed state. It is intentionally separate from the blob write
	// path (WriteBlob): WAL.Append and memTable.Put are each
	// independently thread-safe, so blob writes never take a lock at all
	// - see Phase 1/2 of docs/benchmarks/phase0-baseline.md.
	cap        storage.CapabilitySet
	memTable   *memtable.Manager
	docsByHash *shardedDocByHashStore
	// coldLoader re-reads a document version that docsByHash has evicted,
	// from wherever it is durable. Nil means nothing can serve an evicted
	// version, and docsByHash is left unbounded - see SetColdLoader.
	// Read on the miss path only, so an atomic load rather than a lock.
	coldLoader atomic.Pointer[coldDocLoader]
	// coldLoads counts versions re-read from durable storage because the
	// version store had evicted them - from the object store or, failing
	// that, the delta log; both are the same event from a caller's point of
	// view. A number that climbs steadily under ordinary reads means the
	// budget is too small for the live working set, which is the one way
	// this design degrades quietly rather than loudly - so it is worth
	// being able to see.
	coldLoads atomic.Int64

	// Historical document trees are not restored by a checkpoint; they are
	// rebuilt from the delta log on the first read that needs one. See
	// SetTreeRebuilder.
	treeRebuildMu   sync.Mutex
	treeRebuild     treeRebuilder
	treeRebuildOnce *sync.Once
	treeRebuildErr  error

	// treeChain remembers how many delta tree objects stand behind each
	// tree, so putTreeObject knows when to write a full one instead. Lost
	// on restart, which only means the first tree written in a new process
	// starts a fresh chain - never a correctness issue, since resolution
	// stops at the first full object it finds.
	treeChainMu sync.Mutex
	treeChain   map[codec.Hash]int
	// replaying is set while this engine is being rebuilt from the delta
	// log rather than taking new writes - see SetReplaying.
	replaying        atomic.Bool
	pending          *shardedPendingStore
	enlistmentStates map[codec.UUID]storage.EnlistmentEvictionState

	// treeMu guards tree, a running DocumentTree updated incrementally
	// (O(delta) via its persistent trie - see
	// document/document_tree_trie.go) as CommitTree flushes staged
	// writes, instead of being rebuilt from a full docs snapshot each
	// time. Reintroducing staging (this commit) put PutDocument/
	// DeleteDocument's visibility back behind CommitTree - an earlier
	// pass had them update docs+tree immediately, which was faster but
	// silently dropped the "not visible until commit" guarantee
	// transactions depend on for write-phase rollback. The O(delta) tree
	// update from that pass is preserved; only the "when" changed.
	treeMu sync.Mutex
	tree   document.DocumentTree

	// treesMu orders the pair of writes below - the tree store and the
	// published snapshot - so a reader can never see a latestTree that the
	// store has not been told about. The store has its own lock for its own
	// contents; this one exists purely for that pairing.
	//
	// Historical note, still worth keeping: treesByHash holds the trees
	// produced, keyed by TreeHash - mirrors InMemoryStorageAdapter.trees.
	// Without this, GetDocument/ScanDocuments had no way to resolve a
	// specific atCommit and silently always answered from current state
	// (tree, not treesMu/treesByHash), which made ConflictPolicyStrict's
	// base-vs-head comparison always compare identical content and never
	// detect a real concurrent conflict. Kept as its own RWMutex rather
	// than reusing treeMu: treeMu only ever needs to be held by the single
	// in-flight CommitTree writer, while treesByHash is read on every
	// GetDocument/ScanDocuments call and must not serialize reads behind
	// that writer lock.
	treesMu     sync.RWMutex
	treesByHash *boundedTreeStore
	// latestTree republishes the most recent entry of treesByHash behind an atomic pointer.
	// Reads overwhelmingly ask for the current head's tree, and taking treesMu for that was
	// two atomic read-modify-writes on one shared cache line per read - the same contention
	// the DAG's head snapshot removes one layer up (see InMemoryCommitDag.head), and after
	// that fix landed this was the single largest remaining cost in a 1024-reader profile.
	// A DocumentTree is a persistent trie, immutable once published, so handing the same
	// value to any number of concurrent readers is safe.
	//
	// INVARIANT: published together with every write to treesByHash, under treesMu.
	latestTree atomic.Pointer[treeSnapshot]
	_          [128 - 8]byte // keep it off treesMu's cache line

	asyncStop chan struct{}
	asyncDone chan struct{}
}

// treeSnapshot is an immutable pairing of a tree hash with its tree, published whole.
type treeSnapshot struct {
	hash codec.Hash
	tree document.DocumentTree
}

// treeAt resolves atCommit to its DocumentTree, answering the common case - the most
// recently committed tree - without touching treesMu at all.
// treeAt resolves the document tree named by atCommit. A miss triggers the
// historical-tree rebuild (see SetTreeRebuilder) once, because after a
// checkpoint restore the only trees resident are the live one and whatever
// the delta tail produced - every older tree is derivable from the log but
// not yet built.
func (e *ServerEngine) treeAt(atCommit codec.Hash) (document.DocumentTree, bool, error) {
	if s := e.latestTree.Load(); s != nil && s.hash == atCommit {
		return s.tree, true, nil
	}
	if tree, ok := e.treesByHash.Get(atCommit); ok {
		return tree, true, nil
	}
	// Addressed lookup first: the tree object store answers directly from
	// this tree's hash. Only if it cannot - a store written under the
	// replay strategy, or one whose objects have not survived - is the
	// tree folded back out of the log.
	if tree, ok := e.treeFromObjects(atCommit); ok {
		e.treesByHash.Put(tree)
		return tree, true, nil
	}
	tree, found, err := e.rebuildTree(atCommit)
	if err != nil || !found {
		return document.DocumentTree{}, false, err
	}
	e.treesByHash.Put(tree)
	return tree, true, nil
}

// publishTreeLocked records tree under its hash and republishes it as the latest. Must be
// called with treesMu held exclusively.
func (e *ServerEngine) publishTreeLocked(tree document.DocumentTree) {
	e.treesByHash.Put(tree)
	e.latestTree.Store(&treeSnapshot{hash: tree.TreeHash, tree: tree})
}

// NewServerEngine constructs a server engine; wal may be nil for in-memory targets.
func NewServerEngine(namespaceID string, config storage.StorageEngineConfig, w wal.WriteAheadLog) *ServerEngine {
	cache := sstable.NewBlockCache(config.ResolvedGlobalMemoryBudgetBytes() / 4)
	blobStore := sstable.NewLsmBlobStore(config.IOShim, namespaceID, cache)
	if config.IOShim != nil {
		// Pick up tables written by previous runs. Without this the store
		// starts empty every process and everything ever flushed - tree
		// objects included - is invisible after a restart.
		_ = blobStore.DiscoverTables()
	}
	cap := storage.CapabilitySet{
		PersistsDeltaLog:          true,
		PersistsAcrossReload:      w != nil,
		SupportsGpuBulkRead:       false,
		SupportsDirectDeltaIngest: false,
		IndexRetentionDefault:     storage.IndexRetentionEvictable,
	}
	emptyTree := document.EmptyDocumentTree()
	e := &ServerEngine{
		namespaceID:      namespaceID,
		config:           config,
		wal:              w,
		groupCommit:      wal.NewGroupCommitter(),
		cap:              cap,
		memTable:         memtable.NewManager(namespaceID, config.IOShim, blobStore),
		docsByHash:       newShardedDocByHashStore(0),
		pending:          newShardedPendingStore(),
		enlistmentStates: make(map[codec.UUID]storage.EnlistmentEvictionState),
		tree:             emptyTree,
		treesByHash:      newBoundedTreeStore(storage.ResolvedHistoryTreeBytes(config)),
	}
	e.latestTree.Store(&treeSnapshot{hash: emptyTree.TreeHash, tree: emptyTree})
	if w != nil && config.Durability == storage.DurabilityAsync {
		e.startAsyncSync()
	}
	return e
}

func (e *ServerEngine) startAsyncSync() {
	interval := time.Duration(e.config.AsyncSyncIntervalMillis) * time.Millisecond
	if interval <= 0 {
		interval = 5 * time.Millisecond
	}
	e.asyncStop = make(chan struct{})
	e.asyncDone = make(chan struct{})
	go func() {
		defer close(e.asyncDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = e.wal.Sync()
			case <-e.asyncStop:
				_ = e.wal.Sync() // final flush on shutdown
				return
			}
		}
	}()
}

// Close stops the background async-sync ticker, if one is running. Safe
// to call on engines without one (no-op).
func (e *ServerEngine) Close() error {
	// Flush before stopping anything: whatever is still only in the
	// memtable is not on disk yet, and for tree objects that is the
	// difference between a historical read resolving by address next time
	// and having to replay the log to rebuild what it needed.
	var firstErr error
	// Only where there is somewhere to flush to: an engine built without an
	// IO shim (in-memory targets, and much of the test suite) has a
	// memtable whose writer would dereference a nil store.
	if e.memTable != nil && e.config.IOShim != nil {
		if _, err := e.memTable.Flush(0); err != nil {
			firstErr = err
		}
	}
	if e.asyncStop == nil {
		return firstErr
	}
	close(e.asyncStop)
	<-e.asyncDone
	return firstErr
}

func (e *ServerEngine) NamespaceID() string { return e.namespaceID }

func (e *ServerEngine) Capabilities() storage.CapabilitySet { return e.cap }

// WriteBlob is on the hot path for the 1M writes/sec target, so it
// deliberately takes no namespace-wide lock. WAL.Append and memTable.Put
// are each independently thread-safe (their own internal mutexes), and
// durability is provided by GroupCommitter, which coalesces concurrent
// fsync requests into as few physical syncs as possible instead of
// serializing writers behind one lock held across the fsync call. See
// docs/benchmarks/phase0-baseline.md for the before/after measurements
// that motivated this.
func (e *ServerEngine) WriteBlob(bytes []byte) (codec.Hash, error) {
	sum := document.SHA256Digest(bytes)
	hash, err := codec.HashFromBytes(sum)
	if err != nil {
		return codec.Hash{}, err
	}
	if e.wal != nil {
		result, err := e.wal.Append(wal.Record{
			Timestamp: codec.TimestampNow(),
			Kind:      wal.RecordKindPutBlob,
			Payload:   wal.EncodePutBlob(wal.PutBlob{ContentHash: hash, Bytes: bytes}),
		})
		if err != nil {
			return codec.Hash{}, err
		}
		switch e.config.Durability {
		case storage.DurabilitySync:
			fsyncStart := time.Now()
			if err := e.groupCommit.SyncTo(result.Sequence, e.wal.Sync); err != nil {
				return codec.Hash{}, err
			}
			metrics.Default.Record(metrics.StageFsyncWait, time.Since(fsyncStart))
		case storage.DurabilityAsync:
			// Acknowledged once appended; a background ticker (started in
			// NewServerEngine) syncs periodically instead of per-write. A
			// crash can lose up to one sync interval of writes.
		case storage.DurabilityMemoryOnly:
			// Never synced by this engine; caller owns any checkpointing.
		}
	}
	e.memTable.Put(hash, bytes)
	return hash, nil
}

func (e *ServerEngine) ReadBlob(contentHash codec.Hash) ([]byte, error) {
	b := e.memTable.Get(contentHash)
	if b == nil {
		return nil, nil
	}
	return append([]byte(nil), b...), nil
}

// RecoverBlobsFromWal replays PutBlob records into the memtable.
func (e *ServerEngine) RecoverBlobsFromWal() error {
	if e.wal == nil {
		return nil
	}
	_, err := e.wal.Recover(func(record wal.Record) error {
		if record.Kind != wal.RecordKindPutBlob {
			return nil
		}
		payload := record.Payload
		if len(payload) < 32 {
			return nil
		}
		h, err := codec.HashFromBytes(payload[:32])
		if err != nil {
			return err
		}
		bytes := append([]byte(nil), payload[32:]...)
		e.memTable.Put(h, bytes)
		return nil
	})
	return err
}

// PutDocument stages doc; it is not visible via GetDocument until
// CommitTree flushes staged writes.
func (e *ServerEngine) PutDocument(namespaceID string, doc document.Document) error {
	e.pending.Put(doc)
	return nil
}

func (e *ServerEngine) GetDocument(namespaceID string, docID codec.UUID, atCommit codec.Hash) (*document.Document, error) {
	tree, ok, err := e.treeAt(atCommit)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	h, ok := tree.HashFor(docID)
	if !ok {
		return nil, nil
	}
	d, ok := e.docsByHash.Get(h)
	if !ok {
		// The object store first: under the objects strategy this is a
		// direct fetch by content hash, where loadCold's fallback has to
		// index the delta log to find the same bytes.
		if obj, found := e.documentFromObject(docID, h); found {
			e.coldLoads.Add(1)
			e.docsByHash.Put(h, obj)
			return &obj, nil
		}
		loaded, found, err := e.loadCold(docID, h)
		if err != nil || !found {
			return nil, err
		}
		d = loaded
	}
	cp := d
	return &cp, nil
}

// loadCold re-reads a version docsByHash no longer holds, and re-admits it
// so a run of reads over the same history pays for it once. A nil loader
// or a version the loader cannot find reports "not found" rather than an
// error, matching what a miss meant when this store never evicted.
func (e *ServerEngine) loadCold(docID codec.UUID, contentHash codec.Hash) (document.Document, bool, error) {
	loader := e.coldLoader.Load()
	if loader == nil {
		return document.Document{}, false, nil
	}
	doc, found, err := (*loader)(docID, contentHash)
	if err != nil || !found {
		return document.Document{}, false, err
	}
	e.coldLoads.Add(1)
	e.docsByHash.Put(contentHash, doc)
	return doc, true, nil
}

// ColdDocumentLoads reports how many document versions have been re-read
// from durable storage after being evicted from the in-memory version
// store. Zero on a namespace whose history has never been read.
func (e *ServerEngine) ColdDocumentLoads() int64 { return e.coldLoads.Load() }

// coldDocLoader finds one document version by the content hash a
// DocumentTree recorded for it. See SetColdLoader.
type coldDocLoader func(docID codec.UUID, contentHash codec.Hash) (document.Document, bool, error)

// SetColdLoader installs the fallback that serves document versions
// docsByHash has evicted, and with it the byte budget that lets eviction
// happen at all. Both together, never one without the other: bounding the
// store without a loader would silently lose history, and a loader without
// a budget would never be consulted.
//
// budgetBytes <= 0 leaves the store unbounded even with a loader present.
func (e *ServerEngine) SetColdLoader(loader coldDocLoader, budgetBytes int64) {
	if loader == nil {
		e.coldLoader.Store(nil)
		e.docsByHash.SetBudget(0)
		return
	}
	e.coldLoader.Store(&loader)
	e.docsByHash.SetBudget(budgetBytes)
}

func (e *ServerEngine) GetDocumentOrThrow(namespaceID string, docID codec.UUID, atCommit codec.Hash) (document.Document, error) {
	d, err := e.GetDocument(namespaceID, docID, atCommit)
	if err != nil {
		return document.Document{}, err
	}
	if d == nil {
		return document.Document{}, storage.NewDocumentNotFoundError(
			"not found", namespaceID, docID, atCommit)
	}
	return *d, nil
}

func (e *ServerEngine) GetDocuments(namespaceID string, docIDs []codec.UUID, atCommit codec.Hash) ([]*document.Document, error) {
	out := make([]*document.Document, len(docIDs))
	for i, id := range docIDs {
		d, err := e.GetDocument(namespaceID, id, atCommit)
		if err != nil {
			return nil, err
		}
		out[i] = d
	}
	return out, nil
}

func (e *ServerEngine) ScanDocuments(namespaceID string, atCommit codec.Hash, batchSize int, onBatch func([]document.Document) error) error {
	if batchSize <= 0 {
		batchSize = 256
	}
	tree, ok, err := e.treeAt(atCommit)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// Walk, not MaterializedEntries: materializing built the entire namespace's flat
	// map[UUID]Hash before the first batch was emitted - an O(namespace) allocation that no
	// LIMIT, row budget, or admission grant could see or bound, because it happened inside the
	// adapter before the executor's first callback. Streaming the trie keeps a bounded scan's
	// memory proportional to what it keeps (the batch buffer) rather than to what exists.
	buf := make([]document.Document, 0, batchSize)
	var scanErr error
	tree.Walk(func(id codec.UUID, h codec.Hash) bool {
		d, ok := e.docsByHash.Get(h)
		if !ok {
			// Evicted history rather than a genuinely absent document:
			// skipping here would silently drop rows from a scan at an
			// older atCommit. A version the loader also cannot find is
			// skipped, which is what a miss has always meant.
			if obj, found := e.documentFromObject(id, h); found {
				e.coldLoads.Add(1)
				e.docsByHash.Put(h, obj)
				d = obj
				buf = append(buf, d)
				if len(buf) >= batchSize {
					if err := onBatch(append([]document.Document(nil), buf...)); err != nil {
						scanErr = err
						return false
					}
					buf = buf[:0]
				}
				return true
			}
			loaded, found, err := e.loadCold(id, h)
			if err != nil {
				scanErr = err
				return false
			}
			if !found {
				return true
			}
			d = loaded
		}
		buf = append(buf, d)
		if len(buf) >= batchSize {
			if err := onBatch(append([]document.Document(nil), buf...)); err != nil {
				scanErr = err
				return false
			}
			buf = buf[:0]
		}
		return true
	})
	if scanErr != nil {
		return scanErr
	}
	if len(buf) > 0 {
		return onBatch(buf)
	}
	return nil
}

// DeleteDocument stages a deletion; it is not applied via GetDocument
// until CommitTree flushes staged writes.
func (e *ServerEngine) DeleteDocument(namespaceID string, docID codec.UUID) error {
	e.pending.Delete(docID)
	return nil
}

// DiscardPending drops any PutDocument/DeleteDocument calls made since
// the last CommitTree, restoring the last-committed visible state.
// Used to roll back a transaction whose write phase failed partway
// through.
func (e *ServerEngine) DiscardPending(namespaceID string) error {
	e.pending.DiscardAll()
	return nil
}

// CommitTree flushes staged puts/deletes into docs (committed, sharded)
// and applies them to the running tree incrementally (O(delta) per
// changed doc via its persistent trie - document/document_tree_trie.go),
// then returns it. parentTreeHash is ignored, matching the pre-existing
// behavior this preserves: ServerEngine has always reflected current
// live (now: current committed) state rather than tracking per-branch
// history.
func (e *ServerEngine) CommitTree(namespaceID string, parentTreeHash codec.Hash) (document.DocumentTree, error) {
	tree, err := e.commitTreeLocked(namespaceID, parentTreeHash)
	if err != nil {
		return tree, err
	}
	// After the tree locks are released: a flush writes an SSTable, and
	// holding treeMu across that would put every concurrent commit behind
	// a disk write.
	e.maybeFlushMemtable()
	return tree, nil
}

// maybeFlushMemtable writes the in-memory blob generation out once it
// reaches its budget.
//
// The memtable has no bound of its own - it grows on every Put and shrinks
// only when flushed - and under the objects history strategy every
// document version written passes through it. Without this a writing
// session held every version it had written until close: 350MB for one
// document rewritten 463 times, against 41MB with objects turned off,
// which is the same unbounded retention this tier's budgets exist to
// prevent, arriving by a different door.
//
// Best-effort: a failed flush leaves the generation reachable through
// pendingFlush, so nothing written is lost, and the next commit tries
// again.
func (e *ServerEngine) maybeFlushMemtable() {
	if e.memTable == nil || e.config.IOShim == nil {
		return
	}
	if e.memTable.SizeBytes() < storage.ResolvedMemtableFlushBytes(e.config) {
		return
	}
	_, _ = e.memTable.Flush(0)
}

func (e *ServerEngine) commitTreeLocked(namespaceID string, parentTreeHash codec.Hash) (document.DocumentTree, error) {
	puts, deletes := e.pending.TakeAllAndClear()

	e.treeMu.Lock()
	defer e.treeMu.Unlock()
	// Pins follow the live tree exactly: whatever the tree points at now is
	// un-evictable, and a version the tree stops pointing at becomes a
	// candidate. That is the invariant shardedDocByHashStore's bound rests
	// on - see its type doc for why anything looser is unsafe.
	for _, id := range deletes {
		if prev, ok := e.tree.HashFor(id); ok {
			e.docsByHash.Unpin(prev)
		}
		var err error
		e.tree, err = e.tree.Without(id)
		if err != nil {
			return document.DocumentTree{}, err
		}
	}
	baseTree := e.tree
	changed := make([]TreeChange, 0, len(puts))
	for _, doc := range puts {
		h, err := doc.ContentHash()
		if err != nil {
			return document.DocumentTree{}, err
		}
		changed = append(changed, TreeChange{DocID: doc.ID, ContentHash: h})
		e.putDocumentObject(h, doc)
		prev, hadPrev := e.tree.HashFor(doc.ID)
		// Pin before Put, never after: Put evicts to stay within budget as
		// part of the insert, so a version pinned afterwards can already be
		// gone - and for a document larger than one shard's share of the
		// budget it always was, which made every read of live data take the
		// cold path.
		//
		// Rewriting a document with byte-identical content leaves the tree
		// pointing at the same hash, so the pin it already holds is the
		// right one and taking a second would never be released.
		if !hadPrev || prev != h {
			e.docsByHash.Pin(h)
		}
		e.docsByHash.Put(h, doc)
		if hadPrev && prev != h {
			e.docsByHash.Unpin(prev)
		}
		e.tree, err = e.tree.With(doc.ID, h)
		if err != nil {
			return document.DocumentTree{}, err
		}
	}
	// Record what this commit's tree looks like, addressed by its own
	// hash, so a later read at this commit resolves by lookup instead of
	// by replaying the log. See tree_objects.go.
	if len(changed) > 0 || len(deletes) > 0 {
		e.putTreeObject(baseTree, e.tree, changed, deletes)
	}
	e.treesMu.Lock()
	e.publishTreeLocked(e.tree)
	e.treesMu.Unlock()
	return e.tree, nil
}

func (e *ServerEngine) Flush(namespaceID string) error {
	_, err := e.memTable.Flush(0)
	if err != nil {
		return err
	}
	if e.wal != nil {
		return e.wal.Sync()
	}
	return nil
}

func (e *ServerEngine) IngestDeltaSegment(segment storage.DeltaSegmentRef) error {
	return nil
}

func (e *ServerEngine) EvictDocuments(enlistmentID codec.UUID) error {
	e.enlistmentStates[enlistmentID] = storage.EnlistmentEvictionDocEvicted
	return nil
}

func (e *ServerEngine) EvictIndex(enlistmentID codec.UUID) error {
	e.enlistmentStates[enlistmentID] = storage.EnlistmentEvictionEvicted
	return nil
}

func (e *ServerEngine) RebuildDocuments(enlistmentID codec.UUID, from storage.DeltaSegmentReader) error {
	return nil
}

func (e *ServerEngine) RebuildIndex(enlistmentID codec.UUID, from storage.Adapter) error {
	return nil
}

func (e *ServerEngine) EvictionState(enlistmentID codec.UUID) storage.EnlistmentEvictionState {
	if s, ok := e.enlistmentStates[enlistmentID]; ok {
		return s
	}
	return storage.EnlistmentEvictionFull
}

// DefaultFactory opens engines per target.
type DefaultFactory struct {
	EngineTarget Target
}

func (f DefaultFactory) Target() Target { return f.EngineTarget }

func (f DefaultFactory) Open(namespaceID string, config storage.StorageEngineConfig) (Handle, error) {
	var w wal.WriteAheadLog
	if f.EngineTarget == TargetServer {
		var err error
		w, err = (&wal.DefaultFactory{}).OpenOrCreate(namespaceID, config, config.IOShim)
		if err != nil {
			return nil, err
		}
	}
	var eng *ServerEngine
	switch f.EngineTarget {
	case TargetServer, TargetBrowser:
		eng = NewServerEngine(namespaceID, config, w)
	case TargetInMemory, TargetGPU, TargetReadOnly:
		eng = NewServerEngine(namespaceID, config, nil)
	default:
		eng = NewServerEngine(namespaceID, config, nil)
	}
	deltaFactory := delta.Factory{Config: config}
	var writer storage.DeltaSegmentWriter
	if f.EngineTarget == TargetServer {
		wr, err := deltaFactory.OpenWriter(namespaceID)
		if err != nil {
			return nil, err
		}
		writer = wr
	}
	reader := deltaFactory.OpenReader(namespaceID)
	if f.EngineTarget == TargetServer || f.EngineTarget == TargetReadOnly {
		// Only these two have a delta log behind them to re-read an evicted
		// version from. Every other target keeps the version store
		// unbounded, because for them memory is the only tier there is.
		loader := newDeltaColdLoader(reader)
		eng.SetColdLoader(loader.load, storage.ResolvedDocumentCacheBytes(config))
	}
	return &defaultHandle{
		namespaceID: namespaceID,
		adapter:     eng,
		deltaWriter: writer,
		deltaReader: reader,
	}, nil
}

// BrowserEngine is an alias for server engine without WAL in typical browser wiring.
type BrowserEngine = ServerEngine

// InMemoryEngine is server engine without WAL.
type InMemoryEngine = ServerEngine

type defaultHandle struct {
	namespaceID string
	adapter     storage.EvictableAdapter
	deltaWriter storage.DeltaSegmentWriter
	deltaReader storage.DeltaSegmentReader
}

func (h *defaultHandle) NamespaceID() string                     { return h.namespaceID }
func (h *defaultHandle) Adapter() storage.EvictableAdapter       { return h.adapter }
func (h *defaultHandle) DeltaWriter() storage.DeltaSegmentWriter { return h.deltaWriter }
func (h *defaultHandle) DeltaReader() storage.DeltaSegmentReader { return h.deltaReader }
func (h *defaultHandle) Close() error {
	if closer, ok := h.adapter.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}
