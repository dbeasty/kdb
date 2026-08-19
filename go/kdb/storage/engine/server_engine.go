package engine

import (
	"sync"
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

	// docs holds committed (visible) documents, sharded (see doc_shard.go)
	// rather than guarded by one namespace-wide mutex. PutDocument/
	// DeleteDocument stage into pending instead of writing here directly:
	// writes are not visible via GetDocument until CommitTree flushes
	// them, matching the pre-existing InMemoryStorageAdapter contract
	// (see the "reintroduce staging" note below) and letting a failed
	// transaction's write phase be rolled back via DiscardPending without
	// ever having mutated committed state. It is intentionally separate
	// from the blob write path (WriteBlob): WAL.Append and memTable.Put
	// are each independently thread-safe, so blob writes never take a
	// lock at all - see Phase 1/2 of docs/benchmarks/phase0-baseline.md.
	cap              storage.CapabilitySet
	memTable         *memtable.Manager
	docs             *shardedDocStore
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

	asyncStop chan struct{}
	asyncDone chan struct{}
}

// NewServerEngine constructs a server engine; wal may be nil for in-memory targets.
func NewServerEngine(namespaceID string, config storage.StorageEngineConfig, w wal.WriteAheadLog) *ServerEngine {
	cache := sstable.NewBlockCache(config.ResolvedGlobalMemoryBudgetBytes() / 4)
	blobStore := sstable.NewLsmBlobStore(config.IOShim, namespaceID, cache)
	cap := storage.CapabilitySet{
		PersistsDeltaLog:          true,
		PersistsAcrossReload:      w != nil,
		SupportsGpuBulkRead:       false,
		SupportsDirectDeltaIngest: false,
		IndexRetentionDefault:     storage.IndexRetentionEvictable,
	}
	e := &ServerEngine{
		namespaceID:      namespaceID,
		config:           config,
		wal:              w,
		groupCommit:      wal.NewGroupCommitter(),
		cap:              cap,
		memTable:         memtable.NewManager(namespaceID, config.IOShim, blobStore),
		docs:             newShardedDocStore(),
		pending:          newShardedPendingStore(),
		enlistmentStates: make(map[codec.UUID]storage.EnlistmentEvictionState),
		tree:             document.EmptyDocumentTree(),
	}
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
	if e.asyncStop == nil {
		return nil
	}
	close(e.asyncStop)
	<-e.asyncDone
	return nil
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
	d, ok := e.docs.Get(docID)
	if !ok {
		return nil, nil
	}
	cp := d
	return &cp, nil
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
	vals := e.docs.Snapshot()
	for i := 0; i < len(vals); i += batchSize {
		end := i + batchSize
		if end > len(vals) {
			end = len(vals)
		}
		if err := onBatch(vals[i:end]); err != nil {
			return err
		}
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
	puts, deletes := e.pending.TakeAllAndClear()

	e.treeMu.Lock()
	defer e.treeMu.Unlock()
	for _, id := range deletes {
		e.docs.Delete(id)
		var err error
		e.tree, err = e.tree.Without(id)
		if err != nil {
			return document.DocumentTree{}, err
		}
	}
	for _, doc := range puts {
		e.docs.Put(doc)
		h, err := doc.ContentHash()
		if err != nil {
			return document.DocumentTree{}, err
		}
		e.tree, err = e.tree.With(doc.ID, h)
		if err != nil {
			return document.DocumentTree{}, err
		}
	}
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
	case TargetInMemory, TargetGPU:
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

func (h *defaultHandle) NamespaceID() string                      { return h.namespaceID }
func (h *defaultHandle) Adapter() storage.EvictableAdapter        { return h.adapter }
func (h *defaultHandle) DeltaWriter() storage.DeltaSegmentWriter  { return h.deltaWriter }
func (h *defaultHandle) DeltaReader() storage.DeltaSegmentReader  { return h.deltaReader }
func (h *defaultHandle) Close() error {
	if closer, ok := h.adapter.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}
