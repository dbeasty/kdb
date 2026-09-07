package embed

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

// OpenFileRuntime opens an embedded runtime backed by a data directory.
// When KDB_S3_BUCKET is set, sealed segments and snapshots are replicated to S3 (see FileRuntimeOptionsFromEnv).
func OpenFileRuntime(dataRoot, catalog, namespaceID string, sch schema.KdbSchema) (*EmbeddedKdbRuntime, error) {
	return OpenFileRuntimeWithOptions(dataRoot, catalog, namespaceID, sch, FileRuntimeOptionsFromEnv())
}

// OpenReadOnlyFileRuntime opens a data directory for reading only, under a shared directory
// lock. Several of these may be open at once, in this process or others, but never while a
// writer holds the directory exclusively - and a writer cannot start while any of them is open.
//
// The view is a snapshot as of the open; call EmbeddedKdbRuntime.Refresh to advance it. Unix
// only: the shared lock needs flock(2).
func OpenReadOnlyFileRuntime(dataRoot, catalog, namespaceID string, sch schema.KdbSchema) (*EmbeddedKdbRuntime, error) {
	opts := FileRuntimeOptionsFromEnv()
	opts.ReadOnly = true
	return OpenFileRuntimeWithOptions(dataRoot, catalog, namespaceID, sch, opts)
}

// OpenFileRuntimeWithOptions opens a file runtime with explicit storage options.
//
// This is the single-namespace shape, and it stays exactly what it has always been: one
// namespace, owning the data root for as long as it is open, closing the whole thing on Close.
// It is now expressed as what it always was underneath - a Host with one namespace under it -
// so that a caller who wants several namespaces in one process can open the Host directly and
// share the lock, the I/O shim and (Phase C) the memory budget between them, instead of needing
// a data root per namespace. See OpenFileHost and
// docs/kdb-spec-layer17-multi-namespace-runtime.md.
func OpenFileRuntimeWithOptions(dataRoot, catalog, namespaceID string, sch schema.KdbSchema, opts FileRuntimeOptions) (*EmbeddedKdbRuntime, error) {
	host, err := OpenFileHost(dataRoot, opts)
	if err != nil {
		return nil, err
	}
	rt, err := host.NamespaceWithOptions(catalog, namespaceID, sch, opts.Storage)
	if err != nil {
		_ = host.Close()
		return nil, err
	}
	// This runtime owns the host it was opened under, so closing it closes the host and
	// releases the directory lock - the contract every existing caller was written against.
	// A runtime obtained from a Host the caller opened itself leaves release nil: the lock is
	// the host's to drop, not one namespace's.
	rt.release = func() { _ = host.Close() }
	return rt, nil
}

// openNamespace opens one namespace under an already-locked host, using the host's shared I/O
// shim. It returns the host's entry for the namespace - the runtime plus the func that shuts its
// storage down - and this namespace's side of the host's memory pool. Wiring all three up is the
// host's job, because on the multi-namespace path both the lock and the pool outlive any one
// namespace.
//
// The participant is nil for a namespace with no ServerEngine behind it, which is the honest
// answer: there is nothing there whose budget could be re-cut.
func (h *Host) openNamespace(
	catalog, namespaceID string, sch schema.KdbSchema, opts FileRuntimeOptions,
) (*namespaceEntry, storage.BudgetParticipant, error) {
	dataRoot, io := h.dataRoot, h.io

	// A read-only open creates nothing: the directory belongs to the writer, and a reader that
	// materialized missing namespace directories or a meta.json would be writing to the very
	// thing it promised not to touch. A namespace that is not there yet simply has nothing to
	// read, which the delta replay below reports on its own.
	// Before ensureNamespaceDirs, which creates the marker file that
	// decides whether this namespace counts as pre-existing.
	namespaceIsNew := !namespaceDirExists(dataRoot, namespaceID)
	if !opts.ReadOnly {
		if err := ensureNamespaceDirs(dataRoot, namespaceID); err != nil {
			return nil, nil, err
		}
	}
	historyStrategy, err := resolveHistoryStrategy(
		dataRoot, namespaceID, opts.Storage.HistoryStrategy, namespaceIsNew, opts.ReadOnly, opts.forceHistoryStrategy)
	if err != nil {
		return nil, nil, err
	}

	compression := storage.CompressionZSTD
	if opts.Storage.Compression != nil {
		compression = *opts.Storage.Compression
	}
	memoryBudget := int64(64 * 1024 * 1024)
	if opts.Storage.MemoryBudgetBytes > 0 {
		memoryBudget = opts.Storage.MemoryBudgetBytes
	}
	cfg := storage.StorageEngineConfig{
		GlobalMemoryBudgetBytes: memoryBudget,
		CompressionCodec:        compression,
		DefaultIndexRetention:   storage.IndexRetentionEvictable,
		IOShim:                  io,
		Durability:              opts.Storage.Durability,
		AsyncSyncIntervalMillis: opts.Storage.AsyncSyncIntervalMillis,
		HistoryStrategy:         historyStrategy,
		DocumentCacheBytes:      opts.Storage.DocumentCacheBytes,
		CommitOpsBytes:          opts.Storage.CommitOpsBytes,
		TreeChainLimit:          opts.Storage.TreeChainLimit,
		HistoryTreeCacheBytes:   opts.Storage.HistoryTreeCacheBytes,
		DisableCheckpoints:      opts.Storage.DisableCheckpoints,
	}
	target := engine.TargetServer
	if opts.ReadOnly {
		// TargetReadOnly skips both the WAL and the delta writer, so nothing in this process
		// opens a file the writer also has open for appending.
		target = engine.TargetReadOnly
	}
	handle, err := engine.DefaultFactory{EngineTarget: target}.Open(namespaceID, cfg)
	if err != nil {
		return nil, nil, err
	}
	// Every return between here and the success path below used to call lock.Release() alone,
	// leaking handle's open file descriptors and unsealed WAL on any post-open failure (schema
	// sync, delta replay, ...). handleClosed tracks whether the success path already took over
	// responsibility for handle - the deferred cleanup only fires on an early return.
	handleClosed := false
	defer func() {
		if !handleClosed {
			_ = handle.Close()
		}
	}()

	d, err := dag.NewInMemoryCommitDag(namespaceID)
	if err != nil {
		return nil, nil, err
	}
	// Before restore, not after: the generation numbers the pruned ancestry
	// walks read are derived as commits are admitted, and a checkpoint
	// admits its commits in map order - so the DAG has to already know
	// whether it is deriving them by the time restoreNamespace starts
	// putting commits in.
	d.SetGraphSettings(opts.Storage.Graph)

	store := handle.Adapter()
	if store == nil {
		return nil, nil, fmt.Errorf("file runtime missing storage adapter")
	}

	eng, _ := store.(*engine.ServerEngine)
	if eng != nil {
		// One owner for trees. The engine already keeps every tree it
		// commits; leaving the DAG's own map in place would mean two
		// structures holding the same values under the same keys, and a
		// budget on either would reclaim nothing while the other held on.
		d.SetTreeStore(eng)
	}
	if r := handle.DeltaReader(); r != nil {
		// Installed before replay, so replay's own commits are subject to
		// the budget as they arrive rather than all landing resident first
		// - which is the case that made opening a long-lived namespace cost
		// the sum of its whole history (docs/benchmarks/open-cost.md).
		d.SetOperationsLoader(newCommitOpsLoader(r).load, storage.ResolvedCommitOpsBytes(cfg))
	}
	replayedInFull, err := restoreNamespace(d, store, handle.DeltaReader(), io, namespaceID, opts.Storage.DisableCheckpoints)
	if err != nil {
		return nil, nil, err
	}
	dagOut := dag.CommitDAG(d)
	var persisting *PersistingCommitDAG
	if w := handle.DeltaWriter(); w != nil && !opts.ReadOnly {
		persisting = NewPersistingCommitDAGWithAsyncInterval(
			d, w, cfg.Durability,
			time.Duration(cfg.AsyncSyncIntervalMillis)*time.Millisecond,
		)
		dagOut = persisting
		if eng, ok := store.(*engine.ServerEngine); ok {
			// Told where each commit landed, so the versions it wrote can
			// be found again by position instead of by scanning.
			persisting.SetPersistListener(eng.RecordCommitLocation)
		}
		if replayedInFull {
			checkpointAfterFullReplay(d, store, handle.DeltaReader(), w, io, namespaceID, opts.Storage.DisableCheckpoints)
		}
	}

	rt := &EmbeddedKdbRuntime{
		Catalog:          catalog,
		DAG:              dagOut,
		Storage:          store,
		Schema:           sch,
		DefaultNamespace: namespaceID,
		DataRoot:         dataRoot,
		ReadOnly:         opts.ReadOnly,
		deltaReader:      handle.DeltaReader(),
	}
	if !sch.IsNone() && !opts.ReadOnly {
		// syncEmbedSchema commits a schema migration when the stored schema differs - a write,
		// and therefore not something a read-only runtime may do. It reads whatever schema the
		// writer has already persisted instead.
		if err := syncEmbedSchema(rt, namespaceID, sch); err != nil {
			return nil, nil, err
		}
	}
	if opts.ReadOnly {
		// Refresh re-reads the writer's delta log onto this runtime's DAG. Held as a closure
		// because the reader needs the same handle and namespace the open resolved, and nothing
		// else in EmbeddedKdbRuntime carries them.
		rt.refresh = func() error {
			return replayDeltaNamespace(d, store, handle.DeltaReader())
		}
	}
	handleClosed = true
	// storageClose is what makes Close() an orderly shutdown rather than a no-op that only
	// releases the directory lock (kdb-spec-layer13 Component 47 §2.4/§4.5) - previously nothing
	// retained handle past this function returning, so even a clean process exit never flushed
	// the delta writer, sealed its segment, or reached ServerEngine.Close()'s final WAL sync.
	storageClose := func() error {
		var firstErr error
		// Drain the commit log first: under DurabilityAsync there can be
		// records queued but not yet written, and sealing the segment out from
		// under them would drop acknowledged commits on a clean shutdown.
		if persisting != nil {
			if err := persisting.Close(); err != nil {
				firstErr = err
			}
		}
		if w := handle.DeltaWriter(); w != nil {
			if err := w.Flush(); err != nil && firstErr == nil {
				firstErr = err
			}
			if !w.IsSealed() {
				if _, err := w.Seal(); err != nil && firstErr == nil {
					firstErr = err
				}
			}
			// After the seal, so no segment can gain another commit and the
			// checkpoint can claim the newest one - see checkpointOnClose.
			checkpointOnClose(d, store, handle.DeltaReader(), io, namespaceID, opts.Storage.DisableCheckpoints)
		}
		if err := handle.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	// Closing the runtime closes this namespace through the host, which is what owns the
	// shutdown sequence now that a lock can outlive any one namespace under it.
	rt.storageClose = func() error { return h.CloseNamespace(namespaceID) }

	// This namespace's side of the host's memory pool. Registered by the caller, after the
	// entry is in the host's map, so a rebalance can never reach a namespace the host does not
	// yet consider open.
	var budget storage.BudgetParticipant
	if eng != nil {
		budget = namespaceBudget{eng: eng, dag: d, pinnedOps: opts.Storage.CommitOpsBytes > 0}
	}
	return &namespaceEntry{rt: rt, close: storageClose}, budget, nil
}

// LockDataDir takes dataRoot's attach lock exclusively and returns its release func. For
// maintenance tooling (kdb-inspect verify/repair/restore/backup) that must not run concurrently
// with any live runtime: holding this proves no service, embedded runtime, or read-only replica
// has the directory open, and blocks one from starting mid-operation. Stronger than what a
// writable runtime holds, which is a *shared* attach plus the exclusive writer lock - a writer
// excludes other writers, this excludes everybody.
func LockDataDir(dataRoot string) (release func(), err error) {
	lock, err := acquireDirLockExclusive(dataRoot)
	if err != nil {
		return nil, err
	}
	return lock.Release, nil
}

func ensureNamespaceDirs(dataRoot, namespaceID string) error {
	nsDir := filepath.Join(dataRoot, "ns", namespaceID)
	for _, sub := range []string{"", "delta", "meta"} {
		p := nsDir
		if sub != "" {
			p = filepath.Join(nsDir, sub)
		}
		if err := os.MkdirAll(p, 0o755); err != nil {
			return err
		}
	}
	meta := filepath.Join(nsDir, "meta.json")
	if _, err := os.Stat(meta); os.IsNotExist(err) {
		if err := os.WriteFile(meta, []byte(`{"namespaceId":"`+namespaceID+`"}`), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func namespaceDirExists(dataRoot, namespaceID string) bool {
	_, err := os.Stat(filepath.Join(dataRoot, "ns", filepath.FromSlash(namespaceID)))
	return err == nil
}

// CatalogFromNamespace returns the catalog segment of a namespace id (before '/').
func CatalogFromNamespace(namespaceID string) string {
	if i := indexByte(namespaceID, '/'); i >= 0 {
		return namespaceID[:i]
	}
	return namespaceID
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
