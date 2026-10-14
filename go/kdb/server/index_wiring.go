package server

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/index/stores"
)

// Index wiring (kdb-spec-layer16 Components 67, 68 and 69): opening a namespace's index
// registry, keeping it current on the commit path, and persisting it.
//
// The registry lives behind a RegistryIndexProvider rather than on the runtime struct, so a
// runtime with no indexes carries no index state at all and the existing full-scan behaviour
// is untouched.

// Index snapshots are written off the write path (spec §6.5). A commit only applies its update to
// the in-memory stores and nudges the provider's flusher; the flusher writes the snapshots once
// the namespace has gone quiet, in the background, never under the write gate, and only for the
// stores that changed since they were last written (a store that did not change has just its
// manifest moved to the new head). Close writes them a last time.
//
// Snapshots are an open-time optimization, never a correctness dependency: every manifest names
// the commit its snapshot describes, and on open a store whose manifest does not name the DAG
// head - an older snapshot, or none, or an unreadable one - is rebuilt by scan before it serves
// a query. A kill at any point therefore costs a rebuild, not a wrong answer. Writing snapshots
// while commits are still arriving would buy nothing (the head would move past them before a
// crash could use them), which is why the flusher waits for a quiet spell.
//
// They used to be written synchronously under the write gate every 64 commits, every store in
// full, which made a commit's cost grow with (number of indexes x their total size).
var (
	// indexFlushIdle is how long a namespace must go without an index-touching commit before
	// its snapshots are written.
	indexFlushIdle = time.Second
	// indexFlushMinGap is the least time between the starts of two background flushes.
	indexFlushMinGap = 5 * time.Second
	// indexFlushBackoff stretches that gap for a slow flush: one that took d is not followed by
	// another for indexFlushBackoff*d, so the flusher is busy at most 1/indexFlushBackoff of the
	// time however large the indexes grow.
	indexFlushBackoff = 10
)

// OpenIndexes loads the namespace's index registry from the runtime's data directory, rebuilds
// any store whose snapshot is missing or stale, and wires it up as the SQL index provider and
// the search provider. A memory-backed runtime (no DataRoot) gets an in-memory registry that
// is never persisted.
//
// Call it once after the runtime is open and before it serves connections.
func (s *KdbServerRuntime) OpenIndexes(opts stores.Options) (*RegistryIndexProvider, error) {
	// A registry opened earlier on this runtime is written out and stopped first, so two
	// flushers never write one directory.
	if old, ok := s.SQLIndexProvider().(*RegistryIndexProvider); ok && old != nil {
		old.Close()
	}
	dir := s.indexDir()
	var (
		reg    *index.Registry
		report index.LoadReport
		err    error
	)
	if dir == "" {
		reg = index.NewRegistry(s.Runtime.DefaultNamespace, s.dag, stores.NewFactory(s.dag, opts))
	} else {
		reg, report, err = stores.Open(dir, s.Runtime.DefaultNamespace, s.dag, opts)
		if err != nil {
			return nil, err
		}
	}

	provider := NewRegistryIndexProvider(reg, s)
	provider.dir = dir
	provider.opts = opts

	// What disk holds: the stores restored from a snapshot at head. Everything else is written
	// by the first flush.
	gens := reg.Generations()
	provider.saved = make(map[codec.UUID]uint64, len(report.Fresh))
	for _, d := range report.Fresh {
		provider.saved[d.IndexID] = gens[d.IndexID]
	}
	if len(report.Stale) == 0 {
		provider.savedHead, provider.hasSavedHead = reg.Head()
	}

	if len(report.Stale) > 0 {
		if err := provider.rebuild(report.StaleIDs()); err != nil {
			return nil, err
		}
	}

	if dir != "" {
		provider.startFlusher()
		// Written out while the directory is still ours, on every path that closes the
		// namespace: Release, an idle close, the host's close.
		s.Runtime.AddCloseHook(provider.Close)
		if len(report.Stale) > 0 {
			provider.kickFlush()
		}
	}

	s.SetSQLIndexProvider(provider)
	s.SetSearchProvider(provider)
	return provider, nil
}

// indexDir is where this namespace's index snapshots and catalog live, or "" for a
// memory-backed runtime.
//
// Per namespace: <dataRoot>/ns/<namespace>/index. It used to be per *catalog*
// (<dataRoot>/<catalog>/index), which was only right while one process served one namespace:
// with several namespaces of one catalog under a host, each would have loaded the others' index
// definitions from the shared catalog.json and overwritten it on save. A catalog-level directory
// already on disk is still used by the one namespace its catalog.json names, so an existing
// deployment keeps its indexes without a migration.
func (s *KdbServerRuntime) indexDir() string {
	if s.Runtime == nil || s.Runtime.DataRoot == "" {
		return ""
	}
	legacy := filepath.Join(s.Runtime.DataRoot, s.Runtime.Catalog, "index")
	if cat, err := index.LoadCatalog(legacy); err == nil && cat.NamespaceID == s.Runtime.DefaultNamespace {
		return legacy
	}
	return filepath.Join(s.Runtime.DataRoot, "ns", filepath.FromSlash(s.Runtime.DefaultNamespace), "index")
}

// scanAtCommit yields every document in the namespace at commitHash, for a rebuild.
func (s *KdbServerRuntime) scanAtCommit(commitHash codec.Hash) index.ScanFunc {
	return func(yield func(docID codec.UUID, jsonText string) bool) error {
		commit, ok := s.Runtime.DAG.GetCommit(commitHash)
		if !ok {
			return fmt.Errorf("kdb server: rebuild commit %s missing", commitHash.Hex())
		}
		stop := false
		return s.Runtime.Storage.ScanDocuments(s.Runtime.DefaultNamespace, commit.DocumentTreeHash, 256, func(batch []document.Document) error {
			for _, doc := range batch {
				if stop {
					return nil
				}
				if !yield(doc.ID, doc.JSON) {
					stop = true
					return nil
				}
			}
			return nil
		})
	}
}

// getDocumentAt reads one document body at an explicit commit, for search hits.
func (s *KdbServerRuntime) getDocumentAt(namespaceID string, docID codec.UUID, at codec.Hash) (string, codec.Hash, bool, error) {
	commit, ok := s.Runtime.DAG.GetCommit(at)
	if !ok {
		return "", at, false, fmt.Errorf("kdb server: commit %s missing", at.Hex())
	}
	doc, err := s.Runtime.Storage.GetDocument(namespaceID, docID, commit.DocumentTreeHash)
	if err != nil || doc == nil {
		return "", at, false, err
	}
	return doc.JSON, at, true, nil
}

// rebuild re-indexes the named stores (all when only is empty) from a scan at head. Their
// snapshots are written by the next flush, off this call.
func (p *RegistryIndexProvider) rebuild(only []codec.UUID) error {
	p.persistMu.Lock()
	defer p.persistMu.Unlock()
	return p.rebuildLocked(only)
}

// rebuildLocked is rebuild with persistMu held, so no flush captures a store mid-rebuild.
//
// It also holds applyMu, so no commit is applied to the stores while one is rebuilt: a commit
// applied mid-rebuild landed in a store the scan was replacing and was lost or left stale (CREATE
// INDEX under concurrent writes left an entry for a field the document no longer had). The scan
// is at the registry's head, not the DAG's: a commit already in the DAG can be waiting on
// applyMu to reach the stores, and a scan that included it would have it applied twice.
func (p *RegistryIndexProvider) rebuildLocked(only []codec.UUID) error {
	p.applyMu.Lock()
	defer p.applyMu.Unlock()
	head, ok := p.registry.Head()
	if !ok {
		var err error
		if head, err = p.runtime.Runtime.DAG.Head(); err != nil {
			return err
		}
	}
	if _, err := p.registry.RebuildFromScan(p.runtime.scanAtCommit(head), head, only); err != nil {
		return err
	}
	if len(only) == 0 {
		p.inconsistent = false
	}
	p.kickFlush()
	return nil
}

// saveIndexCatalog persists the descriptor catalog after a DDL change. Only the catalog: the
// snapshots follow in the background, and a catalog entry with no current snapshot is rebuilt
// on open.
func (s *KdbServerRuntime) saveIndexCatalog() error {
	p, ok := s.SQLIndexProvider().(*RegistryIndexProvider)
	if !ok {
		return nil
	}
	p.persistMu.Lock()
	defer p.persistMu.Unlock()
	return p.saveCatalogLocked()
}

// saveCatalogLocked writes the catalog; persistMu must be held. A memory-backed registry, or one
// already closed, writes nothing.
func (p *RegistryIndexProvider) saveCatalogLocked() error {
	if p.dir == "" || p.closed {
		return nil
	}
	return index.SaveCatalog(p.dir, p.registry.Catalog())
}

// flushLocked writes the snapshots of every store that changed since it was last written, and
// moves the manifests of the rest to the current head; persistMu must be held.
//
// The capture is the only part that excludes commits (applyMu), and it is in-memory
// serialization of the changed stores; the disk writes run with commits flowing.
func (p *RegistryIndexProvider) flushLocked() error {
	if p.dir == "" || p.closed {
		return nil
	}
	p.applyMu.Lock()
	if p.inconsistent {
		// A commit failed part-way through: the stores describe no single commit until a full
		// rebuild, so there is nothing true to write. The next open rebuilds them.
		p.applyMu.Unlock()
		return nil
	}
	capture, ok, err := p.registry.CaptureSnapshots(p.saved)
	p.applyMu.Unlock()
	if err != nil || !ok {
		return err
	}
	if p.hasSavedHead && capture.Head == p.savedHead && !capture.Changed() {
		return nil
	}
	written, err := index.WriteSnapshots(p.dir, capture)
	if p.saved == nil {
		p.saved = make(map[codec.UUID]uint64, len(written))
	}
	for id, gen := range written {
		p.saved[id] = gen
	}
	if err != nil {
		return err
	}
	p.savedHead, p.hasSavedHead = capture.Head, true
	return nil
}

// Flush writes the index snapshots now, synchronously. The write path never needs it; it is for
// callers that want the snapshots on disk at a moment of their choosing.
func (p *RegistryIndexProvider) Flush() error {
	p.persistMu.Lock()
	defer p.persistMu.Unlock()
	return p.flushLocked()
}

// Close stops the background flusher and writes the catalog and the snapshots a last time, so a
// graceful shutdown reopens without rebuilding. After Close nothing is written to the index
// directory. Safe to call more than once; it runs as a close hook of the runtime.
func (p *RegistryIndexProvider) Close() {
	p.stopFlusher()
	p.persistMu.Lock()
	defer p.persistMu.Unlock()
	if p.closed {
		return
	}
	err := p.saveCatalogLocked()
	if ferr := p.flushLocked(); err == nil {
		err = ferr
	}
	p.closed = true
	if err != nil {
		slog.Warn("writing index snapshots on close failed; the indexes are rebuilt on the next open",
			"namespace", p.registry.NamespaceID(), "error", err)
	}
}

// abandon stops the flusher and marks the provider closed without writing anything - what a
// kill leaves behind, for tests.
func (p *RegistryIndexProvider) abandon() {
	p.stopFlusher()
	p.persistMu.Lock()
	p.closed = true
	p.persistMu.Unlock()
}

// indexFlusher is the background goroutine that writes snapshots once commits go quiet.
type indexFlusher struct {
	kick chan struct{}
	stop chan struct{}
	done chan struct{}
	once sync.Once
}

func (p *RegistryIndexProvider) startFlusher() {
	f := &indexFlusher{kick: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	p.flusher = f
	go p.flushLoop(f)
}

// kickFlush tells the flusher the stores moved. It never blocks: one pending kick is enough.
func (p *RegistryIndexProvider) kickFlush() {
	if f := p.flusher; f != nil {
		select {
		case f.kick <- struct{}{}:
		default:
		}
	}
}

// stopFlusher stops the flusher and waits for a flush in progress to finish.
func (p *RegistryIndexProvider) stopFlusher() {
	f := p.flusher
	if f == nil {
		return
	}
	f.once.Do(func() { close(f.stop) })
	<-f.done
}

func (p *RegistryIndexProvider) flushLoop(f *indexFlusher) {
	defer close(f.done)
	var notBefore time.Time
	for {
		select {
		case <-f.kick:
		case <-f.stop:
			return
		}
		if !f.waitQuiet(notBefore) {
			return
		}
		start := time.Now()
		p.persistMu.Lock()
		err := p.flushLocked()
		p.persistMu.Unlock()
		took := time.Since(start)
		if err != nil {
			slog.Warn("writing index snapshots failed; retried at the next quiet spell, and a "+
				"restart before then rebuilds the indexes", "namespace", p.registry.NamespaceID(), "error", err)
		}
		gap := indexFlushMinGap
		if b := time.Duration(indexFlushBackoff) * took; b > gap {
			gap = b
		}
		notBefore = start.Add(gap)
	}
}

// waitQuiet returns once indexFlushIdle has passed with no kick and notBefore has passed, or
// false when the flusher is stopped first.
func (f *indexFlusher) waitQuiet(notBefore time.Time) bool {
	for {
		wait := indexFlushIdle
		if d := time.Until(notBefore); d > wait {
			wait = d
		}
		t := time.NewTimer(wait)
		select {
		case <-f.kick:
			t.Stop()
		case <-f.stop:
			t.Stop()
			return false
		case <-t.C:
			return true
		}
	}
}

// prepareCommitForIndexes extracts and validates every indexed value the transaction would
// write, without mutating any store (Component 68, spec §10). It runs before the DAG append, so
// a document the indexes cannot accept - a vector of the wrong length - rejects the whole commit
// rather than leaving the indexes half-applied.
func (p *RegistryIndexProvider) prepareCommitForIndexes(ops []document.Op) ([]*index.PreparedWrite, error) {
	var prepared []*index.PreparedWrite
	for _, op := range ops {
		switch o := op.(type) {
		case document.WriteOp:
			pw, err := p.registry.PrepareWrite(o.DocID, o.Patch)
			if err != nil {
				return nil, err
			}
			prepared = append(prepared, pw)
		case document.DeleteOp:
			prepared = append(prepared, p.registry.PrepareDelete(o.DocID))
		}
	}
	return prepared, nil
}

// preparedForStored returns the index updates for what a commit actually stored, given the ones
// prepared from the request before it landed. They differ for a merge patch - the request names
// only the keys it changes, the commit stores the merged document - and for a document a conflict
// resolver rewrote. Indexing the request instead dropped a document from the index of every
// field a partial write left out, while the stored document still had it, so `WHERE g = 'a'`
// missed a row whose g was 'a'. An op whose stored form is its request reuses the preparation.
// requested and stored are the transaction's ops before and after staging (same length and
// order: operationsAsStored only substitutes documents).
func (p *RegistryIndexProvider) preparedForStored(requested []document.Op, prepared []*index.PreparedWrite, stored []document.Op) ([]*index.PreparedWrite, error) {
	if len(stored) != len(requested) {
		return p.prepareCommitForIndexes(stored)
	}
	out := make([]*index.PreparedWrite, 0, len(prepared))
	next := 0
	for i, op := range requested {
		switch o := op.(type) {
		case document.WriteOp:
			pw := prepared[next]
			next++
			if s, ok := stored[i].(document.WriteOp); ok && s.Patch != o.Patch {
				fresh, err := p.registry.PrepareWrite(s.DocID, s.Patch)
				if err != nil {
					return nil, err
				}
				pw = fresh
			}
			out = append(out, pw)
		case document.DeleteOp:
			out = append(out, prepared[next])
			next++
		}
	}
	return out, nil
}

// commitToIndexes applies prepared updates under the landed commit hash and returns the hints
// that replicate them. It runs after the DAG append and still under the write gate, so index
// state advances in the same order the log does. It writes nothing to disk: it nudges the
// background flusher, which writes the snapshots once the namespace goes quiet.
func (p *RegistryIndexProvider) commitToIndexes(prepared []*index.PreparedWrite, commitHash codec.Hash) ([]index.Hint, error) {
	hints, err := p.applyCommit(prepared, commitHash)
	if err == nil {
		p.repairHeads()
	}
	return hints, err
}

// repairHeads rebuilds, from the documents at head, any store whose head state could not follow
// the commit graph (index.MemoryStore.NeedsRebuild: history it would need to replay was dropped).
// It runs on the commit path, after the commit is applied, so the stores are current before the
// next read; until it runs, reads of such a store fall back to a scan rather than a wrong answer.
func (p *RegistryIndexProvider) repairHeads() {
	ids := p.registry.NeedingRebuild()
	if len(ids) == 0 {
		return
	}
	if err := p.rebuild(ids); err != nil {
		slog.Warn("rebuilding an index whose head state was lost failed; its reads fall back to scans until the next rebuild",
			"namespace", p.registry.NamespaceID(), "error", err)
	}
}

// applyCommit applies one commit's prepared updates to every store.
func (p *RegistryIndexProvider) applyCommit(prepared []*index.PreparedWrite, commitHash codec.Hash) ([]index.Hint, error) {
	// One commit is applied to every store, and the registry head moved, as one step with
	// respect to a snapshot capture.
	p.applyMu.Lock()
	defer p.kickFlush()
	defer p.applyMu.Unlock()
	if len(prepared) == 0 {
		// A commit that touched no document still moves head; without this the snapshots read
		// stale on the next open and force a needless rebuild.
		p.registry.MarkCommit(commitHash)
		return nil, nil
	}
	var hints []index.Hint
	for _, pw := range prepared {
		h, err := pw.Commit(commitHash)
		if err != nil {
			p.inconsistent = true
			return hints, err
		}
		hints = append(hints, h...)
	}
	return hints, nil
}

// prepareIndexes is the runtime-side entry point for the pre-append half of Component 68. It
// returns nil when no index provider is configured, which is the no-index fast path.
func (s *KdbServerRuntime) prepareIndexes(tx document.Transaction) ([]*index.PreparedWrite, *RegistryIndexProvider, error) {
	p, ok := s.SQLIndexProvider().(*RegistryIndexProvider)
	if !ok || len(p.registry.Descriptors()) == 0 {
		return nil, nil, nil
	}
	prepared, err := p.prepareCommitForIndexes(tx.Operations)
	if err != nil {
		return nil, nil, err
	}
	return prepared, p, nil
}
