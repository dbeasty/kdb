package server

import (
	"fmt"
	"path/filepath"

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

// indexFlushEvery is how many index-touching commits pass before the registry is written back
// to disk (spec §6.5). A crash between flushes costs a rebuild-from-scan on the next open,
// never correctness: the manifest records the commit each snapshot describes.
const indexFlushEvery = 64

// OpenIndexes loads the namespace's index registry from the runtime's data directory, rebuilds
// any store whose snapshot is missing or stale, and wires it up as the SQL index provider and
// the search provider. A memory-backed runtime (no DataRoot) gets an in-memory registry that
// is never persisted.
//
// Call it once after the runtime is open and before it serves connections.
func (s *KdbServerRuntime) OpenIndexes(opts stores.Options) (*RegistryIndexProvider, error) {
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

	if len(report.Stale) > 0 {
		if err := provider.rebuild(report.StaleIDs()); err != nil {
			return nil, err
		}
	}

	s.SetSQLIndexProvider(provider)
	s.SetSearchProvider(provider)
	return provider, nil
}

// indexDir is where this namespace's index snapshots and catalog live, or "" for a
// memory-backed runtime.
func (s *KdbServerRuntime) indexDir() string {
	if s.Runtime == nil || s.Runtime.DataRoot == "" {
		return ""
	}
	return filepath.Join(s.Runtime.DataRoot, s.Runtime.Catalog, "index")
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

// rebuild re-indexes the named stores (all when only is empty) from a scan at head.
func (p *RegistryIndexProvider) rebuild(only []codec.UUID) error {
	head, err := p.runtime.Runtime.DAG.Head()
	if err != nil {
		return err
	}
	if _, err := p.registry.RebuildFromScan(p.runtime.scanAtCommit(head), head, only); err != nil {
		return err
	}
	return p.save()
}

// rebuildIndex re-indexes one store, used when CREATE INDEX adds it to a populated namespace.
func (s *KdbServerRuntime) rebuildIndex(indexID codec.UUID) error {
	p, ok := s.SQLIndexProvider().(*RegistryIndexProvider)
	if !ok {
		return nil
	}
	return p.rebuild([]codec.UUID{indexID})
}

// saveIndexCatalog persists the descriptor catalog after a DDL change.
func (s *KdbServerRuntime) saveIndexCatalog() error {
	p, ok := s.SQLIndexProvider().(*RegistryIndexProvider)
	if !ok {
		return nil
	}
	return p.save()
}

// save writes the catalog and every store snapshot. A memory-backed registry saves nothing.
func (p *RegistryIndexProvider) save() error {
	if p.dir == "" {
		return nil
	}
	if err := index.SaveCatalog(p.dir, p.registry.Catalog()); err != nil {
		return err
	}
	return p.registry.SaveAll(p.dir)
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

// commitToIndexes applies prepared updates under the landed commit hash and returns the hints
// that replicate them. It runs after the DAG append and still under the write gate, so index
// state advances in the same order the log does.
func (p *RegistryIndexProvider) commitToIndexes(prepared []*index.PreparedWrite, commitHash codec.Hash) ([]index.Hint, error) {
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
			return hints, err
		}
		hints = append(hints, h...)
	}
	p.flushed++
	if p.flushed >= indexFlushEvery {
		p.flushed = 0
		if err := p.save(); err != nil {
			return hints, err
		}
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
