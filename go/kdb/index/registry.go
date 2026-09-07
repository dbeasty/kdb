package index

import (
	"errors"
	"fmt"
	"io/fs"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
)

// Registry holds one namespace's index stores and is the commit path's single entry point
// (Layer 16 §10): it prepares every store's update from a document before the DAG append
// (so a document's fault rejects the commit with nothing half-applied), applies the prepared
// updates tagged with the new commit hash afterwards, and persists the catalog and snapshots.
// It is independent of the server and transaction packages.
type Registry struct {
	namespaceID string
	dag         *dag.InMemoryCommitDag
	factory     StoreFactory

	mu      sync.RWMutex
	entries []*registryEntry
	head    codec.Hash
	hasHead bool
}

type registryEntry struct {
	desc  Descriptor
	store DocumentStore
}

// ScanFunc feeds every live document at head to yield; returning false from yield stops the
// scan. It is the shape a document-tree walk naturally has.
type ScanFunc func(yield func(docID codec.UUID, jsonText string) bool) error

// NewRegistry creates an empty registry for namespaceID whose stores are built by factory.
func NewRegistry(namespaceID string, d *dag.InMemoryCommitDag, factory StoreFactory) *Registry {
	return &Registry{namespaceID: namespaceID, dag: d, factory: factory}
}

// NamespaceID returns the namespace this registry indexes.
func (r *Registry) NamespaceID() string { return r.namespaceID }

// Add creates the store for desc and registers it. A descriptor whose index name or
// (first field, type) pair is already registered is rejected, as is a store that cannot
// index documents.
func (r *Registry) Add(desc Descriptor) (Store, error) {
	if desc.NamespaceID == "" {
		desc.NamespaceID = r.namespaceID
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.entries {
		if e.desc.IndexID == desc.IndexID {
			return nil, fmt.Errorf("index %s already registered", desc.IndexID)
		}
		if n := desc.IndexName(); n != "" && e.desc.IndexName() == n {
			return nil, fmt.Errorf("index name %q already registered", n)
		}
		if e.desc.Type == desc.Type && e.desc.FirstField() == desc.FirstField() {
			return nil, fmt.Errorf("a %s index on %s already exists", desc.Type, desc.FirstField())
		}
	}
	store, err := r.factory.Create(desc)
	if err != nil {
		return nil, err
	}
	ds, ok := store.(DocumentStore)
	if !ok {
		return nil, fmt.Errorf("index %s: store type %T cannot index documents", desc.IndexID, store)
	}
	r.entries = append(r.entries, &registryEntry{desc: desc, store: ds})
	return store, nil
}

// Remove drops the index named name (DROP INDEX). It reports whether one was registered.
func (r *Registry) Remove(name string) (Descriptor, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, e := range r.entries {
		if e.desc.IndexName() == name {
			r.entries = append(r.entries[:i], r.entries[i+1:]...)
			return e.desc, true
		}
	}
	return Descriptor{}, false
}

// Descriptors returns the registered descriptors in registration order.
func (r *Registry) Descriptors() []Descriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Descriptor, len(r.entries))
	for i, e := range r.entries {
		out[i] = e.desc
	}
	return out
}

// Catalog returns the registry's persistable catalog.
func (r *Registry) Catalog() Catalog {
	return Catalog{NamespaceID: r.namespaceID, Indexes: r.Descriptors()}
}

// Stores returns the registered stores in registration order.
func (r *Registry) Stores() []Store {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Store, len(r.entries))
	for i, e := range r.entries {
		out[i] = e.store
	}
	return out
}

// ByName finds the index named name (Options["index_name"]).
func (r *Registry) ByName(name string) (Store, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, e := range r.entries {
		if name != "" && e.desc.IndexName() == name {
			return e.store, true
		}
	}
	return nil, false
}

// ByField finds the index of type typ whose first field is field.
func (r *Registry) ByField(field string, typ IndexType) (Store, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, e := range r.entries {
		if e.desc.Type == typ && e.desc.FirstField() == field {
			return e.store, true
		}
	}
	return nil, false
}

// Resolve is the SQL/wire rule for MATCH and SIMILARITY: nameOrField names an index of type
// typ, or is the first field of one.
func (r *Registry) Resolve(nameOrField string, typ IndexType) (Store, bool) {
	if s, ok := r.ByName(nameOrField); ok && s.Descriptor().Type == typ {
		return s, true
	}
	return r.ByField(nameOrField, typ)
}

// ByID finds an index by its id.
func (r *Registry) ByID(id codec.UUID) (Store, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, e := range r.entries {
		if e.desc.IndexID == id {
			return e.store, true
		}
	}
	return nil, false
}

// PreparedWrite is one document's validated, not-yet-applied update across every store.
type PreparedWrite struct {
	registry *Registry
	docID    codec.UUID
	puts     []preparedStorePut
	isDelete bool
}

type preparedStorePut struct {
	entry *registryEntry
	put   PreparedPut
}

// PrepareWrite extracts and validates docID's update for every store without mutating any.
// An error (a vector of the wrong length is a *DimensionMismatchError) means the commit must
// be rejected.
func (r *Registry) PrepareWrite(docID codec.UUID, jsonText string) (*PreparedWrite, error) {
	r.mu.RLock()
	entries := append([]*registryEntry(nil), r.entries...)
	r.mu.RUnlock()
	pw := &PreparedWrite{registry: r, docID: docID}
	for _, e := range entries {
		p, err := e.store.PrepareDocument(docID, jsonText)
		if err != nil {
			return nil, err
		}
		pw.puts = append(pw.puts, preparedStorePut{entry: e, put: p})
	}
	return pw, nil
}

// PrepareDelete is PrepareWrite's counterpart for a DeleteOp; it cannot fail.
func (r *Registry) PrepareDelete(docID codec.UUID) *PreparedWrite {
	return &PreparedWrite{registry: r, docID: docID, isDelete: true}
}

// Commit applies the prepared update to every store under commitHash and returns the hints
// that replicate it (one per store, in registration order). The registry's head advances to
// commitHash.
func (p *PreparedWrite) Commit(commitHash codec.Hash) ([]Hint, error) {
	r := p.registry
	var hints []Hint
	if p.isDelete {
		r.mu.RLock()
		entries := append([]*registryEntry(nil), r.entries...)
		r.mu.RUnlock()
		for _, e := range entries {
			if err := e.store.Delete(p.docID, commitHash); err != nil {
				return hints, err
			}
			hints = append(hints, Hint{
				IndexID: e.desc.IndexID, FieldName: e.desc.FirstField(), Type: e.desc.Type,
				Action: HintActionDelete, DocID: p.docID, CommitHash: commitHash,
			})
		}
	} else {
		for _, sp := range p.puts {
			h, err := sp.put.Apply(commitHash)
			if err != nil {
				return hints, err
			}
			hints = append(hints, h)
		}
	}
	r.MarkCommit(commitHash)
	return hints, nil
}

// ApplyWrite is PrepareWrite followed by Commit, for callers that already hold the commit
// hash (replication, rebuild).
func (r *Registry) ApplyWrite(docID codec.UUID, commitHash codec.Hash, jsonText string) ([]Hint, error) {
	pw, err := r.PrepareWrite(docID, jsonText)
	if err != nil {
		return nil, err
	}
	return pw.Commit(commitHash)
}

// ApplyDelete tombstones docID in every store at commitHash.
func (r *Registry) ApplyDelete(docID codec.UUID, commitHash codec.Hash) ([]Hint, error) {
	return r.PrepareDelete(docID).Commit(commitHash)
}

// ApplyHints applies replicated hints (Layer 16 §10) to the stores they name. Hints for
// unknown indexes are ignored: a peer may carry indexes this node does not.
func (r *Registry) ApplyHints(hints []Hint) error {
	for _, h := range hints {
		store, ok := r.ByID(h.IndexID)
		if !ok {
			continue
		}
		var err error
		switch h.Action {
		case HintActionDelete:
			err = store.Delete(h.DocID, h.CommitHash)
		default:
			err = store.Put(Entry{DocID: h.DocID, Key: h.Key, CommitHash: h.CommitHash})
		}
		if err != nil {
			return err
		}
		r.MarkCommit(h.CommitHash)
	}
	return nil
}

// MarkCommit records that every store reflects commitHash. Call it for commits that touched
// no indexed document too, so a snapshot taken afterwards is not judged stale on open.
func (r *Registry) MarkCommit(commitHash codec.Hash) {
	r.mu.Lock()
	r.head = commitHash
	r.hasHead = true
	r.mu.Unlock()
}

// Head returns the last commit the registry applied, if any.
func (r *Registry) Head() (codec.Hash, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.head, r.hasHead
}

// RebuildStats summarises a RebuildFromScan.
type RebuildStats struct {
	Documents int
	// Skipped counts (document, index) pairs whose extraction failed - a stored document with
	// a wrong-length vector cannot reject a commit that already happened, so it is left out of
	// that index. FirstError keeps the first such failure for diagnostics.
	Skipped    int
	FirstError error
}

// RebuildFromScan clears the selected stores (all when only is empty) and re-indexes every
// document scan yields at headCommit. Every entry is tagged with headCommit, which is what an
// index rebuilt from a head scan should say: its content is exactly head's.
func (r *Registry) RebuildFromScan(scan ScanFunc, headCommit codec.Hash, only []codec.UUID) (RebuildStats, error) {
	r.mu.RLock()
	var selected []*registryEntry
	for _, e := range r.entries {
		if len(only) == 0 || containsID(only, e.desc.IndexID) {
			selected = append(selected, e)
		}
	}
	r.mu.RUnlock()
	var stats RebuildStats
	if len(selected) == 0 {
		r.MarkCommit(headCommit)
		return stats, nil
	}
	for _, e := range selected {
		if err := e.store.Clear(); err != nil {
			return stats, err
		}
	}
	var applyErr error
	err := scan(func(docID codec.UUID, jsonText string) bool {
		stats.Documents++
		for _, e := range selected {
			p, err := e.store.PrepareDocument(docID, jsonText)
			if err != nil {
				stats.Skipped++
				if stats.FirstError == nil {
					stats.FirstError = fmt.Errorf("index %s, document %s: %w", e.desc.IndexName(), docID, err)
				}
				continue
			}
			if _, err := p.Apply(headCommit); err != nil {
				applyErr = err
				return false
			}
		}
		return true
	})
	if err != nil {
		return stats, err
	}
	if applyErr != nil {
		return stats, applyErr
	}
	r.MarkCommit(headCommit)
	return stats, nil
}

func containsID(ids []codec.UUID, id codec.UUID) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

// SaveAll persists the catalog and every store's snapshot under dir
// (`<dir>/catalog.json`, `<dir>/<indexId>/{manifest.json,snapshot.bin}`), each file atomically.
// The manifests record the registry head, or the DAG head when nothing has been applied yet.
func (r *Registry) SaveAll(dir string) error {
	head, ok := r.Head()
	if !ok {
		h, err := r.dag.Head()
		if err != nil {
			return err
		}
		head = h
	}
	if err := SaveCatalog(dir, r.Catalog()); err != nil {
		return err
	}
	r.mu.RLock()
	entries := append([]*registryEntry(nil), r.entries...)
	r.mu.RUnlock()
	for _, e := range entries {
		if err := SaveStoreSnapshot(IndexDir(dir, e.desc.IndexID), e.store, head); err != nil {
			return err
		}
	}
	return nil
}

// LoadReport says what LoadRegistry found on disk.
type LoadReport struct {
	CatalogFound bool
	// Fresh stores were restored from a snapshot taken at the current DAG head.
	Fresh []Descriptor
	// Stale stores had no snapshot, an unreadable one, or one taken at a different head; they
	// are registered but empty and must be rebuilt (RebuildFromScan with StaleIDs()).
	Stale []Descriptor
}

// StaleIDs returns the ids of the stores that need RebuildFromScan.
func (l LoadReport) StaleIDs() []codec.UUID {
	out := make([]codec.UUID, len(l.Stale))
	for i, d := range l.Stale {
		out[i] = d.IndexID
	}
	return out
}

// LoadRegistry opens a namespace's registry from dir: reads the catalog (a missing catalog
// yields an empty registry), creates every store, and restores each from its snapshot when
// the snapshot's headCommitHex equals the DAG head. Anything else is reported Stale for the
// caller to rebuild by scan (Layer 16 §6.5).
func LoadRegistry(dir, namespaceID string, d *dag.InMemoryCommitDag, factory StoreFactory) (*Registry, LoadReport, error) {
	r := NewRegistry(namespaceID, d, factory)
	var report LoadReport
	cat, err := LoadCatalog(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return r, report, nil
		}
		return nil, report, err
	}
	report.CatalogFound = true
	dagHead, err := d.Head()
	if err != nil {
		return nil, report, err
	}
	allFresh := true
	for _, desc := range cat.Indexes {
		store, err := r.Add(desc)
		if err != nil {
			return nil, report, err
		}
		m, err := LoadStoreSnapshot(IndexDir(dir, desc.IndexID), store)
		if err != nil || m.HeadCommitHex != dagHead.Hex() {
			_ = store.Clear()
			report.Stale = append(report.Stale, desc)
			allFresh = false
			continue
		}
		report.Fresh = append(report.Fresh, desc)
	}
	if allFresh && len(cat.Indexes) > 0 {
		r.MarkCommit(dagHead)
	}
	return r, report, nil
}
