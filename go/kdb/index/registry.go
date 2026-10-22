package index

import (
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

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

	// genSeq hands out store generations. It is registry-wide, so a store created under an id
	// that an earlier (dropped) store used never repeats that store's generation.
	genSeq atomic.Uint64
	// epoch changes (under mu) whenever a store is added or removed. A write prepared at another
	// epoch is prepared again when it commits (PreparedWrite.Commit).
	epoch atomic.Uint64
}

type registryEntry struct {
	desc  Descriptor
	store DocumentStore
	// gen changes whenever the registry changes the store's content (a commit, a hint, a
	// rebuild). Equal generations mean equal content, which is what lets a snapshot writer
	// skip re-serializing a store that has not changed since it was last written.
	gen atomic.Uint64
}

// touch records that e's content changed.
func (r *Registry) touch(e *registryEntry) { e.gen.Store(r.genSeq.Add(1)) }

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
	defer r.epoch.Add(1)
	for _, e := range r.entries {
		if e.desc.IndexID == desc.IndexID {
			return nil, fmt.Errorf("index %s already registered", desc.IndexID)
		}
		if n := desc.IndexName(); n != "" && e.desc.IndexName() == n {
			return nil, fmt.Errorf("index name %q already registered", n)
		}
		// The same fields, in order: a composite BTREE on (a, b) and one on (a, c), or on (a) alone,
		// are different indexes, as a single field's lookups never pick a composite (ByField).
		if e.desc.Type == desc.Type && slices.Equal(e.desc.FieldPaths(), desc.FieldPaths()) && e.desc.IsSystem() == desc.IsSystem() {
			return nil, fmt.Errorf("a %s index on %s already exists", desc.Type, strings.Join(desc.FieldPaths(), ", "))
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
	e := &registryEntry{desc: desc, store: ds}
	r.touch(e)
	r.entries = append(r.entries, e)
	return store, nil
}

// Remove drops the index named name (DROP INDEX). It reports whether one was registered.
func (r *Registry) Remove(name string) (Descriptor, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.epoch.Add(1)
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

// ByField finds the index of type typ whose first field is field. System indexes are never
// returned: their keys are typed for the server's own use, not for a user's predicate. Nor is a
// composite BTREE: its keys are tuples, so a lookup of its first field alone would match nothing;
// it is read only through its prefix walk (CompositeGroups).
func (r *Registry) ByField(field string, typ IndexType) (Store, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, e := range r.entries {
		if e.desc.Type == IndexTypeBTree && len(e.desc.FieldPaths()) > 1 {
			continue
		}
		if e.desc.Type == typ && e.desc.FirstField() == field && !e.desc.IsSystem() {
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

// entryByID finds the registry entry of an index by its id.
func (r *Registry) entryByID(id codec.UUID) (*registryEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, e := range r.entries {
		if e.desc.IndexID == id {
			return e, true
		}
	}
	return nil, false
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
	// jsonText and epoch let Commit prepare the write again when stores were added or removed
	// since it was prepared: a store CREATE INDEX added after the prepare, and rebuilt by a scan
	// at a head without this commit, would otherwise never get this document's update.
	jsonText string
	epoch    uint64
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
	epoch := r.epoch.Load()
	r.mu.RUnlock()
	pw := &PreparedWrite{registry: r, docID: docID, jsonText: jsonText, epoch: epoch}
	doc := &parsedDoc{jsonText: jsonText}
	for _, e := range entries {
		p, err := doc.prepare(e.store, docID)
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
			r.touch(e)
			if err := e.store.Delete(p.docID, commitHash); err != nil {
				return hints, err
			}
			hints = append(hints, Hint{
				IndexID: e.desc.IndexID, FieldName: e.desc.FirstField(), Type: e.desc.Type,
				Action: HintActionDelete, DocID: p.docID, CommitHash: commitHash,
			})
		}
	} else {
		if p.epoch != r.epoch.Load() {
			fresh, err := r.PrepareWrite(p.docID, p.jsonText)
			if err != nil {
				return hints, err
			}
			p.puts, p.epoch = fresh.puts, fresh.epoch
		}
		for _, sp := range p.puts {
			r.touch(sp.entry)
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
		e, ok := r.entryByID(h.IndexID)
		if !ok {
			continue
		}
		store := e.store
		r.touch(e)
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
		r.touch(e)
		if err := e.store.Clear(); err != nil {
			return stats, err
		}
	}
	var applyErr error
	err := scan(func(docID codec.UUID, jsonText string) bool {
		stats.Documents++
		doc := &parsedDoc{jsonText: jsonText}
		for _, e := range selected {
			p, err := doc.prepare(e.store, docID)
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
	// The rebuilt content is head's, with no history behind it: a store that keeps history must
	// not answer a read at an older commit by replaying these entries as if they were made then.
	for _, e := range selected {
		if hf, ok := e.store.(historyForgetter); ok {
			hf.ForgetHistory()
		}
	}
	r.MarkCommit(headCommit)
	return stats, nil
}

// DocumentChange is one document a catch-up applies: its body at the target commit, or Deleted.
type DocumentChange struct {
	DocID    codec.UUID
	JSONText string
	Deleted  bool
}

// CatchUpStores brings the stores ids - restored from snapshots at an earlier commit (Behind) -
// to headCommit by applying changes, the documents that differ between the commit their content
// describes and headCommit, as of headCommit. Every entry is tagged headCommit and the stores'
// history is dropped, as after a rebuild: the intermediate commits' states were never applied, so
// a read at one of them reports ErrHistoryUnavailable rather than a wrong answer. Marks the
// registry at headCommit, so every store must be at it once this returns (the Fresh ones are).
//
// This is what lets an open after an unclean stop cost the commits since the last snapshot
// instead of a rebuild of every index from every document (qtask production plan U5).
func (r *Registry) CatchUpStores(ids []codec.UUID, headCommit codec.Hash, changes func(yield func(DocumentChange) bool) error) (RebuildStats, error) {
	r.mu.RLock()
	var selected []*registryEntry
	for _, e := range r.entries {
		if containsID(ids, e.desc.IndexID) {
			selected = append(selected, e)
		}
	}
	r.mu.RUnlock()
	var stats RebuildStats
	for _, e := range selected {
		r.touch(e)
	}
	var applyErr error
	err := changes(func(c DocumentChange) bool {
		stats.Documents++
		doc := &parsedDoc{jsonText: c.JSONText}
		for _, e := range selected {
			if c.Deleted {
				if err := e.store.Delete(c.DocID, headCommit); err != nil {
					applyErr = err
					return false
				}
				continue
			}
			p, err := doc.prepare(e.store, c.DocID)
			if err != nil {
				// As a rebuild does: a stored document the index cannot take is left out of it -
				// and out means its previous entry goes too.
				stats.Skipped++
				if stats.FirstError == nil {
					stats.FirstError = fmt.Errorf("index %s, document %s: %w", e.desc.IndexName(), c.DocID, err)
				}
				if err := e.store.Delete(c.DocID, headCommit); err != nil {
					applyErr = err
					return false
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
	if err == nil {
		err = applyErr
	}
	if err != nil {
		return stats, err
	}
	for _, e := range selected {
		if hf, ok := e.store.(historyForgetter); ok {
			hf.ForgetHistory()
		}
	}
	r.MarkCommit(headCommit)
	return stats, nil
}

// historyForgetter is a store that keeps history and can be told its content has none.
type historyForgetter interface{ ForgetHistory() }

// headRepairer is a store that can lose its head state (see MemoryStore.NeedsRebuild).
type headRepairer interface{ NeedsRebuild() bool }

// NeedingRebuild returns the ids of the stores that can no longer answer at the DAG head and
// must be rebuilt with RebuildFromScan. It brings every store that maintains a head state up to
// the DAG head first, so the commit path calling it after each commit keeps those states current
// and finds out at once when one cannot be.
func (r *Registry) NeedingRebuild() []codec.UUID {
	r.mu.RLock()
	entries := append([]*registryEntry(nil), r.entries...)
	r.mu.RUnlock()
	var out []codec.UUID
	for _, e := range entries {
		if hr, ok := e.store.(headRepairer); ok && hr.NeedsRebuild() {
			out = append(out, e.desc.IndexID)
		}
	}
	return out
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
//
// SaveAll serializes and writes every store, whatever changed; the server's write path uses
// CaptureSnapshots and WriteSnapshots instead, which skip stores that have not changed.
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

// Generations returns every store's current generation, by index id. A caller that has just
// restored stores from snapshots on disk records these as what disk holds.
func (r *Registry) Generations() map[codec.UUID]uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[codec.UUID]uint64, len(r.entries))
	for _, e := range r.entries {
		out[e.desc.IndexID] = e.gen.Load()
	}
	return out
}

// SnapshotCapture is an in-memory copy of a registry's persistable state at one head, taken by
// CaptureSnapshots and written by WriteSnapshots - possibly much later and off the write path.
type SnapshotCapture struct {
	Head   codec.Hash
	Stores []CapturedStore
}

// Changed reports whether any store in the capture carries new snapshot bytes.
func (c SnapshotCapture) Changed() bool {
	for _, cs := range c.Stores {
		if cs.Data != nil {
			return true
		}
	}
	return false
}

// CapturedStore is one store's part of a SnapshotCapture.
type CapturedStore struct {
	Desc Descriptor
	// Gen is the store generation the capture describes.
	Gen uint64
	// Data is the serialized snapshot, or nil when the store's generation equals the one the
	// caller said disk already holds - then only the manifest is rewritten, naming the new head.
	Data  []byte
	Stats map[string]float64
}

// CaptureSnapshots serializes, in memory, every store whose generation differs from saved[id]
// (the generation the caller last wrote for it; absent means never), and records the head they
// all describe. ok is false when the registry has applied no commit yet, so has no head its
// contents are known to be at.
//
// The caller must stop the registry from changing for the duration - no commit applied, no
// rebuild - or the stores may describe different commits than Head. The result is then a
// consistent cut, and writing it needs no lock at all.
func (r *Registry) CaptureSnapshots(saved map[codec.UUID]uint64) (c SnapshotCapture, ok bool, err error) {
	head, ok := r.Head()
	if !ok {
		return SnapshotCapture{}, false, nil
	}
	c.Head = head
	r.mu.RLock()
	entries := append([]*registryEntry(nil), r.entries...)
	r.mu.RUnlock()
	for _, e := range entries {
		cs := CapturedStore{Desc: e.desc, Gen: e.gen.Load(), Stats: storeStats(e.store)}
		if g, have := saved[e.desc.IndexID]; !have || g != cs.Gen {
			data, err := e.store.Snapshot()
			if err != nil {
				return SnapshotCapture{}, false, fmt.Errorf("index %s: snapshot: %w", e.desc.IndexID, err)
			}
			cs.Data = data
		}
		c.Stores = append(c.Stores, cs)
	}
	return c, true, nil
}

// WriteSnapshots writes a capture under dir, store by store, each snapshot before its manifest
// (see writeStoreSnapshot). It returns the generation now on disk for every store it finished,
// including on error, so the caller's record of what disk holds stays exact.
func WriteSnapshots(dir string, c SnapshotCapture) (map[codec.UUID]uint64, error) {
	written := make(map[codec.UUID]uint64, len(c.Stores))
	for _, cs := range c.Stores {
		if err := writeStoreSnapshot(IndexDir(dir, cs.Desc.IndexID), cs.Desc, cs.Data, cs.Stats, c.Head); err != nil {
			return written, fmt.Errorf("index %s: %w", cs.Desc.IndexID, err)
		}
		written[cs.Desc.IndexID] = cs.Gen
	}
	return written, nil
}

// LoadReport says what LoadRegistry found on disk.
type LoadReport struct {
	CatalogFound bool
	// Fresh stores were restored from a snapshot taken at the current DAG head.
	Fresh []Descriptor
	// Stale stores had no snapshot, an unreadable one, or one taken at a different head; they
	// are registered but empty and must be rebuilt (RebuildFromScan with StaleIDs()).
	Stale []Descriptor
	// Behind stores (LoadOptions.KeepBehind only) were restored from a snapshot taken at an
	// earlier commit than the DAG head: they hold that commit's content and must be brought to
	// the head (CatchUpStores) - or cleared and rebuilt - before they are searched.
	Behind []BehindStore
}

// BehindStore is a store restored at Head, a commit other than the DAG head.
type BehindStore struct {
	Descriptor Descriptor
	Head       codec.Hash
}

// LoadOptions tunes LoadRegistryWithOptions.
type LoadOptions struct {
	// KeepBehind keeps a store whose snapshot was taken at another commit the DAG holds,
	// reporting it Behind instead of clearing it as Stale. The caller must catch it up.
	KeepBehind bool
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
	return LoadRegistryWithOptions(dir, namespaceID, d, factory, LoadOptions{})
}

// LoadRegistryWithOptions is LoadRegistry with options (see LoadOptions).
func LoadRegistryWithOptions(dir, namespaceID string, d *dag.InMemoryCommitDag, factory StoreFactory, opts LoadOptions) (*Registry, LoadReport, error) {
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
		if err == nil && m.HeadCommitHex != dagHead.Hex() && opts.KeepBehind {
			if at, herr := codec.HashFromHex(m.HeadCommitHex); herr == nil && d.HasCommit(at) {
				report.Behind = append(report.Behind, BehindStore{Descriptor: desc, Head: at})
				allFresh = false
				continue
			}
		}
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

// CompositeBTrees returns the composite BTREE indexes (two fields or more), system ones excepted.
func (r *Registry) CompositeBTrees() []Store {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Store
	for _, e := range r.entries {
		if e.desc.Type == IndexTypeBTree && len(e.desc.FieldPaths()) > 1 && !e.desc.IsSystem() {
			out = append(out, e.store)
		}
	}
	return out
}
