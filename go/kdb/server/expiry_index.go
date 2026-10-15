package server

import (
	"errors"
	"log/slog"
	"math"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/index"
)

// The expiry index (qtask production plan U1).
//
// Expiry used to be answered from document bodies alone: every COUNT(*) under an expiry policy
// read every document to learn which were hidden (and a namespace-wide count of 100k documents
// with embeddings took the server past 4 GiB), and the sweeper read the whole namespace on every
// tick to find the few documents that had expired since the last one. Both only need to know
// which documents carry a deadline at or before now - an ordered index on the expiry field
// answers that without touching a body.
//
// The index is a BTREE on the expiry field whose keys are typed TIMESTAMP, so an RFC 3339 string
// and a number of epoch milliseconds - the two forms expiryTimestamp accepts - land on the same
// TimestampKey. It is a system index (index.OptionSystem): the SQL planner never picks it for a
// user's predicate, since its keys are timestamps where a user's comparison holds strings.
//
// The index only nominates candidates. Every candidate's body is read and checked with
// isExpiredAtHead, the same predicate reads apply, so a document the index files differently
// (an array of timestamps, which expiryTimestamp treats as "never expires"; a deadline within the
// last millisecond) is decided exactly as a read decides it. Candidates are the documents that
// expired since the last sweep, so this costs a handful of body reads, not a namespace.

// expiryIndexName names the system expiry index in the namespace's index catalog.
const expiryIndexName = "_kdb_expiry"

// maxExpiredForCount bounds the expired documents a count corrects for by reading them. Past it
// (a sweeper far behind, or one that cannot run on a read-only follower) the count reads the
// documents as it did before the index existed.
const maxExpiredForCount = 4096

func expiryIndexDescriptor(namespaceID, field string) index.Descriptor {
	return index.Descriptor{
		IndexID:     index.DerivedIndexID(namespaceID, expiryIndexName, index.IndexTypeBTree),
		NamespaceID: namespaceID,
		FieldName:   field,
		Fields:      []string{field},
		Type:        index.IndexTypeBTree,
		Options: map[string]string{
			index.OptionIndexName: expiryIndexName,
			index.OptionFieldType: "TIMESTAMP",
			index.OptionSystem:    "true",
		},
	}
}

// ensureExpiryIndex makes the registry hold an expiry index for the active policy's field, and
// nothing else: it builds one when a policy is set and none matches, and drops a stale one (a
// different field, or no policy). A no-op without an index registry. Building scans the namespace
// once; the index is persisted and maintained by every commit after that, like any other.
//
// Failure is logged, not returned: without the index expiry still works, by reading bodies.
func (s *KdbServerRuntime) ensureExpiryIndex() {
	p, ok := s.SQLIndexProvider().(*RegistryIndexProvider)
	if !ok || p == nil || p.runtime == nil {
		return
	}
	e := s.expirySetting()
	var existing *index.Descriptor
	for _, d := range p.registry.Descriptors() {
		if d.IndexName() == expiryIndexName && d.IsSystem() {
			dd := d
			existing = &dd
		}
	}
	if existing != nil && e != nil && existing.FirstField() == e.fieldPath {
		return
	}
	p.persistMu.Lock()
	defer p.persistMu.Unlock()
	if existing != nil {
		p.registry.Remove(expiryIndexName)
		if err := p.saveCatalogLocked(); err != nil {
			slog.Warn("expiry index: saving catalog after drop", "error", err)
		}
	}
	if e == nil {
		return
	}
	desc := expiryIndexDescriptor(s.Runtime.DefaultNamespace, e.fieldPath)
	if _, err := p.registry.Add(desc); err != nil {
		slog.Warn("expiry index: not created; expiry reads document bodies", "field", e.fieldPath, "error", err)
		return
	}
	if p.dir != "" && !p.closed {
		if err := index.InvalidateStoreSnapshot(index.IndexDir(p.dir, desc.IndexID)); err != nil {
			p.registry.Remove(expiryIndexName)
			slog.Warn("expiry index: not created; expiry reads document bodies", "field", e.fieldPath, "error", err)
			return
		}
		delete(p.saved, desc.IndexID)
	}
	if err := p.rebuildLocked([]codec.UUID{desc.IndexID}); err != nil {
		p.registry.Remove(expiryIndexName)
		slog.Warn("expiry index: build failed; expiry reads document bodies", "field", e.fieldPath, "error", err)
		return
	}
	if err := p.saveCatalogLocked(); err != nil {
		slog.Warn("expiry index: saving catalog", "error", err)
	}
}

// errTooManyExpired stops a candidate walk that passed its limit.
var errTooManyExpired = errors.New("expiry index: more expired candidates than the limit")

// expiredDocsAt returns the documents expiry hides in the live head tree treeHash, found through
// the expiry index and confirmed against their bodies. ok=false means the index cannot answer
// for this tree right now - no policy or no index, the index not yet at the DAG head, or more
// than limit candidates (limit <= 0: no limit) - and the caller must fall back to reading.
func (s *KdbServerRuntime) expiredDocsAt(treeHash codec.Hash, limit int) ([]document.Document, bool, error) {
	e := s.expirySetting()
	if e == nil {
		return nil, false, nil
	}
	p, ok := s.SQLIndexProvider().(*RegistryIndexProvider)
	if !ok || p == nil {
		return nil, false, nil
	}
	store, ok := p.registry.ByName(expiryIndexName)
	if !ok || store.Descriptor().FirstField() != e.fieldPath {
		return nil, false, nil
	}
	headCommit, commit, ok, err := s.Runtime.DAG.HeadCommit()
	if err != nil || !ok || commit.DocumentTreeHash != treeHash {
		return nil, false, err
	}
	if regHead, ok := p.registry.Head(); !ok || regHead != headCommit {
		// A commit is in the DAG but not yet applied to the stores: the index describes an
		// older tree than the one being read.
		return nil, false, nil
	}
	cutoff := s.clock().Add(-e.grace)
	// Keys are whole milliseconds, floored; a deadline in cutoff's own millisecond may fall either
	// side of it, so it is a candidate and its body decides.
	cutoffMillis := cutoff.UnixMilli()
	ids, err := store.Range(index.TimestampKey{EpochMillis: math.MinInt64}, index.TimestampKey{EpochMillis: cutoffMillis}, &headCommit, 0, true)
	if errors.Is(err, index.ErrHistoryUnavailable) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if limit > 0 && len(ids) > limit {
		return nil, false, nil
	}
	if len(ids) == 0 {
		return nil, true, nil
	}
	docs, err := s.Runtime.Storage.GetDocuments(s.Runtime.DefaultNamespace, dedupeIDs(ids), treeHash)
	if err != nil {
		return nil, false, err
	}
	out := make([]document.Document, 0, len(docs))
	for _, d := range docs {
		if d != nil && s.isExpiredAtHead(d.JSON) {
			out = append(out, *d)
		}
	}
	return out, true, nil
}

// dedupeIDs drops repeats (a multikey entry files one document under several keys), keeping order.
func dedupeIDs(ids []codec.UUID) []codec.UUID {
	seen := make(map[codec.UUID]struct{}, len(ids))
	out := ids[:0:0]
	for _, id := range ids {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
