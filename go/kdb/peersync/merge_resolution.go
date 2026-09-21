package peersync

import (
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// Divergence resolution by value and origin.
//
// For every document either side wrote since they parted, it asks two questions: what is the
// document's value at each head, and did each side *change* it - where a side changed a
// document when the write that produced its current value is not shared history. Earlier
// versions answered the second question by replaying each side's operations from one common
// ancestor, which was wrong in two ways a mesh hits routinely:
//
//   - a merge commit restates the other side's writes, so a node's own earlier write, echoed
//     back inside a peer's merge, counted as that peer changing the document - a permanent
//     spurious conflict under STRICT, and under LAST_WRITE a newer local write lost to the echo;
//   - with several lowest common ancestors (a criss-cross), history under the one not chosen
//     landed in both sides' change sets, and the two nodes chose differently, so they built
//     different merge commits.
//
// Following each value to the write that originated it - through merges, into whichever parent
// already held it - is exact and symmetric: it does not depend on which ancestor is picked, or
// on which side is local.
//
// The merge commit this produces writes every document on which its parents differ, with the
// merged value. Applied on top of either parent it yields the same tree, which is what lets
// replay adopt it from whichever parent the log reached first.

// docVal is a document's state at a commit: its body, or nil when absent, and the commit whose
// write produced it (zero when the document has never been written on that history).
type docVal struct {
	body   *string
	writer codec.Hash
}

func opBody(op document.Op) *string {
	if w, ok := op.(document.WriteOp); ok {
		b := w.Patch
		return &b
	}
	return nil
}

func sameBody(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func bodyOp(id codec.UUID, body *string) document.Op {
	if body == nil {
		return document.DeleteOp{DocID: id}
	}
	return document.WriteOp{DocID: id, Patch: *body}
}

// valueIndex answers "what is document id at commit x" under replay semantics: every commit's
// operations are the change from its first parent, so a document's value at x is the nearest
// write along x's first-parent chain. At a commit whose parents this node does not hold (a
// shallow root), the value comes from that commit's tree in storage.
type valueIndex struct {
	d     *dag.InMemoryCommitDag
	store storage.Adapter
	ns    string
}

// valuesAt resolves ids at x in one walk down the first-parent chain.
func (vi valueIndex) valuesAt(x codec.Hash, ids []codec.UUID) (map[codec.UUID]docVal, error) {
	out := make(map[codec.UUID]docVal, len(ids))
	pending := make(map[codec.UUID]bool, len(ids))
	for _, id := range ids {
		pending[id] = true
	}
	for c := x; len(pending) > 0; {
		commit, ok := vi.d.GetCommit(c)
		if !ok {
			break
		}
		ops, err := vi.d.CommitOperations(c)
		if err != nil {
			return nil, err
		}
		for _, op := range ops {
			id := opDocID(op)
			if pending[id] {
				out[id] = docVal{body: opBody(op), writer: c}
				delete(pending, id)
			}
		}
		if len(pending) == 0 {
			break
		}
		if len(commit.ParentHashes) == 0 {
			break // genesis: never written
		}
		if vi.d.IsShallow(c) || !vi.d.HasCommit(commit.ParentHashes[0]) {
			// The chain ends here without the documents' writes; this commit's tree says.
			for id := range pending {
				doc, err := vi.store.GetDocument(vi.ns, id, commit.DocumentTreeHash)
				if err != nil {
					return nil, err
				}
				if doc != nil {
					b := doc.JSON
					out[id] = docVal{body: &b, writer: c}
				}
			}
			return out, nil
		}
		c = commit.ParentHashes[0]
	}
	return out, nil
}

func (vi valueIndex) valueAt(x codec.Hash, id codec.UUID) (docVal, error) {
	m, err := vi.valuesAt(x, []codec.UUID{id})
	return m[id], err
}

// origin follows id's value at x back to the write that created it: a merge that took the value
// from one of its parents passes the question to that parent; any other writer is the origin.
// Zero means the document was never written on x's history.
func (vi valueIndex) origin(x codec.Hash, id codec.UUID) (codec.Hash, error) {
	for {
		v, err := vi.valueAt(x, id)
		if err != nil || v.writer == (codec.Hash{}) {
			return codec.Hash{}, err
		}
		w, ok := vi.d.GetCommit(v.writer)
		if !ok || len(w.ParentHashes) < 2 {
			return v.writer, nil
		}
		next := codec.Hash{}
		for _, p := range w.ParentHashes {
			if !vi.d.HasCommit(p) {
				continue
			}
			pv, err := vi.valueAt(p, id)
			if err != nil {
				return codec.Hash{}, err
			}
			if sameBody(pv.body, v.body) {
				next = p
				break
			}
		}
		if next == (codec.Hash{}) {
			return v.writer, nil // the merge resolved it to a value neither parent had
		}
		x = next
	}
}

// changed reports whether the side at head changed a document whose origin there is o: o is a
// write the other side does not have.
func changed(d *dag.InMemoryCommitDag, o, other codec.Hash) bool {
	if o == (codec.Hash{}) {
		return false
	}
	return o != other && !d.IsAncestor(o, other)
}

// divergedDoc is one document the two sides hold differently.
type divergedDoc struct {
	id                    codec.UUID
	local, remote         *string
	localOrig, remoteOrig codec.Hash
	localChanged          bool
	remoteChanged         bool
}

func resolveDivergedLocked(
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	namespaceID string,
	localHead, incomingHead codec.Hash,
	opts ResolutionOptions,
) (CommitPushOutcome, AdvanceStep, error) {
	ancestor := d.CommonAncestor(localHead, incomingHead)
	if ancestor == nil {
		return CommitPushOutcome{}, AdvanceStep{}, kdberr.NewVersionNotFoundError(
			"no common ancestor between local "+localHead.Hex()+" and incoming "+incomingHead.Hex()+
				" - a commit references a parent this node never received",
			namespaceID, incomingHead.Hex(),
		)
	}
	localCommits, err := commitsBetween(d, localHead, incomingHead)
	if err != nil {
		return CommitPushOutcome{}, AdvanceStep{}, err
	}
	remoteCommits, err := commitsBetween(d, incomingHead, localHead)
	if err != nil {
		return CommitPushOutcome{}, AdvanceStep{}, err
	}
	candidates := map[codec.UUID]struct{}{}
	for _, cs := range [][]document.Commit{localCommits, remoteCommits} {
		for _, c := range cs {
			for _, op := range c.Operations {
				if id := opDocID(op); id != (codec.UUID{}) {
					candidates[id] = struct{}{}
				}
			}
		}
	}
	ids := make([]codec.UUID, 0, len(candidates))
	for id := range candidates {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })

	vi := valueIndex{d: d, store: store, ns: namespaceID}
	vL, err := vi.valuesAt(localHead, ids)
	if err != nil {
		return CommitPushOutcome{}, AdvanceStep{}, err
	}
	vR, err := vi.valuesAt(incomingHead, ids)
	if err != nil {
		return CommitPushOutcome{}, AdvanceStep{}, err
	}

	merged := map[codec.UUID]*string{}
	var differing, conflicting []divergedDoc
	for _, id := range ids {
		l, r := vL[id], vR[id]
		if sameBody(l.body, r.body) {
			continue
		}
		oL, err := vi.origin(localHead, id)
		if err != nil {
			return CommitPushOutcome{}, AdvanceStep{}, err
		}
		oR, err := vi.origin(incomingHead, id)
		if err != nil {
			return CommitPushOutcome{}, AdvanceStep{}, err
		}
		dd := divergedDoc{
			id: id, local: l.body, remote: r.body, localOrig: oL, remoteOrig: oR,
			localChanged: changed(d, oL, incomingHead), remoteChanged: changed(d, oR, localHead),
		}
		differing = append(differing, dd)
		switch {
		case dd.localChanged && !dd.remoteChanged:
			merged[id] = dd.local
		case dd.remoteChanged && !dd.localChanged:
			merged[id] = dd.remote
		default:
			// Both changed it - or neither did and they still differ, which only a criss-cross
			// of earlier conflicting resolutions produces. Either way it is a real conflict.
			conflicting = append(conflicting, dd)
		}
	}

	if len(conflicting) > 0 {
		resolved, report := resolveConflicting(d, localHead, incomingHead, conflicting, opts)
		if report != nil {
			return CommitPushOutcome{Kind: OutcomeConflict, Report: report}, AdvanceStep{}, nil
		}
		for id, body := range resolved {
			merged[id] = body
		}
	}

	storageWrites := map[codec.UUID]document.Op{}
	commitOps := map[codec.UUID]document.Op{}
	for _, dd := range differing {
		m := merged[dd.id]
		commitOps[dd.id] = bodyOp(dd.id, m)
		if !sameBody(m, dd.local) {
			storageWrites[dd.id] = bodyOp(dd.id, m)
		}
	}
	mergeCommit, err := mergeNonConflicting(d, store, namespaceID, localHead, incomingHead, *ancestor, storageWrites, commitOps)
	if err != nil {
		return CommitPushOutcome{}, AdvanceStep{}, err
	}
	applied := make([]document.Op, 0, len(storageWrites))
	for _, id := range sortedDocIDs(storageWrites) {
		applied = append(applied, storageWrites[id])
	}
	step := AdvanceStep{Commits: append(remoteCommits, mergeCommit), Applied: applied}
	return CommitPushOutcome{Kind: OutcomeMerged, MergeCommit: &mergeCommit}, step, nil
}

// resolveConflicting decides each genuinely conflicting document per the resolution options,
// or reports them. Every rule is symmetric in the two sides, so both nodes reach the same
// answer.
func resolveConflicting(d *dag.InMemoryCommitDag, localHead, incomingHead codec.Hash, docs []divergedDoc, opts ResolutionOptions) (map[codec.UUID]*string, *kdberr.ConflictReport) {
	out := map[codec.UUID]*string{}
	if opts.Choose != nil {
		all := true
		for _, dd := range docs {
			op, ok := opts.Choose(dd.id, bodyOp(dd.id, dd.local), bodyOp(dd.id, dd.remote))
			if !ok {
				all = false
				break
			}
			out[dd.id] = opBody(op)
		}
		if all {
			return out, nil
		}
		out = map[codec.UUID]*string{}
	}
	switch opts.Policy {
	case transaction.ConflictPolicyLastWrite:
		// The later origin wins - the write that actually happened later, not whichever side is
		// being applied. Commits are stamped after their parents, so a write made after seeing
		// another counts as later whatever the clocks say. Ties break on the origin's hash.
		for _, dd := range docs {
			if originLater(d, dd.localOrig, dd.remoteOrig) {
				out[dd.id] = dd.local
			} else {
				out[dd.id] = dd.remote
			}
		}
		return out, nil
	case transaction.ConflictPolicyCustom:
		if opts.Resolver != nil {
			p0, _ := mergeCommitParents(localHead, incomingHead)
			ok := true
			for _, dd := range docs {
				// Canonical order: Existing is the merge's first parent's side.
				existing, incoming := dd.local, dd.remote
				if p0 != localHead {
					existing, incoming = incoming, existing
				}
				existingOp, incomingOp := bodyOp(dd.id, existing), bodyOp(dd.id, incoming)
				res, err := opts.Resolver.Resolve(transaction.DocumentConflict{
					DocID:         dd.id,
					OperationType: classifyConflictOp(existingOp, incomingOp),
					ExistingDoc:   documentFromOp(dd.id, existingOp),
					IncomingDoc:   documentFromOp(dd.id, incomingOp),
				})
				if err != nil || res == nil {
					ok = false
					break
				}
				b := res.JSON
				out[dd.id] = &b
			}
			if ok {
				return out, nil
			}
		}
	}
	items := make([]kdberr.ConflictItem, 0, len(docs))
	for _, dd := range docs {
		items = append(items, kdberr.ConflictItem{
			DocumentID:    dd.id.String(),
			OperationType: classifyConflictOp(bodyOp(dd.id, dd.local), bodyOp(dd.id, dd.remote)),
			LocalDoc:      dd.local,
			IncomingDoc:   dd.remote,
		})
	}
	return nil, &kdberr.ConflictReport{
		TransactionID: incomingHead.Hex(), BaseHash: localHead.Hex(), TargetHash: incomingHead.Hex(), Conflicts: items,
	}
}

// originLater reports whether origin a is later than b: by commit timestamp, then by hash. A
// document never written (zero origin) is earliest.
func originLater(d *dag.InMemoryCommitDag, a, b codec.Hash) bool {
	ts := func(h codec.Hash) int64 {
		if c, ok := d.GetCommit(h); ok {
			return c.Timestamp.EpochMicros()
		}
		return -1
	}
	ta, tb := ts(a), ts(b)
	if ta != tb {
		return ta > tb
	}
	return a.Hex() > b.Hex()
}
