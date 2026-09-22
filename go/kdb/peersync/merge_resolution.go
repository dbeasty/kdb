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
	// base is the value at the canonical common ancestor, filled only for conflicting documents.
	base *string
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
	var provisional []codec.UUID
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
		if opts.Chain != nil || opts.Resolver != nil {
			// The base comes from the ancestor found in canonical parent order: CommonAncestor
			// walks from its second argument, so with several nearest ancestors (a criss-cross)
			// the local/incoming order would let two nodes pick different bases.
			p0, p1 := mergeCommitParents(localHead, incomingHead)
			if anc := d.CommonAncestor(p0, p1); anc != nil {
				cids := make([]codec.UUID, len(conflicting))
				for i, dd := range conflicting {
					cids[i] = dd.id
				}
				bv, err := vi.valuesAt(*anc, cids)
				if err != nil {
					return CommitPushOutcome{}, AdvanceStep{}, err
				}
				for i := range conflicting {
					conflicting[i].base = bv[conflicting[i].id].body
				}
			}
		}
		r := resolveConflicting(d, localHead, incomingHead, conflicting, opts)
		if r.report != nil {
			return CommitPushOutcome{Kind: OutcomeConflict, Report: r.report, Details: r.details}, AdvanceStep{}, nil
		}
		for id, body := range r.out {
			merged[id] = body
		}
		provisional = r.provisional
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
	mergeCommit, err := mergeNonConflicting(d, store, namespaceID, localHead, incomingHead, *ancestor, storageWrites, commitOps, mergeMessageFor(provisional))
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

// resolution is what resolveConflicting made of a merge's conflicting documents: either the
// merged values (out, with provisional naming those an authority may still overrule), or a
// report of the documents left undecided with a detail for each.
type resolution struct {
	out         map[codec.UUID]*string
	provisional []codec.UUID
	report      *kdberr.ConflictReport
	details     []ConflictDetail
}

// ConflictDetail is what a resolver needs about one undecided document beyond its two values:
// the value they both started from and which writes produced each side.
type ConflictDetail struct {
	DocumentID     string                     `json:"documentId"`
	Base           *string                    `json:"base,omitempty"`
	LocalOrigin    transaction.ConflictOrigin `json:"localOrigin"`
	IncomingOrigin transaction.ConflictOrigin `json:"incomingOrigin"`
}

// resolveConflicting decides each genuinely conflicting document per the resolution options,
// or reports the ones left undecided. Each document goes to the first of these that decides it:
// an explicit choice (Choose), the namespace's chain, then the policy. Every rule is symmetric in
// the two sides, so both nodes reach the same answer.
func resolveConflicting(d *dag.InMemoryCommitDag, localHead, incomingHead codec.Hash, docs []divergedDoc, opts ResolutionOptions) resolution {
	out := map[codec.UUID]*string{}
	var provisional []codec.UUID
	pending := docs
	if opts.Choose != nil {
		var rest []divergedDoc
		for _, dd := range pending {
			if op, ok := opts.Choose(dd.id, bodyOp(dd.id, dd.local), bodyOp(dd.id, dd.remote)); ok {
				out[dd.id] = opBody(op)
			} else {
				rest = append(rest, dd)
			}
		}
		pending = rest
	}
	p0, _ := mergeCommitParents(localHead, incomingHead)
	localFirst := p0 == localHead
	var queued []divergedDoc
	if opts.Chain != nil && len(pending) > 0 {
		var rest []divergedDoc
		for _, dd := range pending {
			o := opts.Chain.resolve(d, canonicalSides(dd, localFirst), opts.Valid)
			switch {
			case o.decided:
				out[dd.id] = o.winner
				if o.provisional {
					provisional = append(provisional, dd.id)
				}
			case o.queued:
				queued = append(queued, dd)
			default:
				rest = append(rest, dd)
			}
		}
		pending = rest
	}
	if len(pending) > 0 {
		switch opts.Policy {
		case transaction.ConflictPolicyLastWrite:
			// The later origin wins - the write that actually happened later, not whichever side
			// is being applied. Commits are stamped after their parents, so a write made after
			// seeing another counts as later whatever the clocks say. Ties break on the origin's
			// hash.
			for _, dd := range pending {
				if originLater(d, dd.localOrig, dd.remoteOrig) {
					out[dd.id] = dd.local
				} else {
					out[dd.id] = dd.remote
				}
			}
			pending = nil
		case transaction.ConflictPolicyCustom:
			if opts.Resolver != nil {
				decided := map[codec.UUID]*string{}
				ok := true
				for _, dd := range pending {
					// Canonical order: Existing is the merge's first parent's side.
					s := canonicalSides(dd, localFirst)
					existingOp, incomingOp := bodyOp(dd.id, s.body[0]), bodyOp(dd.id, s.body[1])
					conflict := transaction.DocumentConflict{
						DocID:          dd.id,
						OperationType:  classifyConflictOp(existingOp, incomingOp),
						ExistingDoc:    documentFromOp(dd.id, existingOp),
						IncomingDoc:    documentFromOp(dd.id, incomingOp),
						ExistingOrigin: conflictOrigin(d, s.origin[0]),
						IncomingOrigin: conflictOrigin(d, s.origin[1]),
					}
					if dd.base != nil {
						conflict.BaseDoc = &document.Document{ID: dd.id, JSON: *dd.base}
					}
					res, err := opts.Resolver.Resolve(conflict)
					if err != nil || res == nil {
						ok = false
						break
					}
					b := res.JSON
					decided[dd.id] = &b
				}
				if ok {
					for id, b := range decided {
						out[id] = b
					}
					pending = nil
				}
			}
		}
	}
	reported := append(queued, pending...)
	if len(reported) == 0 {
		sort.Slice(provisional, func(i, j int) bool { return provisional[i].String() < provisional[j].String() })
		return resolution{out: out, provisional: provisional}
	}
	sort.Slice(reported, func(i, j int) bool { return reported[i].id.String() < reported[j].id.String() })
	items := make([]kdberr.ConflictItem, 0, len(reported))
	details := make([]ConflictDetail, 0, len(reported))
	for _, dd := range reported {
		items = append(items, kdberr.ConflictItem{
			DocumentID:    dd.id.String(),
			OperationType: classifyConflictOp(bodyOp(dd.id, dd.local), bodyOp(dd.id, dd.remote)),
			LocalDoc:      dd.local,
			IncomingDoc:   dd.remote,
		})
		details = append(details, ConflictDetail{
			DocumentID: dd.id.String(), Base: dd.base,
			LocalOrigin: conflictOrigin(d, dd.localOrig), IncomingOrigin: conflictOrigin(d, dd.remoteOrig),
		})
	}
	return resolution{
		report: &kdberr.ConflictReport{
			TransactionID: incomingHead.Hex(), BaseHash: localHead.Hex(), TargetHash: incomingHead.Hex(), Conflicts: items,
		},
		details: details,
	}
}

// canonicalSides orders a conflicting document's sides by the merge's parents: side 0 is the
// first parent's.
func canonicalSides(dd divergedDoc, localFirst bool) conflictSides {
	s := conflictSides{body: [2]*string{dd.local, dd.remote}, origin: [2]codec.Hash{dd.localOrig, dd.remoteOrig}, base: dd.base}
	if !localFirst {
		s.body[0], s.body[1] = s.body[1], s.body[0]
		s.origin[0], s.origin[1] = s.origin[1], s.origin[0]
	}
	return s
}

// conflictOrigin describes the write o for a resolver.
func conflictOrigin(d *dag.InMemoryCommitDag, o codec.Hash) transaction.ConflictOrigin {
	c, ok := d.GetCommit(o)
	if !ok {
		return transaction.ConflictOrigin{}
	}
	return transaction.ConflictOrigin{NodeID: c.AuthorNodeID, Commit: o, TimestampMicros: c.Timestamp.EpochMicros()}
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
