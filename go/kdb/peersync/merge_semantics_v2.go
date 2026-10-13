package peersync

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	kdbjson "github.com/limidus/kdb/go/kdb/json"
)

// The merge semantics of ResolutionChain version 2 (ChainVersionMergeBases), which #130 found
// version 1 needed: the base a rule is handed (F1), field-merge's fallback (F2) and validity
// without the schema (F4, in resolve). Version 1 chains never reach this file.

// fieldMergeByOrigin is field-merge under ChainVersionMergeBases. A top-level field on which the
// sides differ goes to the side that changed it, where a side changed it when the write that set
// its value there (the field's origin, followed through merges) is not history the other side
// holds - the per-document rule of resolveDivergedLocked, applied per field. It needs no base,
// so a stale one cannot mislead it (F1).
//
// A field both sides changed is a collision. With last-write the next rule, each collision goes to
// the later of the two fields' origins - by commit timestamp, then hash - rather than the whole
// document going to the later document write (F2). Together the two make field-merge then
// last-write a last-writer-wins map: every field ends at its latest write in the union of the
// histories, whatever order the heads are merged in. Without last-write next, a collision leaves
// the document undecided for the next rule, as before.
//
// Why "changed" and "later" agree, which is what makes the result order-independent: every
// commit is stamped after its parents (dag causalTimestampLocked), so a field write a side holds
// in its history is earlier than any write made after it there. A field's value at a head is then
// its latest write in that head's history - by induction over merges, since each merge keeps the
// later of two concurrent writes and passes the other through - and the union of two histories'
// latest writes is the later of the two. The per-field outcome is max over a set, which is
// associative, commutative and idempotent.
//
// The result's key order is side 0's keys, then side 1's additions, as fieldMerge's.
func fieldMergeByOrigin(d *dag.InMemoryCommitDag, s conflictSides, lastWrite bool) (string, bool, error) {
	if s.body[0] == nil || s.body[1] == nil {
		return "", false, nil
	}
	oa, ok := parseObject(*s.body[0])
	if !ok {
		return "", false, nil
	}
	ob, ok := parseObject(*s.body[1])
	if !ok {
		return "", false, nil
	}
	out := kdbjson.ObjectValue{Fields: map[string]kdbjson.Value{}}
	seen := map[string]bool{}
	below0, below1 := d.AncestorTest(s.heads[0]), d.AncestorTest(s.heads[1])
	keys := append(append([]string{}, oa.Keys...), ob.Keys...)
	for _, k := range keys {
		if seen[k] {
			continue
		}
		seen[k] = true
		va, inA := oa.Fields[k]
		vb, inB := ob.Fields[k]
		from := 0
		if inA != inB || (inA && kdbjson.ToJSONString(va) != kdbjson.ToJSONString(vb)) {
			o0, err := s.fieldOrigin(0, k)
			if err != nil {
				return "", false, err
			}
			o1, err := s.fieldOrigin(1, k)
			if err != nil {
				return "", false, err
			}
			ch0, ch1 := changed(below1, o0, s.heads[1]), changed(below0, o1, s.heads[0])
			switch {
			case ch0 && !ch1:
				from = 0
			case ch1 && !ch0:
				from = 1
			case !lastWrite:
				return "", false, nil
			case originLater(d, o0, o1):
				from = 0
			default:
				from = 1
			}
		}
		v, in := va, inA
		if from == 1 {
			v, in = vb, inB
		}
		if in {
			out.Keys = append(out.Keys, k)
			out.Fields[k] = v
		}
	}
	return kdbjson.ToJSONString(out), true, nil
}

// fieldText is field key of body as JSON text, and whether it is there at all. A deleted
// document, or one that is not an object, has no fields.
func fieldText(body *string, key string) (string, bool) {
	if body == nil {
		return "", false
	}
	o, ok := parseObject(*body)
	if !ok {
		return "", false
	}
	v, ok := o.Fields[key]
	if !ok {
		return "", false
	}
	return kdbjson.ToJSONString(v), true
}

type fieldKey struct {
	at    codec.Hash
	id    codec.UUID
	field string
}

// fieldOrigin follows field key of document id at x back to the write that set it - origin()
// for one field. At a commit whose document write left the field as its first parent had it, the
// question passes to that parent; at a merge, to every parent that holds the same field value,
// taking the latest of their answers (the same value written twice, concurrently, counts as its
// later write - which is what a last-writer-wins map keeps). A merge none of whose parents held
// the value made it, and is its origin. Zero means never set on x's history.
func (vi valueIndex) fieldOrigin(x codec.Hash, id codec.UUID, key string, memo map[fieldKey]codec.Hash) (codec.Hash, error) {
	k := fieldKey{at: x, id: id, field: key}
	if o, ok := memo[k]; ok {
		return o, nil
	}
	v, err := vi.valueAt(x, id)
	if err != nil || v.writer == (codec.Hash{}) {
		return codec.Hash{}, err
	}
	text, in := fieldText(v.body, key)
	same := func(p codec.Hash) (bool, error) {
		pv, err := vi.valueAt(p, id)
		if err != nil {
			return false, err
		}
		pt, pin := fieldText(pv.body, key)
		return pin == in && pt == text, nil
	}
	out := v.writer
	w, ok := vi.d.GetCommit(v.writer)
	if ok && len(w.ParentHashes) >= 2 {
		found := false
		for _, p := range w.ParentHashes {
			if !vi.d.HasCommit(p) {
				continue
			}
			eq, err := same(p)
			if err != nil {
				return codec.Hash{}, err
			}
			if !eq {
				continue
			}
			o, err := vi.fieldOrigin(p, id, key, memo)
			if err != nil {
				return codec.Hash{}, err
			}
			if !found || originLater(vi.d, o, out) {
				out, found = o, true
			}
		}
		if !found {
			out = v.writer
		}
	} else if ok && len(w.ParentHashes) == 1 && !vi.d.IsShallow(v.writer) && vi.d.HasCommit(w.ParentHashes[0]) {
		eq, err := same(w.ParentHashes[0])
		if err != nil {
			return codec.Hash{}, err
		}
		if eq {
			if out, err = vi.fieldOrigin(w.ParentHashes[0], id, key, memo); err != nil {
				return codec.Hash{}, err
			}
		}
	}
	memo[k] = out
	return out, nil
}

// baseVal is one document at a (possibly virtual) merge base: its value and the write it came
// from, which a rule looking at origins (last-write, source-priority) needs inside a virtual merge.
type baseVal struct {
	body   *string
	origin codec.Hash
}

// maxVirtualBaseDepth bounds the recursion of virtualBase. Each level is the merge base of the
// level above's bases, so strictly older history; real histories rarely go past two or three.
// Past the bound the first base (in hash order) stands in - still the same on every node.
const maxVirtualBaseDepth = 16

// virtualBase builds the base a merge's rules are handed, from every nearest common ancestor of
// its two heads (#130, F1). One is used as it is. Several are merged into one, as git's recursive
// strategy does: in hash order, each folded into the accumulated result with their own nearest
// common ancestors (recursively) as the base, under the namespace's chain. Every input - the
// bases, their order, the chain - is the same on every node holding the same history, so the
// virtual base is too.
//
// Only the documents the merge needs a base for are built, from the operations and trees the node
// holds (valueIndex: a first-parent walk, and a shallow root's tree where the walk ends).
type virtualBase struct {
	d    *dag.InMemoryCommitDag
	vi   valueIndex
	opts ResolutionOptions
	// held is why a document's base could not be built - a rule that could not run while merging
	// the bases. Such a document is held like any other a rule could not decide.
	held map[codec.UUID]string
	// memo holds what values returned for a given ordered list of bases at a given depth. The
	// result depends on nothing else (ids are fixed for the life of one virtualBase), so reusing
	// it is exact and node-independent. Without it the recursion re-derives the same base sets
	// once per path that reaches them: on a mesh that merges all the time, that is exponential in
	// the depth, runs the conflict rule at every step, and all of it holds the write gate - one
	// node stopped taking writes and peer syncs for minutes (5-node load, Linux). Callers only
	// read the returned map, so it is shared, not copied.
	memo map[string]map[codec.UUID]baseVal
	// noMemo computes every values call afresh, ignoring memo. Tests only: it is how they check the
	// memo changes no result.
	noMemo bool
}

// valuesKey identifies one values call: the bases are in fold order, which changes the result,
// and the depth decides whether the recursion is cut off, which changes it too.
func valuesKey(bases []codec.Hash, depth int) string {
	b := make([]byte, 0, 1+len(bases)*32)
	b = append(b, byte(depth))
	for _, h := range bases {
		b = append(b, h.Bytes[:]...)
	}
	return string(b)
}

func (vb *virtualBase) values(bases []codec.Hash, ids []codec.UUID, depth int) (map[codec.UUID]baseVal, error) {
	if len(bases) == 0 {
		return map[codec.UUID]baseVal{}, nil
	}
	if vb.noMemo {
		return vb.valuesUncached(bases, ids, depth)
	}
	key := valuesKey(bases, depth)
	if got, ok := vb.memo[key]; ok {
		return got, nil
	}
	out, err := vb.valuesUncached(bases, ids, depth)
	if err != nil {
		return nil, err
	}
	if vb.memo == nil {
		vb.memo = map[string]map[codec.UUID]baseVal{}
	}
	vb.memo[key] = out
	return out, nil
}

func (vb *virtualBase) valuesUncached(bases []codec.Hash, ids []codec.UUID, depth int) (map[codec.UUID]baseVal, error) {
	acc, err := vb.at(bases[0], ids)
	if err != nil || len(bases) == 1 || depth >= maxVirtualBaseDepth {
		return acc, err
	}
	folded := []codec.Hash{bases[0]}
	for _, b := range bases[1:] {
		bv, err := vb.at(b, ids)
		if err != nil {
			return nil, err
		}
		inner, err := vb.values(vb.d.MergeBases(folded, []codec.Hash{b}), ids, depth+1)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			acc[id] = vb.merge(id, acc[id], bv[id], inner[id])
		}
		folded = append(folded, b)
	}
	return acc, nil
}

func (vb *virtualBase) at(c codec.Hash, ids []codec.UUID) (map[codec.UUID]baseVal, error) {
	vals, err := vb.vi.valuesAt(c, ids)
	if err != nil {
		return nil, err
	}
	out := make(map[codec.UUID]baseVal, len(ids))
	for _, id := range ids {
		o, err := vb.vi.origin(c, id)
		if err != nil {
			return nil, err
		}
		out[id] = baseVal{body: vals[id].body, origin: o}
	}
	return out, nil
}

// merge three-way merges one document of two bases: a side that left it as the base had it takes
// the other's, and a real conflict goes to the chain. Whatever the chain cannot settle - it queues,
// or runs out - goes to the later write, since a base must have some value; a rule that cannot run
// holds the document.
func (vb *virtualBase) merge(id codec.UUID, a, b, base baseVal) baseVal {
	later := func() baseVal {
		if originLater(vb.d, a.origin, b.origin) {
			return a
		}
		return b
	}
	switch {
	case sameBody(a.body, b.body):
		return baseVal{body: a.body, origin: later().origin}
	case sameBody(a.body, base.body):
		return b
	case sameBody(b.body, base.body):
		return a
	}
	if vb.opts.Chain == nil {
		return later()
	}
	o := vb.opts.Chain.resolve(vb.d, id, conflictSides{
		body: [2]*string{a.body, b.body}, origin: [2]codec.Hash{a.origin, b.origin}, base: base.body,
	}, vb.opts)
	switch {
	case o.held:
		if _, ok := vb.held[id]; !ok {
			vb.held[id] = "building the merge base from several nearest common ancestors: " + o.reason
		}
		return a
	case o.decided:
		switch {
		case sameBody(o.winner, a.body):
			return a
		case sameBody(o.winner, b.body):
			return b
		}
		return baseVal{body: o.winner, origin: later().origin}
	}
	return later()
}
