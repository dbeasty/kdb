package peersync

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	kdbjson "github.com/limidus/kdb/go/kdb/json"
)

// Resolution chains: a namespace's ordered rules for settling a same-document conflict during a
// merge, tried in order until one decides.
//
// Every rule is a pure function of what both nodes merging the same two heads hold alike - the
// two values, their common base, and the commits that wrote them - and is symmetric in the two
// sides. That is what keeps merge commits identical across nodes: a merge's hash covers its
// result, so two nodes whose rules decide differently build different merges and never converge.
// For the same reason a chain must be the same on every node; it is replicated as a definition
// (server.MetaStore), and peers compare chain hashes before they merge (ResolutionChain.Hash).
//
// Resolution that needs arbitrary logic - an application's business rules, a person - does not
// belong here. It happens after the merge: the conflict is queued and its resolution is an
// ordinary commit, which replicates like any write.

// Rule kinds.
const (
	// RuleSourcePriority prefers the value written by the higher-ranked node: Nodes lists node ids,
	// highest first; a node not listed ranks below every listed one. Equal ranks do not decide.
	RuleSourcePriority = "source-priority"
	// RuleValidity prefers the value that passes the namespace's schema when the other does not.
	// A deleted side is not judged, so a delete against a write does not decide.
	RuleValidity = "validity"
	// RuleFieldMerge merges two JSON objects field by field against their common base: a field
	// only one side changed takes that side's value. Top-level fields only; a field both sides
	// changed differently, or a side that is not an object (or is deleted), does not decide.
	RuleFieldMerge = "field-merge"
	// RuleLastWrite prefers the later write: by commit timestamp, then by hash. Always decides.
	RuleLastWrite = "last-write"
	// RuleQueue stops the chain: whatever is still undecided is reported and queued for the
	// application or an operator, regardless of the namespace's conflict policy.
	RuleQueue = "queue"
)

// ResolutionRule is one step of a chain.
type ResolutionRule struct {
	Kind string `json:"kind"`
	// Nodes ranks node ids for RuleSourcePriority, highest first.
	Nodes []string `json:"nodes,omitempty"`
}

// ResolutionChain is a namespace's ordered resolution rules.
type ResolutionChain struct {
	Rules []ResolutionRule `json:"rules"`
}

// Validate reports a chain that cannot run: an unknown rule, a source-priority rule without
// valid node ids, or a rule after a terminal one.
func (c ResolutionChain) Validate() error {
	for i, r := range c.Rules {
		switch r.Kind {
		case RuleSourcePriority:
			if len(r.Nodes) == 0 {
				return fmt.Errorf("rule %d: %s needs at least one node", i, r.Kind)
			}
			seen := map[codec.UUID]bool{}
			for _, n := range r.Nodes {
				id, err := codec.ParseUUID(n)
				if err != nil {
					return fmt.Errorf("rule %d: %q is not a node id: %v", i, n, err)
				}
				if seen[id] {
					return fmt.Errorf("rule %d: node %s is listed twice", i, n)
				}
				seen[id] = true
			}
		case RuleValidity, RuleFieldMerge:
		case RuleLastWrite, RuleQueue:
			if i != len(c.Rules)-1 {
				return fmt.Errorf("rule %d: %s always ends the chain, so no rule may follow it", i, r.Kind)
			}
		default:
			return fmt.Errorf("rule %d: unknown kind %q", i, r.Kind)
		}
		if r.Kind != RuleSourcePriority && len(r.Nodes) > 0 {
			return fmt.Errorf("rule %d: only %s takes nodes", i, RuleSourcePriority)
		}
	}
	return nil
}

// Hash identifies the chain's behaviour: two chains with the same hash decide every conflict
// alike. Empty for no chain, so a node without one and a node with an empty one agree.
func (c *ResolutionChain) Hash() string {
	if c == nil || len(c.Rules) == 0 {
		return ""
	}
	b, _ := json.Marshal(c)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// conflictSides is what a chain sees of one conflicting document, in canonical order: side 0 is
// the merge's first parent's (the lower head hash), so evaluation does not depend on which node
// is doing it.
type conflictSides struct {
	body   [2]*string
	origin [2]codec.Hash
	base   *string
}

// resolve runs the chain over one document. decided is false when no rule settled it - either the
// chain ran out, or it reached RuleQueue (queued is then true).
func (c *ResolutionChain) resolve(d *dag.InMemoryCommitDag, s conflictSides, valid func(string) bool) (winner *string, decided, queued bool) {
	for _, r := range c.Rules {
		switch r.Kind {
		case RuleSourcePriority:
			if i, ok := sourcePriority(d, r.Nodes, s.origin); ok {
				return s.body[i], true, false
			}
		case RuleValidity:
			if valid == nil || s.body[0] == nil || s.body[1] == nil {
				continue
			}
			v0, v1 := valid(*s.body[0]), valid(*s.body[1])
			if v0 != v1 {
				if v0 {
					return s.body[0], true, false
				}
				return s.body[1], true, false
			}
		case RuleFieldMerge:
			if merged, ok := fieldMerge(s.base, s.body[0], s.body[1]); ok {
				return &merged, true, false
			}
		case RuleLastWrite:
			if originLater(d, s.origin[0], s.origin[1]) {
				return s.body[0], true, false
			}
			return s.body[1], true, false
		case RuleQueue:
			return nil, false, true
		}
	}
	return nil, false, false
}

// sourcePriority returns the side whose origin was written by the higher-ranked node.
func sourcePriority(d *dag.InMemoryCommitDag, nodes []string, origin [2]codec.Hash) (int, bool) {
	rank := func(o codec.Hash) int {
		c, ok := d.GetCommit(o)
		if !ok {
			return len(nodes)
		}
		for i, n := range nodes {
			if id, err := codec.ParseUUID(n); err == nil && id == c.AuthorNodeID {
				return i
			}
		}
		return len(nodes)
	}
	r0, r1 := rank(origin[0]), rank(origin[1])
	switch {
	case r0 < r1:
		return 0, true
	case r1 < r0:
		return 1, true
	}
	return 0, false
}

// fieldMerge three-way merges two JSON objects' top-level fields against base (absent base: an
// empty object). The result's key order is side 0's, then side 1's additions, so it is the same
// on every node.
func fieldMerge(base, a, b *string) (string, bool) {
	if a == nil || b == nil {
		return "", false
	}
	oa, ok := parseObject(*a)
	if !ok {
		return "", false
	}
	ob, ok := parseObject(*b)
	if !ok {
		return "", false
	}
	obase := kdbjson.ObjectValue{Fields: map[string]kdbjson.Value{}}
	if base != nil {
		if obase, ok = parseObject(*base); !ok {
			return "", false
		}
	}
	text := func(o kdbjson.ObjectValue, k string) (string, bool) {
		v, ok := o.Fields[k]
		if !ok {
			return "", false
		}
		return kdbjson.ToJSONString(v), true
	}
	out := kdbjson.ObjectValue{Fields: map[string]kdbjson.Value{}}
	keys := append(append(append([]string{}, oa.Keys...), ob.Keys...), obase.Keys...)
	for _, k := range keys {
		if _, done := out.Fields[k]; done {
			continue
		}
		ta, inA := text(oa, k)
		tb, inB := text(ob, k)
		tbase, inBase := text(obase, k)
		var from *kdbjson.ObjectValue
		switch {
		case inA == inB && ta == tb:
			from = &oa
		case inA == inBase && ta == tbase:
			from = &ob // only b changed it
		case inB == inBase && tb == tbase:
			from = &oa // only a changed it
		default:
			return "", false // both changed it, differently
		}
		if v, ok := from.Fields[k]; ok {
			out.Keys = append(out.Keys, k)
			out.Fields[k] = v
		} else {
			out.Fields[k] = nil // removed; marks the key as decided without emitting it
		}
	}
	for k, v := range out.Fields {
		if v == nil {
			delete(out.Fields, k)
		}
	}
	return kdbjson.ToJSONString(out), true
}

func parseObject(s string) (kdbjson.ObjectValue, bool) {
	v, err := kdbjson.ParseValue(s)
	if err != nil {
		return kdbjson.ObjectValue{}, false
	}
	o, ok := v.(kdbjson.ObjectValue)
	return o, ok
}
