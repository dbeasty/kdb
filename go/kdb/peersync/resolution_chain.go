package peersync

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

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
	// RuleAuthority stops the chain and hands whatever is still undecided to a resolver authority
	// - an application service with its own business rules, or a person - holding the "resolve"
	// permission. Pending says what the namespace holds meanwhile: PendingHold (the default)
	// leaves the conflict unmerged, like RuleQueue; PendingProvisional merges now by last write
	// and marks the merge, so the authority can overrule it later with an ordinary write. Node,
	// when set, is the one node that notifies the authority; every node still records the
	// conflict and closes it when the resolution replicates in.
	RuleAuthority = "authority"
)

// Pending modes of RuleAuthority.
const (
	PendingHold        = "hold"
	PendingProvisional = "provisional"
)

// ResolutionRule is one step of a chain.
type ResolutionRule struct {
	Kind string `json:"kind"`
	// Nodes ranks node ids for RuleSourcePriority, highest first.
	Nodes []string `json:"nodes,omitempty"`
	// Node is RuleAuthority's notifying node; empty means every node notifies.
	Node string `json:"node,omitempty"`
	// Pending is RuleAuthority's mode: PendingHold (default) or PendingProvisional.
	Pending string `json:"pending,omitempty"`
	// Timeout, for RuleAuthority, is how long a conflict waits for the authority (a Go duration,
	// "24h") before its fallback becomes final: a held conflict is settled by last write, a
	// provisional decision simply stands. Empty waits forever.
	Timeout string `json:"timeout,omitempty"`
}

// TimeoutDuration is Timeout parsed; zero when unset or invalid (Validate refuses invalid).
func (r *ResolutionRule) TimeoutDuration() time.Duration {
	if r == nil || r.Timeout == "" {
		return 0
	}
	d, err := time.ParseDuration(r.Timeout)
	if err != nil || d <= 0 {
		return 0
	}
	return d
}

// ResolutionChain is a namespace's ordered resolution rules.
type ResolutionChain struct {
	Rules []ResolutionRule `json:"rules"`
	// AllowUnrelated lets the namespace merge a peer's history that shares no commit with its
	// own - another database, or a copy bootstrapped from a snapshot whose source is gone
	// (docs/kdb-distributed-self-healing-research.md, Phase 11). The merge takes the empty tree
	// as the base: a document only one side holds is adopted, one both hold differently is a
	// conflict for the rules. Off, such a sync is refused as unrelated history. Part of the
	// chain, and so of its hash, because two nodes that disagree on it build different graphs.
	AllowUnrelated bool `json:"allowUnrelated,omitempty"`
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
		case RuleLastWrite, RuleQueue, RuleAuthority:
			if i != len(c.Rules)-1 {
				return fmt.Errorf("rule %d: %s always ends the chain, so no rule may follow it", i, r.Kind)
			}
		default:
			return fmt.Errorf("rule %d: unknown kind %q", i, r.Kind)
		}
		if r.Kind != RuleSourcePriority && len(r.Nodes) > 0 {
			return fmt.Errorf("rule %d: only %s takes nodes", i, RuleSourcePriority)
		}
		if r.Kind == RuleAuthority {
			switch r.Pending {
			case "", PendingHold, PendingProvisional:
			default:
				return fmt.Errorf("rule %d: pending must be %q or %q, not %q", i, PendingHold, PendingProvisional, r.Pending)
			}
			if r.Node != "" {
				if _, err := codec.ParseUUID(r.Node); err != nil {
					return fmt.Errorf("rule %d: %q is not a node id: %v", i, r.Node, err)
				}
			}
			if r.Timeout != "" {
				if d, err := time.ParseDuration(r.Timeout); err != nil || d <= 0 {
					return fmt.Errorf("rule %d: timeout %q is not a positive duration", i, r.Timeout)
				}
			}
		} else if r.Node != "" || r.Pending != "" || r.Timeout != "" {
			return fmt.Errorf("rule %d: only %s takes node, pending and timeout", i, RuleAuthority)
		}
	}
	return nil
}

// Authority returns the chain's authority rule, or nil when it has none.
func (c *ResolutionChain) Authority() *ResolutionRule {
	if c == nil || len(c.Rules) == 0 {
		return nil
	}
	if last := c.Rules[len(c.Rules)-1]; last.Kind == RuleAuthority {
		return &last
	}
	return nil
}

// AllowsUnrelated reports whether the chain lets unrelated histories merge.
func (c *ResolutionChain) AllowsUnrelated() bool { return c != nil && c.AllowUnrelated }

// Notifies reports whether node is one that tells the authority about this chain's conflicts.
func (r *ResolutionRule) Notifies(node string) bool {
	return r != nil && r.Kind == RuleAuthority && (r.Node == "" || r.Node == node)
}

// Hash identifies the chain's behaviour: two chains with the same hash decide every conflict
// alike. Empty for no chain, so a node without one and a node with an empty one agree.
func (c *ResolutionChain) Hash() string {
	if c == nil || (len(c.Rules) == 0 && !c.AllowUnrelated) {
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

// chainOutcome is what a chain made of one document.
type chainOutcome struct {
	winner *string
	// fork, when set, is the extra document that keeps the side winner displaced.
	fork *ForkDoc
	// decided: winner is the merged value. provisional additionally marks it as the authority's
	// to overrule.
	decided, provisional bool
	// queued: the chain ended in RuleQueue or a holding RuleAuthority; report it.
	queued bool
}

// resolve runs the chain over one document. Neither decided nor queued means the chain ran out.
func (c *ResolutionChain) resolve(d *dag.InMemoryCommitDag, s conflictSides, valid func(string) bool) chainOutcome {
	for _, r := range c.Rules {
		switch r.Kind {
		case RuleSourcePriority:
			if i, ok := sourcePriority(d, r.Nodes, s.origin); ok {
				return chainOutcome{winner: s.body[i], decided: true}
			}
		case RuleValidity:
			if valid == nil || s.body[0] == nil || s.body[1] == nil {
				continue
			}
			v0, v1 := valid(*s.body[0]), valid(*s.body[1])
			if v0 != v1 {
				if v0 {
					return chainOutcome{winner: s.body[0], decided: true}
				}
				return chainOutcome{winner: s.body[1], decided: true}
			}
		case RuleFieldMerge:
			if merged, ok := fieldMerge(s.base, s.body[0], s.body[1]); ok {
				return chainOutcome{winner: &merged, decided: true}
			}
		case RuleLastWrite:
			return chainOutcome{winner: lastWrite(d, s), decided: true}
		case RuleAuthority:
			if r.Pending == PendingProvisional {
				return chainOutcome{winner: lastWrite(d, s), decided: true, provisional: true}
			}
			return chainOutcome{queued: true}
		case RuleQueue:
			return chainOutcome{queued: true}
		}
	}
	return chainOutcome{}
}

func lastWrite(d *dag.InMemoryCommitDag, s conflictSides) *string {
	if originLater(d, s.origin[0], s.origin[1]) {
		return s.body[0]
	}
	return s.body[1]
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
