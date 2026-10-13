package index

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
)

// Lineage is the commit bookkeeping an index needs to keep a head state current and to answer
// reads at recent commits from that state plus a bounded window of history, without holding an
// ancestor closure or anything per commit of history. The keyed, full-text and vector stores all
// use it the same way:
//
//   - Advance moves the head cutoff to a descendant (the DAG head). Each move is a step; the
//     step's fresh commits are the new ancestors it brought in.
//   - Classify tells, for each logged event as it is applied, whether its commit is an ancestor
//     of the head cutoff and at which step it became one (cstep). Events that are not are
//     pending: if their commit later becomes an ancestor, replay semantics would apply them at
//     their original position, which an incremental state cannot, so Advance refuses.
//   - Locate places a historical cutoff x relative to the steps, and Placement.Visible says
//     whether an applied event is visible at x. A store undoes, newest first, the events that are
//     not - which reproduces replay at x exactly when they are a suffix of the applied events.
//
// Not safe for concurrent use; the owning store's lock guards it.
type Lineage struct {
	dag      *dag.InMemoryCommitDag
	cutoff   codec.Hash
	started  bool
	epoch    uint64
	fresh    map[codec.Hash]struct{}
	inCutoff func(codec.Hash) bool
	// steps are the cutoffs the head has been advanced to, oldest first, each a descendant of
	// the one before: step number stepBase+i is steps[i]. stepIdx maps a cutoff to its number.
	steps    []codec.Hash
	stepBase uint32
	stepIdx  map[codec.Hash]uint32
	pending  map[codec.Hash]struct{}
}

// NewLineage returns an empty lineage over d.
func NewLineage(d *dag.InMemoryCommitDag) *Lineage {
	l := &Lineage{dag: d}
	l.Clear()
	return l
}

// Clear forgets everything: no head, no steps, nothing pending.
func (l *Lineage) Clear() {
	l.cutoff, l.started, l.epoch = codec.Hash{}, false, 0
	l.fresh, l.inCutoff = nil, nil
	l.steps, l.stepBase = nil, 0
	l.stepIdx = make(map[codec.Hash]uint32)
	l.pending = make(map[codec.Hash]struct{})
}

// Start begins a new head at target (after a replay or restore): a single step, nothing fresh.
// Pending commits are kept; a replay that recomputes them calls ClearPending first.
func (l *Lineage) Start(target codec.Hash) {
	l.cutoff, l.started, l.epoch = target, true, l.dag.AncestryEpoch()
	l.fresh, l.inCutoff = nil, nil
	l.steps, l.stepBase = nil, 0
	l.stepIdx = make(map[codec.Hash]uint32)
	l.push(target)
}

// Started reports whether there is a head.
func (l *Lineage) Started() bool { return l.started }

// Cutoff is the head cutoff.
func (l *Lineage) Cutoff() codec.Hash { return l.cutoff }

// ClearPending forgets every pending commit.
func (l *Lineage) ClearPending() { l.pending = make(map[codec.Hash]struct{}) }

// MarkPending records commit as one whose events the head skipped.
func (l *Lineage) MarkPending(commit codec.Hash) { l.pending[commit] = struct{}{} }

// PendingCommits returns the pending commits (for snapshots).
func (l *Lineage) PendingCommits() []codec.Hash {
	out := make([]codec.Hash, 0, len(l.pending))
	for c := range l.pending {
		out = append(out, c)
	}
	return out
}

// Advance moves the head to target, a descendant of the current cutoff. It returns false - and
// leaves the lineage unchanged - when the head cannot follow incrementally: no head yet, the DAG
// rewrote ancestry since (AncestryEpoch), target does not descend from the cutoff, or it brings
// in a commit whose events were skipped. The caller then replays or gives up.
func (l *Lineage) Advance(target codec.Hash) bool {
	if !l.started || l.epoch != l.dag.AncestryEpoch() {
		return false
	}
	if l.cutoff == target {
		return true
	}
	fresh, ok := l.newAncestors(l.cutoff, target)
	if !ok {
		return false
	}
	for c := range fresh {
		if _, p := l.pending[c]; p {
			return false
		}
	}
	l.cutoff, l.fresh, l.inCutoff = target, fresh, nil
	l.push(target)
	return true
}

// newAncestors returns the ancestors of to (itself included) that are not ancestors of from, or
// false when to does not descend from from. The common case - a short chain of single-parent
// commits - is a walk of that chain; anything else (a merge, a long gap) takes two closures,
// once, and keeps neither.
func (l *Lineage) newAncestors(from, to codec.Hash) (map[codec.Hash]struct{}, bool) {
	fresh := make(map[codec.Hash]struct{}, 1)
	for cur := to; cur != from; {
		if len(fresh) >= maxAdvanceSteps {
			return l.newAncestorsByClosure(from, to)
		}
		c, ok := l.dag.GetCommit(cur)
		if !ok || len(c.ParentHashes) != 1 {
			return l.newAncestorsByClosure(from, to)
		}
		fresh[cur] = struct{}{}
		cur = c.ParentHashes[0]
	}
	return fresh, true
}

func (l *Lineage) newAncestorsByClosure(from, to codec.Hash) (map[codec.Hash]struct{}, bool) {
	toAnc := l.dag.AncestorSet(to)
	if _, ok := toAnc[from]; !ok {
		return nil, false
	}
	for c := range l.dag.AncestorSet(from) {
		delete(toAnc, c)
	}
	return toAnc, true
}

func (l *Lineage) push(cutoff codec.Hash) {
	if _, seen := l.stepIdx[cutoff]; seen {
		return
	}
	l.stepIdx[cutoff] = l.stepBase + uint32(len(l.steps))
	l.steps = append(l.steps, cutoff)
}

// Step is the current step number.
func (l *Lineage) Step() uint32 { return l.stepBase + uint32(len(l.steps)) - 1 }

// StepBase is the oldest retained step number.
func (l *Lineage) StepBase() uint32 { return l.stepBase }

// Steps is how many steps are retained.
func (l *Lineage) Steps() int { return len(l.steps) }

// Classify reports whether commit is the head cutoff or one of its ancestors, and if so the step
// at which it became one: exactly (exact) or, for a late event under a commit the head passed
// before the retained steps, an upper bound. A commit that is not is marked pending.
func (l *Lineage) Classify(commit codec.Hash) (in bool, cstep uint32, exact bool) {
	cur := l.Step()
	if _, ok := l.fresh[commit]; ok {
		return true, cur, true
	}
	if s, ok := l.stepIdx[commit]; ok {
		// A step cutoff became an ancestor at its own step: it descends from every earlier one.
		return true, s, true
	}
	if l.inCutoff == nil {
		l.inCutoff = l.dag.AncestorTest(l.cutoff)
	}
	if !l.inCutoff(commit) {
		l.pending[commit] = struct{}{}
		return false, 0, false
	}
	if l.fresh == nil || cur == l.stepBase {
		// The first retained step: everything is at or before it.
		return true, cur, true
	}
	// Not new at this step, so an ancestor of the previous step's cutoff.
	return true, cur - 1, false
}

// DoneClassifying drops the ancestor test Classify may have built for this step.
func (l *Lineage) DoneClassifying() { l.inCutoff = nil }

// TrimSteps drops the steps older than keepFrom, and the oldest beyond maxSteps, never the
// current one. A store passes as keepFrom the newest step of any event it dropped: every
// retained step must be at least that new, since a historical read that stops at a retained
// step counts on every older effect being one it sees.
func (l *Lineage) TrimSteps(keepFrom uint32, maxSteps int) {
	if keepFrom < l.stepBase {
		keepFrom = l.stepBase
	}
	if n := uint32(len(l.steps)); maxSteps > 0 && n > uint32(maxSteps) {
		if b := l.stepBase + n - uint32(maxSteps)*3/4; b > keepFrom {
			keepFrom = b
		}
	}
	if cur := l.Step(); keepFrom > cur {
		keepFrom = cur
	}
	k := int(keepFrom - l.stepBase)
	if k <= 0 {
		return
	}
	for _, c := range l.steps[:k] {
		delete(l.stepIdx, c)
	}
	l.steps = l.steps[:copy(l.steps, l.steps[k:])]
	l.stepBase = keepFrom
}

// Placement is a historical cutoff located relative to the steps (see Locate).
type Placement struct {
	jmax    int64
	stopped bool
	visited map[codec.Hash]struct{}
}

// Locate walks back from x (at most maxAdvanceSteps commits) until every path reaches a retained
// step cutoff. complete says whether nothing was ever dropped from the caller's history; without
// it a walk that reaches no step cannot place x. ok is false when x cannot be placed.
func (l *Lineage) Locate(x codec.Hash, complete bool) (Placement, bool) {
	if j, ok := l.stepIdx[x]; ok {
		return Placement{jmax: int64(j), stopped: true}, true
	}
	p := Placement{jmax: -1, visited: make(map[codec.Hash]struct{})}
	queue := []codec.Hash{x}
	for len(queue) > 0 {
		c := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if j, ok := l.stepIdx[c]; ok {
			if int64(j) > p.jmax {
				p.jmax = int64(j)
			}
			p.stopped = true
			continue
		}
		if _, seen := p.visited[c]; seen {
			continue
		}
		p.visited[c] = struct{}{}
		if len(p.visited) > maxAdvanceSteps {
			return Placement{}, false
		}
		if commit, ok := l.dag.GetCommit(c); ok {
			queue = append(queue, commit.ParentHashes...)
		}
	}
	// An event the head skipped, under a commit x sees, would be applied by a replay at x.
	for c := range p.visited {
		if _, pend := l.pending[c]; pend {
			return Placement{}, false
		}
	}
	if !p.stopped && !complete {
		return Placement{}, false
	}
	return p, true
}

// Visibility of one applied event at a placement.
const (
	VisibleAndOlder = iota // visible, and so is every event applied before it
	Visible
	Invisible
	Unknown // cannot be placed: the read must fall back
)

// Visible classifies an applied event (applied at step, its commit an ancestor since cstep) at
// the placement. If the newest step reached is j, an event whose commit became an ancestor at a
// step <= j is visible: its commit is an ancestor of step j's cutoff, which is an ancestor of x.
// One that became an ancestor later is not an ancestor of step j's cutoff, so it is visible
// exactly when the walk visited its commit. If the walk reached no step it saw x's whole
// ancestry, which places every event.
func (p Placement) Visible(commit codec.Hash, step, cstep uint32, exact bool) int {
	switch {
	case p.stopped && int64(step) <= p.jmax:
		return VisibleAndOlder
	case !p.stopped || (int64(cstep) > p.jmax && exact):
		if _, ok := p.visited[commit]; ok {
			return Visible
		}
		return Invisible
	case int64(cstep) <= p.jmax:
		return Visible
	default:
		return Unknown
	}
}
