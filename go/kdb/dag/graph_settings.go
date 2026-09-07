package dag

// DefaultGraphRebuildCommits is how many commits may accumulate in the
// resident tail before the on-disk graph is rebuilt, when the caller has
// not said.
//
// 50,000 rather than a round 100,000 because the tail is the part still
// held the old way: at the 519 bytes per commit measured in
// docs/kdb-commit-graph-memory.md, a full tail is about 26 MB, which sits
// alongside the other per-namespace budgets rather than dwarfing them. It
// is a memory bound expressed as a count, which is only sound because a
// graph record is fixed width - see GraphSettings.RebuildCommits.
const DefaultGraphRebuildCommits = 50_000

// GraphSettings selects how much of the on-disk commit graph work is
// active for one namespace.
//
// The zero value is the behaviour that existed before any of it: the graph
// lives entirely in the heap, ancestry walks materialize closures, and
// nothing is written or mapped. That is deliberate. Every field here turns
// something *on*, so a caller that has not been updated - and there are
// several, including the in-memory runtime and the auth registry - keeps
// exactly the behaviour it had, and any change in a measurement can be
// attributed to a flag someone set rather than to a default that moved
// underneath them.
//
// See docs/kdb-commit-graph-on-disk.md for what each of these is a phase
// of, and why the phases are ordered the way they are.
type GraphSettings struct {
	// AncestryPruning gives every commit a generation number - one more
	// than the highest of its parents' - and lets IsAncestor,
	// CommonAncestor and CommitsSince stop descending a branch once its
	// generation drops below the one they are looking for.
	//
	// Without it, IsAncestor materializes the *entire* ancestor closure of
	// the descendant and then tests one membership, which allocates a map
	// proportional to history on every call. That is the cost that a
	// million-commit namespace runs into first - before residency, and for
	// a reason unrelated to it.
	//
	// Costs 8 bytes per resident commit (a uint32 and its padding, stored
	// inline in the commit map rather than in a second map, which would
	// cost five times as much). Expected to become the default once the
	// Phase 0 measurement says what it is worth; until then it is opt-in
	// so that the before and after can be run against each other.
	AncestryPruning bool

	// FileEnabled maps the commit graph from a file instead of holding it
	// in the heap. Implies AncestryPruning: the file stores generation
	// numbers in its fixed-width records, and reading them back without
	// using them would be pure cost.
	//
	// Not yet implemented - accepted, normalized and reported so that
	// configuration, plumbing and the flag itself can land and be tested
	// ahead of the format. GraphFileActive reports whether it is actually
	// doing anything, which today is never.
	FileEnabled bool

	// RebuildCommits is how many commits may accumulate in the resident
	// tail before the graph file is rebuilt in the background. This, and
	// not the namespace's commit count, is what bounds resident graph
	// memory once FileEnabled is on.
	//
	// A count rather than a byte budget, unlike DocumentCacheBytes and the
	// rest, because a graph record is fixed width - so here a count *is* a
	// byte bound, and expressing it as bytes would only hide the division.
	// Zero means DefaultGraphRebuildCommits. Ignored when FileEnabled is
	// off, since there is no file to rebuild.
	RebuildCommits int

	// AnchorInterval is how many commits apart materialized document trees
	// are written, bounding what a cold historical read has to fold: with
	// an anchor every N commits there is always one within N below any
	// target, so the worst case stops being "fold from wherever the
	// nearest cached tree happens to be" and becomes O(N).
	//
	// Zero disables anchors, which is today's behaviour. Meaningless under
	// the objects history strategy, where every tree is already an object
	// and there is nothing to fold; callers there should leave it zero.
	AnchorInterval int
}

// normalized returns s with implications applied and out-of-range values
// brought back, so that everything downstream reads settled values instead
// of re-deriving them and disagreeing.
func (s GraphSettings) normalized() GraphSettings {
	if s.FileEnabled {
		s.AncestryPruning = true
		if s.RebuildCommits <= 0 {
			s.RebuildCommits = DefaultGraphRebuildCommits
		}
	} else {
		// Nothing to rebuild, so a value here would only be misleading if
		// something later reported it back.
		s.RebuildCommits = 0
	}
	if s.AnchorInterval < 0 {
		s.AnchorInterval = 0
	}
	return s
}

// SetGraphSettings installs the commit-graph feature settings for this DAG.
//
// Call before the DAG is shared, and before any commit is put: turning
// AncestryPruning on part-way through would leave every commit already
// stored without a generation number, and the pruned walks would then read
// zero for those and conclude they can stop. Enabling it on a populated
// DAG therefore recomputes generations for what is already there rather
// than assuming the caller got the ordering right - which costs one pass
// over the graph and is worth not making a rule nobody can check.
func (d *InMemoryCommitDag) SetGraphSettings(s GraphSettings) {
	d.mu.Lock()
	defer d.mu.Unlock()
	was := d.graph.AncestryPruning
	d.graph = s.normalized()
	if d.graph.AncestryPruning && !was {
		d.recomputeGenerationsLocked()
	}
}

// GraphSettings returns the settings this DAG is running with, normalized.
func (d *InMemoryCommitDag) GraphSettings() GraphSettings {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.graph
}

// GraphFileActive reports whether the on-disk commit graph is doing
// anything for this namespace right now.
//
// Distinct from GraphSettings().FileEnabled, which is what was *asked*
// for: the file is not implemented yet, and there will be namespaces that
// ask for it and do not get it even once it is (a platform whose IO shim
// cannot map a snapshot, a file that failed its header check and was
// discarded in favour of the log). Callers reporting status, and tests
// asserting a fallback actually fell back, want this one.
func (d *InMemoryCommitDag) GraphFileActive() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.graphFile != nil
}
