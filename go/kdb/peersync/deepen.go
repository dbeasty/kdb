package peersync

import (
	"errors"
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Deepen: fetching the history below a shallow root (docs/kdb-distributed-self-healing-research.md,
// Phase 14; git's `fetch --deepen`/`--unshallow`).
//
// A namespace bootstrapped from a snapshot holds its root commit without the root's parents. That
// costs it history reads below the root, and - worse - any sync with a node that has history of
// its own: such a node cannot store the root, whose parents it never saw (see
// UnrelatedHistoryError). Deepen asks a peer that has the history for every ancestor of the root,
// stores them, and stops treating the root as shallow. Where the peer's own history ends too, the
// commits there become this namespace's new shallow roots.
//
// Durability, in order - each step leaves a namespace that opens correctly if the process dies
// right after it:
//  1. the marker names the new horizons alongside the root: a replay that meets them in the log
//     admits them without their parents instead of failing the open;
//  2. every fetched commit, and the root itself, is logged: the history's operations are in the
//     log for anything that later reads them, and a replay stores them (none extends main);
//  3. the root stops being shallow in memory;
//  4. the marker is rewritten without the root. Empty means the history is whole again, and a
//     full replay from genesis rebuilds the namespace.
// A crash between 2 and 4 leaves the root marked shallow over history it holds - Deepen run again
// finishes the job without fetching anything.
//
// Steps 1 and 2 can both fail, and step 2's fsync is waited for after the serialization is
// dropped, so a commit can be resident in the DAG with no frame in the log. That state is not
// undone. Rolling it back would mean deleting commits admitted by PutCommit, and the DAG has no
// general commit delete - dag.DropShallowCommit, which InstallSnapshot and Graft undo a single
// admitted root with, deliberately refuses anything that is not a shallow root or that a resident
// commit names as a parent, which is most of what a deepen stores.
//
// Nor can the log be written before the DAG is: a checkpoint pairs a snapshot of the DAG with the
// log position it has been replayed through (embed.saveCheckpoint). One taken between the two
// would record a DAG that lacks commits the log has already passed, and the next open would
// replay from after their frames - losing them for good. The DAG holding at least what the log
// holds is what makes checkpointing safe.
//
// So Deepen is made re-runnable instead: what has to be in the log is read back out of the DAG
// (commitsBelow) rather than remembered from the store loop, which skips exactly the commits a
// failed attempt left resident. A re-run logs the history below the root again whether or not it
// is already there; a repeated frame costs log space and nothing else, because replay stores a
// commit only if the DAG does not already hold it (embed.applyCommitsTopologically). Paying that
// on a repair is the price of not tracking which commits are logged, and the alternative was a
// deepen that reported success having logged nothing.

// DeepenResult is what one Deepen did.
type DeepenResult struct {
	Root codec.Hash
	// Fetched counts commits newly stored. Zero with Unshallowed set means the history was
	// already here and this run only finished the job - see Deepen on re-running.
	Fetched int
	// Horizon lists the shallow roots this namespace now holds below Root: commits whose own
	// parents the peer did not have either. Read back out of the DAG rather than collected as
	// they were admitted, so a re-run after a failure reports them - and records them in the
	// marker - again instead of losing them.
	Horizon []codec.Hash
	// Unshallowed is set when the root is no longer shallow.
	Unshallowed bool
}

// ErrNotShallow is Deepen asked about a commit that is not a shallow root.
var ErrNotShallow = errors.New("peer sync: not a shallow root")

// ErrPeerLacksHistory is a peer that does not hold a shallow root's parents either - its own history
// ends there too. Whatever it did send is kept; another peer may have the rest.
var ErrPeerLacksHistory = errors.New("peer sync: the peer does not hold the history below this root")

// FetchAncestors fetches every commit the peer holds that is an ancestor-or-self of of, parents
// first, paged.
func (s *RepairSession) FetchAncestors(ns string, of []codec.Hash, pageBytes int) ([]document.Commit, []document.CommitStub, error) {
	var commits []document.Commit
	var stubs []document.CommitStub
	var haves []codec.Hash
	for {
		reply, err := s.conn.request(wire.FetchRequestMessage{
			H: header(wire.MsgFetchRequest, s.conn.next()), Namespace: ns,
			Wants: of, Haves: haves, MaxBytes: pageBytes,
		})
		if err != nil {
			return nil, nil, err
		}
		page, ok := reply.(wire.PackPageMessage)
		if !ok {
			return nil, nil, NewError(fmt.Sprintf("expected PACK_PAGE, got %T", reply), nil)
		}
		commits = append(commits, page.Commits...)
		stubs = append(stubs, page.Stubs...)
		if page.Done || len(page.Commits) == 0 {
			return commits, stubs, nil
		}
		haves = append(haves, pageTips(page.Commits)...)
	}
}

// Deepen fetches root's history from the peer behind s and stops treating root as shallow.
func Deepen(env IngestEnv, s *RepairSession, root codec.Hash) (DeepenResult, error) {
	res := DeepenResult{Root: root}
	if !env.DAG.IsShallow(root) {
		return res, fmt.Errorf("%w: %s", ErrNotShallow, root.Hex())
	}
	rc, err := env.DAG.GetCommitOrThrow(root)
	if err != nil {
		return res, err
	}
	var missing []codec.Hash
	for _, p := range rc.ParentHashes {
		if !env.DAG.HasCommit(p) {
			missing = append(missing, p)
		}
	}
	var fetched []document.Commit
	var stubs []document.CommitStub
	if len(missing) > 0 {
		if fetched, stubs, err = s.FetchAncestors(env.NamespaceID, missing, DefaultPageBytes); err != nil {
			return res, err
		}
	}

	node := env.Node
	if node == nil {
		node = lockOnlyNode{}
	}
	var durable []func() error
	lacks := false
	err = node.Exclusive(func() error {
		lock := divergenceLockFor(env.NamespaceID)
		lock.Lock()
		defer lock.Unlock()
		for _, st := range stubs {
			env.DAG.PutStub(st)
		}
		// held is every commit the peer sent that this namespace now holds, whether this run
		// admitted it or a previous one did. It is not the same as what was newly stored: the
		// skip below is what makes storing idempotent, and a run whose log write failed left
		// exactly those commits resident, so the next run has to log them rather than pass over
		// them. Reporting success for a deepen that logged nothing was the defect.
		var held []document.Commit
		fresh := 0
		for _, c := range fetched { // parents first
			if env.DAG.HasCommit(c.Hash) {
				held = append(held, c)
				continue
			}
			if err := env.checkSkew(c); err != nil {
				return err
			}
			whole := true
			for _, p := range c.ParentHashes {
				if !env.DAG.HasCommit(p) {
					whole = false
					break
				}
			}
			if whole {
				err = env.DAG.PutCommit(c, true) // recomputes the hash: a peer cannot forge history
			} else {
				err = env.DAG.PutShallowCommit(c)
			}
			if err != nil {
				return fmt.Errorf("peer sync: deepen: storing %s: %w", c.Hash.Hex(), err)
			}
			held = append(held, c)
			fresh++
		}
		res.Fetched = fresh
		for _, p := range rc.ParentHashes {
			if !env.DAG.HasCommit(p) {
				lacks = true
			}
		}
		// The history below the root as the DAG now has it, parents first, and the shallow roots
		// in it. Read back rather than collected above for the reason held gives, and it is the
		// horizon too: a run that fetched nothing because a failed predecessor had already stored
		// everything must still name those roots in the marker, or a replay meeting them in the
		// log would fail the open for want of parents nobody has.
		var below []document.Commit
		below, res.Horizon, err = commitsBelow(env.DAG, rc.ParentHashes)
		if err != nil {
			return err
		}
		if env.ShallowRootsChanged != nil && len(res.Horizon) > 0 {
			if err := env.ShallowRootsChanged(env.DAG.ShallowRoots()); err != nil {
				return err
			}
		}
		// Whatever is held is logged, whether or not the root can be completed: a commit in the
		// DAG with no frame in the log would lose its operations at the next restart. below is the
		// history under the root, parents first; held adds anything the peer sent that does not
		// hang off it, which would otherwise be stored and never logged at all.
		toLog := make([]document.Commit, 0, len(below)+len(held)+1)
		seen := make(map[codec.Hash]bool, len(below)+len(held))
		add := func(cs []document.Commit) {
			for _, c := range cs {
				// A parentless commit is the namespace's own genesis, which every DAG makes for
				// itself when it opens (dag.NewInMemoryCommitDag) and which no frame has to
				// restore. It is also the one commit replay applies without first asking whether
				// it extends main, so leaving it out keeps that question from arising.
				if len(c.ParentHashes) == 0 || seen[c.Hash] {
					continue
				}
				seen[c.Hash] = true
				toLog = append(toLog, c)
			}
		}
		add(below)
		add(held)
		if !lacks {
			toLog = append(toLog, rc)
		}
		durable, err = env.persistAll(toLog)
		return err
	})
	for _, wait := range durable {
		if werr := wait(); werr != nil && err == nil {
			err = fmt.Errorf("peer sync: deepen: persisting fetched history: %w", werr)
		}
	}
	if err != nil {
		return res, err
	}
	if lacks {
		return res, fmt.Errorf("%w: %s", ErrPeerLacksHistory, root.Hex())
	}
	if err := env.DAG.Unshallow(root); err != nil {
		return res, err
	}
	res.Unshallowed = true
	if env.ShallowRootsChanged != nil {
		if err := env.ShallowRootsChanged(env.DAG.ShallowRoots()); err != nil {
			return res, err
		}
	}
	return res, nil
}

// commitsBelow lists every commit this namespace holds beneath from, parents first, and which of
// them are still flagged as shallow roots. It is how Deepen decides what has to be in the log,
// read out of the DAG rather than remembered from the store loop - see Deepen on re-running.
//
// The walk ends wherever this DAG's own history ends: a parent that is not resident is a stub, a
// shallow root's missing parent, or history retention truncated away, and there is nothing there
// to log. Commits still flagged shallow are returned separately because they are what the
// namespace marker has to name for a replay to admit them without their parents.
func commitsBelow(d *dag.InMemoryCommitDag, from []codec.Hash) ([]document.Commit, []codec.Hash, error) {
	type frame struct {
		hash     codec.Hash
		commit   document.Commit
		emitting bool
	}
	// Iterative: the history below a bootstrapped root is as deep as the peer's whole log, and
	// recursion over that is a stack the depth of the namespace's history.
	var stack []frame
	push := func(hs []codec.Hash) {
		for i := len(hs) - 1; i >= 0; i-- { // reversed, so siblings come out in their listed order
			stack = append(stack, frame{hash: hs[i]})
		}
	}
	push(from)
	var parentsFirst []document.Commit
	var shallow []codec.Hash
	seen := map[codec.Hash]bool{}
	for len(stack) > 0 {
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if f.emitting { // every parent of this one has been emitted by now
			parentsFirst = append(parentsFirst, f.commit)
			continue
		}
		if seen[f.hash] {
			continue
		}
		seen[f.hash] = true
		if !d.HasCommit(f.hash) {
			continue
		}
		// GetCommitOrThrow, not GetCommit: a commit whose operations the retention budget evicted
		// has to have them loaded back before it is logged, and failing to is worth reporting
		// rather than writing a frame with no operations in it.
		c, err := d.GetCommitOrThrow(f.hash)
		if err != nil {
			return nil, nil, fmt.Errorf("peer sync: deepen: reading %s back out of the DAG: %w", f.hash.Hex(), err)
		}
		if d.IsShallow(f.hash) {
			shallow = append(shallow, f.hash)
		}
		stack = append(stack, frame{hash: f.hash, commit: c, emitting: true})
		push(c.ParentHashes)
	}
	return parentsFirst, shallow, nil
}

// DeepenAll deepens every shallow root until none is left or the peer has no more history to give.
func DeepenAll(env IngestEnv, s *RepairSession) ([]DeepenResult, error) {
	var out []DeepenResult
	tried := map[codec.Hash]bool{}
	for {
		progressed := false
		for _, root := range env.DAG.ShallowRoots() {
			if tried[root] {
				continue
			}
			tried[root] = true
			r, err := Deepen(env, s, root)
			if errors.Is(err, ErrPeerLacksHistory) {
				out = append(out, r)
				continue // this peer's history ends here too; keep the root
			}
			if err != nil {
				return out, err
			}
			out = append(out, r)
			progressed = progressed || r.Fetched > 0 || r.Unshallowed
		}
		if !progressed {
			return out, nil
		}
	}
}
