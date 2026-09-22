package peersync

import (
	"errors"
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
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

// DeepenResult is what one Deepen did.
type DeepenResult struct {
	Root codec.Hash
	// Fetched counts commits newly stored.
	Fetched int
	// Horizon lists fetched commits whose own parents the peer did not have: the new shallow
	// roots.
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
		var stored []document.Commit
		for _, c := range fetched { // parents first
			if env.DAG.HasCommit(c.Hash) {
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
				res.Horizon = append(res.Horizon, c.Hash)
			}
			if err != nil {
				return fmt.Errorf("peer sync: deepen: storing %s: %w", c.Hash.Hex(), err)
			}
			stored = append(stored, c)
		}
		res.Fetched = len(stored)
		for _, p := range rc.ParentHashes {
			if !env.DAG.HasCommit(p) {
				lacks = true
			}
		}
		if env.ShallowRootsChanged != nil && len(res.Horizon) > 0 {
			if err := env.ShallowRootsChanged(env.DAG.ShallowRoots()); err != nil {
				return err
			}
		}
		// Whatever was stored is logged, whether or not the root can be completed: a commit in the
		// DAG with no frame in the log would lose its operations at the next restart.
		toLog := stored
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
