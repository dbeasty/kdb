package peersync

import (
	"fmt"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Extending a shallow boundary: taking a branch that forked below this node's snapshot root.
//
// A node bootstrapped from a snapshot holds its root S and nothing behind it. A pull names S (and
// what is above it) as haves, and the peer sends "everything reachable from its head that is not
// an ancestor of a have" (MissingCommitsFrom) - so every ancestor of S counts as held. That is
// wrong for a shallow node, and it matters as soon as the peer's history gains a commit C whose
// parent P is behind S: a multi-writer mesh, where S was a merge and some node kept writing on P,
// one of S's parents, before it saw S. The peer sends C but not P, and C cannot be stored here -
// "missing parent P" - on every sync, for ever.
//
// Git solves the same thing by telling the server its shallow boundary, and the server then sends
// P's whole history unless told a depth. Neither fits: the v2 FETCH_REQUEST has no field for
// either, a full history is what the snapshot bootstrap exists to avoid, and P's history is not
// what this node needs anyway. What it needs is P's *state*: merges trace a document's value down
// a first-parent chain and, at a commit whose parents are not here, read that commit's tree
// (valueIndex.valuesAt). So P is taken in exactly as Graft takes in a peer's root - its state by
// SNAPSHOT_FETCH at P, verified against P's tree hash, P admitted as a shallow root, durably - and
// the page then stores. No wire change: SNAPSHOT_FETCH at an arbitrary commit is what Graft
// already asks a v2 host for.
//
// Unlike Graft this needs no allowUnrelated: P is not an unrelated history. The peer left it out
// of the page only because it is an ancestor of a commit this node named as a have, and the peer
// is where the hash and the tree are checked from. The node's history stays one history; it just
// has two boundary commits instead of one.
//
// What it costs is one state transfer per such fork point, and there are few: a fork point is a
// commit some node had as its head when the snapshot was taken and built on afterwards, so the
// count is bounded by the writers that were behind the snapshot's source at that moment, once.
//
// Durability is Graft's (graft.go): the marker names P as a graft root before P is logged, so a
// replay that meets P admits it without its parents; P is never applied to main, which reaches it
// only through a merge whose operations already carry every document its parents disagree on.

// ShallowBoundaryError is a pull that cannot proceed because the peer sent a commit whose parent
// lies behind this node's shallow boundary - history it never held because it was bootstrapped
// from a snapshot - and taking that parent's state from the peer failed. Without the fix around
// it this was a bare "missing parent" on every sync; it now says what is wrong and what recovers it.
type ShallowBoundaryError struct {
	Namespace string
	// Commit is the commit the peer sent; Missing is its parent behind the boundary.
	Commit, Missing codec.Hash
	Cause           error
}

func (e *ShallowBoundaryError) Error() string {
	return fmt.Sprintf("peer sync: %s: the peer sent commit %s, whose parent %s is behind this node's snapshot root "+
		"(history it was bootstrapped without); taking that commit's state from the peer failed: %v - "+
		"deepening from a peer that holds the history (the deepen=true peer option, POST /v1/ns/{ns}/deepen or kdb deepen) recovers it",
		e.Namespace, e.Commit.Hex(), e.Missing.Hex(), e.Cause)
}

func (e *ShallowBoundaryError) Unwrap() error { return e.Cause }

// boundaryGap is one parent a page needs that is behind this node's shallow boundary.
type boundaryGap struct {
	child, parent codec.Hash
}

// gapsBelowBoundary lists the parents of page's commits that the peer did not send and this node
// does not hold, when this node has a shallow boundary those parents can be behind. A commit that
// is one of the peer's own shallow roots is left out: its missing parents are behind the *peer's*
// boundary, which is an unrelated history and Graft's business (unsharedRoot).
func gapsBelowBoundary(env IngestEnv, page []document.Commit, peerShallow []string) []boundaryGap {
	if len(env.DAG.ShallowRoots()) == 0 {
		return nil
	}
	peerRoots := make(map[string]bool, len(peerShallow))
	for _, h := range peerShallow {
		peerRoots[h] = true
	}
	inPage := make(map[codec.Hash]bool, len(page))
	for _, c := range page {
		inPage[c.Hash] = true
	}
	seen := map[codec.Hash]bool{}
	var out []boundaryGap
	for _, c := range page {
		if peerRoots[c.Hash.Hex()] || env.DAG.HasCommit(c.Hash) {
			continue
		}
		for _, p := range c.ParentHashes {
			if inPage[p] || seen[p] || env.DAG.HasCommit(p) || env.DAG.HasStub(p) {
				continue
			}
			seen[p] = true
			out = append(out, boundaryGap{child: c.Hash, parent: p})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].parent.Hex() < out[j].parent.Hex() })
	return out
}

// extendBoundary takes in gap.parent's state from the peer and admits it as a shallow root - see
// the comment at the top of this file.
func extendBoundary(env IngestEnv, gap boundaryGap, fetch func(after string) (wire.SnapshotPageMessage, error)) error {
	if _, err := admitRootFromState(env, gap.parent, fetch); err != nil {
		return &ShallowBoundaryError{Namespace: env.NamespaceID, Commit: gap.child, Missing: gap.parent, Cause: err}
	}
	return nil
}

// The push direction.
//
// A node only ever dialled - a hub whose peers push to it (mode=push), or a node with no --peer of
// its own - never runs the pull above, so a push of a branch forked behind its snapshot root
// failed "missing parent" on every push, for ever: the pusher counts the fork point as held, for
// the same reason a pull's peer does, and the receiver cannot fetch from the node that dialled it.
//
// The receiver tells the pusher instead. A REF_UPDATE page whose parents are behind its boundary
// is refused with PEER_ERROR SHALLOW_BOUNDARY (PushBelowBoundaryError), and the receiver notes,
// for this session, which parents those were. The pusher, which holds them, sends each one's state
// by GRAFT_PUSH and sends the page again. A GRAFT_PUSH naming a parent this session was refused
// for is admitted the way a pull admits a fork point (extendBoundary): it needs no
// allowUnrelated, because the receiver itself found the hash as the parent of a commit it was
// pushed, behind its own boundary; the state is checked against the commit's tree hash and the
// commit against its hash. Any other GRAFT_PUSH is a graft, as before.
//
// No message changes shape: SHALLOW_BOUNDARY is a new error code, and GRAFT_PUSH is a frame every
// v2 host already takes. An older pusher (v0.7.0 and before) does not know the code, so its push
// fails with this error - which names both commits and the way out - rather than a bare "missing
// parent"; the receiver heals once it pulls from any peer or is deepened. An older receiver
// answers INTERNAL "missing parent", which a newer pusher reports as it always did.

// PushBelowBoundaryError is a push refused because a pushed commit's parent is behind this
// node's snapshot root and the pusher has not sent that parent's state (see above). It goes back
// to the pusher as PEER_ERROR SHALLOW_BOUNDARY.
type PushBelowBoundaryError struct {
	Namespace string
	// Commit is a pushed commit; Missing are the parents the page needs that are behind the
	// boundary, Missing[0] being Commit's.
	Commit  codec.Hash
	Missing []codec.Hash
	Cause   error
}

func (e *PushBelowBoundaryError) Error() string {
	return fmt.Sprintf("peer sync: %s: pushed commit %s has parent %s behind this node's snapshot root "+
		"(history it was bootstrapped without; missing: %s), and the pusher did not send its state first - "+
		"a pusher newer than v0.7.0 does when told; otherwise this node takes it when it next pulls from any peer, "+
		"or by deepening (POST /v1/ns/{ns}/deepen or kdb deepen): %v",
		e.Namespace, e.Commit.Hex(), e.Missing[0].Hex(), hexList(e.Missing), e.Cause)
}

func (e *PushBelowBoundaryError) Unwrap() error { return e.Cause }

// PeerBoundaryError is a push this node could not complete because the peer needs the state of
// commits behind its snapshot root, and this node could not supply it - it does not hold them,
// or cannot build their state.
type PeerBoundaryError struct {
	Namespace string
	Missing   []codec.Hash
	Cause     error
}

func (e *PeerBoundaryError) Error() string {
	return fmt.Sprintf("peer sync: %s: the peer needs the state of %s, behind its snapshot root, to take this push, "+
		"and this node could not send it: %v", e.Namespace, hexList(e.Missing), e.Cause)
}

func (e *PeerBoundaryError) Unwrap() error { return e.Cause }

func hexList(hs []codec.Hash) string {
	out := ""
	for i, h := range hs {
		if i > 0 {
			out += ", "
		}
		out += h.Hex()
	}
	return out
}

// pushBlockedByBoundary is the receiver's half: when a pushed page could not be stored and this
// node has parents behind its boundary missing for it, the typed refusal naming them; else nil.
func pushBlockedByBoundary(env IngestEnv, page []document.Commit, cause error) (*PushBelowBoundaryError, []boundaryGap) {
	gaps := gapsBelowBoundary(env, page, nil)
	if len(gaps) == 0 {
		return nil, nil
	}
	e := &PushBelowBoundaryError{Namespace: env.NamespaceID, Commit: gaps[0].child, Cause: cause}
	for _, g := range gaps {
		e.Missing = append(e.Missing, g.parent)
	}
	return e, gaps
}

// peerBoundaryGaps is the pusher's half: the parents of page's commits that are not in it and lie
// behind one of the peer's shallow roots (shallow, as the peer advertised them). Only those can be
// missing there: anything else the page leaves out is below a ref the peer named by a path that
// does not cross its boundary, so the peer holds it. It walks the history behind those roots, so
// it is asked only once the peer has refused a page.
func peerBoundaryGaps(env IngestEnv, page []document.Commit, shallow []string) []codec.Hash {
	roots := map[codec.Hash]bool{}
	var held []codec.Hash
	for _, hex := range shallow {
		h, err := codec.HashFromHex(hex)
		if err != nil {
			continue
		}
		roots[h] = true
		if env.DAG.HasCommit(h) {
			held = append(held, h)
		}
	}
	if len(held) == 0 {
		return nil
	}
	behind := env.DAG.AncestorSetOf(held)
	inPage := make(map[codec.Hash]bool, len(page))
	for _, c := range page {
		inPage[c.Hash] = true
	}
	seen := map[codec.Hash]bool{}
	var out []codec.Hash
	for _, c := range page {
		for _, p := range c.ParentHashes {
			if inPage[p] || roots[p] || seen[p] {
				continue
			}
			if _, ok := behind[p]; !ok {
				continue
			}
			seen[p] = true
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Hex() < out[j].Hex() })
	return out
}
