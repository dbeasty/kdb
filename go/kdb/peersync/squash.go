package peersync

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Squash catch-up (docs/kdb-memory-cassandra-plan.md C7): a node far behind a peer on main jumps
// straight to the peer's head instead of replaying every commit in between.
//
// The receiver at head X asks the host for its head T and whether T descends from X (SQUASH_HEAD).
// It then diffs its tree against T's by subtree hash (TREE_NODES), fetches the bodies that differ
// (OBJECT_FETCH, each verified against its content hash), applies them as one batch whose built
// tree must hash to T's, admits T as a shallow root - its parents are simply absent here, as
// below a snapshot root - and moves main to it. Hashes stay the peer's, so the next sync is an
// ordinary one; the commits between X and T can be fetched later by deepen.
//
// T cannot be made durable through the log: a commit carries its own operations inside its hash,
// relative to its real parent, and replaying them onto X would build the wrong tree (replay
// admits a shallow root without applying it - embed.applyCommitsTopologically). So the install
// is made durable by a checkpoint taken through the current log position with main at T
// (IngestEnv.SquashInstalled), inside the same serialization: a local write logged on top of T
// before that checkpoint would replay onto X and be lost. A crash before the checkpoint leaves
// the namespace at X, and the next sync squashes again.
//
// Version 1 is fast-forward only: a receiver with commits the host does not have replays, which
// merges them.

// CatchUpRoute chooses how a pull that is behind on main catches up.
type CatchUpRoute int

const (
	// CatchUpReplay fetches and applies every commit (the default).
	CatchUpReplay CatchUpRoute = iota
	// CatchUpSquash jumps to the peer's head whenever it is a fast-forward.
	CatchUpSquash
	// CatchUpAuto squashes when the backlog is large next to what it changes.
	CatchUpAuto
)

// CatchUpOverride sets the catch-up route for the namespaces Pattern matches (MatchNamespace).
type CatchUpOverride struct {
	Pattern string
	Route   CatchUpRoute
}

// catchUpFor is ns's route under cfg: the first override whose pattern matches, else CatchUp.
func (cfg V2ClientConfig) catchUpFor(ns string) CatchUpRoute {
	for _, o := range cfg.CatchUpNamespaces {
		if MatchNamespace(o.Pattern, ns) {
			return o.Route
		}
	}
	return cfg.CatchUp
}

// ParseCatchUpRoute reads "replay", "squash" or "auto"; empty is replay.
func ParseCatchUpRoute(s string) (CatchUpRoute, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "replay":
		return CatchUpReplay, nil
	case "squash":
		return CatchUpSquash, nil
	case "auto":
		return CatchUpAuto, nil
	}
	return CatchUpReplay, fmt.Errorf("peer sync: unknown catch-up route %q (want replay, squash or auto)", s)
}

func (r CatchUpRoute) String() string {
	switch r {
	case CatchUpSquash:
		return "squash"
	case CatchUpAuto:
		return "auto"
	}
	return "replay"
}

const (
	// AutoSquashMinBacklog is the smallest backlog auto considers squashing: below it replay is
	// cheap whatever it changes, and keeps the history.
	AutoSquashMinBacklog = 1000
	// AutoSquashBacklogPerChange is how many backlog commits per changed document auto wants
	// before it squashes. Measured (TestCatchUpRoutes, 100k-commit backlogs, 200-byte bodies),
	// squash is faster at every shape - 7x even when half the backlog's documents differ - but it
	// sends bodies uncompressed, so near one change per commit it costs more bytes than replay's
	// zstd packs (42 MiB against 13.5 MiB at 100k:100k). Two keeps squash where it saves both.
	AutoSquashBacklogPerChange = 2
)

// squashHeadPage answers SQUASH_HEAD.
func squashHeadPage(env IngestEnv, m wire.SquashHeadMessage) (wire.SquashHeadResultMessage, error) {
	res := wire.SquashHeadResultMessage{Namespace: m.Namespace}
	if env.SquashRefused != "" {
		res.Refused = env.SquashRefused
		return res, nil
	}
	_, head, ok, err := env.DAG.HeadCommit()
	if err != nil {
		return res, err
	}
	if !ok {
		res.Refused = "namespace has no head"
		return res, nil
	}
	res.Commit = head
	have, err := codec.HashFromHex(m.HaveHex)
	if err != nil || !env.DAG.HasCommit(have) {
		return res, nil
	}
	if have == head.Hash {
		res.Descends = true
		return res, nil
	}
	if env.DAG.IsAncestor(have, head.Hash) {
		res.Descends = true
		if d, ok := env.DAG.GenerationDistance(have, head.Hash); ok {
			res.Backlog = d
		}
	}
	return res, nil
}

// errSquashDeclined is a squash this pull will not do - the peer refused it, it is not a
// fast-forward, or auto judged replay cheaper. The pull replays instead; it is not a failure.
var errSquashDeclined = errors.New("peer sync: squash declined")

// trySquash catches main up by squash when cfg's route says to and the host can serve one. It
// returns whether main moved; errSquashDeclined (or false with nil) leaves the pull to replay.
func (c *v2Conn) trySquash(cfg V2ClientConfig, env IngestEnv, ns string, remoteMain codec.Hash, res *NamespaceSyncResult) (bool, error) {
	route := cfg.catchUpFor(ns)
	if route == CatchUpReplay || env.DAG.HasCommit(remoteMain) {
		return false, nil
	}
	if env.SquashRefused != "" {
		// A namespace that refuses to serve squashes keeps full history here too: the host may
		// not know the policy yet, and taking a squash from it would leave this replica the gap.
		return false, nil
	}
	if !slices.Contains(c.caps, wire.SyncCapSquash) || !slices.Contains(c.caps, wire.SyncCapRepair) {
		return false, nil
	}
	_, local, ok, err := env.DAG.HeadCommit()
	if err != nil || !ok {
		return false, err
	}
	head, err := c.squashHead(ns, local.Hash)
	if err != nil {
		return false, err
	}
	if head.Refused != "" || !head.Descends {
		return false, nil
	}
	target := head.Commit
	if recomputed, err := document.ComputeCommitHash(target); err != nil || recomputed != target.Hash {
		return false, fmt.Errorf("peer sync: squash head %s does not hash to itself", target.Hash.Hex())
	}
	if route == CatchUpAuto && head.Backlog < AutoSquashMinBacklog {
		return false, nil
	}
	localTree, err := treeAt(env.Storage, env.NamespaceID, local.DocumentTreeHash)
	if err != nil {
		return false, err
	}
	s := &RepairSession{conn: c, caps: c.caps}
	treeHex, diff, err := s.Diff(ns, localTree, target.DocumentTreeHash.Hex())
	if err != nil {
		return false, err
	}
	if route == CatchUpAuto && head.Backlog < int64(AutoSquashBacklogPerChange*len(diff)) {
		return false, nil
	}
	wanted := make(map[codec.UUID]codec.Hash, len(diff))
	for _, d := range diff {
		if d.Remote != (codec.Hash{}) {
			wanted[d.DocID] = d.Remote
		}
	}
	bodies, err := s.Fetch(ns, wanted, treeHex)
	if err != nil {
		return false, err
	}
	if len(bodies) != len(wanted) {
		return false, fmt.Errorf("peer sync: squash to %s: the peer supplied %d of %d changed documents", target.Hash.Hex(), len(bodies), len(wanted))
	}
	if err := InstallSquash(env, local, target, diff, bodies); err != nil {
		if errors.Is(err, errSquashDeclined) {
			return false, nil
		}
		return false, err
	}
	res.Squashed = target.Hash.Hex()
	res.SquashedBacklog = head.Backlog
	return true, nil
}

// squashHead asks the peer for ns's head and whether it descends from have (SQUASH_HEAD).
func (c *v2Conn) squashHead(ns string, have codec.Hash) (wire.SquashHeadResultMessage, error) {
	reply, err := c.request(wire.SquashHeadMessage{H: header(wire.MsgSquashHead, c.next()), Namespace: ns, HaveHex: have.Hex()})
	if err != nil {
		return wire.SquashHeadResultMessage{}, err
	}
	head, isHead := reply.(wire.SquashHeadResultMessage)
	if !isHead {
		return wire.SquashHeadResultMessage{}, NewError(fmt.Sprintf("expected SQUASH_HEAD_RESULT, got %T", reply), nil)
	}
	return head, nil
}

// SquashHead asks the peer what a squash catch-up of ns from have would jump to - its head,
// whether that descends from have, and the backlog - or why it refuses one. For tools and tests;
// a pull asks this itself.
func (s *RepairSession) SquashHead(ns string, have codec.Hash) (wire.SquashHeadResultMessage, error) {
	if !slices.Contains(s.caps, wire.SyncCapSquash) {
		return wire.SquashHeadResultMessage{}, fmt.Errorf("peer sync: the peer does not serve squash catch-ups")
	}
	return s.conn.squashHead(ns, have)
}

// InstallSquash moves env's main from its head from to target, a descendant of it whose
// commits in between this node does not hold: diff is the tree difference from from's tree to
// target's, and bodies the verified bodies of every document diff changes. target is admitted as a
// shallow root. Refused with errSquashDeclined when main has moved off from meanwhile.
func InstallSquash(env IngestEnv, from, target document.Commit, diff []document.TreeDifference, bodies map[codec.UUID]string) error {
	node := env.Node
	if node == nil {
		node = lockOnlyNode{}
	}
	return node.Exclusive(func() error {
		lock := divergenceLockFor(env.NamespaceID)
		lock.Lock()
		defer lock.Unlock()
		head, err := env.DAG.Head()
		if err != nil {
			return err
		}
		if head != from.Hash {
			return errSquashDeclined
		}
		if env.DAG.HasCommit(target.Hash) {
			return errSquashDeclined
		}
		if err := env.checkSkew(target); err != nil {
			return err
		}
		if env.CheckCommit != nil {
			if err := env.CheckCommit(target); err != nil {
				return err
			}
		}
		// What each changed document was at from, to put back if the install fails part-way.
		previous := make(map[codec.UUID]*document.Document, len(diff))
		for _, d := range diff {
			if d.Local == (codec.Hash{}) {
				previous[d.DocID] = nil
				continue
			}
			doc, err := env.Storage.GetDocument(env.NamespaceID, d.DocID, from.DocumentTreeHash)
			if err != nil {
				return err
			}
			previous[d.DocID] = doc
		}
		restore := func(cause error) error {
			_ = env.Storage.DiscardPending(env.NamespaceID)
			for id, doc := range previous {
				if doc == nil {
					_ = env.Storage.DeleteDocument(env.NamespaceID, id)
				} else {
					_ = env.Storage.PutDocument(env.NamespaceID, *doc)
				}
			}
			if _, err := env.Storage.CommitTree(env.NamespaceID, from.DocumentTreeHash); err != nil {
				return fmt.Errorf("%w (and the documents could not be put back: %v)", cause, err)
			}
			return cause
		}
		ops := make([]document.Op, 0, len(diff))
		for _, d := range diff {
			if d.Remote == (codec.Hash{}) {
				if err := env.Storage.DeleteDocument(env.NamespaceID, d.DocID); err != nil {
					return restore(err)
				}
				ops = append(ops, document.DeleteOp{DocID: d.DocID})
				continue
			}
			body := bodies[d.DocID]
			if err := env.Storage.PutDocument(env.NamespaceID, document.Document{ID: d.DocID, JSON: body}); err != nil {
				return restore(err)
			}
			ops = append(ops, document.WriteOp{DocID: d.DocID, Patch: body})
		}
		built, err := env.Storage.CommitTree(env.NamespaceID, from.DocumentTreeHash)
		if err != nil {
			return restore(err)
		}
		if built.TreeHash != target.DocumentTreeHash {
			return restore(&TreeMismatchError{CommitHash: target.Hash, Declared: target.DocumentTreeHash, Built: built.TreeHash})
		}
		env.DAG.PutDocumentTree(built)
		if err := env.DAG.PutShallowCommit(target); err != nil {
			return restore(err)
		}
		undo := func(cause error) error {
			if err := env.DAG.SetHead(mainBranch, from.Hash); err != nil {
				return fmt.Errorf("peer sync: %w (and main could not be put back: %v)", cause, err)
			}
			if err := env.DAG.DropShallowCommit(target.Hash); err != nil {
				return fmt.Errorf("peer sync: %w (and the squash root could not be taken back: %v)", cause, err)
			}
			return restore(cause)
		}
		if err := env.DAG.SetHead(mainBranch, target.Hash); err != nil {
			return undo(err)
		}
		if env.SquashInstalled != nil {
			changed := make(map[codec.Hash]string, len(bodies))
			for _, d := range diff {
				if d.Remote != (codec.Hash{}) {
					changed[d.Remote] = bodies[d.DocID]
				}
			}
			if err := env.SquashInstalled(target, changed); err != nil {
				return undo(fmt.Errorf("peer sync: squash could not be made durable: %w", err))
			}
		}
		// Durable from here: like a snapshot install, Advanced's error is reported but never
		// undoes the head move.
		return node.Advanced(AdvanceStep{Commits: []document.Commit{target}, Applied: ops})
	})
}
