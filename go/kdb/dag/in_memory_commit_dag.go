package dag

import (
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

const mainBranch = "main"

// cacheLinePadBytes pads the published head snapshot pointer out to its own cache line.
// 128 rather than 64: Apple silicon's L2 line and Intel's adjacent-line prefetcher both make
// 64 too small to reliably isolate a line, and the cost is 120 bytes per namespace.
const cacheLinePadBytes = 128 - 8

// headSnapshot is an immutable view of the default branch's tip: the head hash and the
// commit it names. publishHeadLocked builds one and stores it whole; nothing mutates a
// snapshot after it is published, so a reader may hold one for as long as it likes and
// still see a coherent (if momentarily stale) instant. Go's GC is the reclamation
// mechanism here - the part an RCU implementation in a non-managed language has to build
// by hand as epochs or hazard pointers.
type headSnapshot struct {
	// hasBranch is false only when the default branch is missing, which the readers below
	// surface as a consistency error rather than a zero hash.
	hasBranch bool
	hash      codec.Hash
	// hasCommit is false when the head names a commit that is not resident - an archived
	// stub, or a head pointed at a stub by SetHead.
	hasCommit bool
	commit    document.Commit
}

// InMemoryCommitDag is an in-memory commit DAG for one namespace.
type InMemoryCommitDag struct {
	NamespaceID string

	// head is the read path's fast lane. Every point read resolves the current head and
	// then that head's commit; taking mu for both meant four atomic read-modify-writes on
	// sync.RWMutex's single shared readerCount word per read, which does not scale with
	// core count - a CPU profile of 1024 concurrent readers on 16 cores put 40% of all
	// samples in sync/atomic.(*Int32).Add and measured aggregate read throughput *below* a
	// single-threaded loop doing the same work (docs/benchmarks/workload-matrix.md,
	// Finding 2). Reading through this pointer instead is one atomic load and no writes to
	// shared memory at all, so the line stays Shared in every core's cache rather than
	// being invalidated on each read.
	//
	// INVARIANT: every mutation of d.branches or d.commits must call publishHeadLocked
	// before releasing mu. The two locked cores (putCommitLocked, appendCommitLocked) and
	// each direct mutator below do; a new mutator must too, or readers will serve a stale
	// head indefinitely.
	head atomic.Pointer[headSnapshot]
	// Keep head off mu's cache line. Readers load head continuously while writers dirty mu
	// and the maps that follow it; sharing a line would reintroduce exactly the
	// invalidation traffic this is here to remove.
	_ [cacheLinePadBytes]byte

	mu        sync.RWMutex
	commits   map[codec.Hash]document.Commit
	stubs     map[codec.Hash]document.CommitStub
	trees     map[codec.Hash]document.DocumentTree
	branches  map[string]document.Branch
	tags      map[string]document.Tag
	hexSorted []string
	// txIndex maps a transaction id to the commit it produced, so idempotent-retry detection
	// (GetCommitByTransactionID, used by transaction.Engine's findExistingCommit) is O(1)
	// instead of walking history. See GetCommitByTransactionID's own doc comment for why this
	// exists.
	txIndex map[codec.UUID]codec.Hash
	// pins counts the live readers holding each commit, keyed by hash - the DAG's retention
	// roots for readers, next to branches/tags for writers. Squash and StubCommit refuse to
	// reclaim a pinned commit. See Pin in retention.go for what pins and why.
	pins map[codec.Hash]int
	// ancestryVersion is bumped by every mutation that can change what IsAncestor /
	// AncestorSet answer for an already-known commit: a commit appearing (putCommitLocked) or
	// disappearing (Squash, StubCommit). Readers that memoize ancestry-derived state - see
	// index.eventLog's bucket cache - key their memo on it so they don't have to recompute
	// against a DAG that has not moved.
	ancestryVersion uint64
}

// publishHeadLocked recomputes the head snapshot from the maps and publishes it. Must be
// called with mu held exclusively, on every path that changes d.branches or d.commits.
//
// It recomputes from scratch rather than patching the previous snapshot: that keeps the
// invariant checkable by inspection at each call site, and mutations are rare next to reads
// (hundreds to tens of thousands per second against millions), so neither the extra map
// lookup nor the one small allocation is worth optimizing away.
func (d *InMemoryCommitDag) publishHeadLocked() {
	snap := &headSnapshot{}
	if b, ok := d.branches[mainBranch]; ok {
		snap.hasBranch = true
		snap.hash = b.HeadHash
		if c, ok := d.commits[b.HeadHash]; ok {
			snap.hasCommit = true
			snap.commit = c
		}
	}
	d.head.Store(snap)
}

// HeadCommit returns the default branch's head hash together with the commit it names, both
// taken from a single atomic snapshot load. Unlike Head followed by GetCommit, the two are
// guaranteed to describe the same instant, and the pair costs one atomic load instead of
// four atomic read-modify-writes on a contended word - this is the lane every point read
// takes (see KdbServerRuntime.GetDocument).
//
// The bool reports whether the head's commit is resident; false means the head names an
// archived stub. The error reports a missing default branch.
func (d *InMemoryCommitDag) HeadCommit() (codec.Hash, document.Commit, bool, error) {
	if s := d.head.Load(); s != nil {
		if !s.hasBranch {
			return codec.Hash{}, document.Commit{}, false,
				NewConsistencyError("missing default branch", d.NamespaceID, nil)
		}
		return s.hash, s.commit, s.hasCommit, nil
	}
	// No snapshot published: a zero-value InMemoryCommitDag that never went through
	// NewInMemoryCommitDag. Answer from the maps rather than reporting nonsense.
	d.mu.RLock()
	defer d.mu.RUnlock()
	b, ok := d.branches[mainBranch]
	if !ok {
		return codec.Hash{}, document.Commit{}, false,
			NewConsistencyError("missing default branch", d.NamespaceID, nil)
	}
	c, hasCommit := d.commits[b.HeadHash]
	return b.HeadHash, c, hasCommit, nil
}

// AncestryVersion returns a counter that changes whenever the commit graph changes shape.
// Callers may cache anything derived from ancestry for as long as this value is unchanged.
func (d *InMemoryCommitDag) AncestryVersion() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.ancestryVersion
}

// NewInMemoryCommitDag creates a DAG with genesis commit and main branch.
func NewInMemoryCommitDag(namespaceID string) (*InMemoryCommitDag, error) {
	d := &InMemoryCommitDag{
		NamespaceID: namespaceID,
		commits:     make(map[codec.Hash]document.Commit),
		stubs:       make(map[codec.Hash]document.CommitStub),
		trees:       make(map[codec.Hash]document.DocumentTree),
		branches:    make(map[string]document.Branch),
		tags:        make(map[string]document.Tag),
		txIndex:     make(map[codec.UUID]codec.Hash),
		pins:        make(map[codec.Hash]int),
	}
	empty := document.EmptyDocumentTree()
	d.trees[empty.TreeHash] = empty

	genesisTx, _ := codec.UUIDFromString("00000000-0000-4000-8000-000000000001")
	genesisAuthor, _ := codec.UUIDFromString("00000000-0000-4000-8000-000000000002")
	genesisTs := codec.Timestamp{}
	genesis, err := document.BuildCommit(
		nil, namespaceID, genesisTx, genesisTs, genesisAuthor,
		nil, empty.TreeHash, nil, "genesis",
	)
	if err != nil {
		return nil, err
	}
	d.commits[genesis.Hash] = genesis
	d.insertHex(genesis.Hash.Hex())
	d.txIndex[genesis.TransactionID] = genesis.Hash
	now := codec.TimestampNow()
	d.branches[mainBranch] = document.Branch{
		Name: mainBranch, NamespaceID: namespaceID,
		HeadHash: genesis.Hash, CreatedAt: now, UpdatedAt: now,
	}
	// d is not shared yet, but publish through the same helper so there is exactly one
	// place that builds a snapshot.
	d.publishHeadLocked()
	return d, nil
}

func (d *InMemoryCommitDag) insertHex(hexLower string) {
	hex := strings.ToLower(hexLower)
	i := sort.SearchStrings(d.hexSorted, hex)
	if i < len(d.hexSorted) && d.hexSorted[i] == hex {
		return
	}
	d.hexSorted = append(d.hexSorted, hex)
	copy(d.hexSorted[i+1:], d.hexSorted[i:])
	d.hexSorted[i] = hex
}

func (d *InMemoryCommitDag) removeHex(hexLower string) {
	hex := strings.ToLower(hexLower)
	i := sort.SearchStrings(d.hexSorted, hex)
	if i < len(d.hexSorted) && d.hexSorted[i] == hex {
		d.hexSorted = append(d.hexSorted[:i], d.hexSorted[i+1:]...)
	}
}

// LookupHashPrefix returns hashes whose hex starts with prefix (lowercase).
func (d *InMemoryCommitDag) LookupHashPrefix(hexPrefixLower string) []codec.Hash {
	d.mu.RLock()
	defer d.mu.RUnlock()
	p := strings.ToLower(hexPrefixLower)
	var out []codec.Hash
	for _, h := range d.hexSorted {
		if strings.HasPrefix(h, p) {
			hash, _ := codec.HashFromHex(h)
			out = append(out, hash)
		}
	}
	return out
}

func (d *InMemoryCommitDag) GetCommit(hash codec.Hash) (document.Commit, bool) {
	// Fast path: "the commit at the current head" is the lookup every point read performs,
	// and the published snapshot already holds it - no lock, no map, no rehash. Any other
	// hash (history walks, ancestry, sync) falls through to the map, which is the rare case
	// on the read path. Callers wanting head and commit together should prefer HeadCommit,
	// which also guarantees the two agree.
	if s := d.head.Load(); s != nil && s.hasCommit && s.hash == hash {
		return s.commit, true
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	c, ok := d.commits[hash]
	return c, ok
}

func (d *InMemoryCommitDag) GetCommitOrThrow(hash codec.Hash) (document.Commit, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if stub, ok := d.stubs[hash]; ok {
		return document.Commit{}, kdberr.NewIceStorageError(
			"commit archived", d.NamespaceID, hash.Hex(), stub.ArchiveLocation,
		)
	}
	c, ok := d.commits[hash]
	if !ok {
		return document.Commit{}, kdberr.NewVersionNotFoundError(
			"commit not found", d.NamespaceID, hash.Hex(),
		)
	}
	return c, nil
}

func (d *InMemoryCommitDag) GetStub(hash codec.Hash) (document.CommitStub, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	s, ok := d.stubs[hash]
	return s, ok
}

func (d *InMemoryCommitDag) HasCommit(hash codec.Hash) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, ok := d.commits[hash]
	return ok
}

func (d *InMemoryCommitDag) HasStub(hash codec.Hash) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, ok := d.stubs[hash]
	return ok
}

// PutCommit stores a commit (idempotent if hash already present).
func (d *InMemoryCommitDag) PutCommit(commit document.Commit, requireParents bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	// verifyHash: a commit arriving through the public PutCommit came from
	// somewhere else (delta replay, a peer sync push), so its claimed hash is
	// not to be trusted.
	return d.putCommitLocked(commit, requireParents, true)
}

func (d *InMemoryCommitDag) putCommitLocked(commit document.Commit, requireParents, verifyHash bool) error {
	if _, ok := d.commits[commit.Hash]; ok {
		return nil
	}
	// Re-deriving the hash means encoding the whole commit payload and running
	// SHA-256 over it again. Worth it for a commit from an untrusted source;
	// pure waste for one document.BuildCommit produced from these exact fields
	// microseconds earlier, which is the per-write hot path (appendCommitLocked).
	if verifyHash {
		recomputed, err := document.ComputeCommitHash(commit)
		if err != nil {
			return err
		}
		if recomputed != commit.Hash {
			return NewConsistencyError("commit hash mismatch", d.NamespaceID, &commit.Hash)
		}
	}
	if requireParents && len(commit.ParentHashes) > 0 {
		for _, p := range commit.ParentHashes {
			if _, ok := d.commits[p]; !ok {
				if _, ok2 := d.stubs[p]; !ok2 {
					return NewConsistencyError("missing parent "+p.Hex(), d.NamespaceID, &commit.Hash)
				}
			}
		}
	}
	d.commits[commit.Hash] = commit
	d.insertHex(commit.Hash.Hex())
	d.ancestryVersion++
	// Only the first commit for a given transaction id is indexed - a caller retrying the same
	// transaction always expects to find that original result, not a later, unrelated commit
	// that happens to reuse the id (which should never legitimately happen, since ids are random
	// UUIDs minted fresh per transaction attempt, but "first wins" is the safer tie-break if it
	// ever did).
	if _, exists := d.txIndex[commit.TransactionID]; !exists {
		d.txIndex[commit.TransactionID] = commit.Hash
	}
	// A commit arriving here can be the one the head already names (delta replay onto a
	// head pointed at a not-yet-resident hash), so the snapshot has to be rebuilt.
	d.publishHeadLocked()
	return nil
}

// GetCommitByTransactionID returns the commit produced by transaction id, if any - an O(1)
// lookup used by transaction.Engine's idempotent-retry detection (a caller resubmitting the same
// transaction after e.g. a network timeout should see the original result, not create a
// duplicate). Before this existed, that check walked up to 8192 commits of history on every
// single Commit/Replay call regardless of whether a retry was actually happening, which measured
// as the dominant cost (~88% of all allocation in a profiled run) behind kdb-service getting
// OOM-killed under sustained write load once a namespace's commit history grew past a few
// thousand entries - see docs/benchmarks/lightsail-sim/README.md.
func (d *InMemoryCommitDag) GetCommitByTransactionID(txID codec.UUID) (document.Commit, bool) {
	d.mu.RLock()
	hash, ok := d.txIndex[txID]
	d.mu.RUnlock()
	if !ok {
		return document.Commit{}, false
	}
	return d.GetCommit(hash)
}

// StubCommit archives a commit in place.
func (d *InMemoryCommitDag) StubCommit(hash codec.Hash, archiveLocation string) (document.CommitStub, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.commits[hash]; !ok {
		return document.CommitStub{}, NewConsistencyError("cannot stub unknown commit", d.NamespaceID, &hash)
	}
	// Archiving is a reclamation like squashing: a stub satisfies requireCommitPresentLocked but
	// not GetCommitOrThrow, so a reader pinned here would start failing. Same refusal.
	if err := d.assertUnpinnedLocked(hash, "archive requested"); err != nil {
		return document.CommitStub{}, err
	}
	delete(d.commits, hash)
	d.removeHex(hash.Hex())
	d.ancestryVersion++
	stub := document.CommitStub{
		OriginalHash: hash, ArchiveLocation: archiveLocation, StubbedAt: codec.TimestampNow(),
	}
	d.stubs[hash] = stub
	// Archiving the commit the head names flips the snapshot's hasCommit to false.
	d.publishHeadLocked()
	return stub, nil
}

func (d *InMemoryCommitDag) GetDocumentTree(treeHash codec.Hash) (document.DocumentTree, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	t, ok := d.trees[treeHash]
	return t, ok
}

func (d *InMemoryCommitDag) GetDocumentTreeOrThrow(treeHash codec.Hash) (document.DocumentTree, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	t, ok := d.trees[treeHash]
	if !ok {
		return document.DocumentTree{}, kdberr.NewVersionNotFoundError(
			"document tree not found", d.NamespaceID, treeHash.Hex(),
		)
	}
	return t, nil
}

func (d *InMemoryCommitDag) PutDocumentTree(tree document.DocumentTree) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.trees[tree.TreeHash] = tree
}

func (d *InMemoryCommitDag) Head() (codec.Hash, error) {
	hash, _, _, err := d.HeadCommit()
	return hash, err
}

func (d *InMemoryCommitDag) SetHead(branchName string, hash codec.Hash) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	b, ok := d.branches[branchName]
	if !ok {
		return NewBranchNotFoundError("branch not found", d.NamespaceID, branchName)
	}
	if err := d.requireCommitPresentLocked(hash); err != nil {
		return err
	}
	now := codec.TimestampNow()
	b.HeadHash = hash
	b.UpdatedAt = now
	d.branches[branchName] = b
	d.publishHeadLocked()
	return nil
}

func (d *InMemoryCommitDag) GetBranch(name string) (document.Branch, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	b, ok := d.branches[name]
	return b, ok
}

func (d *InMemoryCommitDag) GetBranchOrThrow(name string) (document.Branch, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	b, ok := d.branches[name]
	if !ok {
		return document.Branch{}, NewBranchNotFoundError("branch not found", d.NamespaceID, name)
	}
	return b, nil
}

func (d *InMemoryCommitDag) ListBranches() []document.Branch {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]document.Branch, 0, len(d.branches))
	for _, b := range d.branches {
		out = append(out, b)
	}
	return out
}

func (d *InMemoryCommitDag) CreateBranch(name string, fromHash codec.Hash) (document.Branch, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.branches[name]; ok {
		return document.Branch{}, NewConsistencyError("branch exists", d.NamespaceID, nil)
	}
	if err := d.requireCommitPresentLocked(fromHash); err != nil {
		return document.Branch{}, err
	}
	now := codec.TimestampNow()
	b := document.Branch{
		Name: name, NamespaceID: d.NamespaceID,
		HeadHash: fromHash, CreatedAt: now, UpdatedAt: now,
	}
	d.branches[name] = b
	// Cannot currently touch mainBranch (it always exists, so this returns "branch exists"
	// first), but publish anyway so the invariant holds by construction rather than by
	// depending on that argument staying true.
	d.publishHeadLocked()
	return b, nil
}

func (d *InMemoryCommitDag) DeleteBranch(name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if name == mainBranch {
		return NewBranchNotFoundError("cannot delete default branch", d.NamespaceID, name)
	}
	if _, ok := d.branches[name]; !ok {
		return NewBranchNotFoundError("branch not found", d.NamespaceID, name)
	}
	delete(d.branches, name)
	// Refuses mainBranch above, so this cannot invalidate the snapshot today; published for
	// the same reason as CreateBranch.
	d.publishHeadLocked()
	return nil
}

func (d *InMemoryCommitDag) Walk(from codec.Hash, until *codec.Hash, limit int) []TraversalEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()
	type frontierItem struct {
		hash codec.Hash
		ts   codec.Timestamp
	}
	var frontier []frontierItem
	enqueue := func(h codec.Hash) {
		ts := codec.Timestamp{}
		if c, ok := d.commits[h]; ok {
			ts = c.Timestamp
		} else if s, ok := d.stubs[h]; ok {
			ts = s.StubbedAt
		}
		frontier = append(frontier, frontierItem{hash: h, ts: ts})
	}
	enqueue(from)
	visited := make(map[codec.Hash]struct{})
	var out []TraversalEntry
	for len(frontier) > 0 && len(out) < limit {
		best := 0
		for i := 1; i < len(frontier); i++ {
			if frontier[i].ts.EpochMicros() > frontier[best].ts.EpochMicros() {
				best = i
			}
		}
		item := frontier[best]
		frontier = append(frontier[:best], frontier[best+1:]...)
		h := item.hash
		if until != nil && h == *until {
			// Prune this branch only - do NOT abort the whole traversal. On a DAG with merge
			// commits, `until` can surface from the frontier while a sibling branch is still
			// pending; a plain break here silently dropped that entire branch (peer-sync's
			// CommitsToPush/CommitFetch then omitted commits, and a pushed merge commit
			// arrived at the peer before its other parent - "missing parent" rejections
			// observed live in the 3-node e2e scenario).
			visited[h] = struct{}{}
			continue
		}
		if _, ok := visited[h]; ok {
			continue
		}
		visited[h] = struct{}{}
		if stub, ok := d.stubs[h]; ok {
			out = append(out, StubbedEntry{Stub: stub})
			continue
		}
		c, ok := d.commits[h]
		if !ok {
			continue
		}
		out = append(out, FullEntry{Commit: c})
		for _, p := range c.ParentHashes {
			enqueue(p)
		}
	}
	return out
}

func (d *InMemoryCommitDag) Diff(fromHash, toHash codec.Hash) (CommitDiff, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if fromHash == toHash {
		return CommitDiff{FromHash: fromHash, ToHash: toHash}, nil
	}
	fc, ok := d.commits[fromHash]
	if !ok {
		return CommitDiff{}, kdberr.NewVersionNotFoundError("from commit missing", d.NamespaceID, fromHash.Hex())
	}
	tc, ok := d.commits[toHash]
	if !ok {
		return CommitDiff{}, kdberr.NewVersionNotFoundError("to commit missing", d.NamespaceID, toHash.Hex())
	}
	fromTree, ok := d.trees[fc.DocumentTreeHash]
	if !ok {
		return CommitDiff{}, kdberr.NewVersionNotFoundError("from tree missing", d.NamespaceID, fc.DocumentTreeHash.Hex())
	}
	toTree, ok := d.trees[tc.DocumentTreeHash]
	if !ok {
		return CommitDiff{}, kdberr.NewVersionNotFoundError("to tree missing", d.NamespaceID, tc.DocumentTreeHash.Hex())
	}
	// MaterializedEntries, not the .Entries field directly: With/Without-derived trees don't
	// eagerly populate it (see DocumentTree's own doc comment) - Diff is the kind of full-scan
	// operation that genuinely needs the flat map, unlike the per-write hot path that used to
	// force this same materialization on every single commit.
	toEntries := toTree.MaterializedEntries()
	fromEntries := fromTree.MaterializedEntries()
	var entries []DiffEntry
	for id, h := range toEntries {
		if oh, ok := fromEntries[id]; !ok {
			entries = append(entries, DiffAdded{DocID: id, ContentHash: h})
		} else if oh != h {
			entries = append(entries, DiffModified{DocID: id, FromContentHash: oh, ToContentHash: h})
		}
	}
	for id, h := range fromEntries {
		if _, ok := toEntries[id]; !ok {
			entries = append(entries, DiffRemoved{DocID: id, ContentHash: h})
		}
	}
	return CommitDiff{FromHash: fromHash, ToHash: toHash, Entries: entries}, nil
}

// AppendCommit appends a commit onto parentHash and advances the default branch to it, but only
// if the branch is still at parentHash - the compare-and-swap this used to lack.
//
// Every caller reaches here the same way: read Head, plan a transaction against it (conflict
// detection, schema validation, staging writes), then append with that head as the parent. None
// of that planning holds the DAG lock, so between the read and this call another writer can
// advance the branch. Advancing unconditionally then meant the loser's commit was stored but
// unreachable from the branch - an acknowledged write that had silently vanished. Refusing with
// a *HeadConflictError instead turns that into something the caller can see and retry against
// the new head. The server serializes writers through its writeGate so this should not fire
// there; it is the guarantee for everyone else (embedded callers, peer sync, direct API use)
// who has no such gate.
//
// Use AppendCommitDetached for the deliberate exception: re-pointing the branch at a commit
// built somewhere other than its current tip.
func (d *InMemoryCommitDag) AppendCommit(
	tx document.Transaction,
	parentHash codec.Hash,
	newDocumentTree document.DocumentTree,
	schemaHash *codec.Hash,
	message string,
) (document.Commit, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.appendCommitLocked(tx, []codec.Hash{parentHash}, newDocumentTree, schemaHash, message, mainBranch, &parentHash)
}

// AppendCommitDetached is AppendCommit without the head compare-and-swap: it appends onto
// parentHash and re-points the default branch there regardless of where the branch currently is.
// This is for the operations that are deliberately non-linear - replaying a transaction onto an
// explicitly named target commit, rewinding a branch onto a rebuilt history - where "the parent
// is not the current head" is the request, not a race. Anything that means "extend the tip"
// wants AppendCommit.
func (d *InMemoryCommitDag) AppendCommitDetached(
	tx document.Transaction,
	parentHash codec.Hash,
	newDocumentTree document.DocumentTree,
	schemaHash *codec.Hash,
	message string,
) (document.Commit, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.appendCommitLocked(tx, []codec.Hash{parentHash}, newDocumentTree, schemaHash, message, mainBranch, nil)
}

// HeadIs reports whether the default branch is currently at hash. A cheap pre-flight for a
// caller about to do expensive planning work against a head it just read: it does not remove
// the need for AppendCommit's compare-and-swap (the branch can still move immediately after
// this returns), it just lets an already-lost writer stop before staging anything.
func (d *InMemoryCommitDag) HeadIs(hash codec.Hash) bool {
	h, err := d.Head()
	return err == nil && h == hash
}

func (d *InMemoryCommitDag) appendCommitLocked(
	tx document.Transaction,
	parents []codec.Hash,
	newDocumentTree document.DocumentTree,
	schemaHash *codec.Hash,
	message string,
	branchToAdvance string,
	expectedHead *codec.Hash,
) (document.Commit, error) {
	for _, p := range parents {
		if err := d.requireCommitPresentLocked(p); err != nil {
			return document.Commit{}, err
		}
	}
	// The compare-and-swap, checked before anything is written: a caller that planned against a
	// head the branch has since moved off gets a conflict, not a silently orphaned commit. nil
	// means the caller is deliberately re-pointing the branch (see AppendCommitDetached).
	if expectedHead != nil {
		current, ok := d.branches[branchToAdvance]
		if !ok {
			return document.Commit{}, NewBranchNotFoundError("branch not found", d.NamespaceID, branchToAdvance)
		}
		if current.HeadHash != *expectedHead {
			return document.Commit{}, NewHeadConflictError(
				d.NamespaceID, branchToAdvance, *expectedHead, current.HeadHash,
			)
		}
	}
	d.trees[newDocumentTree.TreeHash] = newDocumentTree
	commit, err := document.BuildCommit(
		parents, d.NamespaceID, tx.ID, tx.Timestamp, tx.AuthorNodeID,
		tx.Operations, newDocumentTree.TreeHash, schemaHash, message,
	)
	if err != nil {
		return document.Commit{}, err
	}
	// BuildCommit derived commit.Hash from these fields just now, so there is
	// nothing an immediate recompute could catch - see putCommitLocked.
	if err := d.putCommitLocked(commit, true, false); err != nil {
		return document.Commit{}, err
	}
	b, ok := d.branches[branchToAdvance]
	if !ok {
		return document.Commit{}, NewConsistencyError("branch missing", d.NamespaceID, &commit.Hash)
	}
	now := codec.TimestampNow()
	b.HeadHash = commit.Hash
	b.UpdatedAt = now
	d.branches[branchToAdvance] = b
	// The head just moved: republish so readers see the new commit. Covers every caller of
	// this core - AppendCommit, AppendCommitDetached and AppendMergeCommitOnto.
	d.publishHeadLocked()
	return commit, nil
}

func (d *InMemoryCommitDag) Squash(
	squashHashes []codec.Hash,
	boundary codec.Hash,
	syntheticTree document.DocumentTree,
	syntheticSchemaHash *codec.Hash,
	message string,
) (document.Commit, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	squashSet := make(map[codec.Hash]struct{}, len(squashHashes))
	for _, h := range squashHashes {
		squashSet[h] = struct{}{}
	}
	if _, ok := d.commits[boundary]; !ok {
		return document.Commit{}, kdberr.NewVersionNotFoundError("boundary missing", d.NamespaceID, boundary.Hex())
	}
	for _, h := range squashHashes {
		if _, ok := d.commits[h]; !ok {
			return document.Commit{}, NewConsistencyError("squash target missing", d.NamespaceID, &h)
		}
	}
	for _, b := range d.branches {
		if _, in := squashSet[b.HeadHash]; in {
			return document.Commit{}, NewCompactionSafetyError(
				"branch head inside squash window", d.NamespaceID, b.HeadHash, "branch="+b.Name,
			)
		}
	}
	// Readers are a retention root too. A branch head is the only pin the window check used to
	// know about, which left every open SNAPSHOT read and every queued commit's base version
	// free to be squashed out from under it - they are not branch heads, and until Pin existed
	// there was nowhere to look them up. See Pin in retention.go.
	for _, h := range squashHashes {
		if err := d.assertUnpinnedLocked(h, "inside squash window"); err != nil {
			return document.Commit{}, err
		}
	}
	syntheticTx, _ := codec.UUIDFromString("00000000-0000-4000-8000-000000000003")
	syntheticAuthor, _ := codec.UUIDFromString("00000000-0000-4000-8000-000000000004")
	d.trees[syntheticTree.TreeHash] = syntheticTree
	synthetic, err := document.BuildCommit(
		nil, d.NamespaceID, syntheticTx, codec.TimestampNow(), syntheticAuthor,
		nil, syntheticTree.TreeHash, syntheticSchemaHash, message,
	)
	if err != nil {
		return document.Commit{}, err
	}
	for name, tag := range d.tags {
		if _, in := squashSet[tag.CommitHash]; in {
			tag.CommitHash = synthetic.Hash
			d.tags[name] = tag
		}
	}
	for _, h := range squashHashes {
		delete(d.commits, h)
		delete(d.stubs, h)
		d.removeHex(h.Hex())
	}
	d.commits[synthetic.Hash] = synthetic
	d.insertHex(synthetic.Hash.Hex())
	d.ancestryVersion++
	// Squash refuses to run with a branch head inside the window, so the head's commit
	// survives; republish regardless, since the check above is the only thing guaranteeing
	// that and it is checked against branches this call does not otherwise touch.
	d.publishHeadLocked()
	return synthetic, nil
}

func (d *InMemoryCommitDag) requireCommitPresentLocked(hash codec.Hash) error {
	if _, ok := d.commits[hash]; !ok {
		if _, ok2 := d.stubs[hash]; !ok2 {
			return NewConsistencyError("missing commit "+hash.Hex(), d.NamespaceID, &hash)
		}
	}
	return nil
}
