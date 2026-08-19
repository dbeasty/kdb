package dag

import (
	"sort"
	"strings"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/document"
)

const mainBranch = "main"

// InMemoryCommitDag is an in-memory commit DAG for one namespace.
type InMemoryCommitDag struct {
	NamespaceID string

	mu       sync.RWMutex
	commits  map[codec.Hash]document.Commit
	stubs    map[codec.Hash]document.CommitStub
	trees    map[codec.Hash]document.DocumentTree
	branches map[string]document.Branch
	tags     map[string]document.Tag
	hexSorted []string
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
	now := codec.TimestampNow()
	d.branches[mainBranch] = document.Branch{
		Name: mainBranch, NamespaceID: namespaceID,
		HeadHash: genesis.Hash, CreatedAt: now, UpdatedAt: now,
	}
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
	return d.putCommitLocked(commit, requireParents)
}

func (d *InMemoryCommitDag) putCommitLocked(commit document.Commit, requireParents bool) error {
	if _, ok := d.commits[commit.Hash]; ok {
		return nil
	}
	recomputed, err := document.ComputeCommitHash(commit)
	if err != nil {
		return err
	}
	if recomputed != commit.Hash {
		return NewConsistencyError("commit hash mismatch", d.NamespaceID, &commit.Hash)
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
	return nil
}

// StubCommit archives a commit in place.
func (d *InMemoryCommitDag) StubCommit(hash codec.Hash, archiveLocation string) (document.CommitStub, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.commits[hash]; !ok {
		return document.CommitStub{}, NewConsistencyError("cannot stub unknown commit", d.NamespaceID, &hash)
	}
	delete(d.commits, hash)
	d.removeHex(hash.Hex())
	stub := document.CommitStub{
		OriginalHash: hash, ArchiveLocation: archiveLocation, StubbedAt: codec.TimestampNow(),
	}
	d.stubs[hash] = stub
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
	d.mu.RLock()
	defer d.mu.RUnlock()
	b, ok := d.branches[mainBranch]
	if !ok {
		return codec.Hash{}, NewConsistencyError("missing default branch", d.NamespaceID, nil)
	}
	return b.HeadHash, nil
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
			break
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
	var entries []DiffEntry
	for id, h := range toTree.Entries {
		if oh, ok := fromTree.Entries[id]; !ok {
			entries = append(entries, DiffAdded{DocID: id, ContentHash: h})
		} else if oh != h {
			entries = append(entries, DiffModified{DocID: id, FromContentHash: oh, ToContentHash: h})
		}
	}
	for id, h := range fromTree.Entries {
		if _, ok := toTree.Entries[id]; !ok {
			entries = append(entries, DiffRemoved{DocID: id, ContentHash: h})
		}
	}
	return CommitDiff{FromHash: fromHash, ToHash: toHash, Entries: entries}, nil
}

func (d *InMemoryCommitDag) AppendCommit(
	tx document.Transaction,
	parentHash codec.Hash,
	newDocumentTree document.DocumentTree,
	schemaHash *codec.Hash,
	message string,
) (document.Commit, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.appendCommitLocked(tx, []codec.Hash{parentHash}, newDocumentTree, schemaHash, message, mainBranch)
}

func (d *InMemoryCommitDag) appendCommitLocked(
	tx document.Transaction,
	parents []codec.Hash,
	newDocumentTree document.DocumentTree,
	schemaHash *codec.Hash,
	message string,
	branchToAdvance string,
) (document.Commit, error) {
	for _, p := range parents {
		if err := d.requireCommitPresentLocked(p); err != nil {
			return document.Commit{}, err
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
	if err := d.putCommitLocked(commit, true); err != nil {
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
