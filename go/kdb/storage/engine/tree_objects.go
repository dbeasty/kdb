package engine

import (
	"encoding/binary"
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// HistoryStrategy reports how this engine serves reads at historical
// commits. Resolved from the namespace's own marker at open, never from a
// caller's preference alone - see embed.resolveHistoryStrategy.
func (e *ServerEngine) HistoryStrategy() storage.HistoryStrategy {
	return e.config.HistoryStrategy
}

// Tree objects: what a document tree looked like at one commit, stored on
// disk under that tree's own hash.
//
// This is the piece that lets a read at a historical commit resolve by
// address instead of by search. Without it the docID-to-content-hash
// mapping a commit had exists only in memory, and recovering it after a
// restart means replaying the delta log and re-hashing every document -
// correct, but a full scan for one read. Git does not have that problem
// because a tree is an object: commit names tree, tree names blobs, every
// step a hash lookup.
//
// Two things stop this from being a straight copy of git's tree object.
//
// A KDB document tree is flat - every document in the namespace hangs off
// the root - so a whole-tree object costs O(documents) per commit, which
// is a write-path regression on any namespace with more than a few
// documents. Git avoids that by nesting trees per directory; there are no
// directories here.
//
// The obvious alternative, persisting the in-memory Merkle trie's nodes,
// is worse for the common case: that trie is fixed-depth 32 with no path
// compression (see document/document_tree_trie.go), so touching one
// document creates 32 internal nodes of 513 bytes each - about 16.5KB per
// write, whatever the document's size, and 16.5KB to record a change to a
// 200-byte document is not a trade worth making.
//
// So a tree object is a *delta* against the previous tree - the entries
// that changed, which the commit already knows - with a full object
// written whenever the chain behind it gets long enough that reading it
// would cost more than rewriting it. Resolution walks back to the nearest
// full object and applies forward, bounded by treeChainLimit.

const (
	treeObjectVersion  = 1
	treeObjectKindFull = 0
	treeObjectKindDiff = 1

	// defaultTreeChainLimit bounds how many delta objects may stack up
	// before a full one is written, when nothing configures it. It is the
	// direct trade between write cost (a full object is O(documents)) and
	// historical-read cost (a chain is walked one fetch at a time). 32
	// keeps a historical read to at most 32 small fetches while amortizing
	// the full object across 32 commits.
	defaultTreeChainLimit = 32
)

// treeChainLimit is the configured bound, or the default.
func (e *ServerEngine) treeChainLimit() int {
	if e.config.TreeChainLimit > 0 {
		return e.config.TreeChainLimit
	}
	return defaultTreeChainLimit
}

// treeObject is one tree recorded relative to the one before it.
type treeObject struct {
	kind     byte
	baseHash codec.Hash
	chainLen int32
	puts     []TreeChange
	deletes  []codec.UUID
}

// TreeChange is one document's content hash within a commit's tree - the
// unit a tree object records. Exported because the offline migration
// (embed.MigrateHistoryStrategy) records objects for history the write
// path never saw.
type TreeChange struct {
	DocID       codec.UUID
	ContentHash codec.Hash
}

func encodeTreeObject(o treeObject) []byte {
	size := 1 + 1 + 32 + 4 + 4 + 4 + len(o.puts)*48 + len(o.deletes)*16
	buf := make([]byte, 0, size)
	buf = append(buf, treeObjectVersion, o.kind)
	buf = append(buf, o.baseHash.Bytes[:]...)
	var scratch [4]byte
	binary.BigEndian.PutUint32(scratch[:], uint32(o.chainLen))
	buf = append(buf, scratch[:]...)
	binary.BigEndian.PutUint32(scratch[:], uint32(len(o.puts)))
	buf = append(buf, scratch[:]...)
	for _, p := range o.puts {
		buf = appendUUID(buf, p.DocID)
		buf = append(buf, p.ContentHash.Bytes[:]...)
	}
	binary.BigEndian.PutUint32(scratch[:], uint32(len(o.deletes)))
	buf = append(buf, scratch[:]...)
	for _, d := range o.deletes {
		buf = appendUUID(buf, d)
	}
	return buf
}

func decodeTreeObject(b []byte) (treeObject, error) {
	var o treeObject
	if len(b) < 2+32+8 {
		return o, fmt.Errorf("kdb: tree object is too short to be one")
	}
	if b[0] != treeObjectVersion {
		return o, fmt.Errorf("kdb: tree object is version %d, this build reads %d", b[0], treeObjectVersion)
	}
	o.kind = b[1]
	if o.kind != treeObjectKindFull && o.kind != treeObjectKindDiff {
		return o, fmt.Errorf("kdb: tree object has unknown kind %d", o.kind)
	}
	off := 2
	h, err := codec.HashFromBytes(b[off : off+32])
	if err != nil {
		return o, err
	}
	o.baseHash = h
	off += 32
	o.chainLen = int32(binary.BigEndian.Uint32(b[off:]))
	off += 4
	putCount := int(binary.BigEndian.Uint32(b[off:]))
	off += 4
	if putCount < 0 || off+putCount*48 > len(b) {
		return o, fmt.Errorf("kdb: tree object declares %d entries it does not contain", putCount)
	}
	o.puts = make([]TreeChange, 0, putCount)
	for i := 0; i < putCount; i++ {
		id := readUUID(b[off:])
		off += 16
		ch, err := codec.HashFromBytes(b[off : off+32])
		if err != nil {
			return o, err
		}
		off += 32
		o.puts = append(o.puts, TreeChange{DocID: id, ContentHash: ch})
	}
	if off+4 > len(b) {
		return o, fmt.Errorf("kdb: tree object is missing its delete count")
	}
	delCount := int(binary.BigEndian.Uint32(b[off:]))
	off += 4
	if delCount < 0 || off+delCount*16 > len(b) {
		return o, fmt.Errorf("kdb: tree object declares %d deletes it does not contain", delCount)
	}
	o.deletes = make([]codec.UUID, 0, delCount)
	for i := 0; i < delCount; i++ {
		o.deletes = append(o.deletes, readUUID(b[off:]))
		off += 16
	}
	return o, nil
}

func appendUUID(buf []byte, u codec.UUID) []byte {
	var scratch [8]byte
	binary.BigEndian.PutUint64(scratch[:], uint64(u.MSB))
	buf = append(buf, scratch[:]...)
	binary.BigEndian.PutUint64(scratch[:], uint64(u.LSB))
	return append(buf, scratch[:]...)
}

func readUUID(b []byte) codec.UUID {
	return codec.UUID{
		MSB: int64(binary.BigEndian.Uint64(b)),
		LSB: int64(binary.BigEndian.Uint64(b[8:])),
	}
}

// putTreeObject records the tree that results from applying puts/deletes
// to the tree at baseHash, under the resulting tree's own hash.
//
// Called on the commit path, so it stays proportional to what changed
// except every treeChainLimit-th commit, where it writes the whole tree to
// cap how long a historical read's chain can get.
func (e *ServerEngine) putTreeObject(base document.DocumentTree, result document.DocumentTree, puts []TreeChange, deletes []codec.UUID) {
	if e.memTable == nil || e.config.HistoryStrategy != storage.HistoryStrategyObjects {
		return
	}
	chain := e.treeChainLen(base.TreeHash) + 1
	o := treeObject{
		kind:     treeObjectKindDiff,
		baseHash: base.TreeHash,
		chainLen: int32(chain),
		puts:     puts,
		deletes:  deletes,
	}
	if chain >= e.treeChainLimit() {
		o.kind = treeObjectKindFull
		o.chainLen = 0
		o.baseHash = document.EmptyDocumentTree().TreeHash
		o.deletes = nil
		// A fresh slice, not puts[:0]: that would write the whole tree into
		// the caller's own backing array.
		o.puts = make([]TreeChange, 0, result.Size())
		result.Walk(func(id codec.UUID, h codec.Hash) bool {
			o.puts = append(o.puts, TreeChange{DocID: id, ContentHash: h})
			return true
		})
	}
	e.memTable.Put(result.TreeHash, encodeTreeObject(o))
	e.treeChainMu.Lock()
	if e.treeChain == nil {
		e.treeChain = make(map[codec.Hash]int)
	}
	e.treeChain[result.TreeHash] = int(o.chainLen)
	e.treeChainMu.Unlock()
}

// putDocumentObject stores one document version under its content hash,
// making it addressable the way a git blob is.
//
// Only under the objects strategy, and it is the other half of what that
// strategy buys. Resolving a historical read through tree objects gets as
// far as "this commit's tree says document D had content hash H" and then
// has to find H's bytes; without an object for H the only place they exist
// is the delta log, and finding them there means scanning it - the same
// full pass the tree objects were meant to remove, one level down.
//
// The cost is that a version's text is on disk twice, in the log and here.
// That is the trade the objects strategy makes, and the reason replay
// still exists for deployments that would rather not make it.
func (e *ServerEngine) putDocumentObject(contentHash codec.Hash, doc document.Document) {
	if e.memTable == nil || e.config.HistoryStrategy != storage.HistoryStrategyObjects {
		return
	}
	e.memTable.Put(contentHash, []byte(doc.JSON))
}

// documentFromObject returns a version's bytes from the object store.
func (e *ServerEngine) documentFromObject(docID codec.UUID, contentHash codec.Hash) (document.Document, bool) {
	if e.memTable == nil || e.config.HistoryStrategy != storage.HistoryStrategyObjects {
		return document.Document{}, false
	}
	raw := e.memTable.Get(contentHash)
	if raw == nil {
		return document.Document{}, false
	}
	doc := documentFromPatch(docID, string(raw))
	// Content-addressed, so this is exact: bytes that do not hash to the
	// key they were filed under are not the version being asked for.
	h, err := doc.ContentHash()
	if err != nil || h != contentHash {
		return document.Document{}, false
	}
	return doc, true
}

// RecordDocumentObject stores one historical version as an object - the
// migration's counterpart to putDocumentObject, for versions written
// before the namespace used this strategy.
func (e *ServerEngine) RecordDocumentObject(contentHash codec.Hash, doc document.Document) {
	e.putDocumentObject(contentHash, doc)
}

// RecordTreeObject records the tree that results from applying puts and
// deletes to base, under result's own hash - the same thing the commit
// path does, exposed for the offline migration that has to record objects
// for commits written before the namespace used this strategy.
func (e *ServerEngine) RecordTreeObject(base, result document.DocumentTree, puts []TreeChange, deletes []codec.UUID) {
	e.putTreeObject(base, result, puts, deletes)
}

func (e *ServerEngine) treeChainLen(h codec.Hash) int {
	e.treeChainMu.Lock()
	defer e.treeChainMu.Unlock()
	return e.treeChain[h]
}

// treeFromObjects rebuilds the tree named by hash by walking its object
// back to the nearest full one and applying forward.
//
// The result is checked against the hash it was looked up under before
// being returned. A tree is content-addressed, so that check is exact and
// makes the whole object store self-verifying: a truncated chain, a stale
// object, or a bug in this file's encoding produces a miss and a fall back
// to the delta log rather than a wrong answer to a historical read.
func (e *ServerEngine) treeFromObjects(hash codec.Hash) (document.DocumentTree, bool) {
	if e.memTable == nil || e.config.HistoryStrategy != storage.HistoryStrategyObjects {
		return document.DocumentTree{}, false
	}
	// The walk ends at either a full object or the empty tree. The empty
	// tree is a terminator in its own right and has no object of its own:
	// the first tree a namespace ever writes is a diff against it, so
	// requiring a full object at the end of every chain would make every
	// tree before the thirty-second unresolvable.
	emptyHash := document.EmptyDocumentTree().TreeHash
	var chain []treeObject
	current := hash
	grounded := false
	limit := e.treeChainLimit()
	for i := 0; i <= limit; i++ {
		if current == emptyHash {
			grounded = true
			break
		}
		raw := e.memTable.Get(current)
		if raw == nil {
			return document.DocumentTree{}, false
		}
		o, err := decodeTreeObject(raw)
		if err != nil {
			return document.DocumentTree{}, false
		}
		chain = append(chain, o)
		if o.kind == treeObjectKindFull {
			grounded = true
			break
		}
		current = o.baseHash
	}
	if !grounded {
		// Longer than putTreeObject is supposed to allow: something wrote
		// this store that does not share this file's rules.
		return document.DocumentTree{}, false
	}
	tree := document.EmptyDocumentTree()
	var err error
	for i := len(chain) - 1; i >= 0; i-- {
		o := chain[i]
		for _, d := range o.deletes {
			if tree, err = tree.Without(d); err != nil {
				return document.DocumentTree{}, false
			}
		}
		for _, p := range o.puts {
			if tree, err = tree.With(p.DocID, p.ContentHash); err != nil {
				return document.DocumentTree{}, false
			}
		}
	}
	if tree.TreeHash != hash {
		return document.DocumentTree{}, false
	}
	return tree, true
}
