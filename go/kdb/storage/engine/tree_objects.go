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

// HistoryMode reports how long this namespace keeps the past. Resolved
// from the namespace's marker at open; see storage.HistoryMode.
func (e *ServerEngine) HistoryMode() storage.HistoryMode {
	if e.config.HistoryMode == storage.HistoryModeUnset {
		return storage.DefaultHistoryMode
	}
	return e.config.HistoryMode
}

// RetentionWindow reports how much of the past this namespace keeps under
// storage.HistoryModeNone. Meaningless, and ignored, under Full.
func (e *ServerEngine) RetentionWindow() storage.RetentionWindow {
	return e.config.Retain.Resolve()
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
// full object and applies forward.
//
// "Long enough" is measured in entries, not objects - see treeChainFullAt.
// Measuring it in objects is what made the commit path quadratic in the
// namespace, because it divided a full object's O(documents) by a constant
// number of commits rather than by the work that justified it.

// documentLocationTag marks an object-store record as a pointer into the
// delta log rather than a tree object. Both are keyed by a hash and share
// the store, so the first byte says which is which; a tree object's first
// byte is treeObjectVersion, and decodeTreeObject rejects anything else.
const documentLocationTag = 2

// documentLocationBytes is the whole record: tag, segment sequence, frame
// offset. Seventeen bytes to locate a document version, against a second
// copy of the version's text, which is the entire point of it.
const documentLocationBytes = 1 + 8 + 8

const (
	treeObjectVersion  = 1
	treeObjectKindFull = 0
	treeObjectKindDiff = 1

	// defaultTreeChainLimit is the shortest chain a namespace will use,
	// when nothing configures it. It is the direct trade between write
	// cost (a full object is O(documents)) and historical-read cost (a
	// chain is walked one fetch at a time).
	//
	// It is a floor rather than the bound itself. Held fixed, it makes the
	// commit path quadratic in the namespace: a full object costs O(n) and
	// one is written every 32 commits whatever n is, so the amortized cost
	// per commit grows with the namespace without limit. Measured, that
	// was 63% of commit CPU at 10,000 documents and still climbing - see
	// docs/benchmarks/2026-09-07-perf-rerun-7947cce.md. treeChainLimitFor
	// scales the interval with the tree instead.
	defaultTreeChainLimit = 32

	// maxTreeChainWalk caps how many objects one resolution may fetch,
	// however long a chain claims to be. Nothing legitimate reaches it -
	// it is there so a corrupt chainLen costs a bounded walk and a
	// fallback rather than an unbounded one.
	maxTreeChainWalk = 1 << 20
)

// treeChainLimit is the configured chain floor, or the default.
func (e *ServerEngine) treeChainLimit() int {
	if e.config.TreeChainLimit > 0 {
		return e.config.TreeChainLimit
	}
	return defaultTreeChainLimit
}

// treeChainState is what stands behind a tree in delta objects: how many
// objects, and how many entries they carry between them.
//
// Both are needed and they are not interchangeable. The entry count is
// what decides when to write a full object, because it is what a
// resolution actually pays. The object count is what a resolution is
// allowed to fetch, and it is the number recorded on disk as a chain's
// length - see treeFromObjects.
type treeChainState struct {
	objects int
	entries int64
}

// treeChainFullAt is how many entries may accumulate in the delta objects
// behind a tree of size documents before the next one is written full.
//
// The rule is that a full object is written once the chain behind it has
// cost as much as the full object would: rewrite when the delta exceeds
// the base. That is what keeps both ends bounded.
//
//   - Writes: a full object costs O(size) and is now written only after
//     size entries have accumulated, so each commit carries about one
//     entry's worth of it - constant, whatever the namespace has grown to.
//     Counting objects instead of entries is what made this quadratic: the
//     full object's O(size) was divided by a fixed 32 commits, so the
//     amortized cost per commit grew with the namespace forever.
//   - Reads: resolving a historical tree applies the full object's size
//     entries plus the chain's, and the chain is held to size of them. So
//     a resolution costs at most twice its own floor - the tree it is
//     rebuilding has to be built out of that many entries either way.
//
// What it spends is fetches, not work: the chain may be up to size objects
// long where it was 32, and each object is one memtable lookup paired with
// an entry this read had to apply regardless.
//
// The floor keeps small namespaces on the short chains they have always
// used, and is the meaning of the TreeChainLimit setting.
func (e *ServerEngine) treeChainFullAt(size int) int64 {
	if limit := e.treeChainLimit(); size < limit {
		return int64(limit)
	}
	return int64(size)
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
// except on the commits that write the whole tree to cap what resolving a
// historical read costs - see treeChainFullAt for when that is.
func (e *ServerEngine) putTreeObject(base document.DocumentTree, result document.DocumentTree, puts []TreeChange, deletes []codec.UUID) {
	if e.memTable == nil || e.config.HistoryStrategy != storage.HistoryStrategyObjects {
		return
	}
	if e.skipExistingObject(result.TreeHash) {
		return
	}
	prev := e.treeChainStateOf(base.TreeHash)
	chain := treeChainState{
		objects: prev.objects + 1,
		entries: prev.entries + int64(len(puts)) + int64(len(deletes)),
	}
	o := treeObject{
		kind:     treeObjectKindDiff,
		baseHash: base.TreeHash,
		chainLen: int32(chain.objects),
		puts:     puts,
		deletes:  deletes,
	}
	if chain.entries >= e.treeChainFullAt(result.Size()) {
		chain = treeChainState{}
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
		e.treeChain = make(map[codec.Hash]treeChainState)
	}
	e.treeChain[result.TreeHash] = chain
	e.treeChainMu.Unlock()
}

// putDocumentLocation records where in the delta log a document version
// lives, under that version's content hash.
//
// This is the other half of what the objects strategy buys. Resolving a
// historical read through tree objects gets as far as "this commit's tree
// says document D had content hash H", and then has to find H's bytes.
// Without something addressed by H the only place they exist is the delta
// log, and finding them there means scanning it - the same full pass the
// tree objects were meant to remove, one level down.
//
// What is stored is a pointer, not the bytes. Storing the bytes worked and
// was what this did first, but it put every version's text on disk twice,
// in the log and here. The log is the journal: it is what a repair reads,
// what a peer receives, and the thing every other copy would have to be
// reconciled against. So the version stays there once, and this says where
// - seventeen bytes against a duplicate of the document.
//
// Nothing here is load-bearing for correctness. A missing or unreadable
// location costs a scan (see ServerEngine.loadCold), never an answer.
func (e *ServerEngine) putDocumentLocation(contentHash codec.Hash, segmentSeq, frameOffset int64) {
	if e.memTable == nil || e.config.HistoryStrategy != storage.HistoryStrategyObjects {
		return
	}
	if e.skipExistingObject(contentHash) {
		return
	}
	e.memTable.Put(contentHash, encodeDocumentLocation(segmentSeq, frameOffset))
}

func encodeDocumentLocation(segmentSeq, frameOffset int64) []byte {
	b := make([]byte, documentLocationBytes)
	b[0] = documentLocationTag
	binary.BigEndian.PutUint64(b[1:], uint64(segmentSeq))
	binary.BigEndian.PutUint64(b[9:], uint64(frameOffset))
	return b
}

func decodeDocumentLocation(b []byte) (segmentSeq, frameOffset int64, ok bool) {
	if len(b) != documentLocationBytes || b[0] != documentLocationTag {
		return 0, 0, false
	}
	return int64(binary.BigEndian.Uint64(b[1:])), int64(binary.BigEndian.Uint64(b[9:])), true
}

// skipExistingObject reports an object that is already stored and need not
// be written again.
//
// Only consulted while replaying. Replay re-derives every version the log
// holds, and on a namespace whose objects were already written that is all
// of them - rewriting each one costs a flush cycle to store bytes that are
// already there. On the ordinary write path the answer would always be no
// (the content hash is new), so the lookup would be a guaranteed miss
// walking every table's index on the hot path, which is why this is not
// asked there.
func (e *ServerEngine) skipExistingObject(h codec.Hash) bool {
	if !e.replaying.Load() {
		return false
	}
	return e.memTable.Get(h) != nil
}

// SetReplaying marks the window in which this engine is being rebuilt from
// the delta log rather than taking new writes. Replay is single-threaded
// and happens at open, before the runtime is handed to anyone.
func (e *ServerEngine) SetReplaying(v bool) { e.replaying.Store(v) }

// documentFromObject resolves a version through the object store.
//
// Two record shapes are accepted. A location record points into the delta
// log and is read from there. Anything else is treated as the version's own
// text, which is what this store held before locations replaced them - a
// namespace written by an earlier build keeps resolving at full speed
// instead of falling back to a scan.
//
// Either way the result is checked against the content hash it was filed
// under. Content addressing makes that exact, and it is what lets every
// failure here be a miss rather than a wrong answer.
func (e *ServerEngine) documentFromObject(docID codec.UUID, contentHash codec.Hash) (document.Document, bool) {
	if e.memTable == nil || e.config.HistoryStrategy != storage.HistoryStrategyObjects {
		return document.Document{}, false
	}
	raw := e.memTable.Get(contentHash)
	if raw == nil {
		return document.Document{}, false
	}
	if seq, offset, ok := decodeDocumentLocation(raw); ok {
		return e.documentFromLog(docID, contentHash, seq, offset)
	}
	return verifiedDocument(docID, string(raw), contentHash)
}

// documentFromLog reads the frame a location record points at and picks
// the version out of it.
func (e *ServerEngine) documentFromLog(docID codec.UUID, contentHash codec.Hash, segmentSeq, frameOffset int64) (document.Document, bool) {
	reader := e.frameReader.Load()
	if reader == nil {
		return document.Document{}, false
	}
	commit, err := (*reader)(segmentSeq, frameOffset)
	if err != nil {
		return document.Document{}, false
	}
	for _, op := range commit.Operations {
		w, isWrite := op.(document.WriteOp)
		if !isWrite || w.DocID != docID {
			continue
		}
		// A frame holds a whole commit, which can write the same document
		// more than once and several documents besides; only the operation
		// whose content actually matches is the version being asked for.
		if doc, ok := verifiedDocument(docID, w.Patch, contentHash); ok {
			return doc, true
		}
	}
	return document.Document{}, false
}

func verifiedDocument(docID codec.UUID, text string, want codec.Hash) (document.Document, bool) {
	doc := documentFromPatch(docID, text)
	h, err := doc.ContentHash()
	if err != nil || h != want {
		return document.Document{}, false
	}
	return doc, true
}

// commitFrameReader reads the commit framed at one place in the delta log.
type commitFrameReader func(segmentSeq, frameOffset int64) (document.Commit, error)

// SetCommitFrameReader installs the reader that follows a document
// location record back to the journal. Without one, location records
// resolve to nothing and reads fall through to the scanning loader.
func (e *ServerEngine) SetCommitFrameReader(fn commitFrameReader) {
	if fn == nil {
		e.frameReader.Store(nil)
		return
	}
	e.frameReader.Store(&fn)
}

// RecordDocumentLocation records where a historical version lives - the
// migration's counterpart to putDocumentLocation, for versions written
// before the namespace used this strategy.
func (e *ServerEngine) RecordDocumentLocation(contentHash codec.Hash, segmentSeq, frameOffset int64) {
	e.putDocumentLocation(contentHash, segmentSeq, frameOffset)
}

// RecordTreeObject records the tree that results from applying puts and
// deletes to base, under result's own hash - the same thing the commit
// path does, exposed for the offline migration that has to record objects
// for commits written before the namespace used this strategy.
func (e *ServerEngine) RecordTreeObject(base, result document.DocumentTree, puts []TreeChange, deletes []codec.UUID) {
	e.putTreeObject(base, result, puts, deletes)
}

// cachedTreeFor answers from what is already in memory, without disturbing it: the live
// snapshot first, then the bounded store without promoting what it looks at.
func (e *ServerEngine) cachedTreeFor(h codec.Hash) (document.DocumentTree, bool) {
	if s := e.latestTree.Load(); s != nil && s.hash == h {
		return s.tree, true
	}
	if e.treesByHash == nil {
		return document.DocumentTree{}, false
	}
	return e.treesByHash.Peek(h)
}

func (e *ServerEngine) treeChainStateOf(h codec.Hash) treeChainState {
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
	// What the forward pass starts from. Empty unless the walk finds an ancestor already in
	// memory, which is the difference between a rebuild costing the namespace and costing the
	// commits since that ancestor.
	base := document.EmptyDocumentTree()
	// How far this may walk is set by the chain itself. The writer's
	// interval scales with the tree (treeChainLimitFor), so this end
	// cannot know the bound from configuration alone - but the first
	// object records how many diffs stand behind it, which is exactly it.
	// The configured floor still applies, so a chain written before the
	// interval scaled, or one whose counter was reset by a restart, is
	// walked exactly as it always was.
	limit := e.treeChainLimit()
	for i := 0; i <= limit; i++ {
		if current == emptyHash {
			grounded = true
			break
		}
		// A resident ancestor ends the walk. Grounding on the last full object instead means
		// applying the whole namespace forward - treeChainFullAt lets a chain run to size
		// entries, so a single resolution was O(namespace) however close a usable tree was.
		// Rare rather than frequent, which is the shape a per-commit average hides: at 108,000
		// documents a handful of these were 75% of a benchmark's CPU while every other commit
		// was fine. TestRebuildStopsAtAResidentAncestor measures 869 objects without this and
		// 5 with it.
		//
		// The same move the replay strategy's rebuild already makes (rebuildTreeByFolding folds
		// from the nearest cached ancestor rather than from genesis); the objects strategy
		// simply never did it. Peek rather than Get, so probing for an ancestor does not
		// promote entries and evict the very tree it is walking toward.
		if i > 0 {
			if t, ok := e.cachedTreeFor(current); ok {
				base = t
				grounded = true
				break
			}
		}
		raw := e.memTable.Get(current)
		if raw == nil {
			return document.DocumentTree{}, false
		}
		o, err := decodeTreeObject(raw)
		if err != nil {
			return document.DocumentTree{}, false
		}
		if i == 0 && int(o.chainLen) >= limit {
			limit = int(o.chainLen) + 1
			if limit > maxTreeChainWalk {
				limit = maxTreeChainWalk
			}
		}
		chain = append(chain, o)
		e.treeChainSteps.Add(1)
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
	tree := base
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

// maxPendingLocationTrees bounds how many commits' worth of content hashes
// wait for their log position.
//
// Ordinarily one: a commit is appended to the log immediately after its
// tree is built, and the batch that writes it reports back. The bound is
// for the cases where that never happens - memory-only durability, a
// namespace whose writes are never persisted, a batch that fails - where
// without it this would grow with every write. Dropping the oldest costs
// a scan on a later cold read, never an answer.
const maxPendingLocationTrees = 256

func (e *ServerEngine) stashPendingLocations(treeHash codec.Hash, changed []TreeChange) {
	if e.config.HistoryStrategy != storage.HistoryStrategyObjects || len(changed) == 0 {
		return
	}
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	if e.pendingLocations == nil {
		e.pendingLocations = make(map[codec.Hash][]TreeChange)
	}
	if _, seen := e.pendingLocations[treeHash]; !seen {
		e.pendingOrder = append(e.pendingOrder, treeHash)
	}
	e.pendingLocations[treeHash] = changed
	for len(e.pendingOrder) > maxPendingLocationTrees {
		delete(e.pendingLocations, e.pendingOrder[0])
		e.pendingOrder = e.pendingOrder[1:]
	}
}

// RecordCommitLocation is told where in the delta log the commit naming
// treeHash was written, and files that position under the content hash of
// every version the commit wrote.
//
// Called after the append, because that is when there is a position to
// record. A commit whose stashed hashes have already been dropped, or that
// was never stashed, records nothing - the index is an optimisation, and
// its absence costs a scan rather than an answer.
func (e *ServerEngine) RecordCommitLocation(treeHash codec.Hash, segmentSeq, frameOffset int64) {
	if e.config.HistoryStrategy != storage.HistoryStrategyObjects {
		return
	}
	e.pendingMu.Lock()
	changed, ok := e.pendingLocations[treeHash]
	if ok {
		delete(e.pendingLocations, treeHash)
		for i, h := range e.pendingOrder {
			if h == treeHash {
				e.pendingOrder = append(e.pendingOrder[:i], e.pendingOrder[i+1:]...)
				break
			}
		}
	}
	e.pendingMu.Unlock()
	if !ok {
		return
	}
	for _, c := range changed {
		e.putDocumentLocation(c.ContentHash, segmentSeq, frameOffset)
	}
}
