package document

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage/mmapfile"
)

// Pageable trees: a tree's cold records in a mapped file instead of the heap (A7 stage 2, in the
// spirit of Cassandra's trie-indexed SSTables). The slab (document_tree_slab.go) made the trie
// free to the collector; this makes it independent of memory.
//
// A tree segment (ns/<namespace>/tree/tree-<seq>.kdbt) is one whole tree written out: its leaf
// and node records in the slab's own formats, children as references local to the segment. It is
// immutable once written, and mapped read-only (storage/mmapfile), so the OS pages it and the
// bytes are charged to the memory budget as mapped bytes, not heap.
//
// A slab bound to a tree file (TreeFile) keeps its trees in two tiers, like the commit graph's
// segments and resident tier:
//
//   - the segment: what was written last. A slab reference with refFile set names a record in a
//     segment; a segment lives while anything resident (a record's child, a tree handle) refers
//     into it, and is unmapped and deleted when nothing does.
//   - the resident overlay: records updates created since, in the slab as before. Each update
//     still creates only its own path; the paths it does not touch stay in the segment.
//
// When the overlay passes the configured resident budget, the tree is written out again, whole,
// in the background (a full rewrite: no flushes of partial segments, so a segment never refers
// to another). The next update to any tree made before that rewrite first rebases it onto the
// new segment: wherever the tree's hash at a position equals the segment's, the subtree is the
// same and becomes a reference into the segment; it descends only where they differ, so a rebase
// costs what changed since the rewrite. What the rebased tree no longer refers to is released as
// its old versions are collected.
//
// The files are a cache, never an authority. A checkpoint still carries the packed tree; a
// segment written at a checkpoint is tagged with the digest of that packed image (a .src
// sidecar), so an open finding a segment with the same digest maps it instead of building
// anything, and an open that does not builds the segment from the packed image straight to the
// file - never through the heap. Any failure - an unreadable, mismatched or damaged file, no
// mmap on the platform, a big-endian host - falls back to building the tree in the heap.

const (
	treeSegMagic    = "KDBTRIE1"
	treeSegVersion  = 1
	treeSegHeader   = 128
	treeSegTrailer  = 8
	treeSegTailMark = 0x4b545231 // "KTR1"
	treeSegSuffix   = ".kdbt"
	treeSrcSuffix   = ".src"
	leafRecSize     = 80
	nodeRecSize     = 96

	// DefaultTreeResidentBytes is the resident overlay a file-backed tree keeps before it is
	// flushed to a segment.
	DefaultTreeResidentBytes = 64 << 20
	// DefaultTreeMaxSegments is the longest chain of segments before a compaction.
	DefaultTreeMaxSegments = 8
)

var treeCRC = crc32.MakeTable(crc32.Castagnoli)

// littleEndian: the records are mapped and read in place, so the files are only usable where
// the host's byte order is the file's.
var littleEndian = func() bool {
	x := uint16(1)
	return *(*byte)(unsafe.Pointer(&x)) == 1
}()

// TreeFileOptions configures where and how a namespace's trees are paged.
type TreeFileOptions struct {
	// Dir holds the namespace's tree segments.
	Dir string
	// ResidentBytes is the resident overlay before it is flushed to a segment;
	// DefaultTreeResidentBytes if not positive.
	ResidentBytes int64
	// MaxSegments is the longest chain of segments a tree may span before a write compacts it
	// into one; DefaultTreeMaxSegments if not positive.
	MaxSegments int
	// ReadOnly maps segments but never writes or deletes any.
	ReadOnly bool
}

// TreeFile is one namespace's tree directory: the segment sequence, and which segment to keep
// on disk for the next open. One per open namespace; shared by every slab bound to it.
type TreeFile struct {
	opts TreeFileOptions

	mu      sync.Mutex
	nextSeq uint64
	keep    map[uint64]bool // the newest checkpoint's segment and its chain: never deleted
	onWrite func()
	closed  bool
	failed  bool // a write failed; stop writing, keep serving from what is mapped and the heap
	// background counts rewrites in flight (maybeRewrite), which Close waits for: one that
	// outlived it could take the same next sequence as a TreeFile reopened on the directory, and
	// both rename onto one path. It counts onWrite calls in flight (wrote) too.
	background sync.WaitGroup
}

// treeFile is the slab's view of its binding.
type treeFile = TreeFile

// NewTreeFile opens (creating if need be) a namespace's tree directory. It returns
// ErrTreeFileUnsupported where mapping is not possible, and the caller keeps heap trees.
func NewTreeFile(opts TreeFileOptions) (*TreeFile, error) {
	if !littleEndian || !mmapfile.Supported {
		return nil, ErrTreeFileUnsupported
	}
	if opts.ResidentBytes <= 0 {
		opts.ResidentBytes = DefaultTreeResidentBytes
	}
	if opts.MaxSegments <= 0 {
		opts.MaxSegments = DefaultTreeMaxSegments
	}
	if !opts.ReadOnly {
		if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
			return nil, err
		}
	}
	f := &TreeFile{opts: opts, nextSeq: 1}
	for _, seq := range listTreeSegments(opts.Dir) {
		if seq >= f.nextSeq {
			f.nextSeq = seq + 1
		}
	}
	return f, nil
}

// ErrTreeFileUnsupported means trees cannot be paged on this host.
var ErrTreeFileUnsupported = errors.New("document: tree files are not supported on this platform")

// Close stops the binding deleting files - a later open of the same namespace may be using them -
// and waits for a background rewrite or onWrite call in flight to finish.
func (f *TreeFile) Close() {
	if f == nil {
		return
	}
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	f.background.Wait()
}

// startBackground registers a background write, refusing once f stops being writable. Under mu,
// so none can start after Close has marked f closed and begun waiting.
func (f *TreeFile) startBackground() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.opts.ReadOnly || f.closed || f.failed {
		return false
	}
	f.background.Add(1)
	return true
}

func (f *TreeFile) segPath(seq uint64) string {
	return filepath.Join(f.opts.Dir, "tree-"+strconv.FormatUint(seq, 10)+treeSegSuffix)
}

func (f *TreeFile) takeSeq() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.nextSeq
	f.nextSeq++
	return s
}

func (f *TreeFile) writable() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.opts.ReadOnly && !f.closed && !f.failed
}

func (f *TreeFile) fail(err error) {
	f.mu.Lock()
	f.failed = true
	f.mu.Unlock()
	log.Printf("kdb: tree file %s: %v - trees stay in memory from here", f.opts.Dir, err)
}

func listTreeSegments(dir string) []uint64 {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []uint64
	for _, e := range ents {
		n := e.Name()
		if !strings.HasPrefix(n, "tree-") || !strings.HasSuffix(n, treeSegSuffix) {
			continue
		}
		if seq, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(n, "tree-"), treeSegSuffix), 10, 64); err == nil {
			out = append(out, seq)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
	return out
}

// --- Segments --------------------------------------------------------------------------------

// Segment file layout (little-endian):
//
//	header  128 bytes: magic "KDBTRIE1", version u32, -, leaves u32, nodes u32, root u32 (a local
//	        reference), externs u32, documents u64 (0 when not known), root hash [32]
//	leaves  80 bytes each (leafRec)
//	nodes   96 bytes each (nodeRec; children are local references)
//	externs 16 bytes each: segment sequence u64, reference u32 local to that segment, -
//	trailer crc32c of everything before it u32, "KTR1" u32
//
// A local reference is 0, a leaf (refLeaf | i), a node (i), or an extern (refFile | i): a record
// in an older segment this one was flushed on top of. Indexes count from 1.

// segList is a slab's mapped segments, held apart from the slab so that a cleanup on the slab can
// unmap them (closeSegList) without keeping the slab alive.
type segList struct {
	p atomic.Pointer[[]*treeSegment]
}

func closeSegList(l *segList) {
	p := l.p.Load()
	if p == nil {
		return
	}
	for _, g := range *p {
		g.unmap(true)
	}
}

// treeSegment is one mapped segment.
type treeSegment struct {
	seq      uint64
	path     string
	file     *TreeFile
	mapping  *mmapfile.Mapping
	region   *mmapfile.Region
	data     []byte
	leaves   uint32
	nodes    uint32
	leafBase uint32 // global index of local leaf 0 (locals start at 1)
	nodeBase uint32
	nodeOff  int
	root     tref // local
	rootHash [32]byte
	docs     int
	nExterns uint32 // externs as written stay in the mapping, 16 bytes each at extOff
	extOff   int
	ext      []tref // the externs resolved to slab references (register)
	deps     []*treeSegment
	refs     int // references from outside the segment; under the slab lock
	unmapped bool
}

type segExtern struct {
	seq uint64
	ref tref
}

// extern is the i-th extern as written (from 0), read from the mapping: a segment flushed on top
// of a large tree can have millions, and a heap copy of them is only needed while registering.
func (g *treeSegment) extern(i int) segExtern {
	e := g.data[g.extOff+i*16:]
	return segExtern{seq: binary.LittleEndian.Uint64(e), ref: tref(binary.LittleEndian.Uint32(e[8:]))}
}

func (g *treeSegment) leafAt(local uint32) *leafRec {
	return (*leafRec)(unsafe.Pointer(&g.data[treeSegHeader+int(local-1)*leafRecSize]))
}

func (g *treeSegment) nodeAt(local uint32) *nodeRec {
	return (*nodeRec)(unsafe.Pointer(&g.data[g.nodeOff+int(local-1)*nodeRecSize]))
}

// global turns a reference local to the segment into a slab reference.
func (g *treeSegment) global(c tref) tref {
	switch {
	case c == 0:
		return 0
	case c&refFile != 0:
		return g.ext[uint32(c&refIndexMask)-1]
	case c&refLeaf != 0:
		return refFile | refLeaf | tref(g.leafBase+uint32(c&^refLeaf))
	default:
		return refFile | tref(g.nodeBase+uint32(c))
	}
}

// local is the slab reference r, a record of g, as a reference local to g.
func (g *treeSegment) local(r tref) tref {
	if r&refLeaf != 0 {
		return refLeaf | tref(uint32(r&refIndexMask)-g.leafBase)
	}
	return tref(uint32(r&refIndexMask) - g.nodeBase)
}

func segmentOf(segs []*treeSegment, r tref) *treeSegment {
	i := uint32(r & refIndexMask)
	if r&refLeaf != 0 {
		for _, g := range segs {
			if i > g.leafBase && i <= g.leafBase+g.leaves {
				return g
			}
		}
	} else {
		for _, g := range segs {
			if i > g.nodeBase && i <= g.nodeBase+g.nodes {
				return g
			}
		}
	}
	panic("document: tree reference into no mapped segment")
}

func (s *trieSlab) segBySeq(seq uint64) *treeSegment {
	if p := s.segs.p.Load(); p != nil {
		for _, g := range *p {
			if g.seq == seq {
				return g
			}
		}
	}
	return nil
}

// openTreeSegment maps and verifies a segment file.
// treeFullVerifies counts segments checksummed in full at open; for tests.
var treeFullVerifies atomic.Int64

// segStamp is a segment file's size and modification time as the checkpoint that tagged it saw
// them. A segment is written once and never modified, so an open that finds both unchanged trusts
// the file without reading it all for its checksum - the rule a checkpoint already applies to the
// delta segments it covers (storage.SegmentStamp). What it gives up: damage that changes neither,
// which only the full read would catch.
type segStamp struct {
	size, mtime int64
}

func statStamp(path string) (segStamp, bool) {
	fi, err := os.Stat(path)
	if err != nil {
		return segStamp{}, false
	}
	return segStamp{size: fi.Size(), mtime: fi.ModTime().UnixNano()}, true
}

// openTreeSegment maps and verifies a segment file: fully, by its checksum, unless trusted (its
// stamp matched), when only what can be checked without reading it all is - the header, the
// trailer, the sizes, the extern table and the root record.
func openTreeSegment(f *TreeFile, path string, trusted bool) (g *treeSegment, err error) {
	m, err := mmapfile.Open(path)
	if err != nil {
		return nil, err
	}
	region, err := m.Acquire()
	if err != nil {
		m.Close()
		return nil, err
	}
	defer func() {
		if err != nil {
			region.Release()
			m.Close()
		}
	}()
	data := region.Bytes()
	if len(data) < treeSegHeader+treeSegTrailer {
		return nil, fmt.Errorf("document: tree segment %s: %d bytes is too short", path, len(data))
	}
	var hdr [treeSegHeader]byte
	var crc uint32
	if gerr := region.Guard(func(all []byte) {
		copy(hdr[:], all)
		if !trusted {
			crc = crc32.Checksum(all[:len(all)-treeSegTrailer], treeCRC)
		}
	}); gerr != nil {
		return nil, gerr
	}
	if !trusted {
		treeFullVerifies.Add(1)
	}
	le := binary.LittleEndian
	if string(hdr[:8]) != treeSegMagic || le.Uint32(hdr[8:]) != treeSegVersion {
		return nil, fmt.Errorf("document: tree segment %s: not a version %d tree segment", path, treeSegVersion)
	}
	tail := data[len(data)-treeSegTrailer:]
	if le.Uint32(tail[4:]) != treeSegTailMark || (!trusted && le.Uint32(tail) != crc) {
		return nil, fmt.Errorf("document: tree segment %s: checksum mismatch", path)
	}
	leaves, nodes, externs := le.Uint32(hdr[16:]), le.Uint32(hdr[20:]), le.Uint32(hdr[28:])
	extOff := treeSegHeader + int(leaves)*leafRecSize + int(nodes)*nodeRecSize
	if want := extOff + int(externs)*16 + treeSegTrailer; len(data) != want {
		return nil, fmt.Errorf("document: tree segment %s: %d bytes, header says %d", path, len(data), want)
	}
	g = &treeSegment{
		path: path, file: f, mapping: m, region: region, data: data,
		leaves: leaves, nodes: nodes, nExterns: externs, extOff: extOff,
		nodeOff: treeSegHeader + int(leaves)*leafRecSize,
		root:    tref(le.Uint32(hdr[24:])),
		docs:    int(le.Uint64(hdr[32:])),
	}
	copy(g.rootHash[:], hdr[40:72])
	if base := filepath.Base(path); strings.HasPrefix(base, "tree-") {
		g.seq, _ = strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(base, "tree-"), treeSegSuffix), 10, 64)
	}
	if err := g.checkRef(g.root); err != nil {
		return nil, err
	}
	if g.root != 0 && g.root&refFile == 0 {
		var h [32]byte
		if g.root&refLeaf != 0 {
			h = g.leafAt(uint32(g.root &^ refLeaf)).hash
		} else {
			h = g.nodeAt(uint32(g.root)).hash
		}
		if h != g.rootHash {
			return nil, fmt.Errorf("document: tree segment %s: root record does not carry the root hash", path)
		}
	}
	return g, nil
}

func (g *treeSegment) checkRef(c tref) error {
	switch {
	case c == 0:
		return nil
	case c&refFile != 0:
		if i := uint32(c & refIndexMask); i == 0 || i > g.nExterns || c&(refLeaf|refLocal) != 0 {
			return fmt.Errorf("document: tree segment %s: extern reference %d out of range", g.path, i)
		}
	case c&refLeaf != 0:
		if i := uint32(c &^ refLeaf); i == 0 || i > g.leaves {
			return fmt.Errorf("document: tree segment %s: leaf reference %d out of range", g.path, i)
		}
	default:
		if i := uint32(c); i == 0 || i > g.nodes || c&refLocal != 0 {
			return fmt.Errorf("document: tree segment %s: node reference %d out of range", g.path, i)
		}
	}
	return nil
}

// register makes g addressable from s: index ranges no live segment of s uses, its externs
// resolved against the segments it was flushed on top of (which must already be registered, and
// which it then holds), and a place in the list readers load. Under s.mu.
func (s *trieSlab) register(g *treeSegment) error {
	var cur []*treeSegment
	if p := s.segs.p.Load(); p != nil {
		cur = *p
	}
	ext := make([]tref, g.nExterns)
	var deps []*treeSegment
	for i := range ext {
		e := g.extern(i)
		d := s.segBySeq(e.seq)
		if d == nil {
			return fmt.Errorf("document: tree segment %s refers to segment %d, which is not mapped", g.path, e.seq)
		}
		if e.ref == 0 || e.ref&(refFile|refLocal) != 0 || d.checkRef(e.ref) != nil {
			return fmt.Errorf("document: tree segment %s: extern %d is not a record of segment %d", g.path, i, e.seq)
		}
		ext[i] = d.global(e.ref)
		if !slices.Contains(deps, d) {
			deps = append(deps, d)
		}
	}
	var ok bool
	if g.leafBase, ok = segmentGap(cur, g.leaves, func(o *treeSegment) (uint32, uint32) { return o.leafBase, o.leaves }); !ok {
		return errors.New("document: tree slab has no room for another segment's leaves")
	}
	if g.nodeBase, ok = segmentGap(cur, g.nodes, func(o *treeSegment) (uint32, uint32) { return o.nodeBase, o.nodes }); !ok {
		return errors.New("document: tree slab has no room for another segment's nodes")
	}
	g.ext, g.deps = ext, deps
	for _, r := range ext {
		s.retain(r)
	}
	next := append(append(make([]*treeSegment, 0, len(cur)+1), cur...), g)
	s.segs.p.Store(&next)
	return nil
}

// segmentGap finds base such that (base, base+n] overlaps no live segment's range.
func segmentGap(segs []*treeSegment, n uint32, rng func(*treeSegment) (uint32, uint32)) (uint32, bool) {
	type span struct{ base, n uint32 }
	spans := make([]span, 0, len(segs))
	for _, o := range segs {
		b, c := rng(o)
		spans = append(spans, span{b, c})
	}
	sort.Slice(spans, func(a, b int) bool { return spans[a].base < spans[b].base })
	start := uint32(0)
	for _, sp := range spans {
		if uint64(start)+uint64(n) <= uint64(sp.base) {
			return start, true
		}
		if e := sp.base + sp.n; e > start {
			start = e
		}
	}
	return start, uint64(start)+uint64(n) <= uint64(refIndexMask)
}

// retireSegment drops a segment nothing refers to any more: out of the list, unmapped, its file
// deleted unless the next open needs it, and its hold on the segments under it given back. Under
// s.mu. Safe to unmap here: a reader can only reach a segment through a tree that refers into it,
// which would hold a count.
func (s *trieSlab) retireSegment(g *treeSegment) {
	cur := *s.segs.p.Load()
	next := make([]*treeSegment, 0, len(cur))
	for _, o := range cur {
		if o != g {
			next = append(next, o)
		}
	}
	s.segs.p.Store(&next)
	g.unmap(true)
	for _, r := range g.ext {
		s.release(r)
	}
}

// unmap closes g's mapping, and deletes its file when del and nothing needs it on disk.
// DropMappedPages releases the resident pages of every tree segment t's slab has mapped. A walk of
// the whole tree - a checkpoint packs it twice - touches every page of every segment; left mapped
// they stay resident (nearly 1 GB at 10M documents) and, read twice, count as active page cache
// in a cgroup's working set. The mappings are read-only and file-backed, so nothing is lost: a
// later read faults the page back in. A no-op for a tree not bound to a file.
func (t DocumentTree) DropMappedPages() {
	h := t.trie
	if h == nil {
		return
	}
	s := h.slab
	s.mu.Lock() // unmap runs under it: a segment cannot be released mid-drop
	defer s.mu.Unlock()
	p := s.segs.p.Load()
	if p == nil {
		return
	}
	for _, g := range *p {
		if g.unmapped || g.region == nil {
			continue
		}
		_ = g.region.DropPages(0, len(g.region.Bytes()))
	}
}

func (g *treeSegment) unmap(del bool) {
	if g.unmapped {
		return
	}
	g.unmapped = true
	g.region.Release()
	g.mapping.Close()
	if f := g.file; f != nil && del {
		f.mu.Lock()
		del = !f.closed && !f.opts.ReadOnly && !f.keep[g.seq]
		f.mu.Unlock()
		if del {
			os.Remove(g.path)
			os.Remove(g.path + treeSrcSuffix)
		}
	}
}

// chain is g and every segment it depends on.
func (g *treeSegment) chain(into map[uint64]bool) {
	if into[g.seq] {
		return
	}
	into[g.seq] = true
	for _, d := range g.deps {
		d.chain(into)
	}
}

// --- Writing a segment from a tree -----------------------------------------------------------

// segWriter writes a segment's records to the two sections of a temporary file, numbering them
// in the order written (post-order, so a subtree's records are contiguous). A flush writes only
// the records in the heap and refers to those already in segments as externs.
type segWriter struct {
	lw, nw       *bufio.Writer
	leaves, nods uint32
	flush        bool
	externs      []segExtern
	err          error
}

func (w *segWriter) leaf(rec *leafRec) tref {
	if w.err == nil {
		_, w.err = w.lw.Write(unsafe.Slice((*byte)(unsafe.Pointer(rec)), leafRecSize))
	}
	w.leaves++
	return refLeaf | tref(w.leaves)
}

func (w *segWriter) node(rec *nodeRec) tref {
	if w.err == nil {
		_, w.err = w.nw.Write(unsafe.Slice((*byte)(unsafe.Pointer(rec)), nodeRecSize))
	}
	w.nods++
	return tref(w.nods)
}

// copyOut writes the tree under r (read through v) and returns its local reference.
func (w *segWriter) copyOut(v *trieView, r tref) tref {
	switch {
	case r == 0:
		return 0
	case w.flush && r&refFile != 0:
		g := v.seg(r)
		w.externs = append(w.externs, segExtern{seq: g.seq, ref: g.local(r)})
		return refFile | tref(len(w.externs))
	case r&refLeaf != 0:
		l := *v.leaf(r)
		return w.leaf(&l)
	default:
		children := v.children(r)
		rec := nodeRec{hash: v.node(r).hash}
		for i, c := range children {
			rec.child[i] = w.copyOut(v, c)
		}
		return w.node(&rec)
	}
}

// countRecords counts what writing the tree under r would write: every record, or for a flush
// the records in the heap, noting each segment the flush would refer into.
func countRecords(v *trieView, r tref, flush bool, segs map[*treeSegment]bool) (leaves, nodes uint32) {
	switch {
	case r == 0:
	case flush && r&refFile != 0:
		segs[v.seg(r)] = true
	case r&refLeaf != 0:
		leaves = 1
	default:
		nodes = 1
		for _, c := range v.children(r) {
			l, n := countRecords(v, c, flush, segs)
			leaves += l
			nodes += n
		}
	}
	return
}

// createSegment writes a segment of the given shape with fill, then seals it under path.
func createSegment(path string, leaves, nodes uint32, docs int, fill func(f *os.File, leafOff, nodeOff int64) (root tref, rootHash [32]byte, externs []segExtern, err error)) (err error) {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp*")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
		}
	}()
	leafOff := int64(treeSegHeader)
	nodeOff := leafOff + int64(leaves)*leafRecSize
	extOff := nodeOff + int64(nodes)*nodeRecSize
	root, rootHash, externs, err := fill(tmp, leafOff, nodeOff)
	if err != nil {
		return err
	}
	le := binary.LittleEndian
	ext := make([]byte, 16*len(externs))
	for i, e := range externs {
		le.PutUint64(ext[i*16:], e.seq)
		le.PutUint32(ext[i*16+8:], uint32(e.ref))
	}
	if _, err = tmp.WriteAt(ext, extOff); err != nil {
		return err
	}
	end := extOff + int64(len(ext))
	var hdr [treeSegHeader]byte
	copy(hdr[:], treeSegMagic)
	le.PutUint32(hdr[8:], treeSegVersion)
	le.PutUint32(hdr[16:], leaves)
	le.PutUint32(hdr[20:], nodes)
	le.PutUint32(hdr[24:], uint32(root))
	le.PutUint32(hdr[28:], uint32(len(externs)))
	le.PutUint64(hdr[32:], uint64(max(docs, 0)))
	copy(hdr[40:72], rootHash[:])
	if _, err = tmp.WriteAt(hdr[:], 0); err != nil {
		return err
	}
	// The checksum is taken over what the file now holds, read back (from the page cache): the
	// sections were written from several places at once, so it cannot be streamed.
	crc := crc32.New(treeCRC)
	if _, err = io.Copy(crc, bufio.NewReaderSize(io.NewSectionReader(tmp, 0, end), 1<<20)); err != nil {
		return err
	}
	var tail [treeSegTrailer]byte
	le.PutUint32(tail[:], crc.Sum32())
	le.PutUint32(tail[4:], treeSegTailMark)
	if _, err = tmp.WriteAt(tail[:], end); err != nil {
		return err
	}
	if err = tmp.Sync(); err != nil {
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	if d, derr := os.Open(dir); derr == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

// writeSegmentFrom writes the tree h (of docs documents, or -1 if not known) to a new segment of
// f: a flush of its heap records over the segments it already refers into, or - when that would
// make the chain of segments longer than f allows, or when full is set - every record (a
// compaction). h must be held throughout.
func writeSegmentFrom(f *TreeFile, h *trieTree, docs int, full bool) (*treeSegment, error) {
	v := h.slab.view()
	flush := !full
	refd := map[*treeSegment]bool{}
	leaves, nodes := countRecords(&v, h.root, flush, refd)
	if flush {
		chain := map[uint64]bool{}
		for g := range refd {
			g.chain(chain)
		}
		if len(chain)+1 > f.opts.MaxSegments {
			flush = false
			leaves, nodes = countRecords(&v, h.root, false, nil)
		}
	}
	path := f.segPath(f.takeSeq())
	err := createSegment(path, leaves, nodes, docs, func(file *os.File, leafOff, nodeOff int64) (tref, [32]byte, []segExtern, error) {
		w := segWriter{
			lw:    bufio.NewWriterSize(io.NewOffsetWriter(file, leafOff), 1<<20),
			nw:    bufio.NewWriterSize(io.NewOffsetWriter(file, nodeOff), 1<<20),
			flush: flush,
		}
		root := w.copyOut(&v, h.root)
		if w.err == nil {
			w.err = w.lw.Flush()
		}
		if w.err == nil {
			w.err = w.nw.Flush()
		}
		return root, *v.hash(h.root), w.externs, w.err
	})
	runtime.KeepAlive(h)
	if err != nil {
		return nil, err
	}
	g, err := openTreeSegment(f, path, false)
	if err != nil {
		os.Remove(path)
		return nil, err
	}
	return g, nil
}

// --- Building a segment from a packed image --------------------------------------------------

// countNodes is how many internal nodes the trie over items (sorted, sharing depth nibbles) has.
func countNodes(items []trieItem, depth int) uint32 {
	if len(items) <= 1 || depth == trieDepth {
		return 0
	}
	n := uint32(1)
	forEachRun(items, depth, func(_ int, run []trieItem) { n += countNodes(run, depth+1) })
	return n
}

type segBuilder struct {
	segWriter
}

func (b *segBuilder) build(items []trieItem, depth int) (tref, [32]byte) {
	if len(items) == 1 || depth == trieDepth {
		it := &items[len(items)-1]
		rec := leafRec{hash: foldLeaf(it.key[:], it.hash, depth), id: it.key, content: it.hash}
		return b.leaf(&rec), rec.hash
	}
	var rec nodeRec
	var buf [1 + 16*32]byte
	buf[0] = 0x01
	forEachRun(items, depth, func(nib int, run []trieItem) {
		c, h := b.build(run, depth+1)
		rec.child[nib] = c
		copy(buf[1+nib*32:], h[:])
	})
	rec.hash = sha256.Sum256(buf[:])
	return b.node(&rec), rec.hash
}

// buildSegmentPacked builds the trie over items (sorted, distinct) straight into a new segment
// file: the bulk build of trieBuildItems, with the records going to the file rather than the
// slab, so opening a namespace never holds its whole tree in the heap. The sixteen top-level
// subtrees are built on separate cores, each writing at offsets counted out beforehand.
func buildSegmentPacked(f *TreeFile, items []trieItem) (*treeSegment, error) {
	leaves, nodes := uint32(len(items)), countNodes(items, 0)
	path := f.segPath(f.takeSeq())
	err := createSegment(path, leaves, nodes, len(items), func(file *os.File, leafOff, nodeOff int64) (tref, [32]byte, []segExtern, error) {
		newBuilder := func(leafStart, nodeStart uint32) *segBuilder {
			return &segBuilder{segWriter{
				lw:     bufio.NewWriterSize(io.NewOffsetWriter(file, leafOff+int64(leafStart)*leafRecSize), 256<<10),
				nw:     bufio.NewWriterSize(io.NewOffsetWriter(file, nodeOff+int64(nodeStart)*nodeRecSize), 256<<10),
				leaves: leafStart, nods: nodeStart,
			}}
		}
		flush := func(b *segBuilder) error {
			if b.err == nil {
				b.err = b.lw.Flush()
			}
			if b.err == nil {
				b.err = b.nw.Flush()
			}
			return b.err
		}
		if len(items) < 4096 {
			b := newBuilder(0, 0)
			root, h := b.build(items, 0)
			return root, h, nil, flush(b)
		}
		// The root is the last node; each top-level subtree gets the leaves and nodes before it.
		var root nodeRec
		var buf [1 + 16*32]byte
		buf[0] = 0x01
		var errs [16]error
		var wg sync.WaitGroup
		var leafStart, nodeStart uint32
		forEachRun(items, 0, func(nib int, run []trieItem) {
			ls, ns := leafStart, nodeStart
			leafStart += uint32(len(run))
			nodeStart += countNodes(run, 1)
			wg.Add(1)
			go func() {
				defer wg.Done()
				b := newBuilder(ls, ns)
				c, h := b.build(run, 1)
				root.child[nib] = c
				copy(buf[1+nib*32:], h[:])
				errs[nib] = flush(b)
			}()
		})
		wg.Wait()
		for _, e := range errs {
			if e != nil {
				return 0, [32]byte{}, nil, e
			}
		}
		root.hash = sha256.Sum256(buf[:])
		b := newBuilder(leafStart, nodeStart)
		r := b.node(&root)
		return r, root.hash, nil, flush(b)
	})
	if err != nil {
		return nil, err
	}
	g, err := openTreeSegment(f, path, false)
	if err != nil {
		os.Remove(path)
		return nil, err
	}
	return g, nil
}

// --- Binding, rebasing and writing ----------------------------------------------------------

// attachChain makes a new file-bound slab holding top and the segments it depends on (opened
// from f by sequence, oldest first), and returns the tree top holds.
func attachChain(f *TreeFile, top *treeSegment, trusted map[uint64]bool) (*trieTree, error) {
	s := newTrieSlab()
	s.file.Store(f)
	s.mu.Lock()
	defer s.mu.Unlock()
	var opened []*treeSegment
	fail := func(err error) (*trieTree, error) {
		for _, g := range opened {
			g.unmap(false)
		}
		if p := s.segs.p.Load(); p != nil {
			s.segs.p.Store(new([]*treeSegment))
		}
		return nil, err
	}
	var add func(g *treeSegment, depth int) error
	add = func(g *treeSegment, depth int) error {
		if depth > 64 {
			return fmt.Errorf("document: tree segment chain under %s is too long", top.path)
		}
		for i := range int(g.nExterns) {
			e := g.extern(i)
			if s.segBySeq(e.seq) != nil {
				continue
			}
			if e.seq >= g.seq {
				return fmt.Errorf("document: tree segment %s refers to a later segment %d", g.path, e.seq)
			}
			d, err := openTreeSegment(f, f.segPath(e.seq), trusted[e.seq])
			if err != nil {
				return err
			}
			opened = append(opened, d)
			if err := add(d, depth+1); err != nil {
				return err
			}
		}
		return s.register(g)
	}
	if err := add(top, 0); err != nil {
		return fail(err)
	}
	// Each dependency is held by the segments flushed on top of it; nothing holds the top but
	// the trees in it. Retained twice: the handle's, and the slab's as the newest segment.
	root := top.global(top.root)
	if root == 0 {
		s.retireSegment(top)
		return s.adopt(0), nil
	}
	s.retain(root)
	s.retain(root)
	s.rebaseTo = root
	return s.adopt(root), nil
}

// rebased returns h, or - when h was made before its slab's newest segment - an equal tree that
// refers into that segment wherever it can. Called before an update derives from h.
func (h *trieTree) rebased() *trieTree {
	if h == nil || h.gen == h.slab.gen.Load() {
		return h
	}
	out := []*trieTree{h}
	h.slab.rebaseTrees(out)
	return out[0]
}

// rebaseTrees replaces each tree of hs (all of s) made before s's newest segment with its rebase
// onto it, in one pass: the translation, copies and placements of records several of them reach
// are shared, so rebasing a history of trees costs what they hold between them, not each.
func (s *trieSlab) rebaseTrees(hs []*trieTree) {
	s.mu.Lock()
	// Read with the target, under the lock that writeOut moves both under: a generation read
	// apart from it could be newer than the target, and the trees rebased here stamped with it.
	gen := s.gen.Load()
	target := s.rebaseTo
	if target == 0 {
		s.mu.Unlock()
		return
	}
	// Under the lock: the target segment cannot be retired while this reads it.
	e := getEdit(s.view())
	e.keep = s.chainOf(target)
	e.memo, e.placed = map[tref]tref{}, map[tref]tref{}
	roots := make([]tref, len(hs))
	for i, h := range hs {
		if h == nil || h.gen == gen || h.root == target {
			roots[i] = 0
			continue
		}
		r := e.translate(h.root, target)
		r = s.place(e, r)
		s.retain(r)
		roots[i] = r
	}
	s.mu.Unlock()
	putEdit(e)
	for i, h := range hs {
		if h != nil && !(h.gen == gen || h.root == target) {
			hs[i] = s.adoptAt(roots[i], gen)
			runtime.KeepAlive(h)
		}
	}
}

// RebaseTrees is Rebased for many trees at once, sharing the work and the copies between trees
// of one slab - for a holder rebasing a history of trees (TreeFile.SetOnWrite). The result is
// in the order of ts.
func RebaseTrees(ts []DocumentTree) []DocumentTree {
	out := append([]DocumentTree(nil), ts...)
	bySlab := map[*trieSlab][]int{}
	for i, t := range ts {
		if t.trie != nil && t.trie.slab.file.Load() != nil {
			bySlab[t.trie.slab] = append(bySlab[t.trie.slab], i)
		}
	}
	for s, idx := range bySlab {
		hs := make([]*trieTree, len(idx))
		for k, i := range idx {
			hs[k] = ts[i].trie
		}
		s.rebaseTrees(hs)
		for k, i := range idx {
			if hs[k] != ts[i].trie {
				out[i] = DocumentTree{TreeHash: ts[i].TreeHash, Entries: ts[i].Entries, size: ts[i].size, trie: hs[k]}
			}
		}
	}
	return out
}

// chainOf is the set of segments the segment holding r depends on, itself included: what a
// tree rebased onto r may go on referring into. Under s.mu.
func (s *trieSlab) chainOf(r tref) map[uint64]bool {
	keep := map[uint64]bool{}
	if r&refFile != 0 {
		segmentOf(*s.segs.p.Load(), r).chain(keep)
	}
	return keep
}

// translate returns the tree now (at the same position as file) with every subtree whose hash
// equals file's at that position replaced by file's record. Merkle hashes make equal hashes
// equal subtrees, so the result is the same tree; it descends only where they differ.
//
// What differs and lives in a segment outside e.keep - a segment the newest one does not build
// on, superseded by a compaction - is copied into the heap, so that the rebased tree holds no
// reference into it and it can be released once the trees made before the compaction are gone.
// What differs is what changed since, so that is all a rebase copies.
func (e *trieEdit) translate(now, file tref) tref {
	if now == 0 || now == file {
		return now
	}
	if r, ok := e.memo[now]; ok {
		return r
	}
	r := e.translateRec(now, file)
	if e.memo != nil {
		e.memo[now] = r
	}
	return r
}

func (e *trieEdit) translateRec(now, file tref) tref {
	if file != 0 && *e.v.hash(now) == *e.v.hash(file) {
		return file
	}
	stale := now&refFile != 0 && !e.keep[e.v.seg(now).seq]
	if now&refLeaf != 0 {
		if !stale {
			return now
		}
		l := e.v.leaf(now)
		r, rec := e.next()
		*rec = editRec{hash: l.hash, id: l.id, content: l.content, leaf: true}
		return r
	}
	var fc [16]tref
	if file != 0 && file&refLeaf == 0 {
		fc = e.v.children(file)
	}
	nc := e.v.children(now)
	changed := false
	for i, c := range nc {
		if c == 0 {
			continue
		}
		if t := e.translate(c, fc[i]); t != c {
			nc[i], changed = t, true
		}
	}
	if !changed && !stale {
		return now
	}
	r, rec := e.next()
	*rec = editRec{hash: *e.v.hash(now), child: nc}
	return r
}

// maybeRewrite starts a background flush of h when the slab's resident records have passed its
// file's budget, and at least half the budget has been created since the last one. Records
// created since the last GC cycle are left out of "resident": most are the paths updates
// replaced, garbage the collector has not reached yet, and a flush does nothing about them.
func (s *trieSlab) maybeRewrite(h *trieTree) {
	f := s.file.Load()
	if f == nil || h == nil || h.root == 0 {
		return
	}
	s.mu.Lock()
	live := int64(s.nodes.used) + int64(s.leaves.used) - int64(len(s.nodes.free)+len(s.leaves.free)) -
		int64(s.created-s.cycleCreated)
	since := int64(s.created - s.createdAtRewrite)
	s.mu.Unlock()
	const recBytes = (nodeRecBytes + leafRecBytes) / 2
	if live*recBytes < f.opts.ResidentBytes || since*recBytes < f.opts.ResidentBytes/2 {
		return
	}
	if !s.rewriting.CompareAndSwap(false, true) {
		return
	}
	if !f.startBackground() {
		s.rewriting.Store(false)
		return
	}
	go func() {
		defer f.background.Done()
		defer s.rewriting.Store(false)
		if _, err := s.writeOut(f, h, -1, false); err != nil {
			f.fail(err)
		}
	}()
}

// writeOut writes h to a new segment (writeSegmentFrom), registers it, and makes it what older
// trees rebase onto.
func (s *trieSlab) writeOut(f *TreeFile, h *trieTree, docs int, full bool) (*treeSegment, error) {
	g, err := writeSegmentFrom(f, h, docs, full)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if err := s.register(g); err != nil {
		g.unmap(true)
		s.mu.Unlock()
		return nil, err
	}
	root := g.global(g.root)
	s.retain(root)
	old := s.rebaseTo
	s.rebaseTo = root
	s.createdAtRewrite = s.created
	s.gen.Add(1)
	s.release(old)
	s.mu.Unlock()
	f.wrote()
	return g, nil
}

// SetOnWrite has fn called, on a goroutine of its own, each time a segment of f has been written
// and trees made before it can be rebased onto it (DocumentTree.Rebased). The holder of trees
// that are never updated - the engine's history - rebases them there, or they keep the
// segments they refer into mapped and on disk until they are dropped. Close waits for a call in
// flight, so fn must not wait for f to close.
func (f *TreeFile) SetOnWrite(fn func()) {
	f.mu.Lock()
	f.onWrite = fn
	f.mu.Unlock()
}

// wrote starts onWrite, counted in background so Close waits for it: it reads the segments it
// rebases onto, and one that outlived Close would be reading files a backup or repair may then
// rewrite under it (a SIGBUS once one is truncated). None starts once f is closed.
func (f *TreeFile) wrote() {
	f.mu.Lock()
	fn := f.onWrite
	if fn == nil || f.closed {
		f.mu.Unlock()
		return
	}
	f.background.Add(1)
	f.mu.Unlock()
	go func() {
		defer f.background.Done()
		fn()
	}()
}

// --- Public API ------------------------------------------------------------------------------

// Rebased returns t, or an equal tree moved onto its slab's newest segment: referring into it
// wherever t's subtrees are the same, and holding none of the records of segments the newest one
// supersedes. For trees that are kept but never updated - an update rebases on its own - so that
// segments a compaction superseded can be released (TreeFile.SetOnWrite).
func (t DocumentTree) Rebased() DocumentTree {
	if t.trie == nil {
		return t
	}
	h := t.trie.rebased()
	if h == t.trie {
		return t
	}
	return DocumentTree{TreeHash: t.TreeHash, Entries: t.Entries, size: t.size, trie: h}
}

// Paged reports whether t is bound to a tree file, and whether its root itself is in a mapped
// segment (true right after an open that mapped one, or once an update has rebased onto one).
func (t DocumentTree) Paged() (bound, mapped bool) {
	if t.trie == nil {
		return false, false
	}
	return t.trie.slab.file.Load() != nil, t.trie.root&refFile != 0
}

// OpenTreeDigest maps the segment SaveTreeSegmentDigest tagged with digest, holding entries
// documents, when f has it: an open from a checkpoint that records its tree's digest needs none of
// the tree's bytes then. false means the caller builds the tree from its packed image instead.
func OpenTreeDigest(digest [32]byte, entries int, f *TreeFile) (DocumentTree, bool) {
	if f == nil || entries == 0 {
		return DocumentTree{}, false
	}
	return f.attachDigest(digest, entries)
}

// OpenTreePacked is BuildTrieDocumentTreePacked for a namespace with a tree file: it maps the
// segment written for exactly this packed image if there is one, and otherwise builds a segment
// from the image straight to the file. On any failure it builds in the heap, bound to f.
func OpenTreePacked(packed []byte, f *TreeFile) (DocumentTree, error) {
	items := packedItems(packed)
	for i := 1; i < len(items); i++ {
		if bytes.Compare(items[i-1].key[:], items[i].key[:]) >= 0 {
			return DocumentTree{}, fmt.Errorf("document: packed tree entries out of order at %d", i)
		}
	}
	if f == nil || len(items) == 0 {
		t, err := BuildTrieDocumentTreePacked(packed)
		if err != nil || f == nil {
			return t, err
		}
		return BindTreeFile(t, f), nil
	}
	digest := sha256.Sum256(packed)
	if t, ok := f.attachDigest(digest, len(items)); ok {
		return t, nil
	}
	if f.writable() {
		g, err := buildSegmentPacked(f, items)
		if err == nil {
			var h *trieTree
			if h, err = attachChain(f, g, nil); err == nil {
				if err = f.writeSource(g, digest); err == nil {
					f.dropOthers()
					return DocumentTree{TreeHash: h.rootHash(), trie: h, size: g.docs}, nil
				}
			}
		}
		log.Printf("kdb: tree file %s: building the tree segment failed (%v) - building in memory", f.opts.Dir, err)
	}
	t, err := BuildTrieDocumentTreePacked(packed)
	if err != nil {
		return t, err
	}
	return BindTreeFile(t, f), nil
}

// OpenTreeEntries is OpenTreePacked for a checkpoint that lists its tree as entries (format v2):
// packed in id order they are the same bytes AppendPacked gave the checkpoint's SaveTreeSegment.
func OpenTreeEntries(entries map[codec.UUID]codec.Hash, f *TreeFile) (DocumentTree, error) {
	if f == nil {
		return BuildTrieDocumentTree(entries)
	}
	items := make([]trieItem, 0, len(entries))
	for id, h := range entries {
		items = append(items, trieItem{key: uuidKey(id), hash: h})
	}
	slices.SortFunc(items, func(a, b trieItem) int { return bytes.Compare(a.key[:], b.key[:]) })
	var packed []byte
	if len(items) > 0 {
		packed = unsafe.Slice((*byte)(unsafe.Pointer(&items[0])), len(items)*PackedTreeEntrySize)
	}
	return OpenTreePacked(packed, f)
}

// attachDigest maps the newest segment tagged with digest, and its chain, if they verify.
func (f *TreeFile) attachDigest(digest [32]byte, docs int) (DocumentTree, bool) {
	seqs := listTreeSegments(f.opts.Dir)
	for i := len(seqs) - 1; i >= 0; i-- {
		path := f.segPath(seqs[i])
		src, ok := readSource(path + treeSrcSuffix)
		if !ok || src.digest != digest {
			continue
		}
		trusted := map[uint64]bool{}
		for seq, want := range src.stamps {
			if got, ok := statStamp(f.segPath(seq)); ok && got == want {
				trusted[seq] = true
			}
		}
		g, err := openTreeSegment(f, path, trusted[seqs[i]])
		if err == nil && g.docs != docs {
			g.unmap(false)
			err = fmt.Errorf("document: tree segment %s holds %d documents, the checkpoint %d", path, g.docs, docs)
		}
		var h *trieTree
		if err == nil {
			if h, err = attachChain(f, g, trusted); err != nil {
				g.unmap(false)
			}
		}
		if err != nil {
			log.Printf("kdb: tree file %s: %v - rebuilding", f.opts.Dir, err)
			continue
		}
		f.markKeep(g)
		f.dropOthers()
		return DocumentTree{TreeHash: h.rootHash(), trie: h, size: g.docs}, true
	}
	return DocumentTree{}, false
}

// treeSource is a .src sidecar: the digest of the packed checkpoint image a segment was written
// for, and the stamp of every segment in its chain when it was.
type treeSource struct {
	digest [32]byte
	stamps map[uint64]segStamp
}

// Sidecar layout: digest [32], count u32, count x (sequence u64, size i64, mtime i64), crc32c u32
// of what precedes it. A 36-byte sidecar (digest, crc) is the stampless original.
func readSource(path string) (treeSource, bool) {
	b, err := os.ReadFile(path)
	le := binary.LittleEndian
	if err != nil || len(b) < 36 || le.Uint32(b[len(b)-4:]) != crc32.Checksum(b[:len(b)-4], treeCRC) {
		return treeSource{}, false
	}
	src := treeSource{digest: [32]byte(b[:32]), stamps: map[uint64]segStamp{}}
	if len(b) == 36 {
		return src, true
	}
	n := int(le.Uint32(b[32:]))
	if len(b) != 36+4+n*24 {
		return treeSource{}, false
	}
	for i := 0; i < n; i++ {
		e := b[36+i*24:]
		src.stamps[le.Uint64(e)] = segStamp{size: int64(le.Uint64(e[8:])), mtime: int64(le.Uint64(e[16:]))}
	}
	return src, true
}

// markKeep makes g and its chain the segments the next open needs.
func (f *TreeFile) markKeep(g *treeSegment) {
	keep := map[uint64]bool{}
	g.chain(keep)
	f.mu.Lock()
	f.keep = keep
	f.mu.Unlock()
}

// writeSource tags g as written for the packed image with digest, stamps its chain, and keeps
// them on disk.
func (f *TreeFile) writeSource(g *treeSegment, digest [32]byte) error {
	chain := map[uint64]bool{}
	g.chain(chain)
	le := binary.LittleEndian
	src := append(make([]byte, 0, 40+24*len(chain)), digest[:]...)
	src = le.AppendUint32(src, 0)
	n := 0
	for seq := range chain {
		st, ok := statStamp(f.segPath(seq))
		if !ok {
			continue // unstamped: verified in full at the next open
		}
		src = le.AppendUint64(src, seq)
		src = le.AppendUint64(src, uint64(st.size))
		src = le.AppendUint64(src, uint64(st.mtime))
		n++
	}
	le.PutUint32(src[32:], uint32(n))
	src = le.AppendUint32(src, crc32.Checksum(src, treeCRC))
	tmp := g.path + treeSrcSuffix + ".tmp"
	if err := os.WriteFile(tmp, src, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, g.path+treeSrcSuffix); err != nil {
		return err
	}
	f.markKeep(g)
	return nil
}

// dropOthers deletes segment files of an earlier run that the open did not attach: everything
// but the chain it keeps. Called at open, before this run writes any segment of its own.
func (f *TreeFile) dropOthers() {
	if f.opts.ReadOnly {
		return
	}
	f.mu.Lock()
	keep := f.keep
	f.mu.Unlock()
	for _, seq := range listTreeSegments(f.opts.Dir) {
		if !keep[seq] {
			p := f.segPath(seq)
			os.Remove(p)
			os.Remove(p + treeSrcSuffix)
		}
	}
}

// BindTreeFile returns t bound to f: trees derived from it keep their resident records within
// f's budget, writing the rest out. A tree already in a bound slab is returned as is. A heap
// tree larger than the budget is written out now and returned as the equal tree in that
// segment, so its heap slab can go once nothing else holds it. On failure t is returned unbound.
func BindTreeFile(t DocumentTree, f *TreeFile) DocumentTree {
	if f == nil {
		return t
	}
	h := t.trieRootOrBuild()
	if h == nil {
		s := newTrieSlab()
		s.file.Store(f)
		return DocumentTree{TreeHash: t.TreeHash, trie: s.adopt(0)}
	}
	if h.slab.file.Load() != nil {
		return t
	}
	st := h.slab.stats()
	if int64(st.Nodes)*nodeRecBytes+int64(st.Leaves)*leafRecBytes <= f.opts.ResidentBytes || !f.writable() {
		h.slab.file.CompareAndSwap(nil, f)
		return t
	}
	g, err := writeSegmentFrom(f, h, t.Size(), true)
	if err == nil {
		var nh *trieTree
		if nh, err = attachChain(f, g, nil); err == nil {
			return DocumentTree{TreeHash: t.TreeHash, trie: nh, size: t.Size()}
		}
		g.unmap(true)
	}
	f.fail(err)
	return t
}

// SaveTreeSegment, at a checkpoint whose packed tree image is packed, makes sure a segment holding
// exactly t is on disk and tagged with the image's digest, so the next open maps it (and the
// chain under it) instead of building anything. A no-op for a tree not bound to a writable file.
func SaveTreeSegment(t DocumentTree, packed []byte) error {
	return SaveTreeSegmentDigest(t, sha256.Sum256(packed))
}

// SaveTreeSegmentDigest is SaveTreeSegment for a caller that has the packed image's digest but not
// the image: a checkpoint that streams its tree section (embed checkpoint v6).
func SaveTreeSegmentDigest(t DocumentTree, digest [32]byte) error {
	h := t.trie
	if h == nil || h.root == 0 {
		return nil
	}
	f := h.slab.file.Load()
	if f == nil || !f.writable() {
		return nil
	}
	s := h.slab
	var g *treeSegment
	if h.root&refFile != 0 {
		s.mu.Lock()
		cand := segmentOf(*s.segs.p.Load(), h.root)
		if cand.global(cand.root) == h.root && cand.docs == t.Size() {
			g = cand
		}
		s.mu.Unlock()
	}
	if g == nil {
		var err error
		if g, err = s.writeOut(f, h, t.Size(), false); err != nil {
			return err
		}
	}
	runtime.KeepAlive(h)
	return f.writeSource(g, digest)
}
