package document

import (
	"bufio"
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
	"sort"
	"strconv"
	"strings"
	"sync"
	"unsafe"

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
	// written out again.
	DefaultTreeResidentBytes = 64 << 20
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
	// ResidentBytes is the resident overlay before a full rewrite; DefaultTreeResidentBytes if
	// not positive.
	ResidentBytes int64
	// ReadOnly maps segments but never writes or deletes any.
	ReadOnly bool
}

// TreeFile is one namespace's tree directory: the segment sequence, and which segment to keep
// on disk for the next open. One per open namespace; shared by every slab bound to it.
type TreeFile struct {
	opts TreeFileOptions

	mu      sync.Mutex
	nextSeq uint64
	keepSeq uint64 // the segment carrying the newest checkpoint's digest; never deleted
	closed  bool
	failed  bool // a write failed; stop writing, keep serving from what is mapped and the heap
}

// treeFile is the slab's view of its binding.
type treeFile = TreeFile

// NewTreeFile opens (creating if need be) a namespace's tree directory. It returns
// ErrTreeFileUnsupported where mapping is not possible, and the caller keeps heap trees.
func NewTreeFile(opts TreeFileOptions) (*TreeFile, error) {
	if !littleEndian {
		return nil, ErrTreeFileUnsupported
	}
	if opts.ResidentBytes <= 0 {
		opts.ResidentBytes = DefaultTreeResidentBytes
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

// Close stops the binding deleting files: a later open of the same namespace may be using them.
func (f *TreeFile) Close() {
	if f == nil {
		return
	}
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
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
	refs     int // references from outside the segment; under the slab lock
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
	case c&refLeaf != 0:
		return refFile | refLeaf | tref(g.leafBase+uint32(c&^refLeaf))
	default:
		return refFile | tref(g.nodeBase+uint32(c))
	}
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

// openTreeSegment maps and verifies a segment file.
func openTreeSegment(path string) (g *treeSegment, err error) {
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
		crc = crc32.Checksum(all[:len(all)-treeSegTrailer], treeCRC)
	}); gerr != nil {
		return nil, gerr
	}
	le := binary.LittleEndian
	if string(hdr[:8]) != treeSegMagic || le.Uint32(hdr[8:]) != treeSegVersion {
		return nil, fmt.Errorf("document: tree segment %s: not a version %d tree segment", path, treeSegVersion)
	}
	tail := data[len(data)-treeSegTrailer:]
	if le.Uint32(tail[4:]) != treeSegTailMark || le.Uint32(tail) != crc {
		return nil, fmt.Errorf("document: tree segment %s: checksum mismatch", path)
	}
	leaves, nodes := le.Uint32(hdr[16:]), le.Uint32(hdr[20:])
	if want := treeSegHeader + int(leaves)*leafRecSize + int(nodes)*nodeRecSize + treeSegTrailer; len(data) != want {
		return nil, fmt.Errorf("document: tree segment %s: %d bytes, header says %d", path, len(data), want)
	}
	g = &treeSegment{
		path: path, mapping: m, region: region, data: data,
		leaves: leaves, nodes: nodes,
		nodeOff: treeSegHeader + int(leaves)*leafRecSize,
		root:    tref(le.Uint32(hdr[24:])),
		docs:    int(le.Uint64(hdr[32:])),
	}
	copy(g.rootHash[:], hdr[40:72])
	if err := g.checkRef(g.root); err != nil {
		return nil, err
	}
	if g.root != 0 {
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
	if base := filepath.Base(path); strings.HasPrefix(base, "tree-") {
		g.seq, _ = strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(base, "tree-"), treeSegSuffix), 10, 64)
	}
	return g, nil
}

func (g *treeSegment) checkRef(c tref) error {
	if c == 0 {
		return nil
	}
	if c&refLeaf != 0 {
		if i := uint32(c &^ refLeaf); i == 0 || i > g.leaves {
			return fmt.Errorf("document: tree segment %s: leaf reference %d out of range", g.path, i)
		}
		return nil
	}
	if i := uint32(c); i == 0 || i > g.nodes || c&(refFile|refLocal) != 0 {
		return fmt.Errorf("document: tree segment %s: node reference %d out of range", g.path, i)
	}
	return nil
}

// register makes g addressable from s: it gets index ranges that no live segment of s uses, and
// joins the segment list readers load. Under s.mu.
func (s *trieSlab) register(g *treeSegment) error {
	var cur []*treeSegment
	if p := s.segs.Load(); p != nil {
		cur = *p
	}
	var ok bool
	if g.leafBase, ok = segmentGap(cur, g.leaves, func(o *treeSegment) (uint32, uint32) { return o.leafBase, o.leaves }); !ok {
		return errors.New("document: tree slab has no room for another segment's leaves")
	}
	if g.nodeBase, ok = segmentGap(cur, g.nodes, func(o *treeSegment) (uint32, uint32) { return o.nodeBase, o.nodes }); !ok {
		return errors.New("document: tree slab has no room for another segment's nodes")
	}
	next := append(append(make([]*treeSegment, 0, len(cur)+1), cur...), g)
	s.segs.Store(&next)
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

// retireSegment drops a segment nothing refers to any more: out of the list, unmapped, and its
// file deleted unless it is the one the next open should find. Under s.mu. Safe to unmap here:
// a reader can only reach a segment through a tree that refers into it, which would hold a count.
func (s *trieSlab) retireSegment(g *treeSegment) {
	cur := *s.segs.Load()
	next := make([]*treeSegment, 0, len(cur))
	for _, o := range cur {
		if o != g {
			next = append(next, o)
		}
	}
	s.segs.Store(&next)
	g.region.Release()
	g.mapping.Close()
	if f := g.file; f != nil {
		f.mu.Lock()
		del := !f.closed && !f.opts.ReadOnly && g.seq != f.keepSeq
		f.mu.Unlock()
		if del {
			os.Remove(g.path)
			os.Remove(g.path + treeSrcSuffix)
		}
	}
}

// --- Writing a segment from a tree -----------------------------------------------------------

// segWriter writes a segment's records to the two sections of a temporary file, numbering them
// in the order written (post-order, so a subtree's records are contiguous).
type segWriter struct {
	lw, nw       *bufio.Writer
	leaves, nods uint32
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

func countRecords(v *trieView, r tref) (leaves, nodes uint32) {
	switch {
	case r == 0:
	case r&refLeaf != 0:
		leaves = 1
	default:
		nodes = 1
		for _, c := range v.children(r) {
			l, n := countRecords(v, c)
			leaves += l
			nodes += n
		}
	}
	return
}

// createSegment writes a segment of the given shape with fill, then seals it under path.
func createSegment(path string, leaves, nodes uint32, docs int, fill func(f *os.File, leafOff, nodeOff int64) (root tref, rootHash [32]byte, err error)) (err error) {
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
	end := nodeOff + int64(nodes)*nodeRecSize
	if err = tmp.Truncate(end + treeSegTrailer); err != nil {
		return err
	}
	root, rootHash, err := fill(tmp, leafOff, nodeOff)
	if err != nil {
		return err
	}
	var hdr [treeSegHeader]byte
	le := binary.LittleEndian
	copy(hdr[:], treeSegMagic)
	le.PutUint32(hdr[8:], treeSegVersion)
	le.PutUint32(hdr[16:], leaves)
	le.PutUint32(hdr[20:], nodes)
	le.PutUint32(hdr[24:], uint32(root))
	le.PutUint64(hdr[32:], uint64(docs))
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

// writeSegmentFrom writes the tree h to a new segment of f. h must be held for the duration.
func writeSegmentFrom(f *TreeFile, h *trieTree) (*treeSegment, error) {
	v := h.slab.view()
	leaves, nodes := countRecords(&v, h.root)
	path := f.segPath(f.takeSeq())
	err := createSegment(path, leaves, nodes, int(leaves), func(file *os.File, leafOff, nodeOff int64) (tref, [32]byte, error) {
		w := segWriter{
			lw: bufio.NewWriterSize(io.NewOffsetWriter(file, leafOff), 1<<20),
			nw: bufio.NewWriterSize(io.NewOffsetWriter(file, nodeOff), 1<<20),
		}
		root := w.copyOut(&v, h.root)
		if w.err == nil {
			w.err = w.lw.Flush()
		}
		if w.err == nil {
			w.err = w.nw.Flush()
		}
		return root, *v.hash(h.root), w.err
	})
	runtime.KeepAlive(h)
	if err != nil {
		return nil, err
	}
	g, err := openTreeSegment(path)
	if err != nil {
		os.Remove(path)
		return nil, err
	}
	g.file = f
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
	err := createSegment(path, leaves, nodes, len(items), func(file *os.File, leafOff, nodeOff int64) (tref, [32]byte, error) {
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
			return root, h, flush(b)
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
				return 0, [32]byte{}, e
			}
		}
		root.hash = sha256.Sum256(buf[:])
		b := newBuilder(leafStart, nodeStart)
		r := b.node(&root)
		return r, root.hash, flush(b)
	})
	if err != nil {
		return nil, err
	}
	g, err := openTreeSegment(path)
	if err != nil {
		os.Remove(path)
		return nil, err
	}
	g.file = f
	return g, nil
}

// --- Binding, rebasing and rewriting ---------------------------------------------------------

// attachSegment makes a new file-bound slab whose one tree is g, and returns that tree.
func attachSegment(f *TreeFile, g *treeSegment) (*trieTree, error) {
	s := newTrieSlab()
	s.file.Store(f)
	s.mu.Lock()
	if err := s.register(g); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	root := g.global(g.root)
	if root == 0 {
		s.retireSegment(g) // an empty tree needs no file
	} else {
		s.retain(root) // the handle's
		s.retain(root) // the slab's: the tree the next rebase moves onto
		s.rebaseTo = root
	}
	s.mu.Unlock()
	return s.adopt(root), nil
}

// rebased returns h, or - when h was made before its slab's latest segment - an equal tree that
// refers into that segment wherever it can. Called before an update derives from h.
func (h *trieTree) rebased() *trieTree {
	if h == nil || h.gen == h.slab.gen.Load() {
		return h
	}
	s := h.slab
	s.mu.Lock()
	target := s.rebaseTo
	if target == 0 || h.root == target {
		s.mu.Unlock()
		return h
	}
	// Under the lock: the target segment cannot be retired while this reads it.
	e := getEdit(s.view())
	r := e.translate(h.root, target)
	if r != h.root {
		r = s.place(e, r)
	}
	s.retain(r)
	s.mu.Unlock()
	putEdit(e)
	out := s.adopt(r)
	runtime.KeepAlive(h)
	return out
}

// translate returns the tree now (at the same position as file) with every subtree whose hash
// equals file's at that position replaced by file's record. Merkle hashes make equal hashes
// equal subtrees, so the result is the same tree; it descends only where they differ.
func (e *trieEdit) translate(now, file tref) tref {
	if now == 0 || now == file {
		return now
	}
	if file != 0 && *e.v.hash(now) == *e.v.hash(file) {
		return file
	}
	if now&refLeaf != 0 {
		return now
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
	if !changed {
		return now
	}
	r, rec := e.next()
	*rec = editRec{hash: *e.v.hash(now), child: nc}
	return r
}

// maybeRewrite starts a background rewrite of h when the slab's resident records have passed its
// file's budget, and at least half the budget has been created since the last one. Records
// created since the last GC cycle are left out of "resident": most are the paths updates
// replaced, garbage the collector has not reached yet, and writing the tree out does nothing
// about them.
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
	if !f.writable() || !s.rewriting.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer s.rewriting.Store(false)
		if err := s.rewrite(f, h); err != nil {
			f.fail(err)
		}
	}()
}

// rewrite writes h out as a new segment and makes it what older trees rebase onto.
func (s *trieSlab) rewrite(f *TreeFile, h *trieTree) error {
	g, err := writeSegmentFrom(f, h)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.register(g); err != nil {
		g.region.Release()
		g.mapping.Close()
		os.Remove(g.path)
		return err
	}
	root := g.global(g.root)
	s.retain(root)
	old := s.rebaseTo
	s.rebaseTo = root
	s.createdAtRewrite = s.created
	s.gen.Add(1)
	s.release(old)
	return nil
}

// --- Public API ------------------------------------------------------------------------------

// OpenTreePacked is BuildTrieDocumentTreePacked for a namespace with a tree file: it maps the
// segment written for exactly this packed image if there is one, and otherwise builds a segment
// from the image straight to the file. On any failure it builds in the heap, bound to f.
func OpenTreePacked(packed []byte, f *TreeFile) (DocumentTree, error) {
	items := packedItems(packed)
	for i := 1; i < len(items); i++ {
		if keyLess(&items[i].key, &items[i-1].key) || items[i].key == items[i-1].key {
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
			if err = f.writeSource(g, digest); err == nil {
				if h, aerr := attachSegment(f, g); aerr == nil {
					f.dropOthers(g.seq)
					return DocumentTree{TreeHash: h.rootHash(), trie: h, size: g.docs}, nil
				} else {
					err = aerr
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

func keyLess(a, b *[16]byte) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

// attachDigest maps the newest segment tagged with digest, if one verifies.
func (f *TreeFile) attachDigest(digest [32]byte, docs int) (DocumentTree, bool) {
	seqs := listTreeSegments(f.opts.Dir)
	for i := len(seqs) - 1; i >= 0; i-- {
		path := f.segPath(seqs[i])
		src, err := os.ReadFile(path + treeSrcSuffix)
		if err != nil || len(src) != 36 || binary.LittleEndian.Uint32(src[32:]) != crc32.Checksum(src[:32], treeCRC) || [32]byte(src[:32]) != digest {
			continue
		}
		g, err := openTreeSegment(path)
		if err != nil {
			log.Printf("kdb: tree file %s: %v - rebuilding", f.opts.Dir, err)
			continue
		}
		g.file = f
		if g.docs != docs {
			g.region.Release()
			g.mapping.Close()
			continue
		}
		h, err := attachSegment(f, g)
		if err != nil {
			continue
		}
		f.mu.Lock()
		f.keepSeq = g.seq
		f.mu.Unlock()
		f.dropOthers(g.seq)
		return DocumentTree{TreeHash: h.rootHash(), trie: h, size: g.docs}, true
	}
	return DocumentTree{}, false
}

// writeSource tags g as written for the packed image with digest, and keeps it on disk.
func (f *TreeFile) writeSource(g *treeSegment, digest [32]byte) error {
	var src [36]byte
	copy(src[:], digest[:])
	binary.LittleEndian.PutUint32(src[32:], crc32.Checksum(src[:32], treeCRC))
	tmp := g.path + treeSrcSuffix + ".tmp"
	if err := os.WriteFile(tmp, src[:], 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, g.path+treeSrcSuffix); err != nil {
		return err
	}
	f.mu.Lock()
	f.keepSeq = g.seq
	f.mu.Unlock()
	return nil
}

// dropOthers deletes segment files other than keep that nothing in this process has mapped:
// those of an earlier run. Segments a live slab maps are deleted when retired.
func (f *TreeFile) dropOthers(keep uint64) {
	if f.opts.ReadOnly {
		return
	}
	for _, seq := range listTreeSegments(f.opts.Dir) {
		if seq < keep {
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
	g, err := writeSegmentFrom(f, h)
	if err == nil {
		var nh *trieTree
		if nh, err = attachSegment(f, g); err == nil {
			return DocumentTree{TreeHash: t.TreeHash, trie: nh, size: t.Size()}
		}
	}
	f.fail(err)
	return t
}

// SaveTreeSegment, at a checkpoint whose packed tree image is packed, makes sure a segment holding
// exactly t is on disk and tagged with the image's digest, so the next open maps it instead of
// building anything. A no-op for a tree not bound to a writable tree file.
func SaveTreeSegment(t DocumentTree, packed []byte) error {
	h := t.trie
	if h == nil || h.root == 0 {
		return nil
	}
	f := h.slab.file.Load()
	if f == nil || !f.writable() {
		return nil
	}
	digest := sha256.Sum256(packed)
	s := h.slab
	var g *treeSegment
	if h.root&refFile != 0 {
		s.mu.Lock()
		cand := segmentOf(*s.segs.Load(), h.root)
		if cand.global(cand.root) == h.root {
			g = cand
		}
		s.mu.Unlock()
	}
	if g == nil {
		var err error
		if g, err = writeSegmentFrom(f, h); err != nil {
			return err
		}
		s.mu.Lock()
		if err := s.register(g); err != nil {
			s.mu.Unlock()
			g.region.Release()
			g.mapping.Close()
			os.Remove(g.path)
			return err
		}
		root := g.global(g.root)
		s.retain(root)
		old := s.rebaseTo
		s.rebaseTo = root
		s.createdAtRewrite = s.created
		s.gen.Add(1)
		s.release(old)
		s.mu.Unlock()
	}
	runtime.KeepAlive(h)
	return f.writeSource(g, digest)
}
