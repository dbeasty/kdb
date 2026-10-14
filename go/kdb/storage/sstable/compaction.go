package sstable

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// SSTable compaction: merging the tables a run of memtable flushes leaves
// behind, so on-disk size follows what is stored rather than how many
// times it has been flushed.
//
// Without this, every flush writes another table holding whatever was hot,
// and nothing ever merges or drops them. A namespace rewriting the same
// documents keeps a full copy per flush: measured at 25 documents over 40
// open/write/close cycles, the store grew to 320 KB for a dataset of about
// 13 KB, and it grows without bound. That is the one remaining structure
// whose size is a function of write history rather than of data, and it is
// why history=none could bound its delta log and still not bound its
// footprint.
//
// The merge is unusually simple here because of what the keys are. Every
// key is the SHA-256 of the value stored under it, so two tables holding
// the same key hold identical bytes and "which is newer" cannot change an
// answer for a value. Only tombstones can, and they are handled explicitly
// below.

// DefaultCompactionTrigger is how many runs a store may accumulate before a compaction is
// worth running. A run is what one flush or one merge wrote: a flushed table alone, or the
// tables a merge split its output into (see runOf).
//
// Four, because the cost of merging is proportional to the live data and
// the benefit is proportional to the duplication, so the trigger wants to
// be small - but every compaction rewrites everything, so triggering on
// two would rewrite the whole store on every other flush.
//
// Runs rather than tables, because a merge now writes several tables: counting tables, a store
// whose last merge wrote four would merge again on every pass.
const DefaultCompactionTrigger = 4

// A merge's output is split into tables of at most mergeTableBytes of blocks or mergeTableKeys
// keys, whichever comes first, so what a merge holds for its output - one table's footer index -
// is bounded however large the store is. The keys of one merge's tables do not overlap, and a
// lookup skips the ones that cannot hold a key by their Bloom filters (sidecar.go).
//
// A variable for tests, which shrink it to split small merges.
var (
	mergeTableBytes int64 = 64 << 20
	mergeTableKeys        = 1 << 20
)

// Compact merges inputs into a run of tables at outLevel and returns them, in key order.
//
// inputs are given oldest-first, so a later table's opinion about a key
// wins - which for values is a formality (identical keys carry identical
// bytes) and for tombstones is the whole point.
//
// dropTombstones may be set only when inputs is *every* table in the
// store. A tombstone says "this key is gone, do not consult anything
// older", so dropping one while any un-merged table might still hold the
// key would resurrect it. When in doubt, leave it false: keeping a
// tombstone costs one index entry.
//
// keep, when non-nil, is asked about every key and drops the ones it
// refuses. That is how a superseded document version stops costing disk:
// without it a compaction only removes *duplicates*, and a store that
// writes a new version of a document every time still grows without bound
// because every version is a distinct content hash and none of them is a
// duplicate of anything. Passing nil keeps everything, which is what a
// store whose reachability the caller cannot determine must do.
//
// Inputs are read through their sidecars where they have one (sidecar.go), and otherwise through
// their footers - see openMergeSources.
func Compact(
	io storage.PlatformIOShim,
	namespaceID string,
	outLevel int,
	inputs []Handle,
	dropTombstones bool,
	keep func(codec.Hash) bool,
) ([]Handle, error) {
	return CompactContext(context.Background(), io, namespaceID, outLevel, inputs, dropTombstones, keep)
}

// compactionCheckEvery is how many keys a merge handles between looks at its context: often
// enough that a cancelled merge stops within milliseconds, rarely enough to cost nothing.
const compactionCheckEvery = 4096

// CompactContext is Compact that gives up when ctx is done, returning ctx's error and leaving
// no table behind. Abandoning is always safe: the output is written under partial names
// (io.SegmentNameBuilder.SSTablePartial) that nothing registers as tables, and is deleted when
// the merge stops, so the inputs stay exactly as they were. Once the last key is written the
// footer and renames run to the end - they are the short tail of the merge.
func CompactContext(
	ctx context.Context,
	io storage.PlatformIOShim,
	namespaceID string,
	outLevel int,
	inputs []Handle,
	dropTombstones bool,
	keep func(codec.Hash) bool,
) ([]Handle, error) {
	if len(inputs) == 0 {
		return nil, fmt.Errorf("sstable: compaction needs at least one input table")
	}
	sources, err := openMergeSources(io, inputs, nil)
	if err != nil {
		return nil, err
	}
	defer closeMergeSources(sources)
	out, _, err := compact(ctx, io, namespaceID, outLevel, inputs, sources, dropTombstones, keep)
	return out, err
}

// compact merges what sources yield - sources[i] is inputs[i]'s index, in key order - and also
// reports how many distinct keys keep refused: what LsmBlobStore.CompactContext reports as
// reclaimed, counted here because this is where every key is asked about exactly once.
//
// The merge holds no per-key state. It used to read every input's whole index into memory, sort
// it and only then write - 60 bytes per key of the store, 680 MB to merge the 49 tables of a
// 10M-document namespace. Every input's index is already sorted on disk (its sidecar), so the
// inputs are instead read side by side in key order, a batch per input at a time, and each key's
// final owner is decided as its entries meet: the newest input holding it, the same winner the
// index-loading merge chose. What a merge holds is a batch per input and one output table's index.
//
// Deciding every key's owner before writing it is still what keeps a dropped tombstone from
// resurrecting the value under it: a key is written, or dropped, once, after every input's
// opinion of it has been seen.
func compact(
	ctx context.Context,
	io storage.PlatformIOShim,
	namespaceID string,
	outLevel int,
	inputs []Handle,
	sources []mergeSource,
	dropTombstones bool,
	keep func(codec.Hash) bool,
) ([]Handle, int, error) {
	cursors := make(mergeHeap, 0, len(sources))
	for i, src := range sources {
		c := &mergeCursor{src: src, input: i, buf: make([]indexEntry, mergeBatch)}
		ok, err := c.advance()
		if err != nil {
			return nil, 0, fmt.Errorf("sstable: compaction could not read %s: %w", inputs[i].SegmentName, err)
		}
		if ok {
			cursors = append(cursors, c)
		}
	}
	heap.Init(&cursors)

	w, err := newRunWriter(io, namespaceID, outLevel)
	if err != nil {
		return nil, 0, err
	}
	done := false
	defer func() {
		if !done {
			// Cancelled, failed, or nothing to write: whatever was written goes.
			w.abort()
		}
	}()

	readers := make([]*DefaultReader, len(inputs))
	for i, in := range inputs {
		readers[i] = NewDefaultReader(io, in)
	}
	dropped := 0
	for n := 0; len(cursors) > 0; n++ {
		if n%compactionCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return nil, 0, err
			}
		}
		// Every input's entry for the smallest key, oldest input first (the heap breaks ties by
		// input), so the last one taken is the key's final owner.
		key := cursors[0].head().key
		var bh BlockHandle
		input := -1
		for len(cursors) > 0 && cursors[0].head().key == key {
			c := cursors[0]
			bh, input = c.head().handle, c.input
			ok, err := c.advance()
			if err != nil {
				return nil, 0, fmt.Errorf("sstable: compaction could not read %s: %w", inputs[c.input].SegmentName, err)
			}
			if ok {
				heap.Fix(&cursors, 0)
			} else {
				heap.Pop(&cursors)
			}
		}
		if keep != nil && !keep(key) {
			// Unreachable: the caller says nothing can ask for this key
			// again. Dropping it is the only way a superseded version ever
			// stops costing disk - see LsmBlobStore.Compact for what makes
			// a "no" here safe.
			dropped++
			continue
		}
		if bh.Deleted {
			if dropTombstones {
				continue
			}
			if err := w.delete(key); err != nil {
				return nil, 0, err
			}
			continue
		}
		value, err := readers[input].ReadBlockAt(bh)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"sstable: compaction could not read a block of %s: %w", inputs[input].SegmentName, err)
		}
		if err := w.put(key, value); err != nil {
			return nil, 0, err
		}
	}
	out, err := w.commit()
	if err != nil {
		return nil, 0, err
	}
	done = true
	if len(out) == 0 {
		// Every input was empty, or was nothing but tombstones being
		// dropped. Finish would write a table with no entries, which is a
		// file that can only ever cost a read.
		return nil, dropped, ErrNothingToCompact
	}
	return out, dropped, nil
}

// ErrNothingToCompact reports a merge whose inputs held nothing worth
// writing out. Not a failure: the caller deletes the inputs and is done.
var ErrNothingToCompact = fmt.Errorf("sstable: the inputs hold nothing to write")

// mergeBatch is how many index entries a merge reads from one input at a time: 64 KB per input.
const mergeBatch = 1024

// mergeSource yields one input's index entries in key order, each key at most once.
type mergeSource interface {
	// fill copies up to len(buf) next entries into buf and reports how many; 0 at the end.
	fill(buf []indexEntry) (int, error)
	close()
}

// sidecarSource reads a table's sidecar, whose entries are sorted and unique by construction.
// The entries are copied out a batch at a time under the mapping's fault guard, so a sidecar
// truncated underneath fails the merge rather than crashing it.
type sidecarSource struct {
	sc    *sidecar
	pos   int
	owned bool // close unmaps it: opened for this merge, not borrowed from a store
}

func (s *sidecarSource) fill(buf []indexEntry) (int, error) {
	n := min(len(buf), s.sc.count-s.pos)
	if n <= 0 {
		return 0, nil
	}
	err := s.sc.region.Guard(func([]byte) {
		for i := 0; i < n; i++ {
			at := (s.pos + i) * sidecarEntrySize
			rec := s.sc.entries[at : at+sidecarEntrySize]
			copy(buf[i].key.Bytes[:], rec[:32])
			buf[i].handle = decodeSidecarHandle(rec)
		}
	})
	if err != nil {
		return 0, err
	}
	s.pos += n
	return n, nil
}

func (s *sidecarSource) close() {
	if s.owned {
		s.sc.close()
	}
}

// sliceSource serves a table's index from memory, sorted: for a table with no usable sidecar.
type sliceSource struct {
	entries []indexEntry
	pos     int
}

func (s *sliceSource) fill(buf []indexEntry) (int, error) {
	n := copy(buf, s.entries[s.pos:])
	s.pos += n
	return n, nil
}

func (s *sliceSource) close() {}

// openMergeSources opens a mergeSource per input. sidecarOf, when non-nil, supplies a table's
// sidecar (a store passes the ones it has open, building them where missing); otherwise each
// input's sidecar file is opened if it has a valid one. An input without one is read from its
// footer and sorted in memory - its whole index, so a merge of inputs without sidecars (sidecars
// turned off, or a shim with no local files) holds every key again, as merges did before sidecars
// existed.
func openMergeSources(io storage.PlatformIOShim, inputs []Handle, sidecarOf func(Handle) *sidecar) ([]mergeSource, error) {
	paths := sidecarResolver(io)
	sources := make([]mergeSource, 0, len(inputs))
	for _, in := range inputs {
		src, err := openMergeSource(io, paths, in, sidecarOf)
		if err != nil {
			closeMergeSources(sources)
			// One unreadable table must not cost the whole store its
			// compaction, and must not silently drop the keys it held
			// either - so the compaction is abandoned rather than
			// completed with a hole in it.
			return nil, fmt.Errorf("sstable: compaction could not read %s: %w", in.SegmentName, err)
		}
		sources = append(sources, src)
	}
	return sources, nil
}

func openMergeSource(io storage.PlatformIOShim, paths storage.LocalPathResolver, in Handle, sidecarOf func(Handle) *sidecar) (mergeSource, error) {
	if sidecarOf != nil {
		if sc := sidecarOf(in); sc != nil {
			return &sidecarSource{sc: sc}, nil
		}
	}
	r := NewDefaultReader(io, in)
	size, err := r.segmentSize()
	if err != nil {
		return nil, err
	}
	if size < 40+footerTrailerSize {
		return &sliceSource{}, nil // no footer, so no entries - as Index reads it
	}
	if sidecarOf == nil {
		if path, ok := sidecarPath(paths, in.SegmentName); ok {
			if _, fileHash, err := r.identity(); err == nil {
				if sc, err := openSidecar(path, size, fileHash); err == nil {
					return &sidecarSource{sc: sc, owned: true}, nil
				}
			}
		}
	}
	entries, err := r.indexEntries(size)
	if err != nil {
		return nil, err
	}
	return &sliceSource{entries: sortIndexEntries(entries)}, nil
}

func closeMergeSources(sources []mergeSource) {
	for _, s := range sources {
		s.close()
	}
}

// mergeCursor is one input's position in a merge: a batch of its entries and where in it.
type mergeCursor struct {
	src   mergeSource
	input int
	buf   []indexEntry
	i, n  int
}

func (c *mergeCursor) head() *indexEntry { return &c.buf[c.i] }

// advance moves to the next entry, refilling the batch when it is used up, and reports false at
// the input's end.
func (c *mergeCursor) advance() (bool, error) {
	if c.n > 0 {
		c.i++
	}
	if c.i < c.n {
		return true, nil
	}
	n, err := c.src.fill(c.buf)
	if err != nil {
		return false, err
	}
	c.i, c.n = 0, n
	return n > 0, nil
}

// mergeHeap orders cursors by their current key, then by input, oldest first.
type mergeHeap []*mergeCursor

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	if c := bytes.Compare(h[i].head().key.Bytes[:], h[j].head().key.Bytes[:]); c != 0 {
		return c < 0
	}
	return h[i].input < h[j].input
}

func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x any) { *h = append(*h, x.(*mergeCursor)) }

func (h *mergeHeap) Pop() any {
	old := *h
	c := old[len(old)-1]
	*h = old[:len(old)-1]
	return c
}

// runWriter writes a merge's output as a run of tables, starting a new one whenever the current
// one reaches mergeTableBytes or mergeTableKeys.
//
// Every table is written under its partial name and renamed only once the last is complete
// (commit), because DiscoverTables registers every name under sstable/ and a table a crash left
// half-written there would be read as one. A crash part way through the renames leaves some of
// the run registered beside its inputs, which is harmless: every key is the hash of its value, so
// they hold the same bytes, and the next merge takes them in. A shim that cannot rename writes the
// tables' own names directly, which leaves that window open for it; both shims in this codebase
// rename.
//
// Each table's sidecar is written as soon as the table is complete, at its final name's path, so
// the table's index can be dropped before the next table starts.
type runWriter struct {
	io          storage.PlatformIOShim
	namespaceID string
	level       int
	runID       string
	renamer     storage.SegmentRenamer

	b      *tableBuilder
	tables []runTable
	// index and buf are the finished table's, reused by the next.
	index []indexEntry
	buf   []byte
}

type runTable struct {
	writing, name string // writing is the partial name until commit renames it to name
	fileHash      codec.Hash
}

func newRunWriter(io storage.PlatformIOShim, namespaceID string, level int) (*runWriter, error) {
	runID, err := codec.RandomUUID()
	if err != nil {
		return nil, err
	}
	w := &runWriter{io: io, namespaceID: namespaceID, level: level, runID: runID.String()}
	w.renamer, _ = io.(storage.SegmentRenamer)
	return w, nil
}

// runTableID names a run's seq'th table: the run's id and a sequence number, which is how runOf
// tells a merge's tables from flushed ones and from each other merge's.
func runTableID(runID string, seq int) string { return fmt.Sprintf("%s.%04d", runID, seq) }

// runOf names the run a table belongs to: its file name before the sequence number for a merge's
// table, and the whole file name for a flushed table (or one merged before merges were split),
// which is a run of its own.
func runOf(segmentName string) string {
	base := segmentName[strings.LastIndexByte(segmentName, '/')+1:]
	if i := strings.LastIndexByte(base, '.'); i > 0 {
		return segmentName[:len(segmentName)-len(base)+i]
	}
	return segmentName
}

// countRuns reports how many runs tables hold.
func countRuns(tables []Handle) int {
	runs := make(map[string]struct{}, len(tables))
	for _, t := range tables {
		runs[runOf(t.SegmentName)] = struct{}{}
	}
	return len(runs)
}

func (w *runWriter) start() {
	id := runTableID(w.runID, len(w.tables))
	t := runTable{name: storio.SegmentNameBuilder.SSTable(w.namespaceID, w.level, id)}
	t.writing = t.name
	if w.renamer != nil {
		t.writing = storio.SegmentNameBuilder.SSTablePartial(w.namespaceID, id)
	}
	w.tables = append(w.tables, t)
	if w.buf == nil {
		w.b = newTableBuilder(w.io, t.writing, 0)
		return
	}
	w.b = &tableBuilder{shim: w.io, segmentName: t.writing, buf: w.buf[:0], index: w.index[:0], hash: sha256.New()}
}

func (w *runWriter) put(key codec.Hash, value []byte) error {
	if w.b == nil {
		w.start()
	}
	if err := w.b.put(key, value); err != nil {
		return err
	}
	return w.maybeFinish()
}

func (w *runWriter) delete(key codec.Hash) error {
	if w.b == nil {
		w.start()
	}
	w.b.delete(key)
	return w.maybeFinish()
}

func (w *runWriter) maybeFinish() error {
	if w.b.offset < mergeTableBytes && len(w.b.index) < mergeTableKeys {
		return nil
	}
	return w.finishTable()
}

// finishTable completes the current table and writes its sidecar.
func (w *runWriter) finishTable() error {
	b := w.b
	w.b = nil
	size, fileHash, err := b.finish()
	if err != nil {
		return err
	}
	t := &w.tables[len(w.tables)-1]
	t.fileHash = fileHash
	writeSidecar(w.io, t.name, b.index, size, fileHash)
	w.index, w.buf = b.index, b.buf
	return nil
}

// commit completes the last table and gives every table its own name, sealed.
func (w *runWriter) commit() ([]Handle, error) {
	if w.b != nil {
		if err := w.finishTable(); err != nil {
			return nil, err
		}
	}
	out := make([]Handle, 0, len(w.tables))
	for i := range w.tables {
		t := &w.tables[i]
		if t.writing != t.name {
			if err := w.renamer.RenameSegment(t.writing, t.name); err != nil {
				return nil, err
			}
			// From here a failure deletes the table under its own name, which is complete but
			// not registered: discovered by a later open it would merely duplicate the inputs'
			// keys.
			t.writing = t.name
		}
		if err := w.io.SealSegment(t.name); err != nil {
			return nil, err
		}
		out = append(out, Handle{FileHash: t.fileHash, Level: w.level, SegmentName: t.name})
	}
	return out, nil
}

// abort deletes every table the merge wrote, and their sidecars. Deleting a segment that was
// never created is not an error.
func (w *runWriter) abort() {
	paths := sidecarResolver(w.io)
	for _, t := range w.tables {
		_ = w.io.DeleteSegment(t.writing)
		if path, ok := sidecarPath(paths, t.name); ok {
			_ = os.Remove(path)
		}
	}
}
