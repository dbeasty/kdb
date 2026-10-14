package attachstore

import (
	"bytes"
	"container/list"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
)

// The pointer index (docs/kdb-memory-cassandra-plan.md: data larger than RAM, memory a bounded
// cache). The pointer segments are the truth: each names a blob's bytes, and the newest readable
// generation of a hash is its pointer. What the store keeps of them is an index, so that memory
// does not grow with the blob count - under AttachmentHistoryAll every stored version is a blob:
//
//   - One entry per blob, fixed size: its pointer's generation, whether it is a delta and of what,
//     keys of the segments it names (for Collect's sweep), and when its upload hold started.
//   - Entries live in sorted runs, <ns>/attachment/x-<seq>, newest first, and a bounded overlay
//     of the changes since the newest run. A full overlay is written as a run; a run at least a
//     quarter the size of the one below it is merged into it, so there are few runs and an entry
//     is rewritten a few times, not once per change. A lookup reads the overlay, then binary
//     searches each run.
//   - The pointers themselves (inline bytes and all) are read from their segments on demand and
//     kept in an LRU bounded by bytes (Config.PointerCacheBytes).
//
// The index is never trusted across a restart: the first use lists the segments and rebuilds it
// from the pointers, as the store always did (which is how a torn pointer reads as the generation
// below it), and deletes the runs an earlier process left. So a run needs no checksum, and a crash
// at any point loses nothing but the runs. Without a writable shim the index stays in the overlay,
// unbounded, as the whole map used to.

// nameKey identifies a segment name in an index entry: a prefix of its SHA-256. Two names sharing
// one would only keep an unreferenced segment past a Collect.
type nameKey [16]byte

func keyOf(name string) nameKey {
	var k nameKey
	if name != "" {
		sum := sha256.Sum256([]byte(name))
		copy(k[:], sum[:])
	}
	return k
}

// indexEntry is what the index keeps of one blob.
type indexEntry struct {
	gen         int
	delta       bool
	gone        bool  // a tombstone: the blob was removed since an older run was written
	held        int64 // UnixNano its upload grace started (Store.held used to say); 0 when not held
	base        codec.Hash
	seg, leaves nameKey
}

func entryFor(p pointer, held int64) indexEntry {
	return indexEntry{gen: p.gen, delta: p.delta, held: held, base: p.base, seg: keyOf(p.segment), leaves: keyOf(p.leaves)}
}

// heldAt reports whether e's upload grace has not run out at now.
func (e indexEntry) heldAt(now time.Time, grace time.Duration) bool {
	return e.held != 0 && now.Sub(time.Unix(0, e.held)) < grace
}

const entryBytes = 32 + 4 + 1 + 8 + 32 + 16 + 16

func appendEntry(b []byte, h codec.Hash, e indexEntry) []byte {
	b = append(b, h.Bytes[:]...)
	b = binary.BigEndian.AppendUint32(b, uint32(e.gen))
	flags := byte(0)
	if e.delta {
		flags |= 1
	}
	if e.gone {
		flags |= 2
	}
	b = append(b, flags)
	b = binary.BigEndian.AppendUint64(b, uint64(e.held))
	b = append(b, e.base.Bytes[:]...)
	b = append(b, e.seg[:]...)
	return append(b, e.leaves[:]...)
}

func decodeEntry(b []byte) (codec.Hash, indexEntry) {
	var h codec.Hash
	copy(h.Bytes[:], b)
	e := indexEntry{gen: int(binary.BigEndian.Uint32(b[32:])), delta: b[36]&1 != 0, gone: b[36]&2 != 0}
	e.held = int64(binary.BigEndian.Uint64(b[37:]))
	copy(e.base.Bytes[:], b[45:])
	copy(e.seg[:], b[77:])
	copy(e.leaves[:], b[93:])
	return h, e
}

func hashLess(a, b codec.Hash) bool { return bytes.Compare(a.Bytes[:], b.Bytes[:]) < 0 }

const defaultIndexOverlay = 16 << 10

type indexRun struct {
	name string
	n    int64 // entries
}

// pointerIndex is the index (see above). Not safe for concurrent use: the store calls it under mu.
type pointerIndex struct {
	shim    storage.PlatformIOShim
	prefix  string
	max     int // overlay entries before it is written as a run
	overlay map[codec.Hash]indexEntry
	runs    []indexRun // newest first
	seq     int
	// Runs merged away while an each is reading them, deleted once none is.
	iterating int
	retired   []string
	spillErr  bool // a run could not be written: the overlay grows instead (logged once)
}

func newPointerIndex(shim storage.PlatformIOShim, prefix string, max int) *pointerIndex {
	if max <= 0 {
		max = defaultIndexOverlay
	}
	return &pointerIndex{shim: shim, prefix: prefix, max: max, overlay: map[codec.Hash]indexEntry{}}
}

func (x *pointerIndex) isRun(name string) bool { return strings.HasPrefix(name, x.prefix+"x-") }

// get returns h's entry; ok=false when the index has none.
func (x *pointerIndex) get(h codec.Hash) (indexEntry, bool, error) {
	if e, ok := x.overlay[h]; ok {
		return e, !e.gone, nil
	}
	for _, r := range x.runs {
		e, ok, err := x.search(r, h)
		if err != nil {
			return indexEntry{}, false, err
		}
		if ok {
			return e, !e.gone, nil
		}
	}
	return indexEntry{}, false, nil
}

func (x *pointerIndex) search(r indexRun, h codec.Hash) (indexEntry, bool, error) {
	lo, hi := int64(0), r.n
	for lo < hi {
		mid := lo + (hi-lo)/2
		b, err := x.shim.ReadFromSegment(r.name, mid*entryBytes, entryBytes)
		if err != nil {
			return indexEntry{}, false, err
		}
		if len(b) != entryBytes {
			return indexEntry{}, false, fmt.Errorf("attachstore: pointer index %s is short at entry %d", r.name, mid)
		}
		got, e := decodeEntry(b)
		switch c := bytes.Compare(got.Bytes[:], h.Bytes[:]); {
		case c == 0:
			return e, true, nil
		case c < 0:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return indexEntry{}, false, nil
}

// put records e as h's entry.
func (x *pointerIndex) put(h codec.Hash, e indexEntry) {
	e.gone = false
	x.overlay[h] = e
	x.maybeSpill()
}

// remove drops h's entry.
func (x *pointerIndex) remove(h codec.Hash) {
	if len(x.runs) == 0 {
		delete(x.overlay, h) // nothing older to mask
		return
	}
	x.overlay[h] = indexEntry{gone: true}
	x.maybeSpill()
}

func (x *pointerIndex) maybeSpill() {
	if len(x.overlay) < x.max {
		return
	}
	if err := x.spill(); err != nil {
		if !x.spillErr {
			slog.Warn("kdb: attachment pointer index kept in memory: a run could not be written", "prefix", x.prefix, "err", err)
		}
		x.spillErr = true
		x.max *= 2 // try again once it has doubled, not on every change
	}
}

// spill writes the overlay as the newest run and merges runs that have grown alike.
func (x *pointerIndex) spill() error {
	keys := make([]codec.Hash, 0, len(x.overlay))
	for h := range x.overlay {
		keys = append(keys, h)
	}
	sort.Slice(keys, func(i, j int) bool { return hashLess(keys[i], keys[j]) })
	w := x.newWriter()
	for _, h := range keys {
		if e := x.overlay[h]; !e.gone || len(x.runs) > 0 {
			w.add(h, e)
		}
	}
	r, err := w.finish()
	if err != nil {
		return err
	}
	x.overlay = map[codec.Hash]indexEntry{}
	if r.n > 0 {
		x.runs = append([]indexRun{r}, x.runs...)
	}
	for len(x.runs) >= 2 && x.runs[0].n*4 >= x.runs[1].n {
		if err := x.mergeTop(); err != nil {
			return nil // the runs stay as they are, all of them read
		}
	}
	return nil
}

// mergeTop merges the two newest runs into one; into the oldest, tombstones are dropped.
func (x *pointerIndex) mergeTop() error {
	newer, older := x.runs[0], x.runs[1]
	last := len(x.runs) == 2
	w := x.newWriter()
	a, b := x.cursor(newer), x.cursor(older)
	for {
		ah, ae, aok, err := a.peek()
		if err != nil {
			w.abort()
			return err
		}
		bh, be, bok, err := b.peek()
		if err != nil {
			w.abort()
			return err
		}
		if !aok && !bok {
			break
		}
		h, e := ah, ae
		switch {
		case !bok || (aok && hashLess(ah, bh)):
			a.next()
		case !aok || hashLess(bh, ah):
			h, e = bh, be
			b.next()
		default: // the same hash: the newer run's entry wins
			a.next()
			b.next()
		}
		if !e.gone || !last {
			w.add(h, e)
		}
	}
	r, err := w.finish()
	if err != nil {
		return err
	}
	rest := x.runs[2:]
	x.runs = append([]indexRun{}, rest...)
	if r.n > 0 {
		x.runs = append([]indexRun{r}, x.runs...)
	}
	x.retire(newer.name)
	x.retire(older.name)
	return nil
}

func (x *pointerIndex) retire(name string) {
	if x.iterating > 0 {
		x.retired = append(x.retired, name)
		return
	}
	_ = x.shim.DeleteSegment(name)
}

// each calls fn for every entry in hash order, as the index was when it started: fn may change
// the index (Collect does), which each does not see.
func (x *pointerIndex) each(fn func(codec.Hash, indexEntry) error) error {
	keys := make([]codec.Hash, 0, len(x.overlay))
	for h := range x.overlay {
		keys = append(keys, h)
	}
	sort.Slice(keys, func(i, j int) bool { return hashLess(keys[i], keys[j]) })
	srcs := []entrySource{&overlaySource{keys: keys, m: maps(x.overlay, keys)}}
	for _, r := range x.runs {
		srcs = append(srcs, x.cursor(r))
	}
	x.iterating++
	defer func() {
		x.iterating--
		if x.iterating == 0 {
			for _, name := range x.retired {
				_ = x.shim.DeleteSegment(name)
			}
			x.retired = nil
		}
	}()
	for {
		var (
			min  codec.Hash
			win  indexEntry
			have bool
		)
		for _, s := range srcs { // newest first: the first to hold the least hash wins
			h, e, ok, err := s.peek()
			if err != nil {
				return err
			}
			if ok && (!have || hashLess(h, min)) {
				min, win, have = h, e, true
			}
		}
		if !have {
			return nil
		}
		for _, s := range srcs {
			if h, _, ok, _ := s.peek(); ok && h == min {
				s.next()
			}
		}
		if win.gone {
			continue
		}
		if err := fn(min, win); err != nil {
			return err
		}
	}
}

// maps copies the overlay entries for keys, so each reads a snapshot.
func maps(m map[codec.Hash]indexEntry, keys []codec.Hash) []indexEntry {
	out := make([]indexEntry, len(keys))
	for i, h := range keys {
		out[i] = m[h]
	}
	return out
}

type entrySource interface {
	peek() (codec.Hash, indexEntry, bool, error)
	next()
}

type overlaySource struct {
	keys []codec.Hash
	m    []indexEntry
	i    int
}

func (o *overlaySource) peek() (codec.Hash, indexEntry, bool, error) {
	if o.i >= len(o.keys) {
		return codec.Hash{}, indexEntry{}, false, nil
	}
	return o.keys[o.i], o.m[o.i], true, nil
}

func (o *overlaySource) next() { o.i++ }

// runCursor reads a run in blocks.
type runCursor struct {
	shim storage.PlatformIOShim
	run  indexRun
	pos  int64 // entries read into buf so far
	buf  []byte
	off  int
}

const cursorBlock = 512 // entries read at once

func (x *pointerIndex) cursor(r indexRun) *runCursor { return &runCursor{shim: x.shim, run: r} }

func (c *runCursor) peek() (codec.Hash, indexEntry, bool, error) {
	if c.off >= len(c.buf) {
		if c.pos >= c.run.n {
			return codec.Hash{}, indexEntry{}, false, nil
		}
		n := min(int64(cursorBlock), c.run.n-c.pos)
		b, err := c.shim.ReadFromSegment(c.run.name, c.pos*entryBytes, int(n*entryBytes))
		if err != nil {
			return codec.Hash{}, indexEntry{}, false, err
		}
		if int64(len(b)) != n*entryBytes {
			return codec.Hash{}, indexEntry{}, false, fmt.Errorf("attachstore: pointer index %s is short at entry %d", c.run.name, c.pos)
		}
		c.buf, c.off, c.pos = b, 0, c.pos+n
	}
	h, e := decodeEntry(c.buf[c.off:])
	return h, e, true, nil
}

func (c *runCursor) next() { c.off += entryBytes }

// runWriter streams entries, in hash order, into a new run.
type runWriter struct {
	x   *pointerIndex
	run indexRun
	buf []byte
	err error
}

func (x *pointerIndex) newWriter() *runWriter {
	x.seq++
	return &runWriter{x: x, run: indexRun{name: fmt.Sprintf("%sx-%06d", x.prefix, x.seq)}}
}

func (w *runWriter) add(h codec.Hash, e indexEntry) {
	if w.err != nil {
		return
	}
	if w.buf == nil {
		w.buf = make([]byte, 0, 4096*entryBytes)
	}
	w.buf = appendEntry(w.buf, h, e)
	w.run.n++
	if len(w.buf)+entryBytes > cap(w.buf) {
		w.write()
	}
}

func (w *runWriter) write() {
	if len(w.buf) == 0 || w.err != nil {
		return
	}
	_, w.err = w.x.shim.AppendToSegment(w.run.name, w.buf)
	w.buf = w.buf[:0]
}

func (w *runWriter) finish() (indexRun, error) {
	if w.run.n == 0 {
		return w.run, nil
	}
	w.write()
	if w.err == nil {
		w.err = w.x.shim.FlushSegment(w.run.name)
	}
	if w.err == nil {
		w.err = w.x.shim.SealSegment(w.run.name)
	}
	if w.err != nil {
		w.abort()
		return indexRun{}, w.err
	}
	return w.run, nil
}

func (w *runWriter) abort() {
	if w.run.n > 0 {
		_ = w.x.shim.DeleteSegment(w.run.name)
	}
}

// rebuild indexes the pointer segments among names (sorted, so a hash's generations are
// together), deleting the runs an earlier process left. A hash whose newest generation is torn is
// indexed at the newest one that reads; one with none that does is not indexed (Collect retires
// its segments). Entries go into the overlay while they fit, and once they do not, all of them
// straight into one run, in order (or, should that run not write, the overlay after all).
// A store small enough for one overlay writes no run at all.
func (s *Store) rebuildIndex(names []string, now time.Time) error {
	x := s.idx
	var pointers []string
	for _, name := range names {
		switch {
		case x.isRun(name):
			_ = s.shim.DeleteSegment(name)
			if seq, err := strconv.Atoi(strings.TrimPrefix(name, x.prefix+"x-")); err == nil && seq > x.seq {
				x.seq = seq // never write over one this pass could not delete
			}
		case strings.HasPrefix(name, s.prefix+"h-"):
			pointers = append(pointers, name)
		}
	}
	sort.Strings(pointers)
	walk := func(add func(codec.Hash, indexEntry)) {
		for i := 0; i < len(pointers); {
			j := i + 1
			stem := pointers[i][:strings.LastIndexByte(pointers[i], '.')+1]
			for j < len(pointers) && strings.HasPrefix(pointers[j], stem) {
				j++
			}
			group := pointers[i:j]
			i = j
			sort.Slice(group, func(a, b int) bool { return genOf(group[a]) > genOf(group[b]) })
			for _, name := range group {
				p, h, ok := s.readPointer(name)
				if !ok {
					continue // torn by a crash: its blob is unreachable through it, and Collect retires it
				}
				held := int64(0)
				if !p.finished.IsZero() && now.Sub(p.finished) < s.cfg.UploadGrace {
					held = p.finished.UnixNano()
				}
				add(h, entryFor(p, held))
				break
			}
		}
	}
	var w *runWriter
	walk(func(h codec.Hash, e indexEntry) {
		if w != nil {
			w.add(h, e)
			return
		}
		x.overlay[h] = e
		if len(x.overlay) < x.max {
			return
		}
		w = x.newWriter() // the overlay's entries first, then the rest as they come
		keys := make([]codec.Hash, 0, len(x.overlay))
		for k := range x.overlay {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return hashLess(keys[i], keys[j]) })
		for _, k := range keys {
			w.add(k, x.overlay[k])
		}
		x.overlay = map[codec.Hash]indexEntry{}
	})
	if w == nil {
		return nil
	}
	r, err := w.finish()
	if err != nil {
		slog.Warn("kdb: attachment pointer index kept in memory: a run could not be written", "prefix", x.prefix, "err", err)
		x.spillErr = true
		x.max = int(^uint(0) >> 1)
		x.overlay = map[codec.Hash]indexEntry{}
		walk(func(h codec.Hash, e indexEntry) { x.overlay[h] = e })
		return nil
	}
	if r.n > 0 {
		x.runs = []indexRun{r}
	}
	return nil
}

func genOf(name string) int {
	g, _ := strconv.Atoi(name[strings.LastIndexByte(name, '.')+1:])
	return g
}

// lru is a cache of at most budget cost, least recently used out first.
type lru[K comparable, V any] struct {
	budget int64
	cost   func(V) int64
	used   int64
	m      map[K]*list.Element
	l      list.List
}

type lruItem[K comparable, V any] struct {
	k    K
	v    V
	cost int64
}

func newLRU[K comparable, V any](budget int64, cost func(V) int64) *lru[K, V] {
	return &lru[K, V]{budget: budget, cost: cost, m: map[K]*list.Element{}}
}

func (c *lru[K, V]) get(k K) (V, bool) {
	el, ok := c.m[k]
	if !ok {
		var zero V
		return zero, false
	}
	c.l.MoveToFront(el)
	return el.Value.(*lruItem[K, V]).v, true
}

func (c *lru[K, V]) put(k K, v V) {
	c.drop(k)
	n := c.cost(v)
	if n > c.budget {
		return
	}
	c.m[k] = c.l.PushFront(&lruItem[K, V]{k, v, n})
	c.used += n
	for c.used > c.budget {
		c.drop(c.l.Back().Value.(*lruItem[K, V]).k)
	}
}

func (c *lru[K, V]) drop(k K) {
	if el, ok := c.m[k]; ok {
		c.used -= el.Value.(*lruItem[K, V]).cost
		c.l.Remove(el)
		delete(c.m, k)
	}
}

func (c *lru[K, V]) len() int { return len(c.m) }

func pointerCost(p pointer) int64 {
	return 256 + int64(len(p.name)+len(p.segment)+len(p.leaves)+len(p.inline))
}

func one[V any](V) int64 { return 1 }
