package dag

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"sync/atomic"

	"github.com/limidus/kdb/go/kdb/codec"
)

// Writing a segment: one k-way merge, by commit hash, of whatever the write takes - resident
// commits for a flush, segments for a merge, both for a major compaction - streamed from where the
// records already are. Nothing is copied whole first: resident records are read in place from
// slots the store has frozen for the length of the write (nodeStore.freeze), segments from their
// mappings. What the writer does hold is 4 bytes per record (where each input record lands in the
// output, for parent references) and the small tables the format keeps beside the rows.

// residentEntry is one resident commit a write takes, noted under the DAG's lock: where its record
// is, and what the record alone cannot say.
type residentEntry struct {
	hash  codec.Hash
	slot  uint32
	flags uint32
}

const (
	entryHasOps = 1 << iota // the commit wrote operations (resident: the ops side map or the eviction set says so)
	entryOwnsTx             // the commit owns its transaction id's index entry (first writer wins)
)

// residentSide is a resident commit's variable-length data, captured by reference: the store
// replaces these values, it never mutates them in place.
type residentSide struct {
	msg    string
	extra  []codec.Hash
	attach *codec.Hash
}

// graphWriteInput is everything a write reads, captured under the DAG's lock and read without it.
type graphWriteInput struct {
	resident []residentEntry // sorted by hash; the slots are frozen until the write ends
	side     map[codec.Hash]residentSide
	chunks   [][]byte // the resident tier's chunks as they stood; frozen slots never change in them
	per      uint32   // records per full chunk

	inputs      []*graphBase // segments merged, oldest first; each held by a reference in releases
	releases    []func()
	hidden      map[codec.Hash]struct{} // input records that no longer count, at the snapshot
	genOverride map[codec.Hash]uint32   // input records whose generation the DAG has changed

	stop *atomic.Bool // set when the DAG is closing: the write gives up
}

func (in *graphWriteInput) release() {
	for _, r := range in.releases {
		r()
	}
	in.releases = nil
}

func (in *graphWriteInput) residentRecord(i int) []byte {
	slot := in.resident[i].slot
	c := in.chunks[slot/in.per]
	off := int(slot%in.per) * graphRecordSize
	return c[off : off+graphRecordSize]
}

// errGraphWriteStopped is a write abandoned because the DAG is closing.
var errGraphWriteStopped = errors.New("dag: graph segment write stopped: closing")

const noPos = ^uint32(0)

// mergeStream is one input of the k-way merge: a segment (seg != nil) or the resident entries.
type mergeStream struct {
	seg *graphBase
	n   int
	cur int // the item at the head of the stream; == n when exhausted
}

// graphMerge walks the union of its streams in hash order. Streams are disjoint in practice; if
// two ever showed the same commit the newer (later) stream's record would be written and the
// older one mapped onto it, so nothing is written twice.
type graphMerge struct {
	in      *graphWriteInput
	streams []mergeStream
	heap    []int // stream indexes, a min-heap by the head's hash
}

func newGraphMerge(in *graphWriteInput) *graphMerge {
	m := &graphMerge{in: in}
	for _, b := range in.inputs {
		m.streams = append(m.streams, mergeStream{seg: b, n: b.count, cur: -1})
	}
	if len(in.resident) > 0 {
		m.streams = append(m.streams, mergeStream{n: len(in.resident), cur: -1})
	}
	for i := range m.streams {
		m.advance(i)
		if m.streams[i].cur < m.streams[i].n {
			m.push(i)
		}
	}
	return m
}

func (m *graphMerge) key(s int) []byte {
	st := &m.streams[s]
	if st.seg != nil {
		return st.seg.sec[secOIDS][st.cur*32 : st.cur*32+32]
	}
	return m.in.resident[st.cur].hash.Bytes[:]
}

// advance moves stream s to its next record that counts.
func (m *graphMerge) advance(s int) {
	st := &m.streams[s]
	for st.cur++; st.cur < st.n; st.cur++ {
		if st.seg == nil || len(m.in.hidden) == 0 {
			return
		}
		if _, gone := m.in.hidden[st.seg.hashAt(st.cur)]; !gone {
			return
		}
	}
}

// less orders heads by hash; on a tie the newer stream comes first, so it is the one written.
func (m *graphMerge) less(a, b int) bool {
	switch c := bytes.Compare(m.key(a), m.key(b)); {
	case c != 0:
		return c < 0
	default:
		return a > b
	}
}

func (m *graphMerge) push(s int) {
	m.heap = append(m.heap, s)
	for i := len(m.heap) - 1; i > 0; {
		p := (i - 1) / 2
		if !m.less(m.heap[i], m.heap[p]) {
			break
		}
		m.heap[i], m.heap[p] = m.heap[p], m.heap[i]
		i = p
	}
}

func (m *graphMerge) fix0() {
	n := len(m.heap)
	for i := 0; ; {
		l, r, best := 2*i+1, 2*i+2, i
		if l < n && m.less(m.heap[l], m.heap[best]) {
			best = l
		}
		if r < n && m.less(m.heap[r], m.heap[best]) {
			best = r
		}
		if best == i {
			return
		}
		m.heap[i], m.heap[best] = m.heap[best], m.heap[i]
		i = best
	}
}

// next returns the next record to write - its stream and position - and every other stream's
// record for the same commit (dups), which the caller maps to the same output position.
func (m *graphMerge) next(dups *[][2]int) (stream, pos int, ok bool) {
	if len(m.heap) == 0 {
		return 0, 0, false
	}
	stream = m.heap[0]
	pos = m.streams[stream].cur
	var k [32]byte
	copy(k[:], m.key(stream))
	m.pop0(stream)
	*dups = (*dups)[:0]
	for len(m.heap) > 0 && bytes.Equal(m.key(m.heap[0]), k[:]) {
		s := m.heap[0]
		*dups = append(*dups, [2]int{s, m.streams[s].cur})
		m.pop0(s)
	}
	return stream, pos, true
}

// pop0 advances the stream at the heap's root and restores the heap.
func (m *graphMerge) pop0(s int) {
	m.advance(s)
	if m.streams[s].cur < m.streams[s].n {
		m.fix0()
		return
	}
	last := len(m.heap) - 1
	m.heap[0] = m.heap[last]
	m.heap = m.heap[:last]
	m.fix0()
}

// graphWriter is the state of one segment write.
type graphWriter struct {
	in       *graphWriteInput
	resOut   []uint32   // resident entry -> output position
	inOut    [][]uint32 // input segment position -> output position (noPos: not written)
	residIdx int        // the resident stream's index in the merge, or -1
}

// posOf is where commit h lands in the output, if it is written.
func (w *graphWriter) posOf(h codec.Hash) (uint32, bool) {
	if n := len(w.in.resident); n > 0 {
		i := sort.Search(n, func(i int) bool { return bytes.Compare(w.in.resident[i].hash.Bytes[:], h.Bytes[:]) >= 0 })
		if i < n && w.in.resident[i].hash == h {
			return w.resOut[i], true
		}
	}
	key := bloomKeyHash(&h)
	for j, b := range w.in.inputs {
		if !b.bloom.has(key) {
			continue
		}
		if pos, ok := b.find(h); ok {
			if p := w.inOut[j][pos]; p != noPos {
				return p, true
			}
		}
	}
	return 0, false
}

// writeGraphSegment writes the merge of in as segment seq, synced, under a temporary name in dir,
// and returns that name and the record count; the caller gives it its final name. A reader only
// ever maps a complete segment.
func writeGraphSegment(dir, namespaceID string, seq uint64, full bool, in *graphWriteInput) (tmpPath string, count int, err error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", 0, err
	}
	tmp, err := os.CreateTemp(dir, graphSegmentName(seq)+".tmp*")
	if err != nil {
		return "", 0, err
	}
	defer func() {
		if err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
		}
	}()
	w := &graphWriter{in: in, resOut: make([]uint32, len(in.resident)), residIdx: -1}
	for i := range w.resOut {
		w.resOut[i] = noPos
	}
	for _, b := range in.inputs {
		out := make([]uint32, b.count)
		for i := range out {
			out[i] = noPos
		}
		w.inOut = append(w.inOut, out)
	}
	if len(in.resident) > 0 {
		w.residIdx = len(in.inputs)
	}
	setOut := func(stream, pos int, p uint32) {
		if stream == w.residIdx {
			w.resOut[pos] = p
		} else {
			w.inOut[stream][pos] = p
		}
	}
	stopped := func(n int) bool {
		return n&4095 == 0 && in.stop != nil && in.stop.Load()
	}

	crc := crc32.New(crcTable)
	bw := bufio.NewWriterSize(io.MultiWriter(&trickleWriter{f: tmp, every: graphTrickleBytes}, crc), 256<<10)
	var off int64
	write := func(p []byte) {
		if err == nil {
			_, err = bw.Write(p)
			off += int64(len(p))
		}
	}
	var secs [secCount][2]int64
	begin := func(s int) { secs[s][0] = off }
	end := func(s int) { secs[s][1] = off - secs[s][0] }

	var hdr [graphHeaderFixed]byte
	copy(hdr[:8], graphMagic)
	le.PutUint32(hdr[8:], graphVersion)
	if full {
		le.PutUint32(hdr[12:], graphFlagFull)
	}
	le.PutUint64(hdr[16:], seq)
	le.PutUint32(hdr[24:], uint32(len(namespaceID)))
	write(hdr[:])
	write([]byte(namespaceID))
	if pad := (8 - off%8) % 8; pad > 0 {
		write(make([]byte, pad))
	}

	// Pass 1: positions, and the OIDS section, written as they are found.
	var dups [][2]int
	begin(secOIDS)
	var fanout [256]uint32
	{
		m := newGraphMerge(in)
		var prev [32]byte
		for {
			s, pos, ok := m.next(&dups)
			if !ok {
				break
			}
			var h []byte
			if s == w.residIdx {
				h = in.resident[pos].hash.Bytes[:]
			} else {
				h = in.inputs[s].sec[secOIDS][pos*32 : pos*32+32]
			}
			if count > 0 && bytes.Compare(prev[:], h) >= 0 {
				return "", 0, fmt.Errorf("dag: graph segment records out of order at %x", h)
			}
			copy(prev[:], h)
			p := uint32(count)
			setOut(s, pos, p)
			for _, d := range dups {
				setOut(d[0], d[1], p)
			}
			write(h)
			fanout[h[0]]++
			count++
			if stopped(count) {
				return "", 0, errGraphWriteStopped
			}
		}
	}
	end(secOIDS)
	for i := 1; i < 256; i++ {
		fanout[i] += fanout[i-1]
	}

	// Pass 2: rows, with the tables they index built as they go.
	var (
		authors   []byte
		authorIdx = map[codec.UUID]uint32{}
		schemas   []byte
		schemaIdx = map[codec.Hash]uint16{}
		ext       []byte
		extIdx    = map[codec.Hash]uint32{}
		edges     []byte
		xpos      []byte
		msgi      []byte
		msgs      bytes.Buffer
		attr      []byte
	)
	author := func(u codec.UUID) uint32 {
		ai, ok := authorIdx[u]
		if !ok {
			ai = uint32(len(authors) / 16)
			authorIdx[u] = ai
			var a [16]byte
			binary.BigEndian.PutUint64(a[:8], uint64(u.MSB))
			binary.BigEndian.PutUint64(a[8:], uint64(u.LSB))
			authors = append(authors, a[:]...)
		}
		return ai
	}
	schema := func(sh codec.Hash) uint16 {
		si, ok := schemaIdx[sh]
		if !ok {
			si = uint16(len(schemas)/32) + 1
			schemaIdx[sh] = si
			schemas = append(schemas, sh.Bytes[:]...)
		}
		return si
	}
	extRef := func(h codec.Hash) uint32 {
		i, ok := extIdx[h]
		if !ok {
			i = uint32(len(ext) / 32)
			extIdx[h] = i
			ext = append(ext, h.Bytes[:]...)
		}
		return refExternal | i
	}
	// ref resolves a parent by hash: a position in the output, or an external reference.
	ref := func(h codec.Hash) (uint32, bool) {
		if p, ok := w.posOf(h); ok {
			return p, false
		}
		return extRef(h), true
	}
	// Per input, cursors into the MSGI and ATTR sections, which are sorted by position as the merge
	// visits each input.
	msgCur := make([]int, len(in.inputs))
	attrCur := make([]int, len(in.inputs))

	begin(secCDAT)
	var row [cdatSize]byte
	var parents []uint32
	m := newGraphMerge(in)
	for pos := 0; ; pos++ {
		s, ip, ok := m.next(&dups)
		if !ok {
			if pos != count {
				return "", 0, errors.New("dag: graph segment source changed between passes")
			}
			break
		}
		if stopped(pos + 1) {
			return "", 0, errGraphWriteStopped
		}
		row = [cdatSize]byte{}
		parents = parents[:0]
		external := false
		var msg string
		var attach *codec.Hash
		if s == w.residIdx {
			e := &in.resident[ip]
			r := in.residentRecord(ip)
			copy(row[cdatTree:], r[grTree:grTree+32])
			copy(row[cdatTxMSB:], r[grTxMSB:grTxMSB+16])
			le.PutUint32(row[cdatGen:], recGeneration(r))
			le.PutUint64(row[cdatEpochMs:], le.Uint64(r[grEpochMs:]))
			le.PutUint16(row[cdatMicro:], uint16(le.Uint64(r[grMicro:])))
			le.PutUint32(row[cdatAuthor:], author(codec.UUID{MSB: int64(le.Uint64(r[grAuthMSB:])), LSB: int64(le.Uint64(r[grAuthLSB:]))}))
			if le.Uint32(r[grFlags:])&grFlagSchema != 0 {
				var sh codec.Hash
				copy(sh.Bytes[:], r[grSchema:grSchema+32])
				le.PutUint16(row[cdatSchema:], schema(sh))
			}
			if e.flags&entryHasOps != 0 {
				le.PutUint16(row[cdatFlags:], cdatFlagHasOps)
			}
			side := in.side[e.hash]
			msg, attach = side.msg, side.attach
			p0, p1, n := recParents01(r)
			le.PutUint16(row[cdatNParents:], uint16(n))
			if n > 0 {
				rf, x := ref(p0)
				external = external || x
				parents = append(parents, rf)
			}
			if n > 1 {
				rf, x := ref(p1)
				external = external || x
				parents = append(parents, rf)
			}
			for _, p := range side.extra {
				rf, x := ref(p)
				external = external || x
				parents = append(parents, rf)
			}
		} else {
			b := in.inputs[s]
			src := b.cdat(ip)
			copy(row[:], src)
			h := b.hashAt(ip)
			if g, ok := in.genOverride[h]; ok {
				le.PutUint32(row[cdatGen:], g)
			}
			ai := int(le.Uint32(src[cdatAuthor:]))
			a := b.sec[secAuth][ai*16 : ai*16+16]
			le.PutUint32(row[cdatAuthor:], author(codec.UUID{MSB: int64(binary.BigEndian.Uint64(a[:8])), LSB: int64(binary.BigEndian.Uint64(a[8:]))}))
			if si := int(le.Uint16(src[cdatSchema:])); si > 0 {
				var sh codec.Hash
				copy(sh.Bytes[:], b.sec[secSchm][(si-1)*32:si*32])
				le.PutUint16(row[cdatSchema:], schema(sh))
			}
			// Parent references: within the input, translated by position; outside it, resolved
			// by hash, since the parent may be in another input now merged into this one.
			b.forEachParentRef(ip, func(r uint32) bool {
				var rf uint32
				var x bool
				if r&refExternal == 0 {
					if p := w.inOut[s][r]; p != noPos {
						rf = p
					} else {
						rf, x = extRef(b.hashAt(int(r))), true
					}
				} else {
					rf, x = ref(b.resolveRef(r))
				}
				external = external || x
				parents = append(parents, rf)
				return true
			})
			mi := b.sec[secMsgI]
			for msgCur[s]*12 < len(mi) && int(le.Uint32(mi[msgCur[s]*12:])) < ip {
				msgCur[s]++
			}
			if i := msgCur[s]; i*12 < len(mi) && int(le.Uint32(mi[i*12:])) == ip {
				o, l := le.Uint32(mi[i*12+4:]), le.Uint32(mi[i*12+8:])
				msg = string(b.sec[secMsgs][o : o+l])
			}
			at := b.sec[secAttr]
			for attrCur[s]*36 < len(at) && int(le.Uint32(at[attrCur[s]*36:])) < ip {
				attrCur[s]++
			}
			if i := attrCur[s]; i*36 < len(at) && int(le.Uint32(at[i*36:])) == ip {
				var root codec.Hash
				copy(root.Bytes[:], at[i*36+4:i*36+36])
				attach = &root
			}
		}
		le.PutUint32(row[cdatParent0:], refNone)
		le.PutUint32(row[cdatParent1:], refNone)
		switch n := len(parents); {
		case n == 1:
			le.PutUint32(row[cdatParent0:], parents[0])
		case n == 2:
			le.PutUint32(row[cdatParent0:], parents[0])
			le.PutUint32(row[cdatParent1:], parents[1])
		case n > 2:
			le.PutUint32(row[cdatParent0:], parents[0])
			le.PutUint32(row[cdatParent1:], uint32(len(edges)/4))
			for _, rf := range parents[1:] {
				edges = le.AppendUint32(edges, rf)
			}
		}
		le.PutUint16(row[cdatNParents:], uint16(len(parents)))
		if external {
			xpos = le.AppendUint32(xpos, uint32(pos))
		}
		if attach != nil {
			attr = le.AppendUint32(attr, uint32(pos))
			attr = append(attr, attach.Bytes[:]...)
		}
		if msg != "" {
			msgi = le.AppendUint32(msgi, uint32(pos))
			msgi = le.AppendUint32(msgi, uint32(msgs.Len()))
			msgi = le.AppendUint32(msgi, uint32(len(msg)))
			msgs.WriteString(msg)
		}
		write(row[:])
	}
	end(secCDAT)

	// Pass 3: the transaction index - every input's entries for records that were written, and the
	// resident commits that own theirs, by key. On a tie the older stream's entry stays: first
	// writer wins, as in txIndex.
	begin(secTIDX)
	type txItem struct {
		key [16]byte
		out uint32
	}
	owned := 0
	for i := range in.resident {
		if in.resident[i].flags&entryOwnsTx != 0 {
			owned++
		}
	}
	resTx := make([]txItem, 0, owned)
	for i := range in.resident {
		if in.resident[i].flags&entryOwnsTx == 0 {
			continue
		}
		resTx = append(resTx, txItem{key: txKey(recTx(in.residentRecord(i))), out: w.resOut[i]})
	}
	sort.Slice(resTx, func(a, b int) bool { return bytes.Compare(resTx[a].key[:], resTx[b].key[:]) < 0 })
	txCur := make([]int, len(in.inputs)+1)
	txHead := func(s int) (txItem, bool) {
		for {
			if s == len(in.inputs) {
				if txCur[s] >= len(resTx) {
					return txItem{}, false
				}
				return resTx[txCur[s]], true
			}
			b := in.inputs[s]
			if txCur[s] >= b.txCount {
				return txItem{}, false
			}
			pos, key := b.txIndexAt(txCur[s])
			if out := w.inOut[s][pos]; out != noPos {
				return txItem{key: key, out: out}, true
			}
			txCur[s]++
		}
	}
	txCount := 0
	var prevKey [16]byte
	for n := 0; ; n++ {
		best, bestS := txItem{}, -1
		for s := range txCur {
			t, ok := txHead(s)
			if !ok {
				continue
			}
			if bestS < 0 || bytes.Compare(t.key[:], best.key[:]) < 0 {
				best, bestS = t, s
			}
		}
		if bestS < 0 {
			break
		}
		txCur[bestS]++
		if txCount > 0 && bytes.Compare(prevKey[:], best.key[:]) >= 0 {
			continue // first writer wins, matching txIndex
		}
		prevKey = best.key
		var e [4]byte
		le.PutUint32(e[:], best.out)
		write(e[:])
		txCount++
		if stopped(n + 1) {
			return "", 0, errGraphWriteStopped
		}
	}
	end(secTIDX)

	begin(secFanout)
	var fb [4]byte
	for _, v := range fanout {
		le.PutUint32(fb[:], v)
		write(fb[:])
	}
	end(secFanout)
	for _, s := range []struct {
		id   int
		data []byte
	}{{secAuth, authors}, {secSchm, schemas}, {secExt, ext}, {secEdge, edges}, {secXPos, xpos}, {secMsgI, msgi}, {secMsgs, msgs.Bytes()}, {secAttr, attr}} {
		begin(s.id)
		write(s.data)
		end(s.id)
	}

	var foot [graphFooterSize]byte
	for i := 0; i < secCount; i++ {
		le.PutUint64(foot[i*20:], uint64(secs[i][0]))
		le.PutUint64(foot[i*20+8:], uint64(secs[i][1]))
		le.PutUint32(foot[i*20+16:], sectionCodecPlain)
	}
	le.PutUint64(foot[secCount*20:], uint64(count))
	le.PutUint64(foot[secCount*20+8:], uint64(txCount))
	write(foot[:graphFooterSize-8])
	if err != nil {
		return "", 0, err
	}
	if err = bw.Flush(); err != nil {
		return "", 0, err
	}
	le.PutUint32(foot[graphFooterSize-8:], crc.Sum32())
	le.PutUint32(foot[graphFooterSize-4:], graphFooterMagic)
	if _, err = tmp.Write(foot[graphFooterSize-8:]); err != nil {
		return "", 0, err
	}
	if err = syncGraphFile(tmp); err != nil {
		return "", 0, err
	}
	if err = tmp.Close(); err != nil {
		return "", 0, err
	}
	return tmp.Name(), count, nil
}

// trickleWriter syncs its file every `every` bytes written (never, for 0) - see graphTrickleBytes.
type trickleWriter struct {
	f        *os.File
	every    int
	unsynced int
}

func (w *trickleWriter) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	if err != nil || w.every <= 0 {
		return n, err
	}
	if w.unsynced += n; w.unsynced >= w.every {
		w.unsynced = 0
		if err := syncGraphFile(w.f); err != nil {
			return n, err
		}
	}
	return n, nil
}
