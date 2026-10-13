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
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage/mmapfile"
)

// The on-disk commit graph (docs/kdb-commit-graph-on-disk.md, docs/kdb-mmap-plan.md).
//
// A namespace's graph on disk is a set of segment files in one directory, each a sorted run of
// commits, mapped rather than read, so a million-commit history costs page cache the kernel can
// evict instead of heap the collector has to trace. The scheme is an LSM's, the way Cassandra
// does it:
//
//   - a flush writes the resident tier, and only it, as a new small segment - O(new commits);
//   - a full rewrite merges every segment and the resident tier into one segment marked full,
//     which supersedes every segment with a lower sequence number - the major compaction. It is
//     what drops records for commits that have since been deleted, and what keeps the number of
//     segments a lookup has to search small.
//
// Segments never share a commit: a flush writes only commits no segment holds, and a full
// segment replaces the ones below it. The one window where two could - a crash after a full
// rewrite's rename and before the old segments are deleted - is closed at the next open, which
// ignores (and, when writable, deletes) anything a full segment supersedes.
//
// Every segment is a cache and never an authority: derived from the DAG, checked end to end on
// mapping, and anything wrong with it means the DAG keeps holding those commits itself.
//
// Layout (version 4). The resident tier holds each commit as a 256-byte record that names its
// parents by hash (graph_record.go); on disk that cost 304 bytes a commit - more than the delta
// log the commits came from. Version 4 is git's commit-graph layout with Cassandra's habit of
// keeping repeated values out of the rows:
//
//	header  magic "KDBGRAF4" | version u32 | flags u32 | seq u64 | nsLen u32 | ns | pad to 8
//	OIDS    count x 32       commit hashes, sorted; a commit's position is its index
//	CDAT    count x 80       fixed-width commit data (cdat* offsets below)
//	TIDX    txCount x u32    positions, sorted by the transaction id in their CDAT row
//	FANOUT  256 x u32        fanout[b] = commits whose hash[0] <= b
//	AUTH    n x 16           distinct authors; CDAT holds an index
//	SCHM    n x 32           distinct schema hashes; CDAT holds index+1, 0 for none
//	EXT     n x 32           parents not in this segment; a parent ref with refExternal set
//	EDGE    n x u32          parent refs beyond the second, for a commit with more than two
//	XPOS    n x u32          positions of commits with any external parent - what Horizon reads
//	MSGI    n x 12           (pos u32, off u32, len u32) for commits with a message, sorted by pos
//	MSGS    bytes            messages
//	footer  (off u64, len u64, codec u32) per section | count u64 | txCount u64 | crc32c of all
//	        before | magic
//
// Every section carries a codec, zero for plain bytes. Nothing writes another value yet; the field
// is there so that encryption at rest (docs/kdb-spec-layer14-encryption-at-rest.md) and block
// compression can arrive per section - most likely Cassandra's shape, fixed-size blocks each
// sealed or compressed on its own with an offset table, decrypted into a small bounded cache on
// read - without a new layout. A reader that meets a codec it does not know treats the segment as
// unusable, which costs a rebuild, never a misread. Measured for reference: zstd shrinks a
// segment only 12-15%, because commit and tree hashes are random bytes, so compression is not
// worth its CPU today; encryption is the reason the field exists.
//
// What a segment holds is all on Layer 14's list of metadata a disk-holder may see (commit graph
// shape, timestamps, transaction and author ids, hashes) except one thing: commit messages
// (MSGS). Whoever implements encryption at rest must either seal MSGS or stop storing messages
// here and load them from the (encrypted) delta log, as operations already are. Under end-to-end
// encryption the server only ever has the client's opaque bytes, so nothing here changes.
//
// About 116 bytes a commit, against 304 in version 3. Parents by position also make an ancestry
// step an array index within a segment instead of a hash search.
//
// Durability: the writer syncs the file and its directory, but plainly, not with the store's
// full-flush primitive. It does not need to - a segment torn by a crash fails its checksum and is
// rebuilt - and a full flush of a file this size would stall every neighbouring writer on macOS.

const (
	graphMagic       = "KDBGRAF4"
	graphVersion     = 4
	graphFooterMagic = 0x4B474634 // "KGF4"
	graphHeaderFixed = 8 + 4 + 4 + 8 + 4

	graphFlagFull = 1 // this segment supersedes every segment with a lower sequence number

	graphSegmentPrefix = "graph-"
	graphSegmentSuffix = ".kdbg"

	cdatSize     = 80
	cdatTree     = 0
	cdatTxMSB    = 32
	cdatTxLSB    = 40
	cdatParent0  = 48
	cdatParent1  = 52 // or, for a commit with more than two parents, an index into EDGE
	cdatGen      = 56
	cdatAuthor   = 60
	cdatEpochMs  = 64
	cdatMicro    = 72
	cdatSchema   = 74
	cdatFlags    = 76
	cdatNParents = 78

	cdatFlagHasOps = 1

	refNone     = ^uint32(0)
	refExternal = uint32(1) << 31
)

// Sections, in file order. The footer records each one's offset and length.
const (
	secOIDS = iota
	secCDAT
	secTIDX
	secFanout
	secAuth
	secSchm
	secExt
	secEdge
	secXPos
	secMsgI
	secMsgs
	secCount
)

const graphFooterSize = secCount*20 + 8 + 8 + 4 + 4

// sectionCodecPlain is the only section codec this build reads or writes.
const sectionCodecPlain = 0

var errGraphFile = errors.New("dag: graph file unusable")

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func graphSegmentName(seq uint64) string {
	return fmt.Sprintf("%s%016d%s", graphSegmentPrefix, seq, graphSegmentSuffix)
}

// graphSegmentSeq parses a segment file name, reporting false for anything else in the directory.
func graphSegmentSeq(name string) (uint64, bool) {
	if !strings.HasPrefix(name, graphSegmentPrefix) || !strings.HasSuffix(name, graphSegmentSuffix) {
		return 0, false
	}
	seq, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, graphSegmentPrefix), graphSegmentSuffix), 10, 64)
	return seq, err == nil
}

// listGraphSegments returns the segment sequence numbers in dir, ascending.
func listGraphSegments(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var seqs []uint64
	for _, e := range entries {
		if seq, ok := graphSegmentSeq(e.Name()); ok && !e.IsDir() {
			seqs = append(seqs, seq)
		}
	}
	sort.Slice(seqs, func(a, b int) bool { return seqs[a] < seqs[b] })
	return seqs, nil
}

// graphBase is one mapped segment.
type graphBase struct {
	path      string
	seq       uint64
	full      bool
	mapping   *mmapfile.Mapping
	region    *mmapfile.Region
	ns        string
	count     int
	txCount   int
	sec       [secCount][]byte
	fanout    [256]uint32
	fileBytes int64
	crc       uint32 // the footer checksum: the segment's identity in a checkpoint's reference
}

// openGraphBase maps path and checks it end to end. The returned base holds one reference on the
// mapping until close.
func openGraphBase(path, namespaceID string) (*graphBase, error) {
	m, err := mmapfile.Open(path)
	if err != nil {
		return nil, err
	}
	r, err := m.Acquire()
	if err != nil {
		m.Close()
		return nil, err
	}
	b := &graphBase{path: path, mapping: m, region: r, fileBytes: int64(m.Size())}
	if err := b.parse(namespaceID); err != nil {
		b.close()
		return nil, fmt.Errorf("%w: %s: %v", errGraphFile, path, err)
	}
	return b, nil
}

func (b *graphBase) parse(namespaceID string) error {
	data := b.region.Bytes()
	if len(data) < graphHeaderFixed+graphFooterSize {
		return errors.New("too short")
	}
	f := data[len(data)-graphFooterSize:]
	if le.Uint32(f[graphFooterSize-4:]) != graphFooterMagic {
		return errors.New("bad footer magic")
	}
	want := le.Uint32(f[graphFooterSize-8:])
	// The one pass that touches every page runs under a fault guard: a file that shrank
	// underneath the mapping is an unusable file, not a crash.
	var got uint32
	if gerr := b.region.Guard(func(all []byte) { got = crc32.Checksum(all[:len(all)-8], crcTable) }); gerr != nil {
		return gerr
	}
	b.crc = got
	if got != want {
		return fmt.Errorf("checksum mismatch (want %08x, got %08x)", want, got)
	}
	if string(data[:8]) != graphMagic {
		return errors.New("bad magic")
	}
	if v := le.Uint32(data[8:]); v != graphVersion {
		return fmt.Errorf("version %d, this build reads %d", v, graphVersion)
	}
	b.full = le.Uint32(data[12:])&graphFlagFull != 0
	b.seq = le.Uint64(data[16:])
	nsLen := int(le.Uint32(data[24:]))
	if graphHeaderFixed+nsLen > len(data) {
		return errors.New("namespace overruns file")
	}
	b.ns = string(data[graphHeaderFixed : graphHeaderFixed+nsLen])
	if b.ns != namespaceID {
		return fmt.Errorf("written for namespace %q", b.ns)
	}
	body := len(data) - graphFooterSize
	for i := 0; i < secCount; i++ {
		off, n := int(le.Uint64(f[i*20:])), int(le.Uint64(f[i*20+8:]))
		if off < 0 || n < 0 || off > body || n > body-off {
			return fmt.Errorf("section %d overruns file", i)
		}
		if c := le.Uint32(f[i*20+16:]); c != sectionCodecPlain {
			return fmt.Errorf("section %d uses codec %d, which this build cannot read", i, c)
		}
		b.sec[i] = data[off : off+n]
	}
	b.count = int(le.Uint64(f[secCount*20:]))
	b.txCount = int(le.Uint64(f[secCount*20+8:]))
	switch {
	case len(b.sec[secOIDS]) != b.count*32, len(b.sec[secCDAT]) != b.count*cdatSize,
		len(b.sec[secTIDX]) != b.txCount*4, len(b.sec[secFanout]) != 256*4,
		len(b.sec[secAuth])%16 != 0, len(b.sec[secSchm])%32 != 0, len(b.sec[secExt])%32 != 0,
		len(b.sec[secEdge])%4 != 0, len(b.sec[secXPos])%4 != 0, len(b.sec[secMsgI])%12 != 0:
		return errors.New("section sizes disagree with the counts")
	}
	for i := range b.fanout {
		b.fanout[i] = le.Uint32(b.sec[secFanout][i*4:])
	}
	if int(b.fanout[255]) != b.count {
		return errors.New("fanout disagrees with record count")
	}
	return nil
}

func (b *graphBase) close() {
	if b.region != nil {
		b.region.Release()
		b.region = nil
	}
	if b.mapping != nil {
		b.mapping.Close()
		b.mapping = nil
	}
}

func (b *graphBase) cdat(pos int) []byte {
	return b.sec[secCDAT][pos*cdatSize : (pos+1)*cdatSize]
}

func (b *graphBase) hashAt(pos int) (h codec.Hash) {
	copy(h.Bytes[:], b.sec[secOIDS][pos*32:pos*32+32])
	return h
}

func (b *graphBase) generation(pos int) uint32 { return le.Uint32(b.cdat(pos)[cdatGen:]) }
func (b *graphBase) hasOps(pos int) bool {
	return le.Uint16(b.cdat(pos)[cdatFlags:])&cdatFlagHasOps != 0
}
func (b *graphBase) nParents(pos int) int { return int(le.Uint16(b.cdat(pos)[cdatNParents:])) }

func (b *graphBase) treeHash(pos int) (h codec.Hash) {
	copy(h.Bytes[:], b.cdat(pos)[cdatTree:cdatTree+32])
	return h
}

func (b *graphBase) tx(pos int) codec.UUID {
	r := b.cdat(pos)
	return codec.UUID{MSB: int64(le.Uint64(r[cdatTxMSB:])), LSB: int64(le.Uint64(r[cdatTxLSB:]))}
}

func (b *graphBase) timestamp(pos int) codec.Timestamp {
	r := b.cdat(pos)
	return codec.Timestamp{EpochMillis: int64(le.Uint64(r[cdatEpochMs:])), MicroRemainder: int(le.Uint16(r[cdatMicro:]))}
}

// resolveRef turns a parent reference into a hash.
func (b *graphBase) resolveRef(ref uint32) codec.Hash {
	if ref&refExternal != 0 {
		var h codec.Hash
		i := int(ref &^ refExternal)
		copy(h.Bytes[:], b.sec[secExt][i*32:i*32+32])
		return h
	}
	return b.hashAt(int(ref))
}

// forEachParent calls fn with each parent of the commit at pos, in order, until fn returns false.
func (b *graphBase) forEachParent(pos int, fn func(codec.Hash) bool) {
	r := b.cdat(pos)
	n := int(le.Uint16(r[cdatNParents:]))
	if n == 0 {
		return
	}
	if !fn(b.resolveRef(le.Uint32(r[cdatParent0:]))) || n == 1 {
		return
	}
	if n == 2 {
		fn(b.resolveRef(le.Uint32(r[cdatParent1:])))
		return
	}
	edge := int(le.Uint32(r[cdatParent1:]))
	for i := 0; i < n-1; i++ {
		if !fn(b.resolveRef(le.Uint32(b.sec[secEdge][(edge+i)*4:]))) {
			return
		}
	}
}

func (b *graphBase) message(pos int) string {
	mi := b.sec[secMsgI]
	n := len(mi) / 12
	i := sort.Search(n, func(i int) bool { return int(le.Uint32(mi[i*12:])) >= pos })
	if i == n || int(le.Uint32(mi[i*12:])) != pos {
		return ""
	}
	off, l := le.Uint32(mi[i*12+4:]), le.Uint32(mi[i*12+8:])
	return string(b.sec[secMsgs][off : off+l])
}

// commit rebuilds the document.Commit at pos. Everything returned is a copy.
func (b *graphBase) commit(pos int, ops []document.Op) document.Commit {
	src := b.toSource(pos)
	return decodeRecord(src.rec[:], b.ns, src.msg, src.extra, ops)
}

// toSource converts the commit at pos back into the resident tier's record form, for a rewrite
// merging segments and for commit. The record carries the segment's generation.
func (b *graphBase) toSource(pos int) graphSource {
	r := b.cdat(pos)
	c := document.Commit{
		Hash:             b.hashAt(pos),
		TransactionID:    b.tx(pos),
		Timestamp:        b.timestamp(pos),
		DocumentTreeHash: b.treeHash(pos),
	}
	ai := int(le.Uint32(r[cdatAuthor:]))
	a := b.sec[secAuth][ai*16 : ai*16+16]
	c.AuthorNodeID = codec.UUID{MSB: int64(binary.BigEndian.Uint64(a[:8])), LSB: int64(binary.BigEndian.Uint64(a[8:]))}
	if si := int(le.Uint16(r[cdatSchema:])); si > 0 {
		var sh codec.Hash
		copy(sh.Bytes[:], b.sec[secSchm][(si-1)*32:si*32])
		c.SchemaHash = &sh
	}
	b.forEachParent(pos, func(p codec.Hash) bool {
		c.ParentHashes = append(c.ParentHashes, p)
		return true
	})
	src := graphSource{msg: b.message(pos)}
	if len(c.ParentHashes) > 2 {
		src.extra = c.ParentHashes[2:]
	}
	encodeRecord(src.rec[:], c, b.generation(pos), 0)
	le.PutUint32(src.rec[grMsgLen:], uint32(len(src.msg)))
	setRecHasOps(src.rec[:], b.hasOps(pos))
	return src
}

// find returns h's position, by fanout then search. Commit hashes are SHA-256, so within a fanout
// bucket they are uniformly spread and interpolation lands within a few records of the target: a
// handful of probes instead of the ~20 a binary search makes over a million records - each one a
// page an ancestry walk would otherwise fault in. Falls back to bisection when an estimate stops
// narrowing the range.
func (b *graphBase) find(h codec.Hash) (int, bool) {
	if b == nil || b.count == 0 {
		return 0, false
	}
	oids := b.sec[secOIDS]
	first := h.Bytes[0]
	lo := 0
	if first > 0 {
		lo = int(b.fanout[first-1])
	}
	hi := int(b.fanout[first])
	key := binary.BigEndian.Uint64(h.Bytes[1:9])
	loKey, hiKey := uint64(0), ^uint64(0)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if hi-lo > 8 && hiKey > loKey && key >= loKey && key <= hiKey {
			frac := float64(key-loKey) / float64(hiKey-loKey)
			est := lo + int(frac*float64(hi-lo-1))
			if est >= lo && est < hi {
				mid = est
			}
		}
		r := oids[mid*32 : mid*32+32]
		c := bytes.Compare(r, h.Bytes[:])
		switch {
		case c == 0:
			return mid, true
		case c < 0:
			lo = mid + 1
			loKey = binary.BigEndian.Uint64(r[1:9])
		default:
			hi = mid
			hiKey = binary.BigEndian.Uint64(r[1:9])
		}
	}
	return 0, false
}

func txKey(tx codec.UUID) [16]byte {
	var k [16]byte
	binary.BigEndian.PutUint64(k[0:], uint64(tx.MSB))
	binary.BigEndian.PutUint64(k[8:], uint64(tx.LSB))
	return k
}

// txIndexAt is the i-th TIDX entry: the position it names and that commit's transaction key.
func (b *graphBase) txIndexAt(i int) (int, [16]byte) {
	pos := int(le.Uint32(b.sec[secTIDX][i*4:]))
	return pos, txKey(b.tx(pos))
}

// findTx returns the commit hash this segment records for transaction tx.
func (b *graphBase) findTx(tx codec.UUID) (codec.Hash, bool) {
	if b == nil || b.txCount == 0 {
		return codec.Hash{}, false
	}
	k := txKey(tx)
	i := sort.Search(b.txCount, func(i int) bool {
		_, key := b.txIndexAt(i)
		return bytes.Compare(key[:], k[:]) >= 0
	})
	if i == b.txCount {
		return codec.Hash{}, false
	}
	pos, key := b.txIndexAt(i)
	if key != k {
		return codec.Hash{}, false
	}
	return b.hashAt(pos), true
}

// externalParentPositions lists the commits with a parent outside this segment.
func (b *graphBase) externalParentPositions() []byte { return b.sec[secXPos] }

// graphSource is one commit to write: its record, message and extra parents.
type graphSource struct {
	rec   [graphRecordSize]byte
	msg   string
	extra []codec.Hash
}

func (s *graphSource) hash() []byte { return s.rec[grHash : grHash+32] }

// graphTx is one transaction-index entry to write.
type graphTx struct {
	key  [16]byte
	hash codec.Hash
}

// writeGraphSegment writes a segment from the stream open returns - records in ascending hash
// order and transaction entries in ascending key order - as segment seq in dir, through a
// temporary file renamed into place, so a reader only ever maps a complete segment. open is
// called twice: once to learn every commit's position, once to write, so the merge is never
// materialized whole. Returns the new segment's path.
func writeGraphSegment(dir, namespaceID string, seq uint64, full bool, open func() (func() (graphSource, bool), func() (graphTx, bool))) (path string, err error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	// Pass 1: positions. 32 bytes a commit, for the length of the write.
	var oids []codec.Hash
	{
		next, _ := open()
		for {
			src, ok := next()
			if !ok {
				break
			}
			var h codec.Hash
			copy(h.Bytes[:], src.hash())
			if n := len(oids); n > 0 && bytes.Compare(oids[n-1].Bytes[:], h.Bytes[:]) >= 0 {
				return "", fmt.Errorf("dag: graph segment records out of order at %s", h.Hex())
			}
			oids = append(oids, h)
		}
	}
	posOf := func(h codec.Hash) (int, bool) {
		i := sort.Search(len(oids), func(i int) bool { return bytes.Compare(oids[i].Bytes[:], h.Bytes[:]) >= 0 })
		return i, i < len(oids) && oids[i] == h
	}

	path = filepath.Join(dir, graphSegmentName(seq))
	tmp, err := os.CreateTemp(dir, graphSegmentName(seq)+".tmp*")
	if err != nil {
		return "", err
	}
	defer func() {
		if err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
		}
	}()
	crc := crc32.New(crcTable)
	bw := bufio.NewWriterSize(io.MultiWriter(tmp, crc), 1<<20)
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

	begin(secOIDS)
	var fanout [256]uint32
	for _, h := range oids {
		write(h.Bytes[:])
		fanout[h.Bytes[0]]++
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
	)
	ref := func(h codec.Hash) (uint32, bool) {
		if p, ok := posOf(h); ok {
			return uint32(p), false
		}
		i, ok := extIdx[h]
		if !ok {
			i = uint32(len(ext) / 32)
			extIdx[h] = i
			ext = append(ext, h.Bytes[:]...)
		}
		return refExternal | i, true
	}
	begin(secCDAT)
	next, nextTx := open()
	var row [cdatSize]byte
	for pos := 0; ; pos++ {
		src, ok := next()
		if !ok {
			break
		}
		if pos >= len(oids) || !bytes.Equal(src.hash(), oids[pos].Bytes[:]) {
			return "", errors.New("dag: graph segment source changed between passes")
		}
		r := src.rec[:]
		row = [cdatSize]byte{}
		copy(row[cdatTree:], r[grTree:grTree+32])
		copy(row[cdatTxMSB:], r[grTxMSB:grTxMSB+16])
		le.PutUint32(row[cdatGen:], recGeneration(r))
		le.PutUint64(row[cdatEpochMs:], le.Uint64(r[grEpochMs:]))
		le.PutUint16(row[cdatMicro:], uint16(le.Uint64(r[grMicro:])))
		author := codec.UUID{MSB: int64(le.Uint64(r[grAuthMSB:])), LSB: int64(le.Uint64(r[grAuthLSB:]))}
		ai, ok := authorIdx[author]
		if !ok {
			ai = uint32(len(authors) / 16)
			authorIdx[author] = ai
			var a [16]byte
			binary.BigEndian.PutUint64(a[:8], uint64(author.MSB))
			binary.BigEndian.PutUint64(a[8:], uint64(author.LSB))
			authors = append(authors, a[:]...)
		}
		le.PutUint32(row[cdatAuthor:], ai)
		if le.Uint32(r[grFlags:])&grFlagSchema != 0 {
			var sh codec.Hash
			copy(sh.Bytes[:], r[grSchema:grSchema+32])
			si, ok := schemaIdx[sh]
			if !ok {
				si = uint16(len(schemas)/32) + 1
				schemaIdx[sh] = si
				schemas = append(schemas, sh.Bytes[:]...)
			}
			le.PutUint16(row[cdatSchema:], si)
		}
		if recHasOps(r) {
			le.PutUint16(row[cdatFlags:], cdatFlagHasOps)
		}
		p0, p1, n := recParents01(r)
		le.PutUint16(row[cdatNParents:], uint16(n))
		le.PutUint32(row[cdatParent0:], refNone)
		le.PutUint32(row[cdatParent1:], refNone)
		external := false
		if n > 0 {
			rf, x := ref(p0)
			external = external || x
			le.PutUint32(row[cdatParent0:], rf)
		}
		switch {
		case n == 2:
			rf, x := ref(p1)
			external = external || x
			le.PutUint32(row[cdatParent1:], rf)
		case n > 2:
			le.PutUint32(row[cdatParent1:], uint32(len(edges)/4))
			for _, p := range append([]codec.Hash{p1}, src.extra...) {
				rf, x := ref(p)
				external = external || x
				edges = le.AppendUint32(edges, rf)
			}
		}
		if external {
			xpos = le.AppendUint32(xpos, uint32(pos))
		}
		if src.msg != "" {
			msgi = le.AppendUint32(msgi, uint32(pos))
			msgi = le.AppendUint32(msgi, uint32(msgs.Len()))
			msgi = le.AppendUint32(msgi, uint32(len(src.msg)))
			msgs.WriteString(src.msg)
		}
		write(row[:])
	}
	end(secCDAT)

	begin(secTIDX)
	txCount := 0
	var prevKey [16]byte
	for {
		t, ok := nextTx()
		if !ok {
			break
		}
		if txCount > 0 && bytes.Compare(prevKey[:], t.key[:]) >= 0 {
			continue // first writer wins, matching txIndex
		}
		pos, ok := posOf(t.hash)
		if !ok {
			continue // its commit is not in this segment
		}
		prevKey = t.key
		var e [4]byte
		le.PutUint32(e[:], uint32(pos))
		write(e[:])
		txCount++
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
	}{{secAuth, authors}, {secSchm, schemas}, {secExt, ext}, {secEdge, edges}, {secXPos, xpos}, {secMsgI, msgi}, {secMsgs, msgs.Bytes()}} {
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
	le.PutUint64(foot[secCount*20:], uint64(len(oids)))
	le.PutUint64(foot[secCount*20+8:], uint64(txCount))
	write(foot[:graphFooterSize-8])
	if err != nil {
		return "", err
	}
	if err = bw.Flush(); err != nil {
		return "", err
	}
	le.PutUint32(foot[graphFooterSize-8:], crc.Sum32())
	le.PutUint32(foot[graphFooterSize-4:], graphFooterMagic)
	if _, err = tmp.Write(foot[graphFooterSize-8:]); err != nil {
		return "", err
	}
	if err = tmp.Sync(); err != nil {
		return "", err
	}
	if err = tmp.Close(); err != nil {
		return "", err
	}
	if err = os.Rename(tmp.Name(), path); err != nil {
		return "", err
	}
	syncGraphDir(dir)
	return path, nil
}

func syncGraphDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}
