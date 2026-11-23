package dag

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
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
// does it (graph_attach.go has the policy):
//
//   - a flush writes the resident tier, and only it, as a new small segment - O(new commits) -
//     when the tier reaches its size bound;
//   - a merge rewrites segments of similar size as one, size-tiered, dropping records for commits
//     that have since been deleted;
//   - a full rewrite merges every segment and the resident tier into one segment marked full,
//     which supersedes every segment with a lower sequence number - the major compaction.
//
// Segments never share a commit: a flush writes only commits no segment holds, and a merge's
// output takes its final name only once no input is left on disk (graph_attach.go, mergeOnce). The
// one window where two could - a crash after a full rewrite's rename and before the old segments
// are deleted - is closed at the next open, which ignores (and, when writable, deletes) anything a
// full segment supersedes.
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
//	ATTR    n x 36           (pos u32, root 32) for commits with an attachment root, sorted by pos
//	                         (version 5; a version 4 segment has no such section)
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
// full-flush primitive (graph_sync_*.go). It does not need to - a segment torn by a crash fails its
// checksum and is rebuilt - and a full flush every few seconds would stall every neighbouring
// writer on macOS. On Linux it syncs every megabyte as it writes (Cassandra's trickle_fsync), so
// no single sync queues a whole segment ahead of the commit log's.

const (
	graphMagic   = "KDBGRAF4"
	graphVersion = 5
	// graphVersionNoAttach is the layout before ATTR, read as a segment with no attachment roots.
	graphVersionNoAttach = 4
	graphFooterMagic     = 0x4B474634 // "KGF4"
	graphHeaderFixed     = 8 + 4 + 4 + 8 + 4

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
	secAttr
	secCount
)

const graphFooterSize = secCount*20 + 8 + 8 + 4 + 4

// footerSizeFor is the footer of a segment of version v, which has one section fewer before ATTR.
func footerSizeFor(v uint32) (size, sections int) {
	if v == graphVersionNoAttach {
		return (secCount-1)*20 + 8 + 8 + 4 + 4, secCount - 1
	}
	return graphFooterSize, secCount
}

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
	// bloom answers "certainly not here" for commit hashes and transaction ids (graph_bloom.go).
	bloom graphBloom

	// diskPath is where the file is now: path, or - for a merge output whose inputs a checkpoint
	// still names - path+graphPendingSuffix until they are gone (see graph_attach.go). waitsOn is
	// the sequence numbers of those inputs (and of any they waited on), which are still on disk
	// and share records with this one.
	diskPath string
	waitsOn  []uint64
	// hiddenRecs is how many of this segment's records the DAG no longer holds (nodeStore.hidden):
	// what decides a tombstone rewrite. Guarded by the DAG's mu, like the overlay it counts.
	hiddenRecs int
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
	b := &graphBase{path: path, diskPath: path, mapping: m, region: r, fileBytes: int64(m.Size())}
	if err := b.parse(namespaceID); err != nil {
		b.close()
		return nil, fmt.Errorf("%w: %s: %v", errGraphFile, path, err)
	}
	return b, nil
}

func (b *graphBase) parse(namespaceID string) error {
	data := b.region.Bytes()
	if len(data) < graphHeaderFixed+graphFooterSize-20 {
		return errors.New("too short")
	}
	version := le.Uint32(data[8:])
	if version != graphVersion && version != graphVersionNoAttach {
		return fmt.Errorf("version %d, this build reads %d and %d", version, graphVersionNoAttach, graphVersion)
	}
	footerSize, sections := footerSizeFor(version)
	if len(data) < graphHeaderFixed+footerSize {
		return errors.New("too short")
	}
	f := data[len(data)-footerSize:]
	if le.Uint32(f[footerSize-4:]) != graphFooterMagic {
		return errors.New("bad footer magic")
	}
	want := le.Uint32(f[footerSize-8:])
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
	body := len(data) - footerSize
	for i := 0; i < sections; i++ {
		off, n := int(le.Uint64(f[i*20:])), int(le.Uint64(f[i*20+8:]))
		if off < 0 || n < 0 || off > body || n > body-off {
			return fmt.Errorf("section %d overruns file", i)
		}
		if c := le.Uint32(f[i*20+16:]); c != sectionCodecPlain {
			return fmt.Errorf("section %d uses codec %d, which this build cannot read", i, c)
		}
		b.sec[i] = data[off : off+n]
	}
	b.count = int(le.Uint64(f[sections*20:]))
	b.txCount = int(le.Uint64(f[sections*20+8:]))
	switch {
	case len(b.sec[secOIDS]) != b.count*32, len(b.sec[secCDAT]) != b.count*cdatSize,
		len(b.sec[secTIDX]) != b.txCount*4, len(b.sec[secFanout]) != 256*4,
		len(b.sec[secAuth])%16 != 0, len(b.sec[secSchm])%32 != 0, len(b.sec[secExt])%32 != 0,
		len(b.sec[secEdge])%4 != 0, len(b.sec[secXPos])%4 != 0, len(b.sec[secMsgI])%12 != 0,
		len(b.sec[secAttr])%36 != 0:
		return errors.New("section sizes disagree with the counts")
	}
	for i := range b.fanout {
		b.fanout[i] = le.Uint32(b.sec[secFanout][i*4:])
	}
	if int(b.fanout[255]) != b.count {
		return errors.New("fanout disagrees with record count")
	}
	b.buildBloom()
	return nil
}

// buildBloom fills the filter from the segment's own commit hashes and every record's
// transaction id (a superset of the transaction index, which holds only the ids a record owns).
// Sequential over OIDS and CDAT, just after the checksum pass read the same pages.
func (b *graphBase) buildBloom() {
	b.bloom = newGraphBloom(2 * b.count)
	oids, cdat := b.sec[secOIDS], b.sec[secCDAT]
	for pos := 0; pos < b.count; pos++ {
		b.bloom.add(bloomKeyHashBytes(oids[pos*32:]))
		r := cdat[pos*cdatSize:]
		b.bloom.add(bloomKeyTx(codec.UUID{MSB: int64(le.Uint64(r[cdatTxMSB:])), LSB: int64(le.Uint64(r[cdatTxLSB:]))}))
	}
}

// mayHold reports whether this segment can have a record for h; false is certain.
func (b *graphBase) mayHold(h *codec.Hash) bool { return b.bloom.has(bloomKeyHash(h)) }

// mayHoldTx reports whether a record here can have transaction id tx; false is certain.
func (b *graphBase) mayHoldTx(tx codec.UUID) bool { return b.bloom.has(bloomKeyTx(tx)) }

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
	b.forEachParentRef(pos, func(ref uint32) bool { return fn(b.resolveRef(ref)) })
}

// forEachParentRef calls fn with each parent reference of the commit at pos - a position here, or
// refExternal and an index into EXT - in order, until fn returns false.
func (b *graphBase) forEachParentRef(pos int, fn func(uint32) bool) {
	r := b.cdat(pos)
	n := int(le.Uint16(r[cdatNParents:]))
	if n == 0 {
		return
	}
	if !fn(le.Uint32(r[cdatParent0:])) || n == 1 {
		return
	}
	if n == 2 {
		fn(le.Uint32(r[cdatParent1:]))
		return
	}
	edge := int(le.Uint32(r[cdatParent1:]))
	for i := 0; i < n-1; i++ {
		if !fn(le.Uint32(b.sec[secEdge][(edge+i)*4:])) {
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

// attachRoot is the attachment root of the commit at pos, nil when it has none.
func (b *graphBase) attachRoot(pos int) *codec.Hash {
	at := b.sec[secAttr]
	n := len(at) / 36
	i := sort.Search(n, func(i int) bool { return int(le.Uint32(at[i*36:])) >= pos })
	if i == n || int(le.Uint32(at[i*36:])) != pos {
		return nil
	}
	var h codec.Hash
	copy(h.Bytes[:], at[i*36+4:i*36+36])
	return &h
}

// commit rebuilds the document.Commit at pos. Everything returned is a copy.
func (b *graphBase) commit(pos int, ops []document.Op) document.Commit {
	src := b.toSource(pos)
	return decodeRecord(src.rec[:], b.ns, src.msg, src.extra, ops, src.attach)
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
	src := graphSource{msg: b.message(pos), attach: b.attachRoot(pos)}
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
	rec    [graphRecordSize]byte
	msg    string
	extra  []codec.Hash
	attach *codec.Hash // the commit's attachment root, if it has one
}

func (s *graphSource) hash() []byte { return s.rec[grHash : grHash+32] }

func syncGraphDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		syncGraphFile(d) // plainly, as the files themselves - see graph_sync_darwin.go
		d.Close()
	}
}
