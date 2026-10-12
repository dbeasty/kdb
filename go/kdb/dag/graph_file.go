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
	"github.com/limidus/kdb/go/kdb/storage/mmapfile"
)

// The on-disk commit graph (docs/kdb-commit-graph-on-disk.md, docs/kdb-mmap-plan.md).
//
// A namespace's graph on disk is a set of segment files in one directory, each a sorted run of
// commit records (graph_record.go - the same bytes the resident tier holds), mapped rather than
// read, so a million-commit history costs page cache the kernel can evict instead of heap the
// collector has to trace. The scheme is an LSM's, the way Cassandra does it:
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
// mapping, and anything wrong with it means the DAG keeps holding those commits itself. Nothing
// is reachable only through a segment (see nodeStore.attach, which hides every record the DAG
// does not also hold).
//
// Segment layout, little-endian:
//
//	header   magic "KDBGRAF2" | version u32 | flags u32 | seq u64 | nsLen u32 | ns | pad to 8
//	records  count x graphRecordSize, sorted by hash
//	tidx     txCount x graphTxEntrySize, sorted by tx    (tx MSB u64 BE, LSB u64 BE, hash)
//	fanout   256 x u32: fanout[b] = records whose hash[0] <= b
//	msgs     concatenated commit messages
//	extra    concatenated parent hashes beyond each commit's second
//	footer   recordsOff, count, tidxOff, txCount, fanoutOff, msgsOff, msgsLen, extraOff, extraLen
//	         (u64 each) | crc32c of every byte before it | magic u32
//
// Counts live in the footer because the writer streams: it does not know how many records
// survive a merge, or how long the message blob is, until it has written them.
//
// Durability: the writer syncs the file and its directory, but plainly, not with the store's
// full-flush primitive. It does not need to - a segment torn by a crash fails its checksum and is
// rebuilt - and a full flush of a file this size would stall every neighbouring writer on macOS.

const (
	graphMagic       = "KDBGRAF2"
	graphVersion     = 2
	graphFooterMagic = 0x4B474632 // "KGF2"
	graphTxEntrySize = 48
	graphFooterSize  = 9*8 + 4 + 4
	graphHeaderFixed = 8 + 4 + 4 + 8 + 4

	graphFlagFull = 1 // this segment supersedes every segment with a lower sequence number

	graphSegmentPrefix = "graph-"
	graphSegmentSuffix = ".kdbg"
)

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
	records   []byte
	count     int
	tidx      []byte
	txCount   int
	fanout    [256]uint32
	msgs      []byte
	extra     []byte
	fileBytes int64
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
	u := func(i int) int { return int(le.Uint64(f[i*8:])) }
	recOff, count, tidxOff, txCount := u(0), u(1), u(2), u(3)
	fanOff, msgsOff, msgsLen, extraOff, extraLen := u(4), u(5), u(6), u(7), u(8)
	body := len(data) - graphFooterSize
	in := func(off, n int) bool { return off >= 0 && n >= 0 && off <= body && n <= body-off }
	if !in(recOff, count*graphRecordSize) || !in(tidxOff, txCount*graphTxEntrySize) ||
		!in(fanOff, 256*4) || !in(msgsOff, msgsLen) || !in(extraOff, extraLen) {
		return errors.New("section overruns file")
	}
	b.records = data[recOff : recOff+count*graphRecordSize]
	b.count = count
	b.tidx = data[tidxOff : tidxOff+txCount*graphTxEntrySize]
	b.txCount = txCount
	for i := range b.fanout {
		b.fanout[i] = le.Uint32(data[fanOff+i*4:])
	}
	if int(b.fanout[255]) != count {
		return errors.New("fanout disagrees with record count")
	}
	b.msgs = data[msgsOff : msgsOff+msgsLen]
	b.extra = data[extraOff : extraOff+extraLen]
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

func (b *graphBase) rec(pos int) []byte {
	return b.records[pos*graphRecordSize : (pos+1)*graphRecordSize]
}

func (b *graphBase) hashAt(pos int) codec.Hash { return recHash(b.rec(pos)) }

// find returns h's position, by fanout then binary search.
func (b *graphBase) find(h codec.Hash) (int, bool) {
	if b == nil || b.count == 0 {
		return 0, false
	}
	first := h.Bytes[0]
	lo := 0
	if first > 0 {
		lo = int(b.fanout[first-1])
	}
	hi := int(b.fanout[first])
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		c := bytes.Compare(b.rec(mid)[grHash:grHash+32], h.Bytes[:])
		switch {
		case c == 0:
			return mid, true
		case c < 0:
			lo = mid + 1
		default:
			hi = mid
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

// findTx returns the commit hash this segment records for transaction tx.
func (b *graphBase) findTx(tx codec.UUID) (codec.Hash, bool) {
	if b == nil || b.txCount == 0 {
		return codec.Hash{}, false
	}
	k := txKey(tx)
	lo, hi := 0, b.txCount
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		e := b.tidx[mid*graphTxEntrySize : (mid+1)*graphTxEntrySize]
		c := bytes.Compare(e[:16], k[:])
		switch {
		case c == 0:
			var h codec.Hash
			copy(h.Bytes[:], e[16:48])
			return h, true
		case c < 0:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return codec.Hash{}, false
}

// messageAt and extraAt read a record's variable-length data. Both copy.
func (b *graphBase) messageAt(pos int) string {
	r := b.rec(pos)
	n := le.Uint32(r[grMsgLen:])
	if n == 0 {
		return ""
	}
	off := le.Uint64(r[grMsgOff:])
	return string(b.msgs[off : off+uint64(n)])
}

func (b *graphBase) extraAt(pos int) []codec.Hash {
	r := b.rec(pos)
	n := recNParents(r)
	if n <= 2 {
		return nil
	}
	off := le.Uint64(r[grExtraOff:])
	out := make([]codec.Hash, n-2)
	for i := range out {
		copy(out[i].Bytes[:], b.extra[off+uint64(i)*32:])
	}
	return out
}

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

// writeGraphSegment writes the records next() yields (ascending hash order) and the transaction
// entries nextTx() yields (ascending key order) as segment seq in dir, through a temporary file
// renamed into place, so a reader only ever maps a complete segment. Returns its path.
func writeGraphSegment(dir, namespaceID string, seq uint64, full bool, next func() (graphSource, bool), nextTx func() (graphTx, bool)) (path string, err error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
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

	recOff := off
	var fanout [256]uint32
	var msgs, extra bytes.Buffer
	count := 0
	var prev [32]byte
	for {
		src, ok := next()
		if !ok {
			break
		}
		h := src.hash()
		if count > 0 && bytes.Compare(prev[:], h) >= 0 {
			return "", fmt.Errorf("dag: graph segment records out of order at %x", h)
		}
		copy(prev[:], h)
		r := src.rec // a copy: the writer owns the blob offsets and clears the memory-only bytes
		clear(r[grNsIdx:])
		le.PutUint64(r[grMsgOff:], 0)
		le.PutUint64(r[grExtraOff:], 0)
		le.PutUint32(r[grMsgLen:], uint32(len(src.msg)))
		if src.msg != "" {
			le.PutUint64(r[grMsgOff:], uint64(msgs.Len()))
			msgs.WriteString(src.msg)
		}
		if len(src.extra) > 0 {
			le.PutUint64(r[grExtraOff:], uint64(extra.Len()))
			for _, p := range src.extra {
				extra.Write(p.Bytes[:])
			}
		}
		write(r[:])
		fanout[h[0]]++
		count++
	}
	for i := 1; i < 256; i++ {
		fanout[i] += fanout[i-1]
	}

	tidxOff := off
	txCount := 0
	var prevKey [16]byte
	var e [graphTxEntrySize]byte
	for {
		t, ok := nextTx()
		if !ok {
			break
		}
		if txCount > 0 && bytes.Compare(prevKey[:], t.key[:]) >= 0 {
			continue // first writer wins, matching txIndex
		}
		prevKey = t.key
		copy(e[:16], t.key[:])
		copy(e[16:], t.hash.Bytes[:])
		write(e[:])
		txCount++
	}

	fanOff := off
	var fb [4]byte
	for _, v := range fanout {
		le.PutUint32(fb[:], v)
		write(fb[:])
	}
	msgsOff := off
	write(msgs.Bytes())
	extraOff := off
	write(extra.Bytes())

	var foot [graphFooterSize]byte
	for i, v := range []int64{recOff, int64(count), tidxOff, int64(txCount), fanOff, msgsOff, int64(msgs.Len()), extraOff, int64(extra.Len())} {
		le.PutUint64(foot[i*8:], uint64(v))
	}
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
