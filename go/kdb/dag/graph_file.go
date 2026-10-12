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

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage/mmapfile"
)

// The on-disk commit graph (docs/kdb-commit-graph-on-disk.md, docs/kdb-mmap-plan.md).
//
// A namespace's commits, sorted by hash, as fixed-width records in a file that is mapped rather
// than read, so a million-commit history costs page cache the kernel can evict instead of heap
// the collector has to trace. The file is a cache and never an authority: it is derived from the
// DAG, checked end to end on mapping, and anything wrong with it - absent, torn, a different
// version or namespace - means the DAG keeps holding those commits itself, exactly as it did
// before the file existed. Nothing is reachable only through it (see nodeStore.attach, which
// hides every file record the DAG does not also hold).
//
// Layout, little-endian:
//
//	header   magic "KDBGRAF1" | version u32 | nsLen u32 | ns bytes
//	records  count x graphRecordSize, sorted by hash          (graphRecord below)
//	tidx     txCount x graphTxEntrySize, sorted by tx          (tx MSB u64 BE, LSB u64 BE, hash)
//	fanout   256 x u32: fanout[b] = records whose hash[0] <= b
//	msgs     concatenated commit messages
//	extra    concatenated parent hashes beyond each commit's second
//	footer   recordsOff, count, tidxOff, txCount, fanoutOff, msgsOff, msgsLen, extraOff, extraLen
//	         (u64 each) | crc32c of every byte before it | magic u32
//
// Counts live in the footer rather than the header because the writer streams: it does not know
// how many records survive the merge, or how long the message blob is, until it has written them.
//
// Durability: the writer syncs the file and its directory, but plainly, not with the store's
// full-flush primitive. It does not need to - a file torn by a crash fails its checksum and is
// rebuilt - and a full flush of a file this size would stall every neighbouring writer on macOS
// (see the fsync device-contention notes).

const (
	graphMagic       = "KDBGRAF1"
	graphVersion     = 1
	graphFooterMagic = 0x4B474631 // "KGF1"
	graphRecordSize  = 240
	graphTxEntrySize = 48
	graphFooterSize  = 9*8 + 4 + 4
)

var errGraphFile = errors.New("dag: graph file unusable")

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// graphRecord offsets.
const (
	grHash     = 0
	grParent0  = 32
	grParent1  = 64
	grTree     = 96
	grSchema   = 128
	grTxMSB    = 160
	grTxLSB    = 168
	grAuthMSB  = 176
	grAuthLSB  = 184
	grEpochMs  = 192
	grMicro    = 200
	grGen      = 208
	grNParents = 212
	grFlags    = 216
	grMsgLen   = 220
	grMsgOff   = 224
	grExtraOff = 232

	grFlagSchema = 1
)

// graphBase is a mapped graph file.
type graphBase struct {
	path      string
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

func (b *graphBase) parse(namespaceID string) (err error) {
	data := b.region.Bytes()
	if len(data) < len(graphMagic)+8+graphFooterSize {
		return errors.New("too short")
	}
	if string(data[:8]) != graphMagic {
		return errors.New("bad magic")
	}
	if v := binary.LittleEndian.Uint32(data[8:]); v != graphVersion {
		return fmt.Errorf("version %d, this build reads %d", v, graphVersion)
	}
	nsLen := int(binary.LittleEndian.Uint32(data[12:]))
	if 16+nsLen > len(data) {
		return errors.New("namespace overruns file")
	}
	b.ns = string(data[16 : 16+nsLen])
	if b.ns != namespaceID {
		return fmt.Errorf("written for namespace %q", b.ns)
	}
	f := data[len(data)-graphFooterSize:]
	if binary.LittleEndian.Uint32(f[graphFooterSize-4:]) != graphFooterMagic {
		return errors.New("bad footer magic")
	}
	want := binary.LittleEndian.Uint32(f[graphFooterSize-8:])
	// The one pass that touches every page runs under a fault guard: a file that shrank
	// underneath the mapping is an unusable file, not a crash.
	var got uint32
	if gerr := b.region.Guard(func(all []byte) { got = crc32.Checksum(all[:len(all)-8], crcTable) }); gerr != nil {
		return gerr
	}
	if got != want {
		return fmt.Errorf("checksum mismatch (want %08x, got %08x)", want, got)
	}
	u := func(i int) int { return int(binary.LittleEndian.Uint64(f[i*8:])) }
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
		b.fanout[i] = binary.LittleEndian.Uint32(data[fanOff+i*4:])
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

func (b *graphBase) hashAt(pos int) codec.Hash {
	var h codec.Hash
	copy(h.Bytes[:], b.rec(pos)[grHash:grHash+32])
	return h
}

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

// findTx returns the commit hash the file records for transaction tx.
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

func (b *graphBase) generation(pos int) uint32 {
	return binary.LittleEndian.Uint32(b.rec(pos)[grGen:])
}

func (b *graphBase) treeHash(pos int) codec.Hash {
	var h codec.Hash
	copy(h.Bytes[:], b.rec(pos)[grTree:grTree+32])
	return h
}

func (b *graphBase) txAt(pos int) codec.UUID {
	r := b.rec(pos)
	return codec.UUID{MSB: int64(binary.LittleEndian.Uint64(r[grTxMSB:])), LSB: int64(binary.LittleEndian.Uint64(r[grTxLSB:]))}
}

// slotAt decodes a record into the same fixed-width form the resident tier holds, plus its
// message and any parents beyond the second. Everything returned is a copy: nothing may keep a
// reference into the mapping, which is unmapped when the file is replaced.
func (b *graphBase) slotAt(pos int) (slot, string, []codec.Hash) {
	r := b.rec(pos)
	var s slot
	copy(s.hash.Bytes[:], r[grHash:grHash+32])
	copy(s.parent0.Bytes[:], r[grParent0:grParent0+32])
	copy(s.parent1.Bytes[:], r[grParent1:grParent1+32])
	copy(s.tree.Bytes[:], r[grTree:grTree+32])
	copy(s.schema.Bytes[:], r[grSchema:grSchema+32])
	le := binary.LittleEndian
	s.tx = codec.UUID{MSB: int64(le.Uint64(r[grTxMSB:])), LSB: int64(le.Uint64(r[grTxLSB:]))}
	s.author = codec.UUID{MSB: int64(le.Uint64(r[grAuthMSB:])), LSB: int64(le.Uint64(r[grAuthLSB:]))}
	s.epochMs = int64(le.Uint64(r[grEpochMs:]))
	s.micro = int64(le.Uint64(r[grMicro:]))
	s.generation = le.Uint32(r[grGen:])
	s.nParents = le.Uint32(r[grNParents:])
	s.hasSchema = le.Uint32(r[grFlags:])&grFlagSchema != 0
	s.live = true
	var msg string
	if n := le.Uint32(r[grMsgLen:]); n > 0 {
		off := le.Uint64(r[grMsgOff:])
		msg = string(b.msgs[off : off+uint64(n)])
	}
	var extra []codec.Hash
	if s.nParents > 2 {
		off := le.Uint64(r[grExtraOff:])
		extra = make([]codec.Hash, s.nParents-2)
		for i := range extra {
			copy(extra[i].Bytes[:], b.extra[off+uint64(i)*32:])
		}
	}
	return s, msg, extra
}

// graphSource is one commit to write: its fixed-width slot, message and extra parents.
type graphSource struct {
	s     slot
	msg   string
	extra []codec.Hash
}

// graphTx is one transaction-index entry to write.
type graphTx struct {
	key  [16]byte
	hash codec.Hash
}

// writeGraphFile writes the records next() yields (which must arrive in ascending hash order)
// and the transaction entries nextTx() yields (ascending key order) to path, through a temporary
// file renamed into place, so a reader only ever maps a complete file.
func writeGraphFile(path, namespaceID string, next func() (graphSource, bool), nextTx func() (graphTx, bool)) (err error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp*")
	if err != nil {
		return err
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
	le := binary.LittleEndian

	var hdr [16]byte
	copy(hdr[:8], graphMagic)
	le.PutUint32(hdr[8:], graphVersion)
	le.PutUint32(hdr[12:], uint32(len(namespaceID)))
	write(hdr[:])
	write([]byte(namespaceID))
	// Align the records so a record never straddles more pages than it must.
	if pad := (8 - off%8) % 8; pad > 0 {
		write(make([]byte, pad))
	}

	recOff := off
	var fanout [256]uint32
	var msgs, extra bytes.Buffer
	var rec [graphRecordSize]byte
	count := 0
	var prev codec.Hash
	for {
		src, ok := next()
		if !ok {
			break
		}
		s := src.s
		if count > 0 && bytes.Compare(prev.Bytes[:], s.hash.Bytes[:]) >= 0 {
			return fmt.Errorf("dag: graph file records out of order at %s", s.hash.Hex())
		}
		prev = s.hash
		rec = [graphRecordSize]byte{}
		copy(rec[grHash:], s.hash.Bytes[:])
		copy(rec[grParent0:], s.parent0.Bytes[:])
		copy(rec[grParent1:], s.parent1.Bytes[:])
		copy(rec[grTree:], s.tree.Bytes[:])
		copy(rec[grSchema:], s.schema.Bytes[:])
		le.PutUint64(rec[grTxMSB:], uint64(s.tx.MSB))
		le.PutUint64(rec[grTxLSB:], uint64(s.tx.LSB))
		le.PutUint64(rec[grAuthMSB:], uint64(s.author.MSB))
		le.PutUint64(rec[grAuthLSB:], uint64(s.author.LSB))
		le.PutUint64(rec[grEpochMs:], uint64(s.epochMs))
		le.PutUint64(rec[grMicro:], uint64(s.micro))
		le.PutUint32(rec[grGen:], s.generation)
		le.PutUint32(rec[grNParents:], s.nParents)
		if s.hasSchema {
			le.PutUint32(rec[grFlags:], grFlagSchema)
		}
		if src.msg != "" {
			le.PutUint32(rec[grMsgLen:], uint32(len(src.msg)))
			le.PutUint64(rec[grMsgOff:], uint64(msgs.Len()))
			msgs.WriteString(src.msg)
		}
		if len(src.extra) > 0 {
			le.PutUint64(rec[grExtraOff:], uint64(extra.Len()))
			for _, p := range src.extra {
				extra.Write(p.Bytes[:])
			}
		}
		write(rec[:])
		fanout[s.hash.Bytes[0]]++
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
		return err
	}
	if err = bw.Flush(); err != nil {
		return err
	}
	le.PutUint32(foot[graphFooterSize-8:], crc.Sum32())
	le.PutUint32(foot[graphFooterSize-4:], graphFooterMagic)
	if _, err = tmp.Write(foot[graphFooterSize-8:]); err != nil {
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
	if d, derr := os.Open(filepath.Dir(path)); derr == nil {
		d.Sync()
		d.Close()
	}
	return nil
}
