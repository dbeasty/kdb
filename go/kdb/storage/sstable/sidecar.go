package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"math/bits"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/mmapfile"
)

// The index sidecar: a second, Go-only file per SSTable holding the table's index in a form that
// is searched in place rather than parsed into the heap. It is KDB's analogue of Cassandra's
// off-heap Bloom filter and index summary (and of the BTI trie index that replaced them):
//
//   - a sorted, fixed-width array of (key, block handle), searched by binary search through a
//     memory mapping, with a fanout table on the leading key bits to narrow the search, and
//   - a blocked Bloom filter (bloom.go), so a probe for a key the table does not hold usually
//     costs one cache line and no search at all.
//
// The footer index this replaces is text, parsed on every index-cache miss into a
// map[codec.Hash]BlockHandle of ~140 B per entry, one entry per stored value. At 1M documents and
// 2M versions that map was 280 MB of heap, growing with the data and bounded only softly by
// tableIndexCache. The sidecar holds 48 B per entry on disk and none on the heap; the OS pages it.
//
// A sidecar is derived state, never authoritative, and never read by Kotlin:
//
//   - the SSTable itself is unchanged, byte for byte - Kotlin reads it, and the golden fixtures pin
//     it - and remains the only source of truth;
//   - a sidecar that is missing, damaged, from another format version, or written for a different
//     table is ignored, and the table is served from its footer exactly as before (and, where the
//     store may write, the sidecar is rebuilt from the footer);
//   - it lives outside the table's segment directory (ns/<ns>/sstidx/ rather than
//     ns/<ns>/sstable/), so nothing that lists tables ever mistakes one for a table, and it is
//     written at a local path rather than through the platform shim, so it is never replicated,
//     archived or backed up. Deleting every sidecar costs a rebuild, never data.
//
// Layout, little-endian, version 1:
//
//	 0  magic        8   "KDBSIDX\0"
//	 8  version      u32 (= sidecarVersion)
//	12  fanoutBits   u32 how many leading key bits the fanout table splits on (0..16)
//	16  count        u64 entries
//	24  bloomBlocks  u64 64-byte filter blocks
//	32  tableSize    u64 the SSTable's length when this was written
//	40  reserved     24  (zero)
//	64  fileHash     32  the SSTable footer's file hash
//	96  fanout       4 << fanoutBits: u32 per prefix, the count of entries whose prefix <= it
//	    (pad to 8)
//	    entries      count * 48: key(32) offset(u64) compressedSize(u32) flags(u32, bit0 deleted)
//	    (pad to 64)
//	    bloom        bloomBlocks * 64
//	    trailer      crc32c(u32) over every byte before it, then magic(u32 "SIDX")
//
// tableSize and fileHash tie a sidecar to one table: table names are random UUIDs and never
// reused, so a mismatch means damage or a stray file, and the sidecar is not used.
const (
	sidecarMagic        = "KDBSIDX\x00"
	sidecarVersion      = 1
	sidecarHeaderSize   = 96
	sidecarEntrySize    = 48
	sidecarTrailerSize  = 8
	sidecarTrailerMagic = 0x58444953 // "SIDX"
	sidecarMaxFanout    = 16
	sidecarSuffix       = ".sidx"
	sidecarFlagDeleted  = 1
	sidecarTempMarker   = ".tmp-"
)

var sidecarCRCTable = crc32.MakeTable(crc32.Castagnoli)

// errSidecarVersion marks a sidecar written in a format this build does not read.
var errSidecarVersion = errors.New("sstable: sidecar format version not supported")

// sidecarSegmentName names the sidecar of the SSTable segment table:
// ns/<ns>/sstable/L<n>/<id> -> ns/<ns>/sstidx/L<n>/<id>.sidx. ok is false for a name that is not
// an SSTable's.
func sidecarSegmentName(table string) (string, bool) {
	const kind = "/sstable/"
	i := strings.LastIndex(table, kind)
	if i < 0 || i+len(kind) == len(table) {
		return "", false
	}
	return table[:i] + "/sstidx/" + table[i+len(kind):] + sidecarSuffix, true
}

// sidecarTableName is sidecarSegmentName's inverse.
func sidecarTableName(sidecar string) (string, bool) {
	const kind = "/sstidx/"
	i := strings.LastIndex(sidecar, kind)
	if i < 0 || !strings.HasSuffix(sidecar, sidecarSuffix) {
		return "", false
	}
	rest := strings.TrimSuffix(sidecar[i+len(kind):], sidecarSuffix)
	if rest == "" {
		return "", false
	}
	return sidecar[:i] + "/sstable/" + rest, true
}

// sidecarPath is where table's sidecar lives on the local filesystem, ok=false when the shim keeps
// no local files.
func sidecarPath(paths storage.LocalPathResolver, table string) (string, bool) {
	if paths == nil {
		return "", false
	}
	name, ok := sidecarSegmentName(table)
	if !ok {
		return "", false
	}
	return paths.LocalSegmentPath(name)
}

// sidecarsEnabled reports whether this process writes and reads sidecars. KDB_SSTABLE_SIDECAR=off
// turns them off - every table is then served from its footer, as before sidecars existed - for
// an A/B comparison or as a safety valve; nothing else needs it.
func sidecarsEnabled() bool {
	switch strings.ToLower(os.Getenv("KDB_SSTABLE_SIDECAR")) {
	case "off", "0", "false", "no":
		return false
	}
	return mmapfile.Supported
}

// sidecarResolver returns io's local-path resolver when sidecars can be used with it.
func sidecarResolver(io storage.PlatformIOShim) storage.LocalPathResolver {
	r, ok := io.(storage.LocalPathResolver)
	if !ok || !sidecarsEnabled() {
		return nil
	}
	return r
}

type sidecarLayout struct {
	count, fanoutBits, bloomBlocks                     int
	fanoutOff, entriesOff, bloomOff, trailerOff, total int
}

func align(n, to int) int { return (n + to - 1) / to * to }

func layoutSidecar(count, fanoutBits, bloomBlocks int) sidecarLayout {
	l := sidecarLayout{count: count, fanoutBits: fanoutBits, bloomBlocks: bloomBlocks}
	l.fanoutOff = sidecarHeaderSize
	l.entriesOff = align(l.fanoutOff+4<<fanoutBits, 8)
	l.bloomOff = align(l.entriesOff+count*sidecarEntrySize, bloomBlockBytes)
	l.trailerOff = l.bloomOff + bloomBlocks*bloomBlockBytes
	l.total = l.trailerOff + sidecarTrailerSize
	return l
}

// fanoutBitsFor picks a fanout that leaves roughly 16-32 entries per bucket, so the binary search
// after it is a handful of steps, while costing a small table almost nothing.
func fanoutBitsFor(n int) int {
	b := bits.Len(uint(n)) - 5
	if b < 0 {
		return 0
	}
	if b > sidecarMaxFanout {
		return sidecarMaxFanout
	}
	return b
}

func keyPrefix(key []byte, fanoutBits int) int {
	if fanoutBits == 0 {
		return 0
	}
	return int(binary.BigEndian.Uint16(key) >> (16 - fanoutBits))
}

func compareEntries(a, b indexEntry) int { return bytes.Compare(a.key.Bytes[:], b.key.Bytes[:]) }

// sortIndexEntries sorts entries by key and drops all but the last of any repeated key - the
// footer parser's rule (a later line for the same key overwrites an earlier one).
func sortIndexEntries(entries []indexEntry) []indexEntry {
	slices.SortStableFunc(entries, compareEntries)
	out := entries[:0]
	for i, e := range entries {
		if i+1 < len(entries) && entries[i+1].key == e.key {
			continue
		}
		out = append(out, e)
	}
	return out
}

// writeSidecarFile writes the sidecar for a table of the given size and footer hash, whose index
// is entries (sorted in place). The file is written beside its final name and renamed into place,
// so a reader never maps a half-written one. It is not fsynced: a sidecar torn by a crash fails
// its checksum and is rebuilt, which costs less than a flush on every table write.
func writeSidecarFile(path string, entries []indexEntry, tableSize int64, fileHash codec.Hash) (err error) {
	entries = sortIndexEntries(entries)
	n := len(entries)
	l := layoutSidecar(n, fanoutBitsFor(n), bloomBlocksFor(n))

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+sidecarTempMarker+"*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	_ = f.Chmod(0o644) // CreateTemp makes it 0600; the tables beside it are world-readable
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(tmp)
		}
	}()
	crc := crc32.New(sidecarCRCTable)
	w := bufio.NewWriterSize(f, 1<<20)
	var written int
	put := func(b []byte) {
		w.Write(b) // bufio records the first error; checked by Flush
		crc.Write(b)
		written += len(b)
	}
	pad := func(to int) {
		var zero [bloomBlockBytes]byte
		put(zero[:to-written])
	}

	var hdr [sidecarHeaderSize]byte
	copy(hdr[0:], sidecarMagic)
	binary.LittleEndian.PutUint32(hdr[8:], sidecarVersion)
	binary.LittleEndian.PutUint32(hdr[12:], uint32(l.fanoutBits))
	binary.LittleEndian.PutUint64(hdr[16:], uint64(n))
	binary.LittleEndian.PutUint64(hdr[24:], uint64(l.bloomBlocks))
	binary.LittleEndian.PutUint64(hdr[32:], uint64(tableSize))
	copy(hdr[64:], fileHash.Bytes[:])
	put(hdr[:])

	fanout := make([]byte, 4<<l.fanoutBits)
	buckets := 1 << l.fanoutBits
	i := 0
	for p := 0; p < buckets; p++ {
		for i < n && keyPrefix(entries[i].key.Bytes[:], l.fanoutBits) <= p {
			i++
		}
		binary.LittleEndian.PutUint32(fanout[p*4:], uint32(i))
	}
	put(fanout)
	pad(l.entriesOff)

	bloom := make([]byte, l.bloomBlocks*bloomBlockBytes)
	var rec [sidecarEntrySize]byte
	for k := range entries {
		e := &entries[k]
		if e.handle.Offset < 0 || e.handle.CompressedSize < 0 || int64(e.handle.CompressedSize) > 1<<32-1 {
			return fmt.Errorf("sstable: block handle %+v does not fit a sidecar entry", e.handle)
		}
		copy(rec[0:32], e.key.Bytes[:])
		binary.LittleEndian.PutUint64(rec[32:], uint64(e.handle.Offset))
		binary.LittleEndian.PutUint32(rec[40:], uint32(e.handle.CompressedSize))
		var flags uint32
		if e.handle.Deleted {
			flags |= sidecarFlagDeleted
		}
		binary.LittleEndian.PutUint32(rec[44:], flags)
		put(rec[:])
		bloomAdd(bloom, uint64(l.bloomBlocks), &e.key)
	}
	pad(l.bloomOff)
	put(bloom)

	var trailer [sidecarTrailerSize]byte
	binary.LittleEndian.PutUint32(trailer[0:], crc.Sum32())
	binary.LittleEndian.PutUint32(trailer[4:], sidecarTrailerMagic)
	if _, err := w.Write(trailer[:]); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// sidecar is one opened, verified sidecar. Immutable once open; safe for concurrent readers.
//
// It holds one reference on its mapping for its whole life, so its bytes stay mapped until close.
// close must not run while any reader may still be searching it: LsmBlobStore guarantees that by
// closing a table's sidecar only when the table itself is retired, which happens only once no
// lookup holds the table (see LsmBlobStore.pin).
type sidecar struct {
	mapping     *mmapfile.Mapping
	region      *mmapfile.Region
	count       int
	fanoutBits  int
	fanout      []byte
	entries     []byte
	entriesOff  int // where entries starts in the mapping
	bloom       []byte
	bloomBlocks uint64
	bytes       int64
}

// openSidecar maps path and verifies it end to end, and that it was written for the table of
// length tableSize and footer hash fileHash.
func openSidecar(path string, tableSize int64, fileHash codec.Hash) (*sidecar, error) {
	m, err := mmapfile.Open(path)
	if err != nil {
		return nil, err
	}
	r, err := m.Acquire()
	if err != nil {
		m.Close()
		return nil, err
	}
	sc := &sidecar{mapping: m, region: r, bytes: int64(m.Size())}
	if err := sc.verify(tableSize, fileHash); err != nil {
		sc.close()
		return nil, fmt.Errorf("sstable: sidecar %s: %w", path, err)
	}
	// The checksum pass read every page, and a lookup needs a few of them - a filter block, a
	// fanout entry, a handful of index pages. Leaving them all resident cost the whole file per
	// table opened: a merge opening the sidecars of 49 inputs raised RSS by ~180 MB before
	// reading any of them. Pages a lookup does need fault back in from the page cache.
	_ = r.DropPages(0, m.Size())
	return sc, nil
}

func (sc *sidecar) verify(tableSize int64, fileHash codec.Hash) (err error) {
	// The checksum pass touches every page, so it is where a file truncated under the mapping
	// would fault; everything after it reads only pages it has already proven readable, but runs
	// under the same guard so a fault anywhere is an error, not a crash.
	gerr := sc.region.Guard(func(data []byte) {
		if len(data) < sidecarHeaderSize+sidecarTrailerSize {
			err = errors.New("too short")
			return
		}
		if string(data[:8]) != sidecarMagic {
			err = errors.New("bad magic")
			return
		}
		if v := binary.LittleEndian.Uint32(data[8:]); v != sidecarVersion {
			err = fmt.Errorf("%w: version %d, this build reads %d", errSidecarVersion, v, sidecarVersion)
			return
		}
		t := data[len(data)-sidecarTrailerSize:]
		if binary.LittleEndian.Uint32(t[4:]) != sidecarTrailerMagic {
			err = errors.New("bad trailer magic")
			return
		}
		if got, want := crc32.Checksum(data[:len(data)-sidecarTrailerSize], sidecarCRCTable), binary.LittleEndian.Uint32(t); got != want {
			err = fmt.Errorf("checksum mismatch (want %08x, got %08x)", want, got)
			return
		}
		fanoutBits := binary.LittleEndian.Uint32(data[12:])
		count := binary.LittleEndian.Uint64(data[16:])
		blocks := binary.LittleEndian.Uint64(data[24:])
		if fanoutBits > sidecarMaxFanout || count > uint64(len(data))/sidecarEntrySize || blocks > uint64(len(data))/bloomBlockBytes {
			err = errors.New("header fields out of range")
			return
		}
		l := layoutSidecar(int(count), int(fanoutBits), int(blocks))
		if l.total != len(data) {
			err = fmt.Errorf("length %d, header implies %d", len(data), l.total)
			return
		}
		if got := int64(binary.LittleEndian.Uint64(data[32:])); got != tableSize {
			err = fmt.Errorf("written for a %d-byte table, this one is %d bytes", got, tableSize)
			return
		}
		if !bytes.Equal(data[64:96], fileHash.Bytes[:]) {
			err = errors.New("written for a different table (file hash differs)")
			return
		}
		sc.count, sc.fanoutBits, sc.bloomBlocks = int(count), int(fanoutBits), blocks
		sc.fanout = data[l.fanoutOff : l.fanoutOff+4<<fanoutBits]
		sc.entries = data[l.entriesOff : l.entriesOff+l.count*sidecarEntrySize]
		sc.entriesOff = l.entriesOff
		sc.bloom = data[l.bloomOff:l.trailerOff]
		prev := uint32(0)
		for p := 0; p < 1<<fanoutBits; p++ {
			v := binary.LittleEndian.Uint32(sc.fanout[p*4:])
			if v < prev || uint64(v) > count {
				err = errors.New("fanout table is not monotonic")
				return
			}
			prev = v
		}
		if uint64(prev) != count {
			err = errors.New("fanout table does not cover every entry")
		}
	})
	if gerr != nil {
		return gerr
	}
	return err
}

func (sc *sidecar) close() {
	sc.region.Release()
	sc.mapping.Close()
}

// find reports what the table says about key: its block handle and found=true, or found=false
// when the table does not hold it. err is a fault reading the mapped file (truncated or failing
// underneath), never "not found".
func (sc *sidecar) find(key *codec.Hash) (bh BlockHandle, found bool, err error) {
	err = sc.region.Guard(func([]byte) { bh, found = sc.search(key) })
	return bh, found, err
}

func (sc *sidecar) search(key *codec.Hash) (BlockHandle, bool) {
	if !bloomMayContain(sc.bloom, sc.bloomBlocks, key) {
		return BlockHandle{}, false
	}
	k := key.Bytes[:]
	p := keyPrefix(k, sc.fanoutBits)
	lo := 0
	if p > 0 {
		lo = int(binary.LittleEndian.Uint32(sc.fanout[(p-1)*4:]))
	}
	hi := int(binary.LittleEndian.Uint32(sc.fanout[p*4:]))
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		rec := sc.entries[mid*sidecarEntrySize : mid*sidecarEntrySize+sidecarEntrySize]
		switch c := bytes.Compare(rec[:32], k); {
		case c == 0:
			return decodeSidecarHandle(rec), true
		case c < 0:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return BlockHandle{}, false
}

func decodeSidecarHandle(rec []byte) BlockHandle {
	return BlockHandle{
		Offset:         int64(binary.LittleEndian.Uint64(rec[32:])),
		CompressedSize: int(binary.LittleEndian.Uint32(rec[40:])),
		Deleted:        binary.LittleEndian.Uint32(rec[44:])&sidecarFlagDeleted != 0,
	}
}

// forEach calls fn for every entry in key order, stopping early when fn returns false.
func (sc *sidecar) forEach(fn func(key codec.Hash, bh BlockHandle) bool) error {
	return sc.region.Guard(func([]byte) {
		var key codec.Hash
		for i := 0; i < sc.count; i++ {
			rec := sc.entries[i*sidecarEntrySize : (i+1)*sidecarEntrySize]
			copy(key.Bytes[:], rec[:32])
			if !fn(key, decodeSidecarHandle(rec)) {
				return
			}
		}
	})
}

// identity returns the table's length and its footer's file hash: what a sidecar is checked
// against. One small read, of the footer's tail.
func (r *DefaultReader) identity() (size int64, fileHash codec.Hash, err error) {
	size, err = r.segmentSize()
	if err != nil {
		return 0, codec.Hash{}, err
	}
	if size < 40+footerTrailerSize {
		return 0, codec.Hash{}, fmt.Errorf("sstable: %s is too short to hold a footer", r.handle.SegmentName)
	}
	tail, err := r.io.ReadFromSegment(r.handle.SegmentName, size-32-footerTrailerSize, 32+footerTrailerSize)
	if err != nil {
		return 0, codec.Hash{}, err
	}
	if len(tail) != 32+footerTrailerSize {
		return 0, codec.Hash{}, fmt.Errorf("sstable: short read of %s's footer", r.handle.SegmentName)
	}
	copy(fileHash.Bytes[:], tail[:32])
	return size, fileHash, nil
}

// footerChunk bounds how much of a footer indexEntries reads at once. A variable for tests, which
// shrink it to exercise lines split across chunks.
var footerChunk = 1 << 20

// indexEntries reads the table's footer index into a slice, in footer order - what a sidecar is
// built from. It parses the same text parseFooter does, line for line, but a chunk at a time and
// into 64 B per entry rather than a map, so rebuilding the sidecar of a large table does not need
// the whole footer text and a map of it in memory at once.
func (r *DefaultReader) indexEntries(size int64) ([]indexEntry, error) {
	indexLen, err := r.readFooterIndexLen(size)
	if err != nil {
		return nil, err
	}
	// Sized once from the footer's length: a line is at least minFooterLine bytes, so this is an
	// upper bound on the entry count, and growing the slice by appending instead allocated about
	// five times its final size on the way there.
	return r.appendIndexEntries(make([]indexEntry, 0, indexLen/minFooterLine+1), size, indexLen)
}

// appendIndexEntries is indexEntries appending to out, for a caller gathering the entries of
// several tables into one slice (a compaction). indexLen is the table's, from readFooterIndexLen.
func (r *DefaultReader) appendIndexEntries(out []indexEntry, size int64, indexLen int) ([]indexEntry, error) {
	var err error
	footerStart := size - int64(40+indexLen) - footerTrailerSize
	if footerStart < 0 {
		return nil, fmt.Errorf("sstable: %s footer overruns the file", r.handle.SegmentName)
	}
	head, err := r.io.ReadFromSegment(r.handle.SegmentName, footerStart, 8)
	if err != nil {
		return nil, err
	}
	if len(head) != 8 || readInt(head, 0) != footerMagic {
		return nil, fmt.Errorf("sstable: %s has no footer magic", r.handle.SegmentName)
	}
	// One buffer for the whole read: the tail of a line split across chunks is moved to its front
	// and the next chunk read in after it.
	buf := make([]byte, 0, footerChunk+128)
	pos, end := footerStart+8, footerStart+8+int64(indexLen)
	for pos < end {
		n := footerChunk
		if end-pos < int64(n) {
			n = int(end - pos)
		}
		carried := len(buf)
		if cap(buf) < carried+n {
			buf = append(make([]byte, 0, carried+n), buf...)
		}
		buf = buf[:carried+n]
		if err := r.readFooterInto(pos, buf[carried:]); err != nil {
			return nil, err
		}
		pos += int64(n)
		rest := buf
		for {
			nl := bytes.IndexByte(rest, '\n')
			if nl < 0 {
				break
			}
			if out, err = appendFooterLine(out, rest[:nl]); err != nil {
				return nil, err
			}
			rest = rest[nl+1:]
		}
		buf = buf[:copy(buf, rest)]
	}
	if out, err = appendFooterLine(out, buf); err != nil {
		return nil, err
	}
	return out, nil
}

// minFooterLine is the shortest line buildFooter writes: a 64-digit key, two one-digit numbers,
// two colons and the newline.
const minFooterLine = 64 + 1 + 1 + 1 + 1 + 1

// readFooterInto fills dst from the table at off, into dst itself where the shim can.
func (r *DefaultReader) readFooterInto(off int64, dst []byte) error {
	if into, ok := r.io.(storage.SegmentIntoReader); ok {
		n, err := into.ReadFromSegmentInto(r.handle.SegmentName, off, dst)
		if err != nil {
			return err
		}
		if n != len(dst) {
			return fmt.Errorf("sstable: short read of %s's footer", r.handle.SegmentName)
		}
		return nil
	}
	chunk, err := r.io.ReadFromSegment(r.handle.SegmentName, off, len(dst))
	if err != nil {
		return err
	}
	if len(chunk) != len(dst) {
		return fmt.Errorf("sstable: short read of %s's footer", r.handle.SegmentName)
	}
	copy(dst, chunk)
	return nil
}

// appendFooterLine parses one "<hex>:<offset>:<size>[:<tombstone>]" line as parseFooter does:
// blank and short lines are skipped, a bad hash is an error, unparseable numbers read as zero.
func appendFooterLine(out []indexEntry, line []byte) ([]indexEntry, error) {
	if len(line) == 0 {
		return out, nil
	}
	var parts [4][]byte
	np := 0
	for np < 4 {
		i := bytes.IndexByte(line, ':')
		if i < 0 {
			parts[np] = line
			np++
			break
		}
		parts[np] = line[:i]
		np++
		line = line[i+1:]
	}
	if np < 3 {
		return out, nil
	}
	if np == 4 {
		if i := bytes.IndexByte(parts[3], ':'); i >= 0 {
			parts[3] = parts[3][:i] // parseFooter ignores any field past the fourth
		}
	}
	var e indexEntry
	if len(parts[0]) != 64 {
		return nil, fmt.Errorf("sstable: footer key %q is not a 32-byte hash", parts[0])
	}
	if _, err := hex.Decode(e.key.Bytes[:], parts[0]); err != nil {
		return nil, err
	}
	e.handle.Offset = parseDecimal(parts[1])
	e.handle.CompressedSize = int(parseDecimal(parts[2]))
	e.handle.Deleted = np > 3 && string(parts[3]) == tombstoneFlag
	return append(out, e), nil
}

// parseDecimal is strconv.ParseInt for the footer's numbers, without the string conversion; like
// parseFooter's use of it, anything unparseable reads as zero.
func parseDecimal(b []byte) int64 {
	if len(b) == 0 || len(b) > 18 {
		return 0
	}
	var v int64
	neg := false
	if b[0] == '-' || b[0] == '+' {
		neg = b[0] == '-'
		b = b[1:]
		if len(b) == 0 {
			return 0
		}
	}
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0
		}
		v = v*10 + int64(c-'0')
	}
	if neg {
		return -v
	}
	return v
}
