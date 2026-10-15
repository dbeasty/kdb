package sstable

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"strconv"
	"strings"

	kdberr "github.com/limidus/kdb/go/kdb/error"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/compression"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/bufpool"
	"github.com/limidus/kdb/go/kdb/storage/io"
)

const footerMagic = 0x4B444253

// footerTrailerSize is the fixed-size trailer buildFooter appends after fileHash: a copy of
// indexLen, duplicated here specifically so a reader can find it (and, from it, the footer's
// start) using only its fixed offset from the end of the file. See buildFooter's doc comment.
const footerTrailerSize = 4

// SSTable block header, v2:
//
//	 0      version   u8  (= blockFormatVersion)
//	 1      codec     u8  (blockCodecNone | blockCodecZSTD)
//	 2..3   reserved  u16 (zero)
//	 4..7   compressed length   u32 (big-endian, body only)
//	 8..11  uncompressed length u32 (big-endian)
//	12..15  crc32 of body       u32 (big-endian)
//
// v1 was 12 bytes with no codec, and decodeBlock inferred "was this compressed?"
// from compSize == uncompSize - which is wrong for any payload whose compressed
// form happens to be exactly its original size, and gave no way to change the
// configured codec without making existing files unreadable.
const (
	blockHeaderSize    = 16
	blockFormatVersion = 2

	blockCodecNone byte = 0
	blockCodecZSTD byte = 1
)

func encodeBlock(payload []byte, compress bool) ([]byte, error) {
	if compress {
		return appendBlock(nil, payload)
	}
	var body []byte
	var err error
	id := blockCodecNone
	if compress {
		id = blockCodecZSTD
		body, err = compression.Compress(payload, 3)
		if err != nil {
			return nil, err
		}
	} else {
		body = payload
	}
	out := make([]byte, blockHeaderSize+len(body))
	out[0] = blockFormatVersion
	out[1] = id
	out[2], out[3] = 0, 0
	writeInt(out, 4, len(body))
	writeInt(out, 8, len(payload))
	writeInt(out, 12, int(compression.CRC32All(body)))
	copy(out[blockHeaderSize:], body)
	return out, nil
}

// appendBlock is encodeBlock(payload, true) appended to dst: compressed straight into place
// behind its header, rather than into a buffer of its own that is then copied into a block that
// is then copied into the table's write buffer - three allocations per value, and a memtable
// flush writes one block per value.
func appendBlock(dst, payload []byte) ([]byte, error) {
	start := len(dst)
	dst = append(dst, make([]byte, blockHeaderSize)...)
	out, err := compression.CompressAppend(dst, payload, 3)
	if err != nil {
		return nil, err
	}
	body := out[start+blockHeaderSize:]
	out[start] = blockFormatVersion
	out[start+1] = blockCodecZSTD
	writeInt(out, start+4, len(body))
	writeInt(out, start+8, len(payload))
	writeInt(out, start+12, int(compression.CRC32All(body)))
	return out, nil
}

// decodeBlock verifies body against the CRC32 encodeBlock wrote at offset 8 before decoding it -
// previously ignored entirely (kdb-finish-up-plan.md's 1-G1), so a corrupted or truncated block
// was silently decompressed (or returned as-is) instead of failing loudly.
func decodeBlock(block []byte) ([]byte, error) {
	if len(block) < blockHeaderSize {
		return nil, kdberr.NewDecodeError(
			fmt.Sprintf("sstable block shorter than its %d-byte header", blockHeaderSize), 0, nil)
	}
	if v := block[0]; v != blockFormatVersion {
		return nil, kdberr.NewDecodeError(
			fmt.Sprintf("unsupported sstable block version %d (this build writes and reads v%d)", v, blockFormatVersion), 0, nil)
	}
	uncompSize := readInt(block, 8)
	wantCRC := uint32(readInt(block, 12))
	body := block[blockHeaderSize:]
	if gotCRC := compression.CRC32All(body); gotCRC != wantCRC {
		return nil, kdberr.NewDecodeError(
			fmt.Sprintf("sstable block CRC mismatch: block is corrupt (want %08x, got %08x)", wantCRC, gotCRC),
			0, nil)
	}
	switch block[1] {
	case blockCodecNone:
		return append([]byte(nil), body...), nil
	case blockCodecZSTD:
		return compression.Decompress(body, uncompSize)
	default:
		return nil, kdberr.NewDecodeError(
			fmt.Sprintf("unknown sstable block codec id %d", block[1]), 0, nil)
	}
}

// tombstoneFlag is the optional fourth field of a footer index line: "<hex>:<offset>:<size>:1"
// means the key was deleted in this table and no block was written for it. It is written only for
// tombstones, so a table containing none is byte-for-byte what the three-field format produced -
// existing segments and the golden fixtures in go/testdata/golden/codec are unaffected.
//
// A reader predating this field takes the first three parts and ignores the rest, so it sees a
// tombstone as a zero-length block, fails its CRC check, and skips the table - falling through to
// older tables exactly as it does today. Wrong, but no more wrong than the behavior this replaces,
// and never silently wrong in a new way.
const tombstoneFlag = "1"

// indexEntry is one footer index line's worth of state, in the order the writer intends to emit
// it. buildFooter takes a slice rather than a map because ranging a Go map yields a *randomized*
// order: the same table produced a different footer on every single run, and never the order
// Kotlin's insertion-ordered index produces, so no two writes of the same content ever agreed
// byte-for-byte within Go, let alone across the two languages.
type indexEntry struct {
	key    codec.Hash
	handle BlockHandle
}

// buildFooter lays out magic(4) indexLen(4) indexBytes(indexLen) fileHash(32), then appends a
// fixed 4-byte trailer duplicating indexLen at the very end of the footer (and, since the footer
// is always the last thing written to a segment, at the very end of the file). That trailer is
// what makes the footer locatable at all: parseFooter/DefaultReader.Get need indexLen to know
// where the footer *starts*, relative to the end of the file, but indexLen itself was previously
// only ever written *inside* the footer at a variable offset that depends on knowing where the
// footer starts - an unsolvable bootstrap the old format never actually provided a way out of.
// DefaultReader.Get could never locate a real footer (proven by round-tripping a single value:
// it always failed with a read past EOF or garbage), which is presumably why this package had
// zero tests before this fix. Mirrors the equivalent fix in kdb-storage-sstable's SsTableCodec.kt
// (Kotlin was missing this trailer too) - the two must stay byte-for-byte identical; see
// go/testdata/golden/codec's regenerated fixtures.
func buildFooter(index []indexEntry, fileHash codec.Hash) []byte {
	var out []byte
	_ = encodeFooter(index, fileHash, make([]byte, 0, 64<<10), func(b []byte) error {
		out = append(out, b...)
		return nil
	})
	return out
}

// encodeFooter lays out exactly what buildFooter returns, a buffer at a time: it fills buf (from
// its start, up to its capacity) and hands it to emit whenever another line might not fit, and
// once more at the end. emit must be done with the bytes when it returns.
//
// For a table writer, which appends the footer as it is encoded. The footer is one text line per
// key, and building it whole - the lines, then their join, then the footer around it - held
// about three copies of the index text at once, the last allocation of a merge and its largest
// after the values: some 800 MB for the 3M keys of a 3M-document namespace.
func encodeFooter(index []indexEntry, fileHash codec.Hash, buf []byte, emit func([]byte) error) error {
	// indexLen comes before the lines, so it is counted first.
	indexLen := 0
	var scratch [maxFooterLine]byte
	for i, e := range index {
		if i > 0 {
			indexLen++
		}
		indexLen += len(appendFooterIndexLine(scratch[:0], e))
	}
	var word [4]byte
	writeInt(word[:], 0, footerMagic)
	buf = append(buf[:0], word[:]...)
	writeInt(word[:], 0, indexLen)
	buf = append(buf, word[:]...)
	for i, e := range index {
		if len(buf) > 0 && len(buf)+1+maxFooterLine > cap(buf) {
			if err := emit(buf); err != nil {
				return err
			}
			buf = buf[:0]
		}
		if i > 0 {
			buf = append(buf, '\n')
		}
		buf = appendFooterIndexLine(buf, e)
	}
	buf = append(buf, fileHash.Bytes[:]...)
	buf = append(buf, word[:]...) // the trailer: indexLen again
	return emit(buf)
}

// maxFooterLine bounds appendFooterIndexLine's output: the key, two colons, two int64s in decimal
// and the tombstone field.
const maxFooterLine = 64 + 1 + 20 + 1 + 20 + 2

// appendFooterIndexLine appends e's footer index line, "<hex>:<offset>:<size>" plus ":1" for a
// tombstone, without its newline.
func appendFooterIndexLine(dst []byte, e indexEntry) []byte {
	dst = hex.AppendEncode(dst, e.key.Bytes[:])
	dst = append(dst, ':')
	dst = strconv.AppendInt(dst, e.handle.Offset, 10)
	dst = append(dst, ':')
	dst = strconv.AppendInt(dst, int64(e.handle.CompressedSize), 10)
	if e.handle.Deleted {
		dst = append(dst, ':')
		dst = append(dst, tombstoneFlag...)
	}
	return dst
}

func parseFooter(footer []byte) (map[codec.Hash]BlockHandle, error) {
	if len(footer) < 40 {
		return map[codec.Hash]BlockHandle{}, nil
	}
	indexLen := readInt(footer, 4)
	indexStr := string(footer[8 : 8+indexLen])
	if indexStr == "" {
		return map[codec.Hash]BlockHandle{}, nil
	}
	out := make(map[codec.Hash]BlockHandle)
	for _, line := range strings.Split(indexStr, "\n") {
		if line == "" {
			continue
		}
		parts := strings.Split(line, ":")
		if len(parts) < 3 {
			continue
		}
		hash, err := codec.HashFromHex(parts[0])
		if err != nil {
			return nil, err
		}
		off, _ := strconv.ParseInt(parts[1], 10, 64)
		cs, _ := strconv.Atoi(parts[2])
		deleted := len(parts) > 3 && parts[3] == tombstoneFlag
		out[hash] = BlockHandle{Offset: off, CompressedSize: cs, Deleted: deleted}
	}
	return out, nil
}

// DefaultWriter writes sorted SSTable blocks to a segment.
type DefaultWriter struct {
	io          storage.PlatformIOShim
	namespaceID string
	level       int
	// entries holds one kv per distinct key, in the order that key was *first* written -
	// exactly the semantics of Kotlin's linkedMapOf-backed writer, which pos below reproduces.
	// Appending blindly instead let a re-written key occupy two slots, so the same put sequence
	// wrote two blocks (one unreachable), and fed both values into the fileHash preimage:
	// identical logical tables got different content hashes in the two languages.
	entries []kv
	pos     map[codec.Hash]int
	// unique: the caller puts each key once (NewDefaultWriterUnique), so pos is not kept.
	unique bool
	// seq names the table (see tiering.go): its store's (LsmBlobStore.NewWriter), or, for a
	// writer made without one, a sequence of its own that lists the namespace at Finish.
	seq *Sequence
}

type kv struct {
	key     codec.Hash
	value   []byte
	deleted bool
}

// NewDefaultWriter creates an SSTable writer.
func NewDefaultWriter(io storage.PlatformIOShim, namespaceID string, level int) *DefaultWriter {
	return &DefaultWriter{io: io, namespaceID: namespaceID, level: level}
}

func (w *DefaultWriter) Put(key codec.Hash, value []byte) {
	w.set(kv{key: key, value: append([]byte(nil), value...)})
}

// NewDefaultWriterUnique is NewDefaultWriter for a caller that puts or deletes each key once and
// knows how many it will: the entries are sized up front and there is no last-word-wins index to
// keep - a memtable generation, whose keys its own index already made unique.
func NewDefaultWriterUnique(io storage.PlatformIOShim, namespaceID string, level, n int) *DefaultWriter {
	w := NewDefaultWriter(io, namespaceID, level)
	w.unique = true
	w.entries = make([]kv, 0, n)
	return w
}

// PutShared is Put without the copy, for a caller whose value will not change before Finish
// returns - a memtable generation being flushed, which nothing writes to any more.
func (w *DefaultWriter) PutShared(key codec.Hash, value []byte) {
	w.set(kv{key: key, value: value})
}

// Delete records a tombstone for key. No block is written - the footer index entry alone carries
// the fact - so a table of nothing but deletes costs one index line per key.
func (w *DefaultWriter) Delete(key codec.Hash) {
	w.set(kv{key: key, deleted: true})
}

// set records e as the writer's final word on e.key: last value wins, first insertion position
// is kept. A later Delete of an already-Put key therefore replaces it with a tombstone in place
// rather than leaving a stale block ahead of it, and vice versa.
func (w *DefaultWriter) set(e kv) {
	if w.unique {
		w.entries = append(w.entries, e)
		return
	}
	if i, ok := w.pos[e.key]; ok {
		w.entries[i] = e
		return
	}
	if w.pos == nil {
		w.pos = make(map[codec.Hash]int)
	}
	w.pos[e.key] = len(w.entries)
	w.entries = append(w.entries, e)
}

// appendLimiter is implemented by platform shims that cap a single append. See
// io.FileBackedPlatformIO.MaxAppendBytes.
type appendLimiter interface{ MaxAppendBytes() int }

// appendAll writes b to segmentName in pieces the shim will accept, and returns the segment's
// size afterwards. The bytes on disk are identical either way - only the number of calls
// changes - so this is not a format concern.
//
// It exists because two of the things a table writes grow with the data rather than being
// bounded by a block size: the footer's index carries a line per key, and a single value can be
// arbitrarily large (a full tree object holds every document in the namespace). At the default
// 16MB ceiling the index alone stopped a table at about 200,000 keys, which a namespace reaches
// with roughly 108,000 commits once its documents, tree objects and version locations are
// counted - at which point compaction failed and quietly kept its existing tables. The
// in-memory shim has no ceiling, which is why no memory-backed test could see it.
func appendAll(shim storage.PlatformIOShim, segmentName string, b []byte) (int64, error) {
	limit := 0
	if l, ok := shim.(appendLimiter); ok {
		limit = l.MaxAppendBytes()
	}
	if limit <= 0 || len(b) <= limit {
		return shim.AppendToSegment(segmentName, b)
	}
	var size int64
	for off := 0; off < len(b); off += limit {
		end := off + limit
		if end > len(b) {
			end = len(b)
		}
		n, err := shim.AppendToSegment(segmentName, b[off:end])
		if err != nil {
			return 0, err
		}
		size = n
	}
	return size, nil
}

// finishBufferBytes is how many bytes of blocks Finish gathers before appending them. A block
// larger than it is appended alone (appendAll splits what exceeds the shim's append limit).
const finishBufferBytes = 1 << 20

func (w *DefaultWriter) Finish() (Handle, error) {
	seq := w.seq
	if seq == nil {
		seq = NewSequence(w.io, w.namespaceID)
	}
	age, err := seq.Next()
	if err != nil {
		return Handle{}, err
	}
	fileID, err := tableFileID(age)
	if err != nil {
		return Handle{}, err
	}
	segmentName := io.SegmentNameBuilder.SSTable(w.namespaceID, w.level, fileID)
	b := newTableBuilder(w.io, segmentName, len(w.entries))
	for _, e := range w.entries {
		if e.deleted {
			b.delete(e.key)
			continue
		}
		if err := b.put(e.key, e.value); err != nil {
			return Handle{}, err
		}
	}
	tableSize, fileHash, err := b.finish()
	if err != nil {
		return Handle{}, err
	}
	if err := w.io.SealSegment(segmentName); err != nil {
		return Handle{}, err
	}
	writeSidecar(w.io, segmentName, b.index, tableSize, fileHash)
	return Handle{FileHash: fileHash, Level: w.level, SegmentName: segmentName}, nil
}

// writeSidecar writes a finished, sealed table's sidecar. The sidecar is an optional extra
// written after the table (see sidecar.go): a failure here costs the table its fast index, never
// its data - a lookup finds no sidecar and reads the footer, as every table did before sidecars,
// and rebuilds it.
func writeSidecar(shim storage.PlatformIOShim, segmentName string, index []indexEntry, tableSize int64, fileHash codec.Hash) {
	if path, ok := sidecarPath(sidecarResolver(shim), segmentName); ok {
		_ = writeSidecarFile(path, index, tableSize, fileHash)
	}
}

// zstdBound is the most zstd can expand n bytes: ZSTD_COMPRESSBOUND.
func zstdBound(n int) int {
	bound := n + n>>8
	if n < 128<<10 {
		bound += (128<<10 - n) >> 11
	}
	return bound
}

// tableBuilder lays a table out as its entries arrive: each value is encoded and appended (in
// finishBufferBytes batches) when it is put, and only the footer index - tens of bytes per key -
// stays in memory until finish. The entries must arrive in the order the table is to hold them;
// DefaultWriter.Finish feeds its first-insertion order and a compaction its sorted keys.
//
// A table used to be built only from a DefaultWriter that held every value until Finish, so a
// compaction, which reads every surviving value of the store, held the whole store in memory:
// 2.7 GB of heap to merge 471 MB of tables at 3M documents.
type tableBuilder struct {
	shim        storage.PlatformIOShim
	segmentName string
	// Blocks are gathered and appended finishBufferBytes at a time, not one append per block: an
	// append is a positional write, a system call each, and at ~150 us apiece on macOS a 10k-entry
	// flush spent 1.6 s in them for 2 MB of data - every memtable flush and compaction paid it.
	buf    []byte
	offset int64
	index  []indexEntry
	// Hashed as a stream rather than into one buffer: the preimage is every key and value in the
	// table, and the digest of a byte sequence does not depend on how it was fed in.
	hash hash.Hash
}

func newTableBuilder(shim storage.PlatformIOShim, segmentName string, entries int) *tableBuilder {
	return &tableBuilder{
		shim:        shim,
		segmentName: segmentName,
		buf:         make([]byte, 0, finishBufferBytes),
		index:       make([]indexEntry, 0, entries),
		hash:        sha256.New(),
	}
}

// put appends value's block under key.
func (b *tableBuilder) put(key codec.Hash, value []byte) error {
	// A block is never larger than its value plus its header and zstd's frame overhead, so
	// flushing first whenever that would not fit keeps the buffer within finishBufferBytes as
	// before, with the block encoded straight into it.
	if len(b.buf) > 0 && len(b.buf)+blockHeaderSize+zstdBound(len(value)) > finishBufferBytes {
		if err := b.flush(); err != nil {
			return err
		}
	}
	start := len(b.buf)
	out, err := appendBlock(b.buf, value)
	if err != nil {
		return err
	}
	b.buf = out
	block := out[start:]
	// CompressedSize is the compressed body's own length - excluding encodeBlock's
	// header (compSize/uncompSize/crc) - matching what Get() expects when it later reads
	// bh.CompressedSize+blockHeaderSize bytes starting at Offset. This used to store
	// len(block) (the full header+body length) instead, over-reading a header's worth of
	// bytes into whatever followed - the next block,
	// or the footer for the last one - on every single Get().
	b.index = append(b.index, indexEntry{
		key:    key,
		handle: BlockHandle{Offset: b.offset, CompressedSize: len(block) - blockHeaderSize},
	})
	b.offset += int64(len(block))
	b.hash.Write(key.Bytes[:])
	b.hash.Write(value)
	return nil
}

// delete records a tombstone for key: an index entry and no block.
func (b *tableBuilder) delete(key codec.Hash) {
	b.index = append(b.index, indexEntry{key: key, handle: BlockHandle{Deleted: true}})
	b.hash.Write(key.Bytes[:])
	// A marker byte, so "deleted K" and "wrote K with an empty value" hash differently.
	// Safe to introduce: no segment written before tombstones existed contains one.
	b.hash.Write([]byte{0xFF})
}

func (b *tableBuilder) flush() error {
	if len(b.buf) == 0 {
		return nil
	}
	size, err := appendAll(b.shim, b.segmentName, b.buf)
	if err != nil {
		return err
	}
	// Flushed before a block is added, so offset is where the buffered blocks end.
	if size != b.offset {
		return fmt.Errorf("sstable: %s is %d bytes after an append, expected %d", b.segmentName, size, b.offset)
	}
	b.buf = b.buf[:0]
	return nil
}

// finish appends what is buffered and the footer, and returns the table's size and file hash.
// It does not seal the segment: a compaction renames it first.
func (b *tableBuilder) finish() (int64, codec.Hash, error) {
	if err := b.flush(); err != nil {
		return 0, codec.Hash{}, err
	}
	fileHash, err := codec.HashFromBytes(b.hash.Sum(nil))
	if err != nil {
		return 0, codec.Hash{}, err
	}
	var tableSize int64
	err = encodeFooter(b.index, fileHash, b.buf, func(p []byte) error {
		tableSize, err = appendAll(b.shim, b.segmentName, p)
		return err
	})
	if err != nil {
		return 0, codec.Hash{}, err
	}
	return tableSize, fileHash, nil
}

// DefaultReader reads from one SSTable handle.
type DefaultReader struct {
	io     storage.PlatformIOShim
	handle Handle
}

// NewDefaultReader returns a reader for handle.
func NewDefaultReader(io storage.PlatformIOShim, handle Handle) *DefaultReader {
	return &DefaultReader{io: io, handle: handle}
}

// Get returns the value stored for key, or nil if this table has never seen it *or* holds a
// tombstone for it. Callers that would otherwise fall through to an older table must use Lookup.
func (r *DefaultReader) Get(key codec.Hash) ([]byte, error) {
	value, _, _, err := r.Lookup(key)
	return value, err
}

// Lookup reports what this table knows about key - see Reader.Lookup.
func (r *DefaultReader) Lookup(key codec.Hash) ([]byte, bool, bool, error) {
	index, err := r.Index()
	if err != nil {
		return nil, false, false, err
	}
	bh, ok := index[key]
	if !ok {
		return nil, false, false, nil
	}
	if bh.Deleted {
		return nil, true, true, nil
	}
	value, err := r.ReadBlockAt(bh)
	if err != nil {
		return nil, false, false, err
	}
	return value, false, true, nil
}

// Index returns everything this table says about every key it holds:
// where each value lives, or that the key is tombstoned.
//
// Split out of Lookup for compaction, which needs to enumerate a table
// rather than probe it - and, unlike a probe, must read the footer exactly
// once for the whole table instead of once per key. An empty or truncated
// segment returns an empty index rather than an error, which is how Lookup
// has always treated one.
func (r *DefaultReader) Index() (map[codec.Hash]BlockHandle, error) {
	size, err := r.segmentSize()
	if err != nil {
		return nil, err
	}
	if size < 40+footerTrailerSize {
		return map[codec.Hash]BlockHandle{}, nil
	}
	indexLen, err := r.readFooterIndexLen(size)
	if err != nil {
		return nil, err
	}
	// The footer body (everything buildFooter writes except its own trailing indexLen copy) is
	// magic(4) + indexLen(4) + indexBytes(indexLen) + fileHash(32) = 40+indexLen bytes, sitting
	// immediately before the footerTrailerSize-byte trailer at the true end of the file.
	bodyLen := 40 + indexLen
	footerStart := size - int64(bodyLen) - footerTrailerSize
	footer, err := r.io.ReadFromSegment(r.handle.SegmentName, footerStart, bodyLen)
	if err != nil {
		return nil, err
	}
	return parseFooter(footer)
}

// ReadBlockAt decodes the value a BlockHandle from this table's Index
// points at. A tombstone has no block and is a programming error to ask
// for, so it reports one rather than returning a plausible empty value.
func (r *DefaultReader) ReadBlockAt(bh BlockHandle) ([]byte, error) {
	if bh.Deleted {
		return nil, fmt.Errorf("sstable: a tombstone has no block to read")
	}
	size := bh.CompressedSize + blockHeaderSize
	if into, ok := r.io.(storage.SegmentIntoReader); ok && size <= bufpool.MaxPooled {
		// The raw block is dead once decoded - decodeBlock returns a fresh copy (uncompressed)
		// or a fresh decompression (zstd), never a slice of its input - so it is read into a
		// pooled buffer rather than a new one per lookup.
		bp := bufpool.Get(size)
		defer bufpool.Put(bp)
		n, err := into.ReadFromSegmentInto(r.handle.SegmentName, bh.Offset, *bp)
		if err != nil {
			return nil, err
		}
		return decodeBlock((*bp)[:n])
	}
	block, err := r.io.ReadFromSegment(r.handle.SegmentName, bh.Offset, size)
	if err != nil {
		return nil, err
	}
	return decodeBlock(block)
}

// segmentSize asks the IO layer for the segment's length, and reads to the end only when it
// cannot say.
//
// Reading was the only way until issue #43's crash-recovery stall: every probe of every table
// read the whole file in 8 KB chunks just to find where its footer was, so a 1.3 MB table cost
// ~160 reads per lookup and a replay of 14,736 commits took 28 seconds.
func (r *DefaultReader) segmentSize() (int64, error) {
	if sz, ok := r.io.(storage.SegmentSizer); ok {
		n, err := sz.SegmentSize(r.handle.SegmentName)
		if err == nil {
			return n, nil
		}
		if !errors.Is(err, storage.ErrSegmentSizeUnknown) {
			return 0, err
		}
	}
	return r.scanSegmentSize()
}

// scanSegmentSize learns the segment's length by reading it, for a shim that cannot report one.
func (r *DefaultReader) scanSegmentSize() (int64, error) {
	const chunk = 8192
	var total int64
	for {
		p, err := r.io.ReadFromSegment(r.handle.SegmentName, total, chunk)
		if err != nil {
			return 0, err
		}
		if len(p) == 0 {
			return total, nil
		}
		total += int64(len(p))
		if len(p) < chunk {
			return total, nil
		}
	}
}

// readFooterIndexLen reads buildFooter's trailing indexLen copy - the last footerTrailerSize
// bytes of the file. Previously read the last 8 bytes and took bytes [4:8] of that as indexLen,
// which (with no trailer in the old format) was actually the tail 4 bytes of the 32-byte
// fileHash - never a real length, and the reason Get() could never locate a real footer at all.
func (r *DefaultReader) readFooterIndexLen(size int64) (int, error) {
	tail, err := r.io.ReadFromSegment(r.handle.SegmentName, size-footerTrailerSize, footerTrailerSize)
	if err != nil {
		return 0, err
	}
	if len(tail) != footerTrailerSize {
		return 0, fmt.Errorf("%w: %s: short read of the footer trailer", ErrCorruptTable, r.handle.SegmentName)
	}
	indexLen := readInt(tail, 0)
	// A footer is magic(4) indexLen(4) index fileHash(32) trailer(4): it cannot be longer than the
	// file. A length past that is a damaged table, and must not reach the callers that size an
	// allocation or a read from it (a compaction would try to hold a 450 MB "index" and never finish).
	if indexLen < 0 || int64(indexLen) > size-40-footerTrailerSize {
		return 0, fmt.Errorf("%w: %s: footer declares a %d-byte index in a %d-byte table", ErrCorruptTable, r.handle.SegmentName, indexLen, size)
	}
	return indexLen, nil
}

// ErrCorruptTable marks an sstable whose structure is damaged (its footer cannot be right). It is
// a verdict about the file, not the I/O: retrying will not help, and the table needs repair or
// replacement from a peer or backup.
var ErrCorruptTable = errors.New("sstable: corrupt table")

func writeInt(arr []byte, off, v int) {
	arr[off] = byte(v >> 24)
	arr[off+1] = byte(v >> 16)
	arr[off+2] = byte(v >> 8)
	arr[off+3] = byte(v)
}

func readInt(b []byte, off int) int {
	return (int(b[off])&0xFF)<<24 |
		(int(b[off+1])&0xFF)<<16 |
		(int(b[off+2])&0xFF)<<8 |
		int(b[off+3])&0xFF
}
