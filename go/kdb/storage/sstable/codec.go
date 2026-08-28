package sstable

import (
	"fmt"
	"strconv"
	"strings"

	kdberr "github.com/limidus/kdb/go/kdb/error"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/compression"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
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
func buildFooter(index map[codec.Hash]BlockHandle, fileHash codec.Hash) []byte {
	var lines []string
	for k, bh := range index {
		lines = append(lines, fmt.Sprintf("%s:%d:%d", k.Hex(), bh.Offset, bh.CompressedSize))
	}
	indexBytes := []byte(strings.Join(lines, "\n"))
	footer := make([]byte, 8+len(indexBytes)+32+footerTrailerSize)
	writeInt(footer, 0, footerMagic)
	writeInt(footer, 4, len(indexBytes))
	copy(footer[8:], indexBytes)
	copy(footer[8+len(indexBytes):8+len(indexBytes)+32], fileHash.Bytes[:])
	writeInt(footer, len(footer)-footerTrailerSize, len(indexBytes))
	return footer
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
		out[hash] = BlockHandle{Offset: off, CompressedSize: cs}
	}
	return out, nil
}

// DefaultWriter writes sorted SSTable blocks to a segment.
type DefaultWriter struct {
	io          storage.PlatformIOShim
	namespaceID string
	level       int
	entries     []kv
}

type kv struct {
	key   codec.Hash
	value []byte
}

// NewDefaultWriter creates an SSTable writer.
func NewDefaultWriter(io storage.PlatformIOShim, namespaceID string, level int) *DefaultWriter {
	return &DefaultWriter{io: io, namespaceID: namespaceID, level: level}
}

func (w *DefaultWriter) Put(key codec.Hash, value []byte) {
	w.entries = append(w.entries, kv{key: key, value: append([]byte(nil), value...)})
}

func (w *DefaultWriter) Finish() (Handle, error) {
	blocks := make(map[codec.Hash]BlockHandle)
	fileID, err := codec.RandomUUID()
	if err != nil {
		return Handle{}, err
	}
	segmentName := io.SegmentNameBuilder.SSTable(w.namespaceID, w.level, fileID.String())
	var offset int64
	for _, e := range w.entries {
		block, err := encodeBlock(e.value, true)
		if err != nil {
			return Handle{}, err
		}
		newSize, err := w.io.AppendToSegment(segmentName, block)
		if err != nil {
			return Handle{}, err
		}
		// CompressedSize is the compressed body's own length - excluding encodeBlock's
		// header (compSize/uncompSize/crc) - matching what Get() expects when it later reads
		// bh.CompressedSize+blockHeaderSize bytes starting at Offset. This used to store
		// len(block) (the full header+body length) instead, over-reading a header's worth of
		// bytes into whatever followed - the next block,
		// or the footer for the last one - on every single Get().
		blocks[e.key] = BlockHandle{Offset: offset, CompressedSize: len(block) - blockHeaderSize}
		offset = newSize
	}
	var concat []byte
	for _, e := range w.entries {
		concat = append(concat, e.key.Bytes[:]...)
		concat = append(concat, e.value...)
	}
	fileHash, err := codec.HashFromBytes(document.SHA256Digest(concat))
	if err != nil {
		return Handle{}, err
	}
	footer := buildFooter(blocks, fileHash)
	if _, err := w.io.AppendToSegment(segmentName, footer); err != nil {
		return Handle{}, err
	}
	if err := w.io.SealSegment(segmentName); err != nil {
		return Handle{}, err
	}
	return Handle{FileHash: fileHash, Level: w.level, SegmentName: segmentName}, nil
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

func (r *DefaultReader) Get(key codec.Hash) ([]byte, error) {
	size, err := r.segmentSize()
	if err != nil {
		return nil, err
	}
	if size < 40+footerTrailerSize {
		return nil, nil
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
	index, err := parseFooter(footer)
	if err != nil {
		return nil, err
	}
	bh, ok := index[key]
	if !ok {
		return nil, nil
	}
	block, err := r.io.ReadFromSegment(r.handle.SegmentName, bh.Offset, bh.CompressedSize+blockHeaderSize)
	if err != nil {
		return nil, err
	}
	return decodeBlock(block)
}

func (r *DefaultReader) segmentSize() (int64, error) {
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
	return readInt(tail, 0), nil
}

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
