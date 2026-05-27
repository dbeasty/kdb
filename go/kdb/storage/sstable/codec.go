package sstable

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/compression"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/io"
)

const footerMagic = 0x4B444253

func encodeBlock(payload []byte, compress bool) ([]byte, error) {
	var body []byte
	var err error
	if compress {
		body, err = compression.Compress(payload, 3)
		if err != nil {
			return nil, err
		}
	} else {
		body = payload
	}
	out := make([]byte, 12+len(body))
	writeInt(out, 0, len(body))
	writeInt(out, 4, len(payload))
	writeInt(out, 8, int(compression.CRC32All(body)))
	copy(out[12:], body)
	return out, nil
}

func decodeBlock(block []byte) ([]byte, error) {
	compSize := readInt(block, 0)
	uncompSize := readInt(block, 4)
	body := block[12:]
	if compSize == uncompSize {
		return append([]byte(nil), body...), nil
	}
	return compression.Decompress(body, uncompSize+1024)
}

func buildFooter(index map[codec.Hash]BlockHandle, fileHash codec.Hash) []byte {
	var lines []string
	for k, bh := range index {
		lines = append(lines, fmt.Sprintf("%s:%d:%d", k.Hex(), bh.Offset, bh.CompressedSize))
	}
	indexBytes := []byte(strings.Join(lines, "\n"))
	footer := make([]byte, 8+len(indexBytes)+32)
	writeInt(footer, 0, footerMagic)
	writeInt(footer, 4, len(indexBytes))
	copy(footer[8:], indexBytes)
	copy(footer[len(footer)-32:], fileHash.Bytes[:])
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
		blocks[e.key] = BlockHandle{Offset: offset, CompressedSize: len(block)}
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
	if size < 40 {
		return nil, nil
	}
	footerLen, err := r.readFooterIndexLen(size)
	if err != nil {
		return nil, err
	}
	footer, err := r.io.ReadFromSegment(r.handle.SegmentName, size-int64(footerLen)-32, footerLen+40)
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
	block, err := r.io.ReadFromSegment(r.handle.SegmentName, bh.Offset, bh.CompressedSize+12)
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

func (r *DefaultReader) readFooterIndexLen(size int64) (int, error) {
	tail, err := r.io.ReadFromSegment(r.handle.SegmentName, size-8, 8)
	if err != nil {
		return 0, err
	}
	return readInt(tail, 4), nil
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
