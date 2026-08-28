package delta

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/compression"
	"github.com/limidus/kdb/go/kdb/storage"
)

// KDBP page frame, v2:
//
//	 0..3   magic 'KDBP'
//	 4      version   u8  (= PageFormatVersion)
//	 5      codec     u8  (pageCodecNone | pageCodecZSTD)
//	 6..7   reserved  u16 (zero)
//	 8..11  compressed length   u32 (big-endian, body only)
//	12..15  uncompressed length u32 (big-endian)
//	16..19  crc32 of body       u32 (big-endian)
//
// v1 was 16 bytes and carried no codec: readers had to be told out-of-band
// which codec a segment was written with, so configuring a different codec
// silently made existing segments unreadable, and integrity.Verify could not
// tell a codec mismatch from real corruption (it said so in its own doc
// comment). Recording the codec per frame removes both problems and lets a
// single segment hold frames written under different settings.
const (
	// PageFrameHeaderSize is the fixed size of a v2 KDBP frame header.
	PageFrameHeaderSize = 20
	// PageFormatVersion is the frame version this build writes.
	PageFormatVersion = 2

	pageCodecNone byte = 0
	pageCodecZSTD byte = 1
)

// pageCodecID maps a configured codec onto its stable on-disk id. The ids are
// spelled out rather than derived from storage.CompressionCodec's iota so the
// enum can be reordered or extended without changing what past segments mean.
func pageCodecID(c storage.CompressionCodec) (byte, error) {
	switch c {
	case storage.CompressionNone:
		return pageCodecNone, nil
	case storage.CompressionZSTD:
		return pageCodecZSTD, nil
	default:
		return 0, fmt.Errorf("delta page: unknown compression codec %d", int(c))
	}
}

// PageCodec frames commit payloads in KDBP pages.
type PageCodec struct{}

func (PageCodec) Frame(payload []byte, codec storage.CompressionCodec) ([]byte, error) {
	id, err := pageCodecID(codec)
	if err != nil {
		// Previously the unknown-codec branch fell through to compressing
		// anyway, which wrote bytes no configuration could describe.
		return nil, err
	}
	var body []byte
	switch id {
	case pageCodecNone:
		body = payload
	default:
		body, err = compression.Compress(payload, 3)
		if err != nil {
			return nil, err
		}
	}
	out := make([]byte, PageFrameHeaderSize+len(body))
	out[0], out[1], out[2], out[3] = 0x4B, 0x44, 0x42, 0x50
	out[4] = PageFormatVersion
	out[5] = id
	out[6], out[7] = 0, 0
	writeInt(out, 8, len(body))
	writeInt(out, 12, len(payload))
	writeInt(out, 16, int(compression.CRC32All(body)))
	copy(out[PageFrameHeaderSize:], body)
	return out, nil
}

// Parse decodes one whole frame (header included) using the codec the frame
// itself records - callers no longer supply it.
func (PageCodec) Parse(frame []byte) ([]byte, error) {
	if len(frame) < PageFrameHeaderSize {
		return nil, fmt.Errorf("delta page: frame shorter than its %d-byte header", PageFrameHeaderSize)
	}
	if v := frame[4]; v != PageFormatVersion {
		return nil, fmt.Errorf("delta page: unsupported frame version %d (this build writes and reads v%d)", v, PageFormatVersion)
	}
	body := frame[PageFrameHeaderSize:]
	uncompressed := readInt(frame, 12)
	switch frame[5] {
	case pageCodecNone:
		return append([]byte(nil), body...), nil
	case pageCodecZSTD:
		// Exactly the recorded size, not a padded bound: the header says how
		// many bytes this frame decodes to, so anything else is corruption.
		return compression.Decompress(body, uncompressed)
	default:
		return nil, fmt.Errorf("delta page: unknown codec id %d in frame", frame[5])
	}
}

func writeInt(a []byte, o, v int) {
	a[o] = byte(v >> 24)
	a[o+1] = byte(v >> 16)
	a[o+2] = byte(v >> 8)
	a[o+3] = byte(v)
}

func readInt(a []byte, o int) int {
	return (int(a[o])&0xFF)<<24 |
		(int(a[o+1])&0xFF)<<16 |
		(int(a[o+2])&0xFF)<<8 |
		int(a[o+3])&0xFF
}
