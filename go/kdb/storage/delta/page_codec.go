package delta

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/compression"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/bufpool"
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

func (c PageCodec) Frame(payload []byte, codec storage.CompressionCodec) ([]byte, error) {
	// Sized for the uncompressed body: a zstd frame of a small, incompressible payload runs a few
	// bytes over, and AppendFrame's append grows it then.
	out, err := c.AppendFrame(make([]byte, 0, PageFrameHeaderSize+len(payload)), payload, codec)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// AppendFrame appends payload's frame - Frame's bytes - to dst, compressing straight into it. A
// writer laying many frames into one buffer (DefaultWriter.AppendBatch) then pays no allocation
// per frame: Frame used to compress into a buffer of its own and copy that into a frame of its
// own, two payload-sized allocations per commit logged, for bytes copied once more into the
// batch.
func (PageCodec) AppendFrame(dst, payload []byte, codec storage.CompressionCodec) ([]byte, error) {
	id, err := pageCodecID(codec)
	if err != nil {
		// Previously the unknown-codec branch fell through to compressing
		// anyway, which wrote bytes no configuration could describe.
		return dst, err
	}
	start := len(dst)
	dst = append(dst, 0x4B, 0x44, 0x42, 0x50, PageFormatVersion, id, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	switch id {
	case pageCodecNone:
		dst = append(dst, payload...)
	default:
		framed, err := compression.CompressAppend(dst, payload, 3)
		if err != nil {
			return dst[:start], err
		}
		dst = framed
	}
	hdr, body := dst[start:], dst[start+PageFrameHeaderSize:]
	writeInt(hdr, 8, len(body))
	writeInt(hdr, 12, len(payload))
	writeInt(hdr, 16, int(compression.CRC32All(body)))
	return dst, nil
}

// Parse decodes one whole frame (header included) using the codec the frame
// itself records - callers no longer supply it.
func (PageCodec) Parse(frame []byte) ([]byte, error) {
	body, codecID, uncompressed, err := frameBody(frame)
	if err != nil {
		return nil, err
	}
	switch codecID {
	case pageCodecNone:
		return append([]byte(nil), body...), nil
	default:
		// Exactly the recorded size, not a padded bound: the header says how
		// many bytes this frame decodes to, so anything else is corruption.
		return compression.Decompress(body, uncompressed)
	}
}

// frameBody validates frame's header and returns its body, codec and recorded uncompressed size.
func frameBody(frame []byte) (body []byte, codecID byte, uncompressed int, err error) {
	if len(frame) < PageFrameHeaderSize {
		return nil, 0, 0, fmt.Errorf("delta page: frame shorter than its %d-byte header", PageFrameHeaderSize)
	}
	if v := frame[4]; v != PageFormatVersion {
		return nil, 0, 0, fmt.Errorf("delta page: unsupported frame version %d (this build writes and reads v%d)", v, PageFormatVersion)
	}
	switch frame[5] {
	case pageCodecNone, pageCodecZSTD:
	default:
		return nil, 0, 0, fmt.Errorf("delta page: unknown codec id %d in frame", frame[5])
	}
	return frame[PageFrameHeaderSize:], frame[5], readInt(frame, 12), nil
}

// parseCommit is Parse followed by document.FromPayloadBytes, without keeping the payload.
//
// Every reader of a frame wants the commit, not the payload, and the payload is dead the moment
// the commit is decoded: the codec copies every field it keeps (codec.cursor.rawN, and string
// conversion), and the commit hash is computed from the payload, not retained with it. So the
// payload need not be a fresh allocation per frame:
//
//   - an uncompressed frame's body is decoded in place, rather than copied first as Parse does;
//   - a compressed one is decompressed into a pooled buffer of the recorded size, returned to the
//     pool once the commit is decoded. TestParseCommitDoesNotAliasThePayload holds the "nothing
//     keeps the payload" half of that bargain.
//
// Errors are Parse's and FromPayloadBytes's, unchanged, so callers report them as before.
func (pc PageCodec) parseCommit(frame []byte) (document.Commit, error) {
	payload, bp, err := framePayload(frame)
	if bp != nil {
		defer bufpool.Put(bp)
	}
	if err != nil {
		return document.Commit{}, err
	}
	return document.FromPayloadBytes(payload)
}

// framePayload is Parse without the copy: it returns frame's payload - the body itself for an
// uncompressed frame, or the body decompressed into a pooled buffer, bp, which the caller returns
// with bufpool.Put once nothing refers to the payload. bp is nil when there is nothing to return.
// The CRC is not checked here; callers check it first.
func framePayload(frame []byte) (payload []byte, bp *[]byte, err error) {
	body, codecID, uncompressed, err := frameBody(frame)
	if err != nil {
		return nil, nil, err
	}
	if codecID == pageCodecNone {
		return body, nil, nil
	}
	if uncompressed <= 0 || uncompressed > bufpool.MaxPooled {
		// Too large to pool (or an implausible size, which Decompress checks against the
		// frame's own header before allocating for it).
		payload, err := compression.Decompress(body, uncompressed)
		return payload, nil, err
	}
	bp = bufpool.Get(uncompressed)
	payload, err = compression.DecompressInto((*bp)[:0], body, uncompressed)
	if err != nil {
		bufpool.Put(bp)
		return nil, nil, err
	}
	return payload, bp, nil
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
