package delta

import (
	"github.com/limidus/kdb/go/kdb/compression"
	"github.com/limidus/kdb/go/kdb/storage"
)

// PageCodec frames commit payloads in KDBP pages.
type PageCodec struct{}

func (PageCodec) Frame(payload []byte, codec storage.CompressionCodec) ([]byte, error) {
	var body []byte
	var err error
	switch codec {
	case storage.CompressionNone:
		body = payload
	case storage.CompressionZSTD:
		body, err = compression.Compress(payload, 3)
		if err != nil {
			return nil, err
		}
	default:
		body, err = compression.Compress(payload, 3)
		if err != nil {
			return nil, err
		}
	}
	out := make([]byte, 16+len(body))
	out[0], out[1], out[2], out[3] = 0x4B, 0x44, 0x42, 0x50
	writeInt(out, 4, len(body))
	writeInt(out, 8, len(payload))
	writeInt(out, 12, int(compression.CRC32All(body)))
	copy(out[16:], body)
	return out, nil
}

func (PageCodec) Parse(frame []byte, codec storage.CompressionCodec) ([]byte, error) {
	body := frame[16:]
	switch codec {
	case storage.CompressionNone:
		return append([]byte(nil), body...), nil
	default:
		return compression.Decompress(body, readInt(frame, 8)+1024)
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
