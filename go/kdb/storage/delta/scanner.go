package delta

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

const frameHeaderSize = 16

// ScannedCommit is one commit frame in a delta segment.
type ScannedCommit struct {
	CommitHash  codec.Hash
	Commit      document.Commit
	FrameOffset int
}

// ScanSegmentBytes scans v1 delta segment bytes (sequential KDBP frames).
func ScanSegmentBytes(bytes []byte, compression storage.CompressionCodec) ([]ScannedCommit, error) {
	var out []ScannedCommit
	var codec PageCodec
	offset := 0
	for offset+frameHeaderSize <= len(bytes) {
		if !isKdbpFrame(bytes, offset) {
			break
		}
		compressedSize := readIntBE(bytes, offset+4)
		frameEnd := offset + frameHeaderSize + compressedSize
		if frameEnd > len(bytes) {
			break
		}
		frame := bytes[offset:frameEnd]
		payload, err := codec.Parse(frame, compression)
		if err != nil {
			return nil, err
		}
		commit, err := document.FromPayloadBytes(payload)
		if err != nil {
			return nil, err
		}
		out = append(out, ScannedCommit{
			CommitHash:  commit.Hash,
			Commit:      commit,
			FrameOffset: offset,
		})
		offset = frameEnd
	}
	return out, nil
}

func isKdbpFrame(bytes []byte, offset int) bool {
	return bytes[offset] == 0x4B && bytes[offset+1] == 0x44 &&
		bytes[offset+2] == 0x42 && bytes[offset+3] == 0x50
}

func readIntBE(bytes []byte, offset int) int {
	return (int(bytes[offset])&0xFF)<<24 |
		(int(bytes[offset+1])&0xFF)<<16 |
		(int(bytes[offset+2])&0xFF)<<8 |
		int(bytes[offset+3])&0xFF
}
