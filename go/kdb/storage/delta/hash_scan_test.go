package delta

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/storage"
)

// A hash scan yields exactly the commits and offsets a decoding scan does - the SHA-256 of each
// frame's payload is its commit's hash - on a clean segment, compressed or not, and past a damaged
// frame, which both skip.
func TestHashScanMatchesTheDecodingScan(t *testing.T) {
	for _, comp := range []storage.CompressionCodec{storage.CompressionNone, storage.CompressionZSTD} {
		shim := newTestShim(t)
		cfg := newConfig(shim, comp)
		writeChainAcross(t, cfg, []int64{0}, 6)
		raw := readSegment(t, shim, 0)
		frames := segmentFrames(t, raw)
		damaged := append([]byte(nil), raw...)
		damaged[frames[2]+frameHeaderSize+1] ^= 0x01 // a CRC failure mid-segment
		for name, seg := range map[string][]byte{"clean": raw, "damaged": damaged} {
			type got struct {
				hash string
				off  int
			}
			var full, hashed []got
			skipFull, err := ScanSkippingCorrupt(seg, func(s ScannedCommit) error {
				full = append(full, got{s.Commit.Hash.Hex(), s.FrameOffset})
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			skipHash, err := ScanHashesSkippingCorrupt(seg, func(s ScannedCommit) error {
				hashed = append(hashed, got{s.CommitHash.Hex(), s.FrameOffset})
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(full) == 0 || len(full) != len(hashed) || len(skipFull) != len(skipHash) {
				t.Fatalf("%v %s: decoding scan %d commits (%d skipped), hash scan %d (%d skipped)", comp, name, len(full), len(skipFull), len(hashed), len(skipHash))
			}
			for i := range full {
				if full[i] != hashed[i] {
					t.Fatalf("%v %s: frame %d: decoding scan %v, hash scan %v", comp, name, i, full[i], hashed[i])
				}
			}
		}
	}
}
