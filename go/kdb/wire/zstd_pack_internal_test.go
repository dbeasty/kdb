package wire

import (
	"bytes"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// TestCommitsPayloadZstdBombRefused: a small compressed payload that inflates past maxPackPayload
// is refused rather than decompressed into memory - a peer is not trusted to size what it sends.
func TestCommitsPayloadZstdBombRefused(t *testing.T) {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	bomb := enc.EncodeAll(bytes.Repeat([]byte{0}, maxPackPayload+1<<20), nil)
	if len(bomb) > 1<<20 {
		t.Fatalf("test setup: the bomb is %d bytes compressed", len(bomb))
	}
	if _, err := decodeCommitsPayload(bomb, true); err == nil {
		t.Fatal("a payload inflating past the bound was accepted")
	}
}
