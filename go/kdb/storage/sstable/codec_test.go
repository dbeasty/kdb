package sstable

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage/io"
)

// TestRoundTrip is the regression test for the finding recorded in docs/kdb-finish-up-plan.md as
// 1-G1: DefaultReader.Get could never locate a real footer at all. buildFooter wrote indexLen
// only inside the footer itself (at an offset that depends on already knowing where the footer
// starts), with no way for a reader to find it from just the end of the file; readFooterIndexLen
// read the last 8 bytes and took bytes [4:8] as indexLen, which was actually the tail of the
// 32-byte fileHash, never a real length. This package had zero tests before this fix - it's
// presumably why the bug went unnoticed: a single write-then-read round trip fails outright.
func TestRoundTrip(t *testing.T) {
	shim := io.NewInMemoryPlatformIO()
	w := NewDefaultWriter(shim, "ns", 0)
	key := randomKey(t)
	value := []byte(`{"hello":"world"}`)
	w.Put(key, value)
	handle, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	r := NewDefaultReader(shim, handle)
	got, err := r.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(value) {
		t.Fatalf("got %q, want %q", got, value)
	}
}

// TestMultiBlockRoundTrip is TestRoundTrip's multi-entry counterpart - also catches the second,
// independent bug this same fix addresses: DefaultWriter.Finish stored each BlockHandle's
// CompressedSize as the *full* encoded block length (12-byte header + compressed body), but Get
// already adds 12 for the header when reading (bh.CompressedSize+12), so every read over-ran 12
// bytes into whatever followed - masked in the single-entry case above only because that 12-byte
// over-read lands inside the footer rather than a following block, decoding "successfully" on
// data that isn't the intended block. With multiple entries, the CRC check this fix also adds
// (decodeBlock, previously ignoring the CRC encodeBlock always wrote) would catch that
// misalignment on any true multi-block file - this test proves each block resolves to its own
// distinct value, not a neighbor's.
func TestMultiBlockRoundTrip(t *testing.T) {
	shim := io.NewInMemoryPlatformIO()
	w := NewDefaultWriter(shim, "ns", 0)
	const n = 20
	keys := make([]codec.Hash, n)
	values := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = randomKey(t)
		values[i] = []byte(fmt.Sprintf(`{"i":%d,"payload":"%040d"}`, i, i))
		w.Put(keys[i], values[i])
	}
	handle, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	r := NewDefaultReader(shim, handle)
	for i, k := range keys {
		got, err := r.Get(k)
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if string(got) != string(values[i]) {
			t.Fatalf("Get(%d): got %q, want %q - resolved to the wrong block", i, got, values[i])
		}
	}
}

// TestGetMissingKeyReturnsNil confirms a key absent from the SSTable is reported cleanly, not as
// an error or as garbage from a misaligned read.
func TestGetMissingKeyReturnsNil(t *testing.T) {
	shim := io.NewInMemoryPlatformIO()
	w := NewDefaultWriter(shim, "ns", 0)
	w.Put(randomKey(t), []byte(`{"present":true}`))
	handle, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	r := NewDefaultReader(shim, handle)
	got, err := r.Get(randomKey(t))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for a missing key, got %q", got)
	}
}

// TestDecodeBlockRejectsCorruptCRC is the regression test for decodeBlock's other half of 1-G1:
// the CRC32 encodeBlock always wrote at offset 8 was never actually checked, so a corrupted or
// truncated block was silently decompressed (or returned as-is, for an incompressible payload)
// instead of failing loudly.
func TestDecodeBlockRejectsCorruptCRC(t *testing.T) {
	block, err := encodeBlock([]byte(`{"v":"original"}`), true)
	if err != nil {
		t.Fatalf("encodeBlock: %v", err)
	}
	corrupt := append([]byte(nil), block...)
	corrupt[len(corrupt)-1] ^= 0xFF // flip a bit in the compressed body
	if _, err := decodeBlock(corrupt); err == nil {
		t.Fatal("expected a corrupted block to fail CRC verification")
	}
	// The original, unmodified block must still decode cleanly.
	if _, err := decodeBlock(block); err != nil {
		t.Fatalf("expected the original block to decode cleanly, got: %v", err)
	}
}

func randomKey(t *testing.T) codec.Hash {
	t.Helper()
	u, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	h, err := codec.HashFromBytes(append(u.Bytes(), u.Bytes()...))
	if err != nil {
		t.Fatal(err)
	}
	return h
}
