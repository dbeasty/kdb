package compression

import (
	"bytes"
	"testing"
)

func TestCRC32KnownVector(t *testing.T) {
	data := []byte("123456789")
	got := CRC32All(data)
	const want uint32 = 0xCBF43926
	if got != want {
		t.Fatalf("crc32: got %08x want %08x", got, want)
	}
}

func TestZstdRoundTrip(t *testing.T) {
	input := bytes.Repeat([]byte("kdb compression test "), 100)
	compressed, err := Compress(input, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(compressed) >= len(input) {
		t.Fatal("expected compression")
	}
	out, err := Decompress(compressed, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(input, out) {
		t.Fatal("round trip mismatch")
	}
}

func TestDecompressExceedsMax(t *testing.T) {
	input := []byte("hello")
	compressed, err := Compress(input, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decompress(compressed, 2); err == nil {
		t.Fatal("expected size error")
	}
}
