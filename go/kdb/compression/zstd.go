package compression

import (
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const defaultMaxOutput = 64 * 1024 * 1024

var (
	decoderOnce sync.Once
	sharedDec   *zstd.Decoder
	decoderErr  error
)

func sharedDecoder() (*zstd.Decoder, error) {
	decoderOnce.Do(func() {
		sharedDec, decoderErr = zstd.NewReader(nil)
	})
	return sharedDec, decoderErr
}

// Compress ZSTD-compresses input at the given level (default 3).
func Compress(input []byte, level int) ([]byte, error) {
	if level <= 0 {
		level = 3
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)))
	if err != nil {
		return nil, err
	}
	defer enc.Close()
	return enc.EncodeAll(input, make([]byte, 0, len(input))), nil
}

// Decompress ZSTD-decompresses input, enforcing maxOutputSize (default 64MiB).
func Decompress(input []byte, maxOutputSize int) ([]byte, error) {
	if maxOutputSize <= 0 {
		maxOutputSize = defaultMaxOutput
	}
	dec, err := sharedDecoder()
	if err != nil {
		return nil, err
	}
	out, err := dec.DecodeAll(input, nil)
	if err != nil {
		return nil, err
	}
	if len(out) > maxOutputSize {
		return nil, fmt.Errorf("decompressed size %d exceeds max %d", len(out), maxOutputSize)
	}
	return out, nil
}
