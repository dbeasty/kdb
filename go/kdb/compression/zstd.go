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

// pooledEncoder carries the construction error alongside the encoder, since
// sync.Pool's New cannot fail.
type pooledEncoder struct {
	enc *zstd.Encoder
	err error
}

// encoderPools holds one sync.Pool of encoders per zstd level (at most four -
// EncoderLevelFromZstd collapses every int onto one of SpeedFastest..
// SpeedBestCompression).
//
// Encoders are pooled, not constructed per call: zstd.NewWriter eagerly builds
// one full encoder state per concurrency slot, and defaults concurrency to
// GOMAXPROCS. At SpeedDefault each state holds a 1<<15 table plus a 1<<17
// longTable of 8-byte entries = 1.25MiB, so on a 16-core host a single
// throwaway NewWriter cost 20MiB. Compress runs once per commit appended to the
// delta log (storage/delta.PageCodec.Frame) and once per entry in an SSTable
// flush (storage/sstable.encodeBlock), so that was ~21MB of garbage per write -
// measured, and the dominant allocation in the whole engine. The decoder below
// was already shared via sync.Once; the encoder simply never got the same
// treatment.
//
// WithEncoderConcurrency(1) keeps each pooled encoder to one state: EncodeAll
// processes its blocks on a single encoder regardless (concurrency only bounds
// how many EncodeAll calls proceed at once), so this costs nothing in output
// bytes or speed, and the pool - not the concurrency setting - is what lets
// concurrent callers scale. Pooling rather than one shared encoder also lets GC
// reclaim the states under memory pressure, which matters on the 1GB tier
// kdb-service targets.
var encoderPools sync.Map // zstd.EncoderLevel -> *sync.Pool

func encoderPool(lvl zstd.EncoderLevel) *sync.Pool {
	if p, ok := encoderPools.Load(lvl); ok {
		return p.(*sync.Pool)
	}
	p := &sync.Pool{New: func() any {
		enc, err := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(lvl),
			zstd.WithEncoderConcurrency(1),
		)
		return &pooledEncoder{enc: enc, err: err}
	}}
	actual, _ := encoderPools.LoadOrStore(lvl, p)
	return actual.(*sync.Pool)
}

// Compress ZSTD-compresses input at the given level (default 3).
func Compress(input []byte, level int) ([]byte, error) {
	if level <= 0 {
		level = 3
	}
	pool := encoderPool(zstd.EncoderLevelFromZstd(level))
	pe := pool.Get().(*pooledEncoder)
	if pe.err != nil {
		// Deliberately not returned to the pool: a failed construction will fail
		// identically next time, and keeping it would hand the same error to
		// every subsequent caller instead of letting them retry construction.
		return nil, pe.err
	}
	defer pool.Put(pe)
	return pe.enc.EncodeAll(input, make([]byte, 0, len(input))), nil
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
