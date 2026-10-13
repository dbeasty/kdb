package engine

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// BenchmarkDocHashStoreGet is the version store's own read path: a pinned live version looked
// up by content hash, from many goroutines - what every point read of live data does after the
// tree lookup. The body is read in full, as a caller serialising it would.
func BenchmarkDocHashStoreGet(b *testing.B) {
	const n = 100_000
	s := newShardedDocByHashStore(0)
	hashes := make([]codec.Hash, n)
	for i := range hashes {
		binary.BigEndian.PutUint64(hashes[i].Bytes[:], uint64(i)*0x9E3779B97F4A7C15)
		var id codec.UUID
		id.MSB = int64(i)
		s.Pin(hashes[i])
		s.Put(hashes[i], document.Document{ID: id, JSON: fmt.Sprintf(`{"v":%d,"name":"user-%d","score":%d}`, i, i, i%997)})
	}
	for _, par := range []int{1, 8} {
		b.Run(fmt.Sprintf("parallel-%d", par), func(b *testing.B) {
			b.SetParallelism(par)
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i, sum := 0, 0
				for pb.Next() {
					d, ok := s.Get(hashes[(i*7919)%n])
					if !ok {
						b.Fatal("missing")
					}
					sum += int(d.JSON[len(d.JSON)-2])
					i++
				}
				_ = sum
			})
		})
	}
}
