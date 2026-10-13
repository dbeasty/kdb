package engine

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/limidus/kdb/go/kdb/storage/delta"
)

// BenchmarkColdPointReadFrameCache is BenchmarkColdPointRead with a frame cache in front of the
// reader. "hit": the cache holds every frame of the log (a warm working set). "churn": a 256 KiB
// cache over a log many times larger, so nearly every read misses and inserts - what the cache
// costs when it cannot help.
func BenchmarkColdPointReadFrameCache(b *testing.B) {
	for _, shape := range []struct{ commits, docs int }{{4000, 1}, {200, 100}} {
		for _, mode := range []struct {
			name   string
			budget int64
		}{{"hit", 256 << 20}, {"churn", 256 << 10}} {
			b.Run(fmt.Sprintf("docs-%d/%s", shape.docs, mode.name), func(b *testing.B) {
				shim := &countingShim{PlatformIOShim: osShim(b)}
				reader, wants := writePointLog(b, shim, shape.commits, shape.docs)
				reader.SetFrameCache(delta.NewFrameCache(mode.budget))
				rand.New(rand.NewSource(1)).Shuffle(len(wants), func(i, j int) { wants[i], wants[j] = wants[j], wants[i] })
				l := newDeltaColdLoader(reader)
				if mode.name == "hit" {
					for _, w := range wants {
						if _, ok, err := l.load(w.id, w.h); !ok || err != nil {
							b.Fatalf("warm-up: ok=%v err=%v", ok, err)
						}
					}
				} else if _, ok, err := l.load(wants[0].id, wants[0].h); !ok || err != nil {
					b.Fatalf("warm-up: ok=%v err=%v", ok, err)
				}
				b.ReportAllocs()
				shim.read.Store(0)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					w := wants[i%len(wants)]
					if _, ok, err := l.load(w.id, w.h); !ok || err != nil {
						b.Fatalf("load: ok=%v err=%v", ok, err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(shim.read.Load())/float64(b.N), "read-B/op")
			})
		}
	}
}
