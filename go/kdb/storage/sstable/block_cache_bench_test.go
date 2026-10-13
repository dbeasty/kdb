package sstable

import (
	"testing"

	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// BenchmarkLsmLookupBlockCache is BenchmarkLsmLookup1MB's file case with the store built the way
// the engine builds it - with a block cache large enough for the table. Before the cache was
// wired in, the cache made no difference and every hit read, CRC-checked and decompressed its
// block.
func BenchmarkLsmLookupBlockCache(b *testing.B) {
	root := b.TempDir()
	osStore, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		b.Fatal(err)
	}
	defer osStore.Close()
	shim := storio.NewFileBackedPlatformIO(storio.DefaultPlatformIOConfig(), osStore)
	ns := "app/docs"
	keys, h := writeSizedTable(b, shim, ns, 2000, 512, 1)
	store := NewLsmBlobStore(shim, ns, NewBlockCache(32<<20))
	store.AddTable(h)
	for _, k := range keys {
		store.Get(k)
	}
	b.Run("hit", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if store.Get(keys[i%len(keys)]) == nil {
				b.Fatal("missing")
			}
		}
	})
	b.Run("hit-parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if store.Get(keys[i%len(keys)]) == nil {
					b.Fatal("missing")
				}
				i++
			}
		})
	})
}
