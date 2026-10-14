package sstable

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
	"github.com/limidus/kdb/go/kdb/storage/mmapfile"
)

const sidecarNS = "app/docs"

// osShim is a file-backed shim over a fresh directory: the only kind that keeps local files, and
// so the only kind sidecars are written for.
func osShim(t testing.TB) (*storio.FileBackedPlatformIO, string) {
	t.Helper()
	if !mmapfile.Supported {
		t.Skip("sidecars need mmap")
	}
	return fileShim(t)
}

// fileShim is osShim on any platform, for tests that hold wherever mapping is unavailable too.
func fileShim(t testing.TB) (*storio.FileBackedPlatformIO, string) {
	t.Helper()
	root := t.TempDir()
	cfg := storio.PlatformIOConfig{RootDirectory: &root}
	store, err := storio.NewOSByteStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return storio.NewFileBackedPlatformIO(cfg, store), root
}

func randomHash(rng *rand.Rand) codec.Hash {
	var b [32]byte
	rng.Read(b[:])
	sum := sha256.Sum256(b[:])
	return codec.Hash{Bytes: sum}
}

// writeMixedTable writes n values (and every fifth key as a tombstone instead) and returns the
// keys written and the table.
func writeMixedTable(t testing.TB, shim *storio.FileBackedPlatformIO, level, n int, seed int64) ([]codec.Hash, Handle) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	w := NewDefaultWriter(shim, sidecarNS, level)
	keys := make([]codec.Hash, n)
	for i := range keys {
		v := make([]byte, 16+rng.Intn(64))
		rng.Read(v)
		sum := sha256.Sum256(v)
		keys[i] = codec.Hash{Bytes: sum}
		if i%5 == 4 {
			w.Delete(keys[i])
		} else {
			w.Put(keys[i], v)
		}
	}
	h, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return keys, h
}

func sidecarFile(t testing.TB, shim *storio.FileBackedPlatformIO, table string) string {
	t.Helper()
	p, ok := sidecarPath(shim, table)
	if !ok {
		t.Fatalf("no sidecar path for %s", table)
	}
	return p
}

func openTableSidecar(t testing.TB, shim *storio.FileBackedPlatformIO, h Handle) (*sidecar, error) {
	t.Helper()
	size, fh, err := NewDefaultReader(shim, h).identity()
	if err != nil {
		t.Fatal(err)
	}
	return openSidecar(sidecarFile(t, shim, h.SegmentName), size, fh)
}

func TestSidecarNamesRoundTrip(t *testing.T) {
	table := "ns/a/sstable/b/sstable/L3/0f0e"
	sc, ok := sidecarSegmentName(table)
	if !ok || sc != "ns/a/sstable/b/sstidx/L3/0f0e.sidx" {
		t.Fatalf("sidecar of %s = %q", table, sc)
	}
	if back, ok := sidecarTableName(sc); !ok || back != table {
		t.Fatalf("table of %s = %q", sc, back)
	}
	if _, ok := sidecarSegmentName("ns/a/delta/0001.seg"); ok {
		t.Fatal("a delta segment has no sidecar")
	}
}

// Every key of every table answers the same from the sidecar as from the footer index, and keys
// the table never held are absent. Through the store, no table's footer reaches the heap.
func TestSidecarLookupsEqualTheFooterIndex(t *testing.T) {
	shim, _ := osShim(t)
	store := NewLsmBlobStore(shim, sidecarNS, nil)
	rng := rand.New(rand.NewSource(1))
	var all []codec.Hash
	for i, n := range []int{0, 1, 2, 31, 32, 33, 200, 1100} {
		keys, h := writeMixedTable(t, shim, i%3, n, int64(i+10))
		if n == 0 {
			continue // an empty writer writes an empty table; covered below
		}
		store.AddTable(h)
		all = append(all, keys...)
		footer, err := NewDefaultReader(shim, h).Index()
		if err != nil {
			t.Fatal(err)
		}
		sc, err := openTableSidecar(t, shim, h)
		if err != nil {
			t.Fatalf("table of %d keys: %v", n, err)
		}
		if sc.count != len(footer) {
			t.Fatalf("sidecar holds %d entries, footer %d", sc.count, len(footer))
		}
		for k, want := range footer {
			got, found, err := sc.find(&k)
			if err != nil || !found || got != want {
				t.Fatalf("key %s: sidecar %+v found=%v err=%v, footer %+v", k.Hex(), got, found, err, want)
			}
		}
		for j := 0; j < 500; j++ {
			k := randomHash(rng)
			if _, found, _ := sc.find(&k); found {
				t.Fatalf("sidecar found key %s the table never held", k.Hex())
			}
		}
		var prev []byte
		seen := 0
		if err := sc.forEach(func(k codec.Hash, bh BlockHandle) bool {
			if prev != nil && bytes.Compare(prev, k.Bytes[:]) >= 0 {
				t.Fatal("forEach out of key order")
			}
			prev = append(prev[:0], k.Bytes[:]...)
			if footer[k] != bh {
				t.Fatalf("forEach handle %+v, footer %+v", bh, footer[k])
			}
			seen++
			return true
		}); err != nil || seen != len(footer) {
			t.Fatalf("forEach saw %d of %d, err %v", seen, len(footer), err)
		}
		sc.close()
	}
	for i, k := range all {
		v, deleted, found := store.Lookup(k)
		if !found {
			t.Fatalf("store lost key %d", i)
		}
		if deleted != (v == nil) {
			t.Fatalf("key %d: deleted=%v value=%v", i, deleted, v != nil)
		}
		if !deleted && store.Contains(k) != true {
			t.Fatalf("Contains disagrees with Lookup for key %d", i)
		}
	}
	if n := store.IndexCacheBytes(); n != 0 {
		t.Fatalf("%d bytes of footer index on the heap with every table sidecarred", n)
	}
	if store.SidecarMappedBytes() == 0 {
		t.Fatal("no sidecar mapped")
	}
	store.Close()
}

func TestBloomFilterFalsePositiveRateAndNoFalseNegatives(t *testing.T) {
	const n = 50_000
	rng := rand.New(rand.NewSource(7))
	blocks := uint64(bloomBlocksFor(n))
	filter := make([]byte, blocks*bloomBlockBytes)
	keys := make([]codec.Hash, n)
	for i := range keys {
		keys[i] = randomHash(rng)
		bloomAdd(filter, blocks, &keys[i])
	}
	for i := range keys {
		if !bloomMayContain(filter, blocks, &keys[i]) {
			t.Fatalf("false negative for key %d", i)
		}
	}
	fp := 0
	const probes = 200_000
	for i := 0; i < probes; i++ {
		k := randomHash(rng)
		if bloomMayContain(filter, blocks, &k) {
			fp++
		}
	}
	rate := float64(fp) / probes
	t.Logf("false-positive rate %.3f%% at %d bits/key", rate*100, bloomBitsPerKey)
	if rate > 0.02 || rate < 0.002 {
		t.Fatalf("false-positive rate %.3f%%, want about 1%%", rate*100)
	}
	// Structured keys (not digests) must still spread.
	blocks = uint64(bloomBlocksFor(n))
	filter = make([]byte, blocks*bloomBlockBytes)
	for i := 0; i < n; i++ {
		var k codec.Hash
		binary.BigEndian.PutUint64(k.Bytes[24:], uint64(i))
		bloomAdd(filter, blocks, &k)
	}
	fp = 0
	for i := n; i < n+probes; i++ {
		var k codec.Hash
		binary.BigEndian.PutUint64(k.Bytes[24:], uint64(i))
		if bloomMayContain(filter, blocks, &k) {
			fp++
		}
	}
	if rate := float64(fp) / probes; rate > 0.03 {
		t.Fatalf("false-positive rate %.3f%% for sequential keys", rate*100)
	}
	if bloomMayContain(nil, 0, &keys[0]) {
		t.Fatal("an empty filter must reject every key")
	}
}

// indexEntries must read exactly what parseFooter reads, whatever the chunking.
func TestIndexEntriesMatchesParseFooter(t *testing.T) {
	shim, _ := fileShim(t)
	_, h := writeMixedTable(t, shim, 0, 200, 3)
	want, err := NewDefaultReader(shim, h).Index()
	if err != nil {
		t.Fatal(err)
	}
	defer func(c int) { footerChunk = c }(footerChunk)
	for _, chunk := range []int{1, 7, 97, 4096, 1 << 20} {
		footerChunk = chunk
		r := NewDefaultReader(shim, h)
		size, _, err := r.identity()
		if err != nil {
			t.Fatal(err)
		}
		got, err := r.indexEntries(size)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(want) {
			t.Fatalf("chunk %d: %d entries, want %d", chunk, len(got), len(want))
		}
		for _, e := range got {
			if want[e.key] != e.handle {
				t.Fatalf("chunk %d: %s = %+v, want %+v", chunk, e.key.Hex(), e.handle, want[e.key])
			}
		}
	}
	// The line parser follows parseFooter on odd input too.
	key := strings.Repeat("ab", 32)
	for _, line := range []string{"", "x", "a:b", key + ":12:34:1:junk", key + ":zz:-5", key + ":7:8:0", key + ":+3:9"} {
		footer := make([]byte, 8, 8+len(line)+32)
		writeInt(footer, 4, len(line))
		footer = append(append(footer, line...), make([]byte, 32)...)
		ref, refErr := parseFooter(footer)
		got, err := appendFooterLine(nil, []byte(line))
		if (err != nil) != (refErr != nil) || len(got) != len(ref) {
			t.Fatalf("line %q: %d entries (err %v), parseFooter %d (err %v)", line, len(got), err, len(ref), refErr)
		}
		for _, e := range got {
			if ref[e.key] != e.handle {
				t.Fatalf("line %q: %+v, parseFooter %+v", line, e.handle, ref[e.key])
			}
		}
	}
}

// corrupt applies fn to the sidecar file of h.
func corrupt(t *testing.T, shim *storio.FileBackedPlatformIO, h Handle, fn func(path string, b []byte) []byte) {
	t.Helper()
	p := sidecarFile(t, shim, h.SegmentName)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if out := fn(p, b); out != nil {
		if err := os.WriteFile(p, out, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// A sidecar that is missing, damaged, truncated, from another format version or for another
// table is never used: the table is served from its footer, and - where the store may write -
// the sidecar is rebuilt.
func TestBadSidecarsFallBackToTheFooter(t *testing.T) {
	cases := map[string]func(path string, b []byte) []byte{
		"missing": func(p string, _ []byte) []byte { os.Remove(p); return nil },
		"flipped": func(_ string, b []byte) []byte { b[len(b)/2] ^= 0x40; return b },
		"truncated": func(_ string, b []byte) []byte {
			return b[:len(b)-100]
		},
		"empty": func(_ string, b []byte) []byte { return []byte{} },
		"version": func(_ string, b []byte) []byte {
			binary.LittleEndian.PutUint32(b[8:], sidecarVersion+1)
			return b
		},
		"other-table": func(_ string, b []byte) []byte {
			b[64] ^= 1 // the footer hash it was written for
			// Re-seal the checksum, so only the identity check can catch it.
			binary.LittleEndian.PutUint32(b[len(b)-8:], crc32Of(b[:len(b)-8]))
			return b
		},
	}
	for name, damage := range cases {
		for _, build := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "/readonly", true: "/rebuild"}[build], func(t *testing.T) {
				shim, _ := osShim(t)
				keys, h := writeMixedTable(t, shim, 0, 120, 5)
				corrupt(t, shim, h, damage)
				store := NewLsmBlobStore(shim, sidecarNS, nil)
				store.SetSidecarBuild(build)
				store.AddTable(h)
				for i, k := range keys {
					_, deleted, found := store.Lookup(k)
					if !found || deleted != (i%5 == 4) {
						t.Fatalf("key %d: found=%v deleted=%v", i, found, deleted)
					}
				}
				absent := randomHash(rand.New(rand.NewSource(9)))
				if _, _, found := store.Lookup(absent); found {
					t.Fatal("found an absent key")
				}
				heap := store.IndexCacheBytes()
				sc, err := openTableSidecar(t, shim, h)
				if build {
					if err != nil {
						t.Fatalf("not rebuilt: %v", err)
					}
					sc.close()
					if heap != 0 {
						t.Fatalf("rebuilt, yet %d bytes of footer index on the heap", heap)
					}
				} else {
					if err == nil {
						sc.close()
						t.Fatal("a read-only store rewrote the sidecar")
					}
					if heap == 0 {
						t.Fatal("served without a sidecar, yet no footer index was loaded")
					}
				}
				store.Close()
			})
		}
	}
}

func crc32Of(b []byte) uint32 { return crc32.Checksum(b, sidecarCRCTable) }

// Compaction writes a sidecar for its output, and its inputs' sidecars go with them.
func TestCompactionKeepsSidecarsConsistent(t *testing.T) {
	shim, root := osShim(t)
	store := NewLsmBlobStore(shim, sidecarNS, nil)
	var all []codec.Hash
	for i := 0; i < 5; i++ {
		keys, h := writeMixedTable(t, shim, 0, 150, int64(100+i))
		store.AddTable(h)
		all = append(all, keys...)
	}
	for _, k := range all[:50] {
		store.Lookup(k) // map every input's sidecar
	}
	inputs := store.Tables()
	dropped := map[codec.Hash]bool{}
	for i := 0; i < len(all); i += 7 {
		dropped[all[i]] = true
	}
	res, err := store.Compact(2, func(k codec.Hash) bool { return !dropped[k] })
	if err != nil {
		t.Fatal(err)
	}
	if res.Merged != 5 || res.Removed != 5 || len(res.Outputs) != 1 {
		t.Fatalf("compaction %+v", res)
	}
	if res.Dropped != len(dropped) {
		t.Fatalf("dropped %d keys, want %d", res.Dropped, len(dropped))
	}
	for _, in := range inputs {
		if _, err := os.Stat(sidecarFile(t, shim, in.SegmentName)); !os.IsNotExist(err) {
			t.Fatalf("input %s's sidecar outlived it: %v", in.SegmentName, err)
		}
	}
	out := store.Tables()
	if len(out) != 1 {
		t.Fatalf("%d tables after compaction", len(out))
	}
	sc, err := openTableSidecar(t, shim, out[0])
	if err != nil {
		t.Fatalf("compaction output has no usable sidecar: %v", err)
	}
	sc.close()
	for i, k := range all {
		// Every fifth key of each table is a tombstone, which a full compaction drops too.
		want := !dropped[k] && i%150%5 != 4
		if _, _, found := store.Lookup(k); found != want {
			t.Fatalf("key %d: found=%v, want %v", i, found, want)
		}
	}
	if n := store.IndexCacheBytes(); n != 0 {
		t.Fatalf("%d bytes of footer index on the heap", n)
	}
	assertSidecarsMatchTables(t, store, root)
	before := mmapfile.MappedBytes()
	store.Close()
	if after := mmapfile.MappedBytes(); after >= before {
		t.Fatalf("Close left the sidecars mapped (%d -> %d bytes)", before, after)
	}
	if _, _, found := store.Lookup(all[1]); !found {
		t.Fatal("a closed store must still answer, from the footers")
	}
}

// assertSidecarsMatchTables checks that every sidecar on disk belongs to a registered table.
func assertSidecarsMatchTables(t *testing.T, store *LsmBlobStore, root string) {
	t.Helper()
	registered := map[string]bool{}
	for _, h := range store.Tables() {
		registered[h.SegmentName] = true
	}
	dir := filepath.Join(root, "ns", filepath.FromSlash(sidecarNS), "sstidx")
	filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		table, ok := sidecarTableName(filepath.ToSlash(rel))
		if !ok || !registered[table] {
			t.Errorf("sidecar %s has no registered table", rel)
		}
		return nil
	})
}

func TestRemoveOrphanSidecars(t *testing.T) {
	shim, root := osShim(t)
	_, keep := writeMixedTable(t, shim, 0, 50, 1)
	_, gone := writeMixedTable(t, shim, 1, 50, 2)
	if err := shim.DeleteSegment(gone.SegmentName); err != nil {
		t.Fatal(err)
	}
	tmp := sidecarFile(t, shim, keep.SegmentName) + sidecarTempMarker + "123"
	if err := os.WriteFile(tmp, []byte("half"), 0o644); err != nil {
		t.Fatal(err)
	}
	store := NewLsmBlobStore(shim, sidecarNS, nil)
	if err := store.DiscoverTables(); err != nil {
		t.Fatal(err)
	}
	if n := len(store.Tables()); n != 1 {
		t.Fatalf("discovered %d tables: a sidecar was taken for a table?", n)
	}
	store.RemoveOrphanSidecars()
	if _, err := os.Stat(sidecarFile(t, shim, gone.SegmentName)); !os.IsNotExist(err) {
		t.Fatal("orphan sidecar survived")
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatal("temporary file survived")
	}
	if _, err := os.Stat(sidecarFile(t, shim, keep.SegmentName)); err != nil {
		t.Fatalf("a live table's sidecar was removed: %v", err)
	}
	assertSidecarsMatchTables(t, store, root)
}

// The in-memory shim keeps no files, so no sidecar: the footer path is what serves it.
func TestNoSidecarWithoutLocalFiles(t *testing.T) {
	io := storio.NewInMemoryPlatformIO()
	keys, h := writeSizedTable(t, io, sidecarNS, 20, 16, 1)
	store := NewLsmBlobStore(io, sidecarNS, nil)
	store.AddTable(h)
	if _, _, found := store.Lookup(keys[3]); !found {
		t.Fatal("lost a key")
	}
	if store.sidecarsOn() || store.IndexCacheBytes() == 0 {
		t.Fatal("expected the footer path")
	}
}

func TestSidecarsCanBeTurnedOff(t *testing.T) {
	t.Setenv("KDB_SSTABLE_SIDECAR", "off")
	shim, _ := osShim(t)
	keys, h := writeMixedTable(t, shim, 0, 20, 1)
	if _, err := os.Stat(sidecarFile(t, shim, h.SegmentName)); !os.IsNotExist(err) {
		t.Fatal("a sidecar was written with sidecars off")
	}
	store := NewLsmBlobStore(shim, sidecarNS, nil)
	store.AddTable(h)
	if _, _, found := store.Lookup(keys[0]); !found || store.IndexCacheBytes() == 0 {
		t.Fatal("expected the footer path")
	}
}

// A sidecar truncated underneath its mapping faults on the next touch. That must surface as an
// error - the table then answers from its footer - and not take the process down with SIGBUS.
func TestTruncatedMappedSidecarIsAnErrorNotACrash(t *testing.T) {
	shim, _ := osShim(t)
	keys, h := writeMixedTable(t, shim, 0, 800, 11)
	store := NewLsmBlobStore(shim, sidecarNS, nil)
	store.SetSidecarBuild(false)
	store.AddTable(h)
	if _, _, found := store.Lookup(keys[0]); !found {
		t.Fatal("lost a key")
	}
	if store.SidecarMappedBytes() == 0 {
		t.Fatal("sidecar not mapped")
	}
	sc := store.sidecarFor(h)
	if err := os.Truncate(sidecarFile(t, shim, h.SegmentName), 0); err != nil {
		t.Fatal(err)
	}
	// The raw search faults: every page of the mapping is now past the end of the file.
	if _, _, err := sc.find(&keys[1]); err == nil {
		t.Fatal("search of a truncated mapping did not fault")
	}
	if err := sc.forEach(func(codec.Hash, BlockHandle) bool { return true }); err == nil {
		t.Fatal("iteration of a truncated mapping did not fault")
	}
	// Through the store every key still answers, from the footer.
	for i, k := range keys {
		if _, deleted, found := store.Lookup(k); !found || deleted != (i%5 == 4) {
			t.Fatalf("key %d after truncation: found=%v deleted=%v", i, found, deleted)
		}
	}
	if store.IndexCacheBytes() == 0 {
		t.Fatal("expected the footer index after the fault")
	}
	store.Close()
}

// Lookups never miss, and never touch an unmapped sidecar, while tables are flushed in and
// compacted away underneath them. Run under -race.
func TestSidecarLookupsUnderConcurrentCompaction(t *testing.T) {
	shim, root := osShim(t)
	store := NewLsmBlobStore(shim, sidecarNS, NewBlockCache(1<<20))
	var mu sync.Mutex
	var present []codec.Hash
	for i := 0; i < 3; i++ {
		keys, h := writeSizedTable(t, shim, sidecarNS, 40, 24, int64(i+800))
		store.AddTable(h)
		present = append(present, keys...)
	}
	stop := make(chan struct{})
	var misses, probes atomic.Int64
	var readers sync.WaitGroup
	for g := 0; g < 6; g++ {
		readers.Add(1)
		go func(g int) {
			defer readers.Done()
			rng := rand.New(rand.NewSource(int64(g)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				mu.Lock()
				k := present[rng.Intn(len(present))]
				mu.Unlock()
				probes.Add(1)
				switch g % 3 {
				case 0:
					if v, _, found := store.Lookup(k); !found || v == nil {
						misses.Add(1)
					}
				case 1:
					if !store.Contains(k) {
						misses.Add(1)
					}
				default:
					absent := randomHash(rng)
					if store.Contains(absent) {
						misses.Add(1)
					}
				}
			}
		}(g)
	}
	var writers sync.WaitGroup
	writers.Add(2)
	go func() {
		defer writers.Done()
		for i := 0; i < 15; i++ {
			keys, h := writeSizedTable(t, shim, sidecarNS, 30, 24, int64(i+900))
			store.AddTable(h)
			mu.Lock()
			present = append(present, keys...)
			mu.Unlock()
		}
	}()
	go func() {
		defer writers.Done()
		for i := 0; i < 20; i++ {
			if _, err := store.Compact(2, nil); err != nil {
				t.Error(err)
			}
		}
	}()
	writers.Wait()
	close(stop)
	readers.Wait()
	if n := misses.Load(); n > 0 {
		t.Fatalf("%d of %d probes answered wrongly while compactions ran", n, probes.Load())
	}
	assertSidecarsMatchTables(t, store, root)
	// Every table still on disk is registered, and every one has its sidecar.
	registered := map[string]bool{}
	for _, h := range store.Tables() {
		registered[h.SegmentName] = true
		if _, err := os.Stat(sidecarFile(t, shim, h.SegmentName)); err != nil {
			t.Errorf("table %s has no sidecar: %v", h.SegmentName, err)
		}
	}
	names, _ := shim.ListSegments(sidecarNS)
	for _, n := range names {
		if strings.Contains(n, "/sstable/") && !registered[n] {
			t.Fatalf("segment %s is on disk but not registered", n)
		}
	}
	store.Close()
	t.Logf("%d probes, 0 wrong answers", probes.Load())
}

// On every platform - including those that cannot map, where no sidecar is ever written - a
// file-backed store answers every key, tombstone and absent key correctly.
func TestFileBackedLookupsOnEveryPlatform(t *testing.T) {
	shim, _ := fileShim(t)
	store := NewLsmBlobStore(shim, sidecarNS, nil)
	var all []codec.Hash
	for i := 0; i < 3; i++ {
		keys, h := writeMixedTable(t, shim, 0, 100, int64(40+i))
		store.AddTable(h)
		all = append(all, keys...)
		_, err := os.Stat(sidecarFile(t, shim, h.SegmentName))
		if mmapfile.Supported != (err == nil) {
			t.Fatalf("mapping supported=%v, sidecar written: %v", mmapfile.Supported, err)
		}
	}
	for i, k := range all {
		_, deleted, found := store.Lookup(k)
		if !found || deleted != (i%100%5 == 4) {
			t.Fatalf("key %d: found=%v deleted=%v", i, found, deleted)
		}
	}
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < 200; i++ {
		if k := randomHash(rng); store.Contains(k) {
			t.Fatal("found an absent key")
		}
	}
	if !mmapfile.Supported && (store.sidecarsOn() || store.IndexCacheBytes() == 0) {
		t.Fatal("without mapping the footer path must serve")
	}
	store.Close()
}
