package interop

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/compression"
)

// The zstd golden corpus - docs/kdb-physical-layer-compat-test-plan.md §6 (roadmap R1).
//
// Fixtures are zstd_corpus_<payload>_<variant>.hex. <payload> names a deterministic payload both
// languages generate (zstdCorpusPayload here, ZstdTestCorpus in kdb-compression's commonTest).
// Go writes klauspost frames at several levels; Kotlin writes zstd-jni frames (the JVM) and
// pure-Kotlin encoder frames (_purekotlin: exactly what Kotlin/JS and Kotlin/Native write - their
// tests assert the bytes). Every target's Kotlin decoder reads Go's; this side reads Kotlin's.

// zstdCorpusJSON must stay byte-identical to ZstdTestCorpus.json in Kotlin: JSON-like records
// drawn from a 32-bit LCG, truncated to exactly size bytes.
func zstdCorpusJSON(size int) []byte {
	x := uint32(0x12345678)
	next := func() uint32 {
		x = x*1664525 + 1013904223
		return x >> 16
	}
	var b strings.Builder
	for i := 0; b.Len() < size; i++ {
		fmt.Fprintf(&b, `{"id":%d,"name":"user-%d","tags":["t%d","t%d"],"score":%d,"active":%t}`+"\n",
			i, next()%1000, next()%17, next()%17, next()%100000, next()%2 == 0)
	}
	return []byte(b.String()[:size])
}

func zstdCorpusPayload(t *testing.T, spec string) []byte {
	t.Helper()
	switch {
	case spec == "empty":
		return []byte{}
	case strings.HasPrefix(spec, "json") && strings.HasSuffix(spec, "k"):
		n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(spec, "json"), "k"))
		if err != nil {
			t.Fatalf("bad corpus payload %q: %v", spec, err)
		}
		return zstdCorpusJSON(n * 1000)
	}
	t.Fatalf("unknown corpus payload %q", spec)
	return nil
}

func TestExportPhysicalGoldenZstdCorpus(t *testing.T) {
	export := func(spec string, level int) {
		body, err := compression.Compress(zstdCorpusPayload(t, spec), level)
		if err != nil {
			t.Fatal(err)
		}
		writePhysicalGolden(t, fmt.Sprintf("zstd_corpus_%s_l%d.hex", spec, level), body)
	}
	// klauspost collapses levels onto four encoders: 1 fastest, 3 default, 7 better, 19 best.
	for _, level := range []int{1, 3, 7, 19} {
		export("json20k", level)
	}
	export("json140k", 3) // two blocks: a match may reach back into the first
}

// The corpus checks the payload generators agree before trusting any decode against them: the
// first records are pinned literally, so drift in either language fails here, not as a
// confusing "decoded to the wrong bytes".
func TestZstdCorpusPayloadIsPinned(t *testing.T) {
	const want = `{"id":0,"name":"user-19","tags":["t15","t1"],"score":19292,"active":true}` + "\n"
	if got := string(zstdCorpusJSON(len(want))); got != want {
		t.Fatalf("corpus prefix %q, want %q", got, want)
	}
	if n := len(zstdCorpusJSON(140_000)); n != 140_000 {
		t.Fatalf("corpus length %d", n)
	}
}

// Go must decode every frame Kotlin wrote: zstd-jni's (the JVM) and the pure-Kotlin encoder's
// (Kotlin/JS and Kotlin/Native).
func TestKotlinZstdCorpusCrossDecodes(t *testing.T) {
	names, err := filepath.Glob(filepath.Join(physicalGoldenDir(t, "kotlin"), "zstd_corpus_*.hex"))
	if err != nil {
		t.Fatal(err)
	}
	if len(names) == 0 {
		t.Skip("no Kotlin zstd corpus exported yet (run make test-physical-golden)")
	}
	sort.Strings(names)
	pure := 0
	for _, path := range names {
		name := filepath.Base(path)
		spec := strings.SplitN(strings.TrimPrefix(name, "zstd_corpus_"), "_", 2)[0]
		frame := readKotlinGolden(t, name)
		want := zstdCorpusPayload(t, spec)
		got, err := compression.Decompress(frame, len(want)+1)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s decoded to %d bytes that differ from the %d-byte payload", name, len(got), len(want))
		}
		if strings.HasSuffix(name, "_purekotlin.hex") {
			pure++
		}
	}
	if pure < 3 {
		t.Fatalf("found %d pure-Kotlin corpus frames, want at least 3 - is the Kotlin export stale?", pure)
	}
}
