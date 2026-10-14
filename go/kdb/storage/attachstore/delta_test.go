package attachstore

import (
	"bytes"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
)

// tinyDeltas configures a store so a few-MB version takes every path a multi-GB one does: pages of
// 4 KiB, frames of a few instructions, an index of a few hundred entries (so the block grows far
// past its minimum), and steps far smaller than a version.
func tinyDeltas(cfg Config) Config {
	cfg.UploadGrace = time.Nanosecond
	cfg.DeltaIndexEntries = 256
	cfg.DeltaStepBytes = 300 << 10
	cfg.deltaPageBytes = 4 << 10
	cfg.deltaFrameOps = 3
	return cfg
}

func splice(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// deltaInSteps runs one pair to its end in budgeted steps and returns the outcome and the steps.
func deltaInSteps(t *testing.T, s *Store, old, base codec.Hash) (DeltaOutcome, int) {
	t.Helper()
	for steps := 1; ; steps++ {
		out, used, err := s.DeltaStep(old, base, 0)
		if err != nil {
			t.Fatal(err)
		}
		if used > s.cfg.DeltaStepBytes+chunk {
			t.Fatalf("a step used %d bytes of a %d budget", used, s.cfg.DeltaStepBytes)
		}
		if out != DeltaPending {
			return out, steps
		}
		s.mu.Lock()
		if job := s.job; job != nil && len(job.enc.index) > s.cfg.DeltaIndexEntries {
			t.Fatalf("index holds %d entries, over %d", len(job.enc.index), s.cfg.DeltaIndexEntries)
		}
		s.mu.Unlock()
	}
}

func segmentBytes(t *testing.T, s *Store, name string) int64 {
	n, err := segmentLength(s.shim, name)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// Versions several chunks long, diffed in many small steps, read back exactly - sequentially and
// at random offsets - whatever changed between them; and a small change costs a small delta.
func TestFramedDeltasOfEveryShape(t *testing.T) {
	r := rand.New(rand.NewSource(11))
	base := randomBytes(r, 6*chunk+12345)
	edited := append([]byte(nil), base...)
	copy(edited[2*chunk+500:], []byte("an edit in the middle of chunk two"))
	copy(edited[5*chunk-3:], []byte("across a chunk boundary"))
	cases := []struct {
		name   string
		target []byte
		max    int64 // the delta may be at most this big; 0: not stored
	}{
		{"in-place edits", edited, 64 << 10},
		{"insertion near the start", splice(base[:777], []byte("INSERTED BYTES"), base[777:]), 64 << 10},
		{"deletion", splice(base[:3*chunk+5], base[3*chunk+5000:]), 64 << 10},
		{"appended", splice(base, randomBytes(r, 300_000)), 400 << 10},
		{"truncated", base[:4*chunk+99], 64 << 10},
		{"reordered", splice(base[4*chunk+1:], base[:4*chunk+1]), 64 << 10},
		{"unrelated", randomBytes(r, 3*chunk), 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, _, _ := newTestStore(t, tinyDeltas(Config{}))
			bi, _ := s.Put(bytes.NewReader(base))
			oi, _ := s.Put(bytes.NewReader(c.target))
			out, steps := deltaInSteps(t, s, oi.Hash, bi.Hash)
			if c.max == 0 {
				if out != DeltaSkipped {
					t.Fatalf("unrelated bytes became a delta: %v", out)
				}
				if again, _, _ := s.DeltaStep(oi.Hash, bi.Hash, 0); again != DeltaSkipped {
					t.Fatal("a declined pair was tried again")
				}
				return
			}
			if out != DeltaStored {
				t.Fatalf("not stored: %v", out)
			}
			if steps < 3 {
				t.Fatalf("done in %d steps; the budget did not bound them", steps)
			}
			p := pointerOf(t, s, oi.Hash)
			if !p.delta || !p.framed {
				t.Fatalf("pointer %+v is not a format 2 delta", p)
			}
			if n := segmentBytes(t, s, p.segment); n > c.max {
				t.Fatalf("a %d-byte delta for this change", n)
			}
			if got := readBack(t, s, oi.Hash); !bytes.Equal(got, c.target) {
				t.Fatal("the delta reads back different bytes")
			}
			rd, err := s.Open(oi.Hash)
			if err != nil {
				t.Fatal(err)
			}
			defer rd.Close()
			for i := 0; i < 200; i++ {
				off := r.Int63n(int64(len(c.target)))
				n := min(int64(r.Intn(3*chunk)), int64(len(c.target))-off)
				got := make([]byte, n)
				if _, err := rd.ReadAt(got, off); err != nil || !bytes.Equal(got, c.target[off:off+n]) {
					t.Fatalf("ReadAt(%d, %d): %v", off, n, err)
				}
			}
		})
	}
}

// A chain of format 2 deltas reads back at every step, and Collect keeps the chain.
func TestFramedDeltaChain(t *testing.T) {
	s, shim, _ := newTestStore(t, tinyDeltas(Config{}))
	v := versions(rand.New(rand.NewSource(12)), 4, 3*chunk+100)
	var hashes []codec.Hash
	for _, b := range v {
		info, _ := s.Put(bytes.NewReader(b))
		hashes = append(hashes, info.Hash)
	}
	for i := len(hashes) - 2; i >= 0; i-- {
		if out, _ := deltaInSteps(t, s, hashes[i], hashes[i+1]); out != DeltaStored {
			t.Fatalf("version %d: %v", i, out)
		}
	}
	if _, err := s.Collect(func(h codec.Hash) bool { return h == hashes[0] }); err != nil {
		t.Fatal(err)
	}
	if got := readBack(t, s, hashes[0]); !bytes.Equal(got, v[0]) {
		t.Fatal("the oldest version reads wrong through its chain")
	}
	if _, err := s.Collect(func(codec.Hash) bool { return false }); err != nil {
		t.Fatal(err)
	}
	if names, _ := shim.ListSegments("app/data"); len(names) != 0 {
		t.Fatalf("left behind: %v", names)
	}
}

// A job in progress survives a Collect that keeps its versions, is abandoned (and its segments
// deleted) by a step for another pair or a Collect that drops its version, and a reader open on
// the old version through the swap finishes on the whole copy.
func TestDeltaJobsAcrossCollectAndOtherPairs(t *testing.T) {
	s, shim, _ := newTestStore(t, tinyDeltas(Config{}))
	v := versions(rand.New(rand.NewSource(13)), 3, 4*chunk+1)
	var h []codec.Hash
	for _, b := range v {
		info, _ := s.Put(bytes.NewReader(b))
		h = append(h, info.Hash)
	}
	all := func(codec.Hash) bool { return true }
	jobSegs := func() []string {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.job == nil {
			return nil
		}
		return []string{s.job.seg, s.job.framesSeg}
	}
	exists := func(name string) bool {
		names, _ := shim.ListSegments("app/data")
		for _, n := range names {
			if n == name {
				return true
			}
		}
		return false
	}

	var segs []string
	for len(segs) == 0 || !exists(segs[0]) || !exists(segs[1]) {
		if out, _, err := s.DeltaStep(h[0], h[1], 50<<10); err != nil || out != DeltaPending {
			t.Fatalf("step: %v %v", out, err)
		}
		segs = jobSegs()
	}
	if _, err := s.Collect(all); err != nil {
		t.Fatal(err)
	}
	for _, name := range segs {
		if !exists(name) {
			t.Fatalf("collect deleted the job's %s", name)
		}
	}
	// Another pair abandons it.
	if out, _, err := s.DeltaStep(h[1], h[2], 0); err != nil || out != DeltaPending {
		t.Fatalf("other pair: %v %v", out, err)
	}
	for _, name := range segs {
		if exists(name) {
			t.Fatalf("the abandoned job's %s survived", name)
		}
	}
	// A Collect dropping its old version abandons it too.
	segs = jobSegs()
	if _, err := s.Collect(func(x codec.Hash) bool { return x != h[1] }); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	gone := s.job == nil
	s.mu.Unlock()
	if !gone {
		t.Fatal("the job outlived its version")
	}
	for _, name := range segs {
		if exists(name) {
			t.Fatalf("the dropped job's %s survived", name)
		}
	}

	// A reader open across the swap.
	rd, err := s.Open(h[0])
	if err != nil {
		t.Fatal(err)
	}
	if out, _ := deltaInSteps(t, s, h[0], h[2]); out != DeltaStored {
		t.Fatalf("delta: %v", out)
	}
	if _, deferred := s.PinnedSegments(); deferred == 0 {
		t.Fatal("nothing waited on the open reader")
	}
	got := make([]byte, len(v[0]))
	if _, err := rd.ReadAt(got, 0); err != nil || !bytes.Equal(got, v[0]) {
		t.Fatalf("the open reader: %v", err)
	}
	rd.Close()
	if got := readBack(t, s, h[0]); !bytes.Equal(got, v[0]) {
		t.Fatal("the delta reads wrong")
	}
}

// A version whose pointer changes mid-job (here: it is re-stored as a delta by a store sharing
// nothing with the job) is not swapped; the job ends without a delta.
func TestDeltaJobLeavesAChangedVersionAlone(t *testing.T) {
	s, _, _ := newTestStore(t, tinyDeltas(Config{}))
	v := versions(rand.New(rand.NewSource(14)), 2, 3*chunk)
	oi, _ := s.Put(bytes.NewReader(v[0]))
	ni, _ := s.Put(bytes.NewReader(v[1]))
	if out, _, err := s.DeltaStep(oi.Hash, ni.Hash, 0); err != nil || out != DeltaPending {
		t.Fatalf("first step: %v %v", out, err)
	}
	cur := pointerOf(t, s, oi.Hash)
	cur.segment = cur.segment + "-moved" // as if replaced under the job
	s.mu.Lock()
	s.cache.put(oi.Hash, cur)
	s.mu.Unlock()
	if out, _, err := s.DeltaStep(oi.Hash, ni.Hash, 0); err != nil || out == DeltaStored {
		t.Fatalf("stored over a changed version: %v %v", out, err)
	}
}

// Damage to a format 2 delta - its footer, its index, its instructions - reads as ErrCorrupt.
func TestDamagedFramedDeltaIsCorrupt(t *testing.T) {
	r := rand.New(rand.NewSource(15))
	base := randomBytes(r, 3*chunk)
	target := splice(base[:1000], []byte("x"), base[1000:])
	for _, at := range []string{"footer", "index", "ops"} {
		t.Run(at, func(t *testing.T) {
			s, shim, _ := newTestStore(t, tinyDeltas(Config{}))
			bi, _ := s.Put(bytes.NewReader(base))
			oi, _ := s.Put(bytes.NewReader(target))
			if did, err := s.StoreAsDelta(oi.Hash, bi.Hash); err != nil || !did {
				t.Fatalf("delta: %v %v", did, err)
			}
			seg := pointerOf(t, s, oi.Hash).segment
			n := segmentBytes(t, s, seg)
			raw, _ := shim.ReadFromSegment(seg, 0, int(n))
			raw = append([]byte(nil), raw...)
			d, err := openFramedDelta(memSource(base), segSource{shim: shim, name: seg, n: n})
			if err != nil {
				t.Fatal(err)
			}
			e, _ := d.entry(0)
			switch at {
			case "footer":
				raw[n-1] ^= 0xff
			case "index":
				raw[d.indexOff+16+7] ^= 0x40 // frame 0's instruction offset
			case "ops":
				raw[e.opsOff] = 0x7f
			}
			shim.DeleteSegment(seg)
			shim.AppendToSegment(seg, raw)
			again := New(shim, "app/data", s.small, s.cfg)
			rd, err := again.Open(oi.Hash)
			if err == nil {
				_, err = rd.ReadAt(make([]byte, len(target)), 0)
				rd.Close()
			}
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("damaged %s read as %v", at, err)
			}
		})
	}
}

// A store with a size limit leaves larger versions whole.
func TestDeltaMaxBytesStillLimits(t *testing.T) {
	s, _, _ := newTestStore(t, tinyDeltas(Config{DeltaMaxBytes: 2 * chunk}))
	v := versions(rand.New(rand.NewSource(16)), 2, 3*chunk)
	oi, _ := s.Put(bytes.NewReader(v[0]))
	ni, _ := s.Put(bytes.NewReader(v[1]))
	if out, used, err := s.DeltaStep(oi.Hash, ni.Hash, 0); err != nil || out != DeltaSkipped || used != 0 {
		t.Fatalf("over the limit: %v %d %v", out, used, err)
	}
}

// A delta that does not read back as its version - damaged between encoding and the swap, or
// mis-encoded - is caught by the verify phase: the whole copy stays, and the pair is not retried.
func TestVerifyKeepsTheWholeCopyOfABadDelta(t *testing.T) {
	s, shim, _ := newTestStore(t, tinyDeltas(Config{}))
	r := rand.New(rand.NewSource(17))
	base := randomBytes(r, 3*chunk)
	target := splice(base[:chunk], randomBytes(r, 40_000), base[chunk+40_000:])
	bi, _ := s.Put(bytes.NewReader(base))
	oi, _ := s.Put(bytes.NewReader(target))
	for {
		out, _, err := s.DeltaStep(oi.Hash, bi.Hash, 0)
		if err != nil || out != DeltaPending {
			t.Fatalf("finished before verifying: %v %v", out, err)
		}
		s.mu.Lock()
		job := s.job
		s.mu.Unlock()
		if job.phase == jobVerify {
			if job.vPos > chunk {
				t.Fatalf("verified past the bytes to damage (%d)", job.vPos)
			}
			n := job.enc.written
			raw, _ := shim.ReadFromSegment(job.seg, 0, int(n))
			raw = append([]byte(nil), raw...)
			d, err := openFramedDelta(memSource(base), segSource{shim: shim, name: job.seg, n: n})
			if err != nil {
				t.Fatal(err)
			}
			if err := d.find(chunk + 100); err != nil {
				t.Fatal(err)
			}
			for _, in := range d.ins {
				if !in.copy {
					raw[in.off] ^= 1 // a literal byte the target needs
					break
				}
			}
			shim.DeleteSegment(job.seg)
			shim.AppendToSegment(job.seg, raw)
			break
		}
	}
	if out, _ := deltaInSteps(t, s, oi.Hash, bi.Hash); out != DeltaSkipped {
		t.Fatalf("a bad delta was stored: %v", out)
	}
	p := pointerOf(t, s, oi.Hash)
	if p.delta {
		t.Fatal("the whole copy was replaced")
	}
	if got := readBack(t, s, oi.Hash); !bytes.Equal(got, target) {
		t.Fatal("the version reads wrong")
	}
	if names, _ := shim.ListSegments("app/data"); len(names) != 6 { // 2 pointers, 2 segments, 2 leaves
		t.Fatalf("the bad delta's segments were left: %v", names)
	}
}

// With deltas disabled every version stays whole: nothing is encoded or written.
func TestDisableDeltasKeepsEveryVersionWhole(t *testing.T) {
	s, shim, _ := newTestStore(t, tinyDeltas(Config{DisableDeltas: true}))
	v := versions(rand.New(rand.NewSource(18)), 2, 3*chunk)
	oi, _ := s.Put(bytes.NewReader(v[0]))
	ni, _ := s.Put(bytes.NewReader(v[1]))
	before, _ := shim.ListSegments("app/data")
	if out, used, err := s.DeltaStep(oi.Hash, ni.Hash, 0); err != nil || out != DeltaSkipped || used != 0 {
		t.Fatalf("a step with deltas disabled: %v %d %v", out, used, err)
	}
	if did, err := s.StoreAsDelta(oi.Hash, ni.Hash); err != nil || did {
		t.Fatalf("StoreAsDelta with deltas disabled: %v %v", did, err)
	}
	if after, _ := shim.ListSegments("app/data"); len(after) != len(before) {
		t.Fatalf("segments %v became %v", before, after)
	}
	if got := readBack(t, s, oi.Hash); !bytes.Equal(got, v[0]) {
		t.Fatal("the old version reads wrong")
	}
}

// With deltas disabled, InflateStep turns deltas stored before - a whole chain of them - back into
// whole copies, in bounded steps, while a reader open on one finishes on the delta it opened.
func TestInflateTurnsDeltasBackIntoWholeCopies(t *testing.T) {
	s, shim, small := newTestStore(t, tinyDeltas(Config{}))
	v := versions(rand.New(rand.NewSource(19)), 3, 3*chunk+77)
	var h []codec.Hash
	for _, b := range v {
		info, _ := s.Put(bytes.NewReader(b))
		h = append(h, info.Hash)
	}
	for i := len(h) - 2; i >= 0; i-- {
		if did, err := s.StoreAsDelta(h[i], h[i+1]); err != nil || !did {
			t.Fatalf("delta %d: %v %v", i, did, err)
		}
	}
	off := New(shim, "app/data", small, tinyDeltas(Config{DisableDeltas: true}))
	if d, _ := off.Deltas(); len(d) != 2 {
		t.Fatalf("%d deltas, want 2", len(d))
	}
	rd, err := off.Open(h[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range []codec.Hash{h[1], h[0]} {
		steps := 0
		for {
			steps++
			done, used, err := off.InflateStep(x, 1<<20)
			if err != nil || used > 1<<20 {
				t.Fatalf("inflate: %v (used %d)", err, used)
			}
			if done {
				break
			}
		}
		if steps < 4 {
			t.Fatalf("inflated in %d steps; the budget did not bound them", steps)
		}
	}
	if d, _ := off.Deltas(); len(d) != 0 {
		t.Fatalf("still deltas: %v", d)
	}
	got := make([]byte, len(v[0]))
	if _, err := rd.ReadAt(got, 0); err != nil || !bytes.Equal(got, v[0]) {
		t.Fatalf("the reader open across the inflate: %v", err)
	}
	rd.Close()
	for i, x := range h {
		if got := readBack(t, off, x); !bytes.Equal(got, v[i]) {
			t.Fatalf("version %d reads wrong", i)
		}
	}
	if _, err := off.Collect(func(codec.Hash) bool { return true }); err != nil {
		t.Fatal(err)
	}
	if names, _ := shim.ListSegments("app/data"); len(names) != 9 { // 3 pointers, segments, leaves
		t.Fatalf("left behind: %v", names)
	}
	if done, used, err := off.InflateStep(h[2], 0); !done || used != 0 || err != nil {
		t.Fatalf("a whole blob: %v %d %v", done, used, err)
	}
}
