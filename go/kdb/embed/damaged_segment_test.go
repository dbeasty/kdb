package embed_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
)

// N5 (issue #43's third silent recovery path): damage in the middle of a delta segment used to
// read exactly like a torn tail at its end. Every scanner stopped at the first garbled header or
// CRC mismatch and recorded only the bytes before it, so replay dropped every commit after the
// damage with no error - and in the newest segment it accepted ANY fault as a torn tail, even
// with intact frames after it.
//
// The rule (#81): a torn tail can only be bytes at the very end of a segment with nothing intact
// after them. Damage followed by an intact frame is never a torn tail. Open must refuse it with a
// typed error naming the segment and offset - or recover provably without loss, which is what a
// checkpoint that already holds the damaged segment's commits allows - and never silently
// truncate. A real torn tail must keep recovering, in the newest segment and, after a restart has
// opened a newer one, in an older one: that is the kill -9 sequence.
//
// Frames are written uncompressed so a test can damage a known frame; every test asserts the head
// and which commits exist, not just that open succeeded.

const n5NS = "app/n5"

type n5Options struct {
	preallocate bool
}

func n5Open(t *testing.T, root string, o n5Options) (*embed.EmbeddedKdbRuntime, error) {
	t.Helper()
	opts := embed.FileRuntimeOptions{}
	none := storage.CompressionNone
	opts.Storage.Compression = &none
	if o.preallocate {
		opts.Storage.PreallocateSegments = true
		opts.Storage.DeltaMaxSegmentBytes = 1 << 20
	}
	return embed.OpenFileRuntimeWithOptions(root, "app", n5NS, schema.None(), opts)
}

// n5Run opens the namespace, writes n documents and closes it, returning their commits. Each run
// writes into a segment of its own, because every open starts a fresh one.
func n5Run(t *testing.T, root string, o n5Options, run, n int) []codec.Hash {
	t.Helper()
	rt, err := n5Open(t, root, o)
	if err != nil {
		t.Fatalf("run %d: open: %v", run, err)
	}
	defer rt.Close()
	var out []codec.Hash
	for i := 0; i < n; i++ {
		res, err := embed.PutJSONDocument(rt, n5NS, fmt.Sprintf(`{"id":"doc-%d-%d","needle":"needle-%d-%d"}`, run, i, run, i))
		if err != nil {
			t.Fatalf("run %d: put %d: %v", run, i, err)
		}
		out = append(out, res.Commit)
	}
	return out
}

// n5Segments returns the namespace's delta segment files holding at least one frame, in order.
func n5Segments(t *testing.T, root string) []string {
	t.Helper()
	dir := filepath.Join(root, "ns", "app", "n5", "delta")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".seg") {
			continue
		}
		p := filepath.Join(dir, e.Name())
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if len(frameOffsets(raw)) > 0 {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

// frameOffsets walks a segment's KDBP frames by their declared lengths, returning where each
// starts. Stops at zero padding.
func frameOffsets(raw []byte) []int {
	var out []int
	for off := 0; off+delta.PageFrameHeaderSize <= len(raw) && string(raw[off:off+4]) == "KDBP"; {
		n := int(raw[off+8])<<24 | int(raw[off+9])<<16 | int(raw[off+10])<<8 | int(raw[off+11])
		if n <= 0 || off+delta.PageFrameHeaderSize+n > len(raw) {
			break
		}
		out = append(out, off)
		off += delta.PageFrameHeaderSize + n
	}
	return out
}

func frameEnd(raw []byte, off int) int {
	n := int(raw[off+8])<<24 | int(raw[off+9])<<16 | int(raw[off+10])<<8 | int(raw[off+11])
	return off + delta.PageFrameHeaderSize + n
}

// damage kinds, each applied to the frame starting at off.
var n5Damages = map[string]func(raw []byte, off int){
	// A garbled header: the scanner stops here as quietly as at a torn tail.
	"magic": func(raw []byte, off int) { raw[off] ^= 0xFF },
	// A flipped bit in the body: the frame fails its CRC.
	"body": func(raw []byte, off int) { raw[(off+delta.PageFrameHeaderSize+frameEnd(raw, off))/2] ^= 0x01 },
	// The codec byte, outside the CRC: the frame is intact by CRC and will not decode.
	"codec": func(raw []byte, off int) { raw[off+5] = 0x7f },
}

func n5Damage(t *testing.T, path string, frame int, kind string) int {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	offs := frameOffsets(raw)
	if frame < 0 {
		frame += len(offs)
	}
	off := offs[frame]
	n5Damages[kind](raw, off)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return off
}

// assertRefusedAsDamaged checks open failed with a *delta.DamagedSegmentError naming the damaged
// segment file and the byte the damage starts at.
func assertRefusedAsDamaged(t *testing.T, rt *embed.EmbeddedKdbRuntime, err error, path string, offset int, last codec.Hash) {
	t.Helper()
	if err == nil {
		head, _ := rt.DAG.Head()
		rt.Close()
		t.Fatalf("open succeeded over a segment damaged in the middle: head %s, last acknowledged commit %s",
			head.Hex(), last.Hex())
	}
	var damaged *delta.DamagedSegmentError
	if !errors.As(err, &damaged) {
		t.Fatalf("open failed, but not with *delta.DamagedSegmentError: %v", err)
	}
	if !strings.Contains(filepath.Base(path), fmt.Sprintf("%020d", damaged.Sequence)) {
		t.Fatalf("error names segment %d, the damaged file is %s", damaged.Sequence, path)
	}
	if damaged.Offset != int64(offset) {
		t.Fatalf("error names byte %d, the damage is at %d: %v", damaged.Offset, offset, err)
	}
	for _, want := range []string{"kdb-inspect verify", "repair-segments"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error does not point the operator at %q: %v", want, err)
		}
	}
}

// assertHistory checks the open namespace's head and that exactly the commits in have, and none
// of those in lost, are in its graph.
func assertHistory(t *testing.T, rt *embed.EmbeddedKdbRuntime, head codec.Hash, have, lost []codec.Hash) {
	t.Helper()
	if got, _ := rt.DAG.Head(); got != head {
		t.Fatalf("head %s, want %s", got.Hex(), head.Hex())
	}
	for i, h := range have {
		if _, ok := rt.DAG.GetCommit(h); !ok {
			t.Fatalf("commit %d (%s) is missing after recovery", i, h.Hex())
		}
	}
	for i, h := range lost {
		if _, ok := rt.DAG.GetCommit(h); ok {
			t.Fatalf("commit %d (%s) should have been dropped with the torn tail", i, h.Hex())
		}
	}
}

func cat(parts ...[]codec.Hash) []codec.Hash {
	var out []codec.Hash
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func TestN5DamageInTheMiddleOfASegmentIsNeverSilentlyTruncated(t *testing.T) {
	for _, where := range []string{"not-newest", "newest"} {
		for _, kind := range []string{"magic", "body", "codec"} {
			for _, checkpoint := range []string{"checkpoint", "no-checkpoint"} {
				t.Run(where+"/"+kind+"/"+checkpoint, func(t *testing.T) {
					root := t.TempDir()
					var o n5Options
					runs := [][]codec.Hash{n5Run(t, root, o, 0, 4), n5Run(t, root, o, 1, 4)}
					all := cat(runs...)
					segs := n5Segments(t, root)
					if len(segs) != 2 {
						t.Fatalf("want one segment per run, got %d", len(segs))
					}
					victim := segs[0]
					if where == "newest" {
						victim = segs[1]
					}
					off := n5Damage(t, victim, 1, kind) // the second of four frames
					if checkpoint == "no-checkpoint" {
						removeCheckpoints(t, root)
					}

					rt, err := n5Open(t, root, o)
					if checkpoint == "no-checkpoint" {
						assertRefusedAsDamaged(t, rt, err, victim, off, all[len(all)-1])
						return
					}
					// A checkpoint written after the damaged segment was complete already holds
					// every commit it recorded; #81's in-place detection lets open use it, losing
					// only the damaged frame's body. That is recovery without loss of history.
					if err != nil {
						var damaged *delta.DamagedSegmentError
						if !errors.As(err, &damaged) {
							t.Fatalf("open failed, and not with *delta.DamagedSegmentError: %v", err)
						}
						t.Fatalf("a checkpoint holds every commit, so open should use it: %v", err)
					}
					defer rt.Close()
					assertHistory(t, rt, all[len(all)-1], all, nil)
				})
			}
		}
	}
}

// The exact probe #121 recorded: six commits in one run, a run that wrote nothing, the fourth
// frame's magic flipped, the checkpoint gone. Open succeeded at commit 3 of 6.
func TestN5ProbeFromU6c(t *testing.T) {
	root := t.TempDir()
	var o n5Options
	commits := n5Run(t, root, o, 0, 6)
	n5Run(t, root, o, 1, 0)
	segs := n5Segments(t, root)
	off := n5Damage(t, segs[0], 3, "magic")
	removeCheckpoints(t, root)

	rt, err := n5Open(t, root, o)
	assertRefusedAsDamaged(t, rt, err, segs[0], off, commits[5])
}

func TestN5TornTailInTheNewestSegmentStillRecovers(t *testing.T) {
	for _, shape := range []string{"cut-short", "body-unwritten"} {
		t.Run(shape, func(t *testing.T) {
			root := t.TempDir()
			var o n5Options
			first := n5Run(t, root, o, 0, 3)
			second := n5Run(t, root, o, 1, 3)
			segs := n5Segments(t, root)
			tearLastFrame(t, segs[1], shape)
			removeCheckpoints(t, root)

			rt, err := n5Open(t, root, o)
			if err != nil {
				t.Fatalf("a torn tail must still open: %v", err)
			}
			defer rt.Close()
			assertHistory(t, rt, second[1], cat(first, second[:2]), second[2:])
		})
	}
}

// tearLastFrame models an append that never completed: either the file ends partway through the
// frame (kill -9 during the write), or the frame is at full length with the second half of its
// body never written (zeros: a preallocated segment, or a power loss that kept the size).
func tearLastFrame(t *testing.T, path, shape string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	offs := frameOffsets(raw)
	off := offs[len(offs)-1]
	end := frameEnd(raw, off)
	switch shape {
	case "cut-short":
		raw = raw[:end-5]
	case "body-unwritten":
		mid := (off + delta.PageFrameHeaderSize + end) / 2
		for i := mid; i < end; i++ {
			raw[i] = 0
		}
	case "header-only":
		// Only the magic reached the file; the rest of the frame is still zero.
		for i := off + 4; i < end; i++ {
			raw[i] = 0
		}
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

// The kill -9 sequence: a run dies mid-append, leaving a torn tail at the end of its segment; the
// next run opens a newer segment and writes on top of the last intact commit, so the torn tail
// now sits at the end of a segment that is no longer the newest. That must keep recovering, with
// and without a checkpoint, and whether or not the run in between wrote anything.
func TestN5TornTailInAnOlderSegmentStillRecovers(t *testing.T) {
	for _, shape := range []string{"cut-short", "body-unwritten", "header-only"} {
		for _, between := range []int{0, 3} {
			for _, checkpoint := range []string{"checkpoint", "no-checkpoint"} {
				t.Run(fmt.Sprintf("%s/%d-written-after/%s", shape, between, checkpoint), func(t *testing.T) {
					root := t.TempDir()
					var o n5Options
					first := n5Run(t, root, o, 0, 3)
					segs := n5Segments(t, root)
					tearLastFrame(t, segs[0], shape)
					removeCheckpoints(t, root) // the killed run never wrote one
					second := n5Run(t, root, o, 1, between)
					if checkpoint == "no-checkpoint" {
						removeCheckpointsIfAny(t, root)
					}

					rt, err := n5Open(t, root, o)
					if err != nil {
						t.Fatalf("a torn tail in an older segment must still open: %v", err)
					}
					defer rt.Close()
					head := first[1]
					if len(second) > 0 {
						head = second[len(second)-1]
					}
					assertHistory(t, rt, head, cat(first[:2], second), first[2:])
				})
			}
		}
	}
}

func removeCheckpointsIfAny(t *testing.T, root string) {
	t.Helper()
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.Contains(filepath.ToSlash(path), "/snap/kdb_checkpoint_") {
			return os.Remove(path)
		}
		return nil
	})
}

// A preallocated segment's file is full size from the start; the bytes after its last frame are
// zeros. Those must read as the end of the segment, clean or after a torn frame, and in an older
// segment as well as the newest. Damage in the middle of one must still be refused.
func TestN5PreallocatedSegments(t *testing.T) {
	o := n5Options{preallocate: true}
	t.Run("clean", func(t *testing.T) {
		root := t.TempDir()
		first := n5Run(t, root, o, 0, 3)
		second := n5Run(t, root, o, 1, 3)
		removeCheckpoints(t, root)
		rt, err := n5Open(t, root, o)
		if err != nil {
			t.Fatalf("clean preallocated segments must open: %v", err)
		}
		defer rt.Close()
		assertHistory(t, rt, second[2], cat(first, second), nil)
	})
	for _, shape := range []string{"body-unwritten", "header-only"} {
		t.Run("torn-newest/"+shape, func(t *testing.T) {
			root := t.TempDir()
			first := n5Run(t, root, o, 0, 3)
			segs := n5Segments(t, root)
			tearLastFrame(t, segs[0], shape)
			removeCheckpoints(t, root)
			rt, err := n5Open(t, root, o)
			if err != nil {
				t.Fatalf("a torn preallocated segment must open: %v", err)
			}
			assertHistory(t, rt, first[1], first[:2], first[2:])
			rt.Close()
		})
		t.Run("torn-older/"+shape, func(t *testing.T) {
			root := t.TempDir()
			first := n5Run(t, root, o, 0, 3)
			segs := n5Segments(t, root)
			tearLastFrame(t, segs[0], shape)
			removeCheckpoints(t, root)
			second := n5Run(t, root, o, 1, 3)
			removeCheckpointsIfAny(t, root)
			rt, err := n5Open(t, root, o)
			if err != nil {
				t.Fatalf("a torn tail in an older preallocated segment must open: %v", err)
			}
			defer rt.Close()
			assertHistory(t, rt, second[2], cat(first[:2], second), first[2:])
		})
	}
	for _, checkpoint := range []string{"checkpoint", "no-checkpoint"} {
		t.Run("damaged-middle/"+checkpoint, func(t *testing.T) {
			root := t.TempDir()
			first := n5Run(t, root, o, 0, 4)
			second := n5Run(t, root, o, 1, 2)
			all := cat(first, second)
			segs := n5Segments(t, root)
			off := n5Damage(t, segs[0], 1, "magic")
			if checkpoint == "no-checkpoint" {
				removeCheckpoints(t, root)
				rt, err := n5Open(t, root, o)
				assertRefusedAsDamaged(t, rt, err, segs[0], off, all[len(all)-1])
				return
			}
			// The file of a preallocated segment is always full size, so in-place detection
			// compares where its frames end instead.
			rt, err := n5Open(t, root, o)
			if err != nil {
				t.Fatalf("a checkpoint holds every commit, so open should use it: %v", err)
			}
			defer rt.Close()
			assertHistory(t, rt, all[len(all)-1], all, nil)
		})
	}
}

// Damage in the LAST frame of a segment that is no longer the newest. Nothing intact follows it,
// so on its own it is indistinguishable from the torn tail an earlier crash leaves there. Two
// things still tell them apart, and both are used:
//   - a torn tail is never built on (it was never acknowledged), so a later segment's commit that
//     names the lost commit as its parent proves the tail was damaged after it was read: open
//     refuses, naming that segment;
//   - a checkpoint that covered the segment recorded its frames ending past the damage, so the
//     damage is in place and the checkpoint holds the commit: open uses it, losing nothing.
//
// What remains is a damaged last frame that nothing was built on and no checkpoint covered. That
// is byte-for-byte what a crash during that append leaves, so open recovers to the commit before
// it, as it must for the crash; this test pins that the decision is the torn-tail one.
func TestN5DamagedLastFrameOfAnOlderSegment(t *testing.T) {
	for _, kind := range []string{"magic", "body"} {
		t.Run("built-on/"+kind, func(t *testing.T) {
			root := t.TempDir()
			var o n5Options
			first := n5Run(t, root, o, 0, 3)
			second := n5Run(t, root, o, 1, 3)
			segs := n5Segments(t, root)
			off := n5Damage(t, segs[0], -1, kind)
			removeCheckpoints(t, root)

			rt, err := n5Open(t, root, o)
			if err == nil {
				head, _ := rt.DAG.Head()
				rt.Close()
				t.Fatalf("opened with a commit later segments build on missing; head %s, want refusal (last acknowledged %s)",
					head.Hex(), second[2].Hex())
			}
			var damaged *delta.DamagedSegmentError
			if !errors.As(err, &damaged) || !damaged.BuiltOn {
				t.Fatalf("want a *delta.DamagedSegmentError with BuiltOn naming the segment, got %v", err)
			}
			if !strings.Contains(filepath.Base(segs[0]), fmt.Sprintf("%020d", damaged.Sequence)) || damaged.Offset != int64(off) {
				t.Fatalf("error names segment %d byte %d, the damage is in %s at %d", damaged.Sequence, damaged.Offset, segs[0], off)
			}
			_ = first
		})
		t.Run("checkpoint/"+kind, func(t *testing.T) {
			root := t.TempDir()
			var o n5Options
			first := n5Run(t, root, o, 0, 3)
			second := n5Run(t, root, o, 1, 0)
			_ = second
			segs := n5Segments(t, root)
			n5Damage(t, segs[0], -1, kind)

			rt, err := n5Open(t, root, o)
			if err != nil {
				t.Fatalf("the checkpoint holds the damaged frame's commit, so open should use it: %v", err)
			}
			defer rt.Close()
			assertHistory(t, rt, first[2], first, nil)
		})
		t.Run("indistinguishable/"+kind, func(t *testing.T) {
			root := t.TempDir()
			var o n5Options
			first := n5Run(t, root, o, 0, 3)
			n5Run(t, root, o, 1, 0)
			segs := n5Segments(t, root)
			n5Damage(t, segs[0], -1, kind)
			removeCheckpoints(t, root)

			rt, err := n5Open(t, root, o)
			if err != nil {
				t.Fatalf("a last frame nothing builds on, with no checkpoint, reads as a torn tail: %v", err)
			}
			defer rt.Close()
			assertHistory(t, rt, first[1], first[:2], first[2:])
		})
	}
}

// When a checkpoint lets a namespace open over a segment damaged in place, the commits after the
// damage are as real as those before it. Their operations are not in the checkpoint, so reading
// them back goes to the log - which must index past the damage rather than stop where the
// segment's readable prefix ends.
func TestN5OperationsAfterDamageStillLoadFromTheLog(t *testing.T) {
	root := t.TempDir()
	var o n5Options
	first := n5Run(t, root, o, 0, 4)
	n5Run(t, root, o, 1, 1)
	segs := n5Segments(t, root)
	n5Damage(t, segs[0], 1, "magic")

	rt, err := n5Open(t, root, o)
	if err != nil {
		t.Fatalf("a checkpoint holds every commit, so open should use it: %v", err)
	}
	defer rt.Close()
	for i := 2; i < len(first); i++ {
		c, err := rt.DAG.GetCommitOrThrow(first[i])
		if err != nil {
			t.Fatalf("commit %d after the damage: %v", i, err)
		}
		want := fmt.Sprintf("needle-0-%d", i)
		found := false
		for _, op := range c.Operations {
			if w, ok := op.(document.WriteOp); ok && strings.Contains(w.Patch, want) {
				found = true
			}
		}
		if !found {
			t.Fatalf("commit %d after the damage lost its operations: %+v", i, c.Operations)
		}
	}
}
