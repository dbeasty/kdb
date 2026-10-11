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
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage/delta"
)

// Issue #43, U6c: a delta segment the reader could not scan used to be skipped by ListSegments,
// so a namespace opened without every commit in it and said nothing - while kdb-inspect verify,
// which reads segments itself, could still pass. Open must now refuse with a typed error naming
// the segment, and a torn tail - the expected shape of a kill - must still recover.

const silentNS = "app/silent"

// writeAcrossRuns writes perRun documents in each of runs separate opens, so each run's commits
// land in their own delta segment, and returns every commit in order.
func writeAcrossRuns(t *testing.T, root string, runs, perRun int) []codec.Hash {
	t.Helper()
	var commits []codec.Hash
	for r := 0; r < runs; r++ {
		rt, err := embed.OpenFileRuntime(root, "app", silentNS, schema.None())
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < perRun; i++ {
			res, err := embed.PutJSONDocument(rt, silentNS, fmt.Sprintf(`{"id":"doc-%d-%d","n":%d}`, r, i, i))
			if err != nil {
				t.Fatal(err)
			}
			commits = append(commits, res.Commit)
		}
		rt.Close()
	}
	return commits
}

// nonEmptySegments returns the namespace's delta segment files that hold at least one frame, in
// sequence order.
func nonEmptySegments(t *testing.T, root string) []string {
	t.Helper()
	dir := filepath.Join(root, "ns", "app", "silent", "delta")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".seg") {
			continue
		}
		if info, err := e.Info(); err == nil && info.Size() > 0 {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(out)
	return out
}

func editFile(t *testing.T, path string, edit func([]byte) []byte) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, edit(raw), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestOpenRefusesAnUnreadableDeltaSegment(t *testing.T) {
	for _, damaged := range []string{"not-newest", "newest"} {
		for _, checkpoint := range []string{"checkpoint", "no-checkpoint"} {
			t.Run(damaged+"/"+checkpoint, func(t *testing.T) {
				root := t.TempDir()
				commits := writeAcrossRuns(t, root, 3, 3)
				segs := nonEmptySegments(t, root)
				if len(segs) != 3 {
					t.Fatalf("want one non-empty segment per run, got %d", len(segs))
				}
				victim := segs[1]
				if damaged == "newest" {
					victim = segs[2]
				}
				// Byte 5 of a KDBP frame is its codec id, outside the body CRC: the frame still
				// walks as intact, so this is not a torn tail, and only decoding it fails.
				editFile(t, victim, func(raw []byte) []byte { raw[5] = 0x7f; return raw })
				if checkpoint == "no-checkpoint" {
					removeCheckpoints(t, root)
				}

				rt, err := embed.OpenFileRuntime(root, "app", silentNS, schema.None())
				if err == nil {
					head, _ := rt.DAG.Head()
					rt.Close()
					t.Fatalf("open succeeded over an unreadable segment; head %s, last acknowledged commit %s",
						head.Hex(), commits[len(commits)-1].Hex())
				}
				var unreadable *delta.UnreadableSegmentError
				if !errors.As(err, &unreadable) {
					t.Fatalf("open failed, but not with *delta.UnreadableSegmentError: %v", err)
				}
				if !strings.Contains(victim, fmt.Sprintf("%020d", unreadable.Sequence)) {
					t.Fatalf("error names segment %d, damaged file is %s", unreadable.Sequence, victim)
				}
			})
		}
	}
}

func TestOpenStillRecoversATornNewestSegment(t *testing.T) {
	root := t.TempDir()
	commits := writeAcrossRuns(t, root, 3, 3)
	segs := nonEmptySegments(t, root)
	// Cut into the last frame, as a kill mid-write would.
	editFile(t, segs[len(segs)-1], func(raw []byte) []byte { return raw[:len(raw)-5] })
	removeCheckpoints(t, root)

	rt, err := embed.OpenFileRuntime(root, "app", silentNS, schema.None())
	if err != nil {
		t.Fatalf("a torn tail must still open: %v", err)
	}
	defer rt.Close()
	if head, _ := rt.DAG.Head(); head != commits[len(commits)-2] {
		t.Fatalf("head %s, want the last intact commit %s", head.Hex(), commits[len(commits)-2].Hex())
	}
}
