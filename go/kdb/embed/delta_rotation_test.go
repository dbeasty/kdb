package embed_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Delta segment rotation, which is what lets a process that never restarts
// reclaim its own writes: retention can only ever delete a *sealed* segment,
// so without rotation everything the running process has written sits in the
// one open segment that truncation is required to leave alone.

// rotatingRuntime opens a namespace whose delta segments rotate at a
// deliberately tiny cap, so a handful of ordinary writes cross it.
func rotatingRuntime(t *testing.T, root string, maxBytes int64) *embed.EmbeddedKdbRuntime {
	t.Helper()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.HistoryMode = storage.HistoryModeNone
	opts.Storage.Retain = storage.RetentionWindow{Duration: storage.RetainNothing}
	opts.Storage.DeltaMaxSegmentBytes = maxBytes
	rt, err := embed.OpenFileRuntimeWithOptions(root, "app", "app/docs", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

func writeRotationDoc(t *testing.T, rt *embed.EmbeddedKdbRuntime, id string, n int) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"id": id, "n": n, "blob": strings.Repeat("x", 512),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := embed.PutJSONDocument(rt, "app/docs", string(body)); err != nil {
		t.Fatal(err)
	}
}

// The point of the whole change: one long-lived session must end up with more
// than one segment, without anybody closing the runtime.
func TestSegmentsRotateWithinOneSession(t *testing.T) {
	root := t.TempDir()
	rt := rotatingRuntime(t, root, 4096)
	defer rt.Close()

	before := deltaSegmentCount(t, root)
	for i := 0; i < 40; i++ {
		writeRotationDoc(t, rt, fmt.Sprintf("doc-%d", i), i)
	}
	after := deltaSegmentCount(t, root)
	if after <= before {
		t.Fatalf("one session wrote %d segments (started at %d); without rotation it stays at one",
			after, before)
	}
}

// Rotation must not lose a write. Every document written across the rotation
// boundary has to read back, which is the only thing a caller notices.
func TestEveryDocumentSurvivesRotation(t *testing.T) {
	root := t.TempDir()
	rt := rotatingRuntime(t, root, 4096)
	defer rt.Close()

	const docs = 40
	for i := 0; i < docs; i++ {
		writeRotationDoc(t, rt, fmt.Sprintf("doc-%d", i), i)
	}
	for i := 0; i < docs; i++ {
		body, ok := readDoc(t, rt, fmt.Sprintf("doc-%d", i))
		if !ok {
			t.Fatalf("doc-%d is missing after rotation", i)
		}
		if want := fmt.Sprintf(`"n":%d`, i); !strings.Contains(body, want) {
			t.Fatalf("doc-%d reads %q, want it to contain %s", i, body, want)
		}
	}
}

// And they have to survive a reopen, which is the path that actually replays
// the rotated segments in sequence order rather than reading from memory.
func TestRotatedSegmentsReplayInOrder(t *testing.T) {
	root := t.TempDir()
	rt := rotatingRuntime(t, root, 4096)
	const docs = 40
	for i := 0; i < docs; i++ {
		writeRotationDoc(t, rt, fmt.Sprintf("doc-%d", i), i)
	}
	// Rewrite one document repeatedly so its *latest* value is only correct
	// if the segments replay in the right order.
	for rev := 0; rev < 5; rev++ {
		writeRotationDoc(t, rt, "doc-0", 1000+rev)
	}
	rt.Close()

	reopened := rotatingRuntime(t, root, 4096)
	defer reopened.Close()
	body, ok := readDoc(t, reopened, "doc-0")
	if !ok {
		t.Fatal("doc-0 is missing after reopening a rotated namespace")
	}
	if !strings.Contains(body, `"n":1004`) {
		t.Fatalf("doc-0 reads %q after replay; the last write should win", body)
	}
	for i := 1; i < docs; i++ {
		if _, ok := readDoc(t, reopened, fmt.Sprintf("doc-%d", i)); !ok {
			t.Fatalf("doc-%d did not survive the reopen", i)
		}
	}
}

// The reason rotation was worth doing: a running process can now reclaim its
// own writes. Before it, every commit this session made lived in the single
// open segment, which truncation must leave alone, so Maintain reclaimed
// nothing no matter how often it ran.
func TestARunningSessionCanReclaimItsOwnWrites(t *testing.T) {
	root := t.TempDir()
	rt := rotatingRuntime(t, root, 4096)
	defer rt.Close()

	for i := 0; i < 60; i++ {
		writeRotationDoc(t, rt, fmt.Sprintf("doc-%d", i), i)
	}
	res, err := rt.Maintain()
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed == 0 {
		t.Fatal("a long-running session reclaimed nothing; rotation is what should have made its " +
			"own sealed segments eligible")
	}
	// The live data is still intact afterwards, which is the whole safety
	// question for a reclamation.
	for i := 0; i < 60; i++ {
		if _, ok := readDoc(t, rt, fmt.Sprintf("doc-%d", i)); !ok {
			t.Fatalf("doc-%d was lost by a reclamation that followed rotation", i)
		}
	}
}

// A cap smaller than a single frame must not produce an endless run of empty
// segments - the writer refuses to rotate an empty one.
func TestAnEmptySegmentNeverRotates(t *testing.T) {
	root := t.TempDir()
	rt := rotatingRuntime(t, root, 1)
	defer rt.Close()

	for i := 0; i < 5; i++ {
		writeRotationDoc(t, rt, fmt.Sprintf("doc-%d", i), i)
	}
	// Five writes at a 1-byte cap rotate after every one of them, but never
	// more than that: an empty segment is never sealed, so the count tracks
	// the writes rather than running away.
	if n := deltaSegmentCount(t, root); n > 12 {
		t.Fatalf("a 1-byte cap produced %d segments for 5 writes, which suggests empty ones are rotating", n)
	}
	for i := 0; i < 5; i++ {
		if _, ok := readDoc(t, rt, fmt.Sprintf("doc-%d", i)); !ok {
			t.Fatalf("doc-%d is missing under an aggressive rotation cap", i)
		}
	}
}
