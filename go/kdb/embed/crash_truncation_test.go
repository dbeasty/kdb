package embed_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

// A crash while writing the delta log leaves the log truncated at an arbitrary byte. This is the
// deterministic model of that: write a known sequence of commits, then reopen the namespace once
// per truncation point and check what survived.
//
// It exists because the property is currently only checked by an end-to-end test that really does
// `kill -9` a service under load (kdb-integration/e2e/test_crash_recovery.py). That test is
// valuable and it is also the one that flakes in CI - see issue #43 - and when it fails it cannot
// say which byte the process died on. Truncating on purpose makes the same question exhaustive and
// repeatable, which is what Layer 13's test plan asked for under the name killDashNineAnyPoint and
// nothing ever implemented.
//
// The property under test is prefix consistency. Documents are written once each, in order, so a
// recovered namespace must hold some prefix of them: if document i survived, every document before
// it must have survived too. A hole - document 5 present, document 3 gone - is data loss that no
// crash point can justify, because document 3's bytes reached the disk before document 5's were
// written.

func TestRecoveryAfterLogTruncationKeepsAPrefix(t *testing.T) {
	const docs = 24
	root := filepath.Join(t.TempDir(), "src")
	written := writeSequentialDocs(t, root, docs)

	segments := deltaSegments(t, root, "demo/users")
	if len(segments) == 0 {
		t.Fatal("no delta segments were written")
	}
	last := segments[len(segments)-1]
	full, err := os.ReadFile(last)
	if err != nil {
		t.Fatal(err)
	}
	if len(full) < 32 {
		t.Fatalf("the last segment is only %d bytes; the fixture is not exercising anything", len(full))
	}

	// Every truncation point, sampled: byte-by-byte over a log this size would be thousands of
	// namespace opens. The sample includes 0 (the segment never made it to disk at all) and one
	// byte short of complete (the most interesting case - a record all but finished).
	offsets := sampleOffsets(len(full))
	for _, off := range offsets {
		t.Run(fmt.Sprintf("truncated_at_%d_of_%d", off, len(full)), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "crashed")
			copyTree(t, root, dir)

			target := filepath.Join(dir, relativeTo(t, root, last))
			if err := os.Truncate(target, int64(off)); err != nil {
				t.Fatal(err)
			}

			rt, err := embed.OpenFileRuntime(dir, "demo", "demo/users", schema.None())
			if err != nil {
				// Refusing to open is a legitimate answer to a damaged log - it is loud, and
				// nothing is silently lost. Losing a document while opening cleanly is not.
				t.Skipf("namespace refused to open after truncation, which is a safe outcome: %v", err)
			}
			defer rt.Close()

			survived := make([]bool, len(written))
			for i, w := range written {
				body, err := readDocBody(rt, w.docID)
				if err != nil {
					t.Fatalf("reading %s: %v", w.docID, err)
				}
				survived[i] = body != nil
				if body != nil && body["n"] != float64(i) {
					t.Errorf("document %d came back with the wrong contents: %v", i, body)
				}
			}
			assertPrefix(t, survived, off, len(full))
		})
	}
}

// assertPrefix is the whole point: the survivors must be a prefix, with no holes.
func assertPrefix(t *testing.T, survived []bool, off, total int) {
	t.Helper()
	firstMissing := -1
	for i, ok := range survived {
		if !ok && firstMissing < 0 {
			firstMissing = i
			continue
		}
		if ok && firstMissing >= 0 {
			t.Fatalf(
				"truncating the log at byte %d of %d lost document %d but kept document %d.\n"+
					"Document %d's bytes reached the disk before document %d's were written, so no "+
					"crash point can justify keeping the later one and dropping the earlier: this is "+
					"a hole in the recovered history, not a torn tail.\nsurvivors: %s",
				off, total, firstMissing, i, firstMissing, i, renderSurvivors(survived))
		}
	}
}

func renderSurvivors(survived []bool) string {
	var b strings.Builder
	for _, ok := range survived {
		if ok {
			b.WriteByte('.')
		} else {
			b.WriteByte('x')
		}
	}
	return b.String()
}

// sampleOffsets picks truncation points across a segment: the ends, the quarters, and a spread
// through the middle. Deterministic, so a failure names a byte someone can go back to.
func sampleOffsets(size int) []int {
	seen := map[int]struct{}{}
	var out []int
	add := func(v int) {
		if v < 0 || v > size {
			return
		}
		if _, dup := seen[v]; dup {
			return
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	add(0)
	add(1)
	for i := 1; i < 16; i++ {
		add(size * i / 16)
	}
	add(size - 1)
	add(size)
	sort.Ints(out)
	return out
}

type writtenDoc struct {
	docID string
	n     int
}

// writeSequentialDocs writes n documents, each exactly once, and closes the runtime cleanly.
func writeSequentialDocs(t *testing.T, root string, n int) []writtenDoc {
	t.Helper()
	rt, err := embed.OpenFileRuntime(root, "demo", "demo/users", schema.None())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer rt.Close()

	out := make([]writtenDoc, 0, n)
	for i := 0; i < n; i++ {
		// A stable, derivable id per document, so the reader does not depend on what the writer
		// happened to mint.
		id := fmt.Sprintf("doc-%03d", i)
		body := fmt.Sprintf(`{"id":%q,"n":%d}`, id, i)
		res, err := embed.PutJSONDocument(rt, "demo/users", body)
		if err != nil {
			t.Fatalf("writing %s: %v", id, err)
		}
		out = append(out, writtenDoc{docID: res.DocID.String(), n: i})
	}
	return out
}

func readDocBody(rt *embed.EmbeddedKdbRuntime, docID string) (map[string]any, error) {
	id, err := codec.UUIDFromString(docID)
	if err != nil {
		return nil, err
	}
	head, err := rt.DAG.Head()
	if err != nil {
		return nil, err
	}
	commit, ok := rt.DAG.GetCommit(head)
	if !ok {
		return nil, fmt.Errorf("head commit %s is not readable", head.Hex())
	}
	doc, err := rt.Storage.GetDocument("demo/users", id, commit.DocumentTreeHash)
	if err != nil || doc == nil {
		return nil, err
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(doc.JSON), &body); err != nil {
		return nil, err
	}
	return body, nil
}

func deltaSegments(t *testing.T, root, namespaceID string) []string {
	t.Helper()
	dir := filepath.Join(root, "ns", filepath.FromSlash(namespaceID), "delta")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("listing %s: %v", dir, err)
	}
	var out []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".seg") {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	// Names are zero-padded, so lexicographic order is sequence order.
	sort.Strings(out)
	return out
}

func relativeTo(t *testing.T, root, p string) string {
	t.Helper()
	rel, err := filepath.Rel(root, p)
	if err != nil {
		t.Fatal(err)
	}
	return rel
}

func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		// The directory lock is the writer's, not part of the data; copying it would carry a
		// stale hold into the copy.
		if strings.Contains(info.Name(), ".lock") {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(target, b, 0o644)
	})
	if err != nil {
		t.Fatalf("copying the data directory: %v", err)
	}
}

// TestRecoveryFromCheckpointPlusTornTailKeepsAPrefix models the crash the e2e test actually
// produces, which the truncation test above does not reach.
//
// Truncating a segment the checkpoint knows about makes the checkpoint invalid - the size no
// longer matches what was recorded - so open discards it and replays the whole log. That is a
// safe path and worth covering, but it is not the interesting one. After a real `kill -9` the
// checkpoint on disk is the one written at the *previous* open, it is still perfectly valid for
// the segments it names, and everything written since has to come back through tail replay
// (replayDeltaNamespaceFrom).
//
// So: write, close, keep that checkpoint, write more, then put the old checkpoint back before
// truncating. The result is a data directory that looks exactly like one whose process died
// mid-write - a valid checkpoint, plus a live segment the checkpoint has never seen.
func TestRecoveryFromCheckpointPlusTornTailKeepsAPrefix(t *testing.T) {
	const firstBatch, secondBatch = 8, 16
	root := filepath.Join(t.TempDir(), "src")

	// Session one: written and closed cleanly, so a checkpoint covers it.
	first := writeSequentialDocs(t, root, firstBatch)
	checkpoint := checkpointPath(t, root, "demo/users")
	stale, err := os.ReadFile(checkpoint)
	if err != nil {
		t.Fatalf("no checkpoint after a clean close: %v", err)
	}

	// Session two: more documents, into segments the stale checkpoint does not describe.
	second := appendSequentialDocs(t, root, firstBatch, secondBatch)
	written := append(append([]writtenDoc{}, first...), second...)

	segments := deltaSegments(t, root, "demo/users")
	last := segments[len(segments)-1]
	full, err := os.ReadFile(last)
	if err != nil {
		t.Fatal(err)
	}

	for _, off := range sampleOffsets(len(full)) {
		t.Run(fmt.Sprintf("tail_truncated_at_%d_of_%d", off, len(full)), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "crashed")
			copyTree(t, root, dir)

			// The checkpoint the crashed process would have left behind: the one from before the
			// second session, not the one its clean close wrote.
			if err := os.WriteFile(filepath.Join(dir, relativeTo(t, root, checkpoint)), stale, 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Truncate(filepath.Join(dir, relativeTo(t, root, last)), int64(off)); err != nil {
				t.Fatal(err)
			}

			rt, err := embed.OpenFileRuntime(dir, "demo", "demo/users", schema.None())
			if err != nil {
				t.Skipf("namespace refused to open, which is a safe outcome: %v", err)
			}
			defer rt.Close()

			survived := make([]bool, len(written))
			for i, w := range written {
				body, err := readDocBody(rt, w.docID)
				if err != nil {
					t.Fatalf("reading %s: %v", w.docID, err)
				}
				survived[i] = body != nil
			}
			// Everything in the first batch is named by the checkpoint and by segments this
			// truncation did not touch, so losing any of it is unambiguous data loss rather than a
			// torn tail.
			for i := 0; i < firstBatch; i++ {
				if !survived[i] {
					t.Fatalf(
						"document %d was committed, checkpointed, and lives in a segment this test "+
							"did not truncate, yet it is gone after recovery (truncation at byte %d "+
							"of the live segment).\nsurvivors: %s",
						i, off, renderSurvivors(survived))
				}
			}
			assertPrefix(t, survived, off, len(full))
		})
	}
}

// appendSequentialDocs reopens an existing namespace and writes n more documents, numbered from
// startAt so their contents stay unique across sessions.
func appendSequentialDocs(t *testing.T, root string, startAt, n int) []writtenDoc {
	t.Helper()
	rt, err := embed.OpenFileRuntime(root, "demo", "demo/users", schema.None())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer rt.Close()

	out := make([]writtenDoc, 0, n)
	for i := startAt; i < startAt+n; i++ {
		id := fmt.Sprintf("doc-%03d", i)
		res, err := embed.PutJSONDocument(rt, "demo/users", fmt.Sprintf(`{"id":%q,"n":%d}`, id, i))
		if err != nil {
			t.Fatalf("writing %s: %v", id, err)
		}
		out = append(out, writtenDoc{docID: res.DocID.String(), n: i})
	}
	return out
}

// checkpointPath is where the namespace checkpoint lands on disk. The key is
// "kdb:checkpoint:<namespace>" and FileBackedPlatformIO maps a snapshot key to a path under
// snap/, with the separators in the key becoming directories.
func checkpointPath(t *testing.T, root, namespaceID string) string {
	t.Helper()
	dir := filepath.Join(root, "snap")
	var found string
	err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		if strings.Contains(p, "checkpoint") {
			found = p
		}
		return nil
	})
	if err != nil || found == "" {
		t.Fatalf("could not find the checkpoint under %s (err=%v)", dir, err)
	}
	return found
}
