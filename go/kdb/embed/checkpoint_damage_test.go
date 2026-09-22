package embed_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

// A byte flipped in the delta log - bit rot, a bad sector - must not roll a namespace back. The
// damaged segment lists only up to the damage, which used to make open distrust the checkpoint
// and replay the log, stop at the damaged frame as if it were a torn tail, and silently drop
// every commit after it; closing then checkpointed that loss. Now open recognises damage in place
// (same length, intact frames still ending where the checkpoint says) and keeps the checkpoint's
// head; only the bodies in the damaged frame are lost, and those a scrub repairs from a peer.

func writeSmallDocs(t *testing.T, root string, n int) ([]codec.Hash, []codec.UUID, []string) {
	t.Helper()
	rt, err := embed.OpenFileRuntime(root, "app", "app/damage", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	var commits []codec.Hash
	var ids []codec.UUID
	var bodies []string
	for i := 0; i < n; i++ {
		body := fmt.Sprintf(`{"id":"doc-%d","needle":"needle-%02d-%d"}`, i, i, i*7919)
		res, err := embed.PutJSONDocument(rt, "app/damage", body)
		if err != nil {
			t.Fatal(err)
		}
		id, _, err := document.ResolveID(body)
		if err != nil {
			t.Fatal(err)
		}
		commits = append(commits, res.Commit)
		ids = append(ids, id)
		bodies = append(bodies, body)
	}
	rt.Close()
	return commits, ids, bodies
}

func flipInLog(t *testing.T, root, needle string) {
	t.Helper()
	dir := filepath.Join(root, "ns", "app", "damage", "delta")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		p := filepath.Join(dir, e.Name())
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if i := bytes.Index(raw, []byte(needle)); i >= 0 {
			raw[i] ^= 0x01
			if err := os.WriteFile(p, raw, 0o644); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatalf("%q is not in the log uncompressed; the test cannot damage it", needle)
}

func openDamaged(t *testing.T, root string) *embed.EmbeddedKdbRuntime {
	t.Helper()
	rt, err := embed.OpenFileRuntime(root, "app", "app/damage", schema.None())
	if err != nil {
		t.Fatalf("a namespace with a damaged frame should still open: %v", err)
	}
	return rt
}

func TestADamagedFrameInTheMiddleOfTheLogDoesNotRollTheHeadBack(t *testing.T) {
	root := t.TempDir()
	commits, ids, bodies := writeSmallDocs(t, root, 8)
	flipInLog(t, root, "needle-03-")

	for round := 0; round < 2; round++ { // and it stays that way across the next close and open
		rt := openDamaged(t, root)
		_, head, ok, err := rt.DAG.HeadCommit()
		if err != nil || !ok || head.Hash != commits[len(commits)-1] {
			t.Fatalf("round %d: head rolled back to %s, want %s", round, head.Hash.Hex(), commits[len(commits)-1].Hex())
		}
		for i, id := range ids {
			got, err := rt.Storage.GetDocument("app/damage", id, head.DocumentTreeHash)
			if i == 3 {
				if err == nil && got != nil && got.JSON == bodies[i] {
					t.Fatalf("round %d: the damaged body still reads - the test is not damaging it", round)
				}
				if err == nil && got != nil && got.JSON != bodies[i] {
					t.Fatalf("round %d: a damaged body must be unreadable, never wrong: %s", round, got.JSON)
				}
				continue
			}
			// Every other body - including those written after the damage in the same segment -
			// still reads.
			if err != nil || got == nil || got.JSON != bodies[i] {
				t.Fatalf("round %d: document %d should be intact: %+v %v", round, i, got, err)
			}
		}
		rt.Close()
	}
}

func TestADamagedLastFrameDoesNotRollTheHeadBack(t *testing.T) {
	root := t.TempDir()
	commits, _, _ := writeSmallDocs(t, root, 4)
	flipInLog(t, root, "needle-03-")
	rt := openDamaged(t, root)
	defer rt.Close()
	if h, _ := rt.DAG.Head(); h != commits[3] {
		t.Fatalf("head rolled back to %s, want %s", h.Hex(), commits[3].Hex())
	}
}

// Truncation is not damage in place: a log cut short still makes open distrust the checkpoint.
func TestATruncatedSegmentIsStillNotTrusted(t *testing.T) {
	root := t.TempDir()
	commits, _, _ := writeSmallDocs(t, root, 6)
	dir := filepath.Join(root, "ns", "app", "damage", "delta")
	entries, _ := os.ReadDir(dir)
	p := filepath.Join(dir, entries[0].Name())
	raw, _ := os.ReadFile(p)
	if err := os.WriteFile(p, raw[:len(raw)/2], 0o644); err != nil {
		t.Fatal(err)
	}
	rt, err := embed.OpenFileRuntime(root, "app", "app/damage", schema.None())
	if err != nil {
		return // refused outright: fine
	}
	defer rt.Close()
	if h, _ := rt.DAG.Head(); h == commits[len(commits)-1] {
		// Serving the checkpoint's head over a log that no longer holds it is exactly the
		// masking the checkpoint check exists to prevent.
		t.Fatal("opened a truncated log with the checkpoint's head as if nothing were missing")
	}
}
