package server

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// TestConcurrentUpsertsToOneDocumentKeepEveryField: Upsert merges its body onto the stored
// document, so concurrent upserts that each set a different field must all survive. They did
// not when the merge read the document at the head Upsert saw before it queued for the write
// gate: a writer that queued behind another one merged onto the version before it and wrote
// that back, dropping whatever the writer ahead had set. LAST_WRITE namespaces skip conflict
// detection, so nothing reported it either.
func TestConcurrentUpsertsToOneDocumentKeepEveryField(t *testing.T) {
	const ns = "app/data"
	const writers = 16
	const rounds = 20
	rt, err := embed.OpenFileRuntimeWithOptions(
		t.TempDir(), "app", ns, schema.None(),
		embed.FileRuntimeOptions{Storage: embed.StorageOptions{Durability: storage.DurabilityMemoryOnly}},
	)
	if err != nil {
		t.Fatalf("OpenFileRuntimeWithOptions: %v", err)
	}
	t.Cleanup(func() { rt.Close() })
	srv := NewKdbServerRuntime(rt)
	srv.SetWriteQueueCapacityForTest(4096)

	for round := 0; round < rounds; round++ {
		id := mustRandomUUID(t)
		if _, err := srv.Upsert(ns, id, `{"seed":true}`, auth.Principal{}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make(chan error, writers)
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				<-start
				if _, err := srv.Upsert(ns, id, fmt.Sprintf(`{"f%d":%d}`, w, w), auth.Principal{}); err != nil {
					errs <- err
				}
			}(w)
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("upsert: %v", err)
		}
		body, _, found, err := srv.GetDocument(ns, id)
		if err != nil || !found {
			t.Fatalf("read back: found=%v err=%v", found, err)
		}
		var got map[string]any
		if err := json.Unmarshal([]byte(body), &got); err != nil {
			t.Fatal(err)
		}
		var missing []string
		for w := 0; w < writers; w++ {
			if _, ok := got[fmt.Sprintf("f%d", w)]; !ok {
				missing = append(missing, fmt.Sprintf("f%d", w))
			}
		}
		if _, ok := got["seed"]; !ok {
			missing = append(missing, "seed")
		}
		if len(missing) > 0 {
			t.Fatalf("round %d: %d of %d concurrent upserts lost: missing %v from %s", round, len(missing), writers+1, missing, body)
		}
	}
}
