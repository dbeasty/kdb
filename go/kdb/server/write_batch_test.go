package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

func openCoalescing(t *testing.T, dir string, durability storage.Durability, on bool) (*KdbServerRuntime, func()) {
	t.Helper()
	rt, err := embed.OpenFileRuntimeWithOptions(dir, "app", "app/data", schema.None(),
		embed.FileRuntimeOptions{Storage: embed.StorageOptions{Durability: durability}})
	if err != nil {
		t.Fatal(err)
	}
	srv := NewKdbServerRuntime(rt)
	srv.SetWriteQueueCapacityForTest(4096)
	srv.CoalesceWrites = on
	return srv, func() { rt.Close() }
}

func (b *writeBatcher) queued() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pending)
}

// holdGate occupies the write gate so writers queue behind it; the returned func lets them go.
func holdGate(t *testing.T, srv *KdbServerRuntime) func() {
	t.Helper()
	release, err := srv.AcquireWriteSlotForTest()
	if err != nil {
		t.Fatal(err)
	}
	return release
}

func waitQueued(t *testing.T, srv *KdbServerRuntime, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for srv.batcher.queued() < n {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d writers queued", srv.batcher.queued(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// commitsOnMain lists the commits on main, newest first.
func commitsOnMain(t *testing.T, srv *KdbServerRuntime) []document.Commit {
	t.Helper()
	head, err := srv.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	var out []document.Commit
	for _, e := range srv.dag.Walk(head, nil, 1<<30) {
		if f, ok := e.(dag.FullEntry); ok {
			out = append(out, f.Commit)
		}
	}
	return out
}

func requireNoCommitWritesADocumentTwice(t *testing.T, commits []document.Commit) {
	t.Helper()
	for _, c := range commits {
		seen := map[codec.UUID]bool{}
		for _, op := range c.Operations {
			id, ok := document.OpDocID(op)
			if !ok {
				continue
			}
			if seen[id] {
				t.Fatalf("commit %s writes document %s twice", c.Hash.Hex()[:8], id)
			}
			seen[id] = true
		}
	}
}

// Writers queued at the gate are committed together: one commit holding every one of their
// writes, each writer answered with it, every document as written.
func TestCoalescedWritesShareOneCommit(t *testing.T) {
	srv, done := openCoalescing(t, t.TempDir(), storage.DurabilityMemoryOnly, true)
	defer done()
	const writers = 40
	ids := make([]codec.UUID, writers)
	for i := range ids {
		ids[i] = mustRandomUUID(t)
	}
	let := holdGate(t, srv)
	hashes := make([]codec.Hash, writers)
	errs := make([]error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := srv.Upsert("app/data", ids[i], fmt.Sprintf(`{"n":%d}`, i), auth.Principal{})
			hashes[i], errs[i] = c.Hash, err
		}(i)
	}
	waitQueued(t, srv, writers)
	let()
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
	}
	for i := range hashes {
		if hashes[i] != hashes[0] {
			t.Fatalf("writer %d answered with commit %s, writer 0 with %s: not one batch", i, hashes[i].Hex()[:8], hashes[0].Hex()[:8])
		}
	}
	c, _ := srv.dag.GetCommit(hashes[0])
	if len(c.Operations) != writers {
		t.Fatalf("batch commit holds %d operations, want %d", len(c.Operations), writers)
	}
	for i, id := range ids {
		body, _, found, err := srv.GetDocument("app/data", id)
		if err != nil || !found || body != fmt.Sprintf(`{"n":%d}`, i) {
			t.Fatalf("document %d = %q found=%v err=%v", i, body, found, err)
		}
	}
	if n := srv.batcher.queued(); n != 0 {
		t.Fatalf("%d writes left queued", n)
	}
}

// A batch never holds two writes to one document: the second closes it and goes in a later
// commit. So every version of a document keeps its own commit, and concurrent merges into one
// document all survive (W1's guarantee, batched).
func TestCoalescedBatchNeverWritesADocumentTwice(t *testing.T) {
	srv, done := openCoalescing(t, t.TempDir(), storage.DurabilityMemoryOnly, true)
	defer done()
	hot := mustRandomUUID(t)
	if _, err := srv.Upsert("app/data", hot, `{"seed":true}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	const writers = 24
	let := holdGate(t, srv)
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Every third writer writes the hot document; the rest write documents of their own.
			id := hot
			if i%3 != 0 {
				id = mustRandomUUID(t)
			}
			if _, err := srv.Upsert("app/data", id, fmt.Sprintf(`{"f%d":%d}`, i, i), auth.Principal{}); err != nil {
				errs <- err
			}
		}(i)
	}
	waitQueued(t, srv, writers)
	let()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	commits := commitsOnMain(t, srv)
	requireNoCommitWritesADocumentTwice(t, commits)
	body, _, _, _ := srv.GetDocument("app/data", hot)
	got := map[string]any{}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < writers; i += 3 {
		if _, ok := got[fmt.Sprintf("f%d", i)]; !ok {
			t.Fatalf("hot document lost f%d: %s", i, body)
		}
	}
	// 24 writes, 8 of them to the hot document: at least 8 commits (one per hot version), far
	// fewer than 24 plus the seed.
	if n := len(commits) - 2; n < 8 || n >= writers { // minus the seed and the root
		t.Fatalf("%d commits for %d writes (8 to one document)", n, writers)
	}
}

// One invalid write in a batch does not fail the others: the batch is rejected, and each of its
// writes is committed (or refused) on its own, with the answer it would have had alone.
func TestCoalescedBatchWithAnInvalidWriteAnswersEachAlone(t *testing.T) {
	srv, done := openCoalescing(t, t.TempDir(), storage.DurabilityMemoryOnly, true)
	defer done()
	const writers = 6
	ids := make([]codec.UUID, writers)
	for i := range ids {
		ids[i] = mustRandomUUID(t)
	}
	let := holdGate(t, srv)
	errs := make([]error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"n":%d}`, i)
			if i == 3 {
				body = `[1,2]` // not an object
			}
			_, errs[i] = srv.Upsert("app/data", ids[i], body, auth.Principal{})
		}(i)
	}
	waitQueued(t, srv, writers)
	let()
	wg.Wait()
	for i, err := range errs {
		_, _, found, _ := srv.GetDocument("app/data", ids[i])
		if i == 3 {
			if err == nil || found {
				t.Fatalf("invalid write: err=%v stored=%v", err, found)
			}
			continue
		}
		if err != nil || !found {
			t.Fatalf("valid write %d failed alongside the invalid one: err=%v stored=%v", i, err, found)
		}
	}
}

// Replace and Delete batch with Upsert.
func TestCoalescedReplaceDeleteAndUpsert(t *testing.T) {
	srv, done := openCoalescing(t, t.TempDir(), storage.DurabilityMemoryOnly, true)
	defer done()
	x, y, z := mustRandomUUID(t), mustRandomUUID(t), mustRandomUUID(t)
	for _, id := range []codec.UUID{x, y} {
		if _, err := srv.Upsert("app/data", id, `{"old":1}`, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	let := holdGate(t, srv)
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	run := func(f func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := f(); err != nil {
				errs <- err
			}
		}()
	}
	run(func() error { _, err := srv.Replace("app/data", x, `{"new":2}`, auth.Principal{}); return err })
	run(func() error { _, err := srv.Delete("app/data", y, auth.Principal{}); return err })
	run(func() error { _, err := srv.Upsert("app/data", z, `{"z":3}`, auth.Principal{}); return err })
	waitQueued(t, srv, 3)
	let()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if body, _, _, _ := srv.GetDocument("app/data", x); body != `{"new":2}` {
		t.Fatalf("replaced document = %s", body)
	}
	if _, _, found, _ := srv.GetDocument("app/data", y); found {
		t.Fatal("deleted document still found")
	}
	if body, _, _, _ := srv.GetDocument("app/data", z); body != `{"z":3}` {
		t.Fatalf("upserted document = %s", body)
	}
	head, _ := srv.Runtime.DAG.Head()
	c, _ := srv.dag.GetCommit(head)
	if len(c.Operations) != 3 {
		t.Fatalf("head commit holds %d operations, want the 3 writes in one batch", len(c.Operations))
	}
}

// Off (the default), every write is its own commit, however many are queued.
func TestWritesAreNotCoalescedByDefault(t *testing.T) {
	srv, done := openCoalescing(t, t.TempDir(), storage.DurabilityMemoryOnly, false)
	defer done()
	const writers = 10
	let := holdGate(t, srv)
	var wg sync.WaitGroup
	hashes := make(chan codec.Hash, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := srv.Upsert("app/data", mustRandomUUID(t), fmt.Sprintf(`{"n":%d}`, i), auth.Principal{})
			if err != nil {
				t.Error(err)
			}
			hashes <- c.Hash
		}(i)
	}
	time.Sleep(50 * time.Millisecond)
	let()
	wg.Wait()
	close(hashes)
	seen := map[codec.Hash]bool{}
	for h := range hashes {
		seen[h] = true
	}
	if len(seen) != writers {
		t.Fatalf("%d commits for %d writes with coalescing off", len(seen), writers)
	}
}

// Batched commits are ordinary commits in the log: after a close and reopen (replay), every
// document is as written and every batch is still one commit.
func TestCoalescedCommitsSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	srv, done := openCoalescing(t, dir, storage.DurabilitySync, true)
	const writers = 20
	ids := make([]codec.UUID, writers)
	for i := range ids {
		ids[i] = mustRandomUUID(t)
	}
	for round := 0; round < 3; round++ {
		let := holdGate(t, srv)
		var wg sync.WaitGroup
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if _, err := srv.Upsert("app/data", ids[i], fmt.Sprintf(`{"r%d":%d}`, round, i), auth.Principal{}); err != nil {
					t.Error(err)
				}
			}(i)
		}
		waitQueued(t, srv, writers)
		let()
		wg.Wait()
	}
	before := len(commitsOnMain(t, srv))
	srv.Release()
	_ = done

	srv, done = openCoalescing(t, dir, storage.DurabilitySync, true)
	defer done()
	if after := len(commitsOnMain(t, srv)); after != before {
		t.Fatalf("%d commits after reopen, %d before", after, before)
	}
	for i, id := range ids {
		body, _, found, err := srv.GetDocument("app/data", id)
		if err != nil || !found {
			t.Fatalf("document %d after reopen: found=%v err=%v", i, found, err)
		}
		got := map[string]any{}
		_ = json.Unmarshal([]byte(body), &got)
		for round := 0; round < 3; round++ {
			if _, ok := got[fmt.Sprintf("r%d", round)]; !ok {
				t.Fatalf("document %d after reopen lost round %d: %s", i, round, body)
			}
		}
	}
	requireNoCommitWritesADocumentTwice(t, commitsOnMain(t, srv))
}

// A writer whose deadline passes while it is queued gives up as before and leaves the queue.
func TestCoalescedWriterTimesOutWhileQueued(t *testing.T) {
	srv, done := openCoalescing(t, t.TempDir(), storage.DurabilityMemoryOnly, true)
	defer done()
	srv.WriteTimeout = 50 * time.Millisecond
	let := holdGate(t, srv)
	defer let()
	_, err := srv.Upsert("app/data", mustRandomUUID(t), `{"a":1}`, auth.Principal{})
	var deadline *DeadlineExceededError
	if !errors.As(err, &deadline) {
		t.Fatalf("queued write past its deadline: %v, want DeadlineExceededError", err)
	}
	if n := srv.batcher.queued(); n != 0 {
		t.Fatalf("%d writes left queued after the deadline", n)
	}
}

// Many writers, no gate held: whatever batches form, every write lands, none twice to one
// document per commit.
func TestCoalescedWritesUnderLoad(t *testing.T) {
	srv, done := openCoalescing(t, t.TempDir(), storage.DurabilityAsync, true)
	defer done()
	const writers, each = 16, 40
	docs := make([]codec.UUID, 8)
	for i := range docs {
		docs[i] = mustRandomUUID(t)
	}
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if _, err := srv.Upsert("app/data", docs[(w+i)%len(docs)], fmt.Sprintf(`{"w%d":%d}`, w, i), auth.Principal{}); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	requireNoCommitWritesADocumentTwice(t, commitsOnMain(t, srv))
	for _, id := range docs {
		body, _, _, _ := srv.GetDocument("app/data", id)
		got := map[string]any{}
		_ = json.Unmarshal([]byte(body), &got)
		for w := 0; w < writers; w++ {
			if _, ok := got[fmt.Sprintf("w%d", w)]; !ok {
				t.Fatalf("document lost every write of writer %d: %s", w, body)
			}
		}
	}
}

// A batch holds at most coalesceMaxBytes of bodies, so a cold read through its frame never
// decompresses more than about that; a write larger than the cap still commits, alone.
func TestCoalescedBatchesStayWithinTheByteCap(t *testing.T) {
	srv, done := openCoalescing(t, t.TempDir(), storage.DurabilityMemoryOnly, true)
	defer done()
	const writers = 40
	pad := strings.Repeat("x", 1000)
	let := holdGate(t, srv)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"n":%d,"p":"%s"}`, i, pad)
			if i == 0 {
				body = fmt.Sprintf(`{"n":0,"p":"%s"}`, strings.Repeat("y", 2*coalesceMaxBytes))
			}
			if _, err := srv.Upsert("app/data", mustRandomUUID(t), body, auth.Principal{}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	waitQueued(t, srv, writers)
	let()
	wg.Wait()
	batches := 0
	for _, c := range commitsOnMain(t, srv) {
		size := 0
		for _, op := range c.Operations {
			if w, ok := op.(document.WriteOp); ok {
				size += len(w.Patch)
			}
		}
		if len(c.Operations) > 1 {
			batches++
			if size > coalesceMaxBytes {
				t.Fatalf("a batch of %d writes holds %d bytes of bodies, over the %d cap", len(c.Operations), size, coalesceMaxBytes)
			}
		}
		if size > coalesceMaxBytes && len(c.Operations) != 1 {
			t.Fatalf("the oversized write was batched with %d others", len(c.Operations)-1)
		}
	}
	if batches < 3 {
		t.Fatalf("%d batches for 39 ~1 KB writes under a 16 KiB cap, want at least 3", batches)
	}
}
