package server

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
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

// openUpsertTestRuntime is a memory-durability file runtime with a server runtime over it.
func openUpsertTestRuntime(t *testing.T, ns string) *KdbServerRuntime {
	t.Helper()
	return openUpsertTestRuntimeWithSchema(t, ns, schema.None())
}

func openUpsertTestRuntimeWithSchema(t *testing.T, ns string, sch schema.KdbSchema) *KdbServerRuntime {
	t.Helper()
	rt, err := embed.OpenFileRuntimeWithOptions(
		t.TempDir(), "app", ns, sch,
		embed.FileRuntimeOptions{Storage: embed.StorageOptions{Durability: storage.DurabilityMemoryOnly}},
	)
	if err != nil {
		t.Fatalf("OpenFileRuntimeWithOptions: %v", err)
	}
	t.Cleanup(func() { rt.Close() })
	return NewKdbServerRuntime(rt)
}

// readFields reads id back and returns its top-level fields.
func readFields(t *testing.T, srv *KdbServerRuntime, ns string, id codec.UUID) map[string]any {
	t.Helper()
	body, _, found, err := srv.GetDocument(ns, id)
	if err != nil || !found {
		t.Fatalf("read back: found=%v err=%v", found, err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	return got
}

// TestStaleUpsertPremergeIsRedoneOffTheGate: an Upsert whose document is written by someone else
// between its premerge and its turn at the gate does not merge under the gate - it hands the gate
// back, merges again on the new head, and installs that - and still keeps the other writer's field.
func TestStaleUpsertPremergeIsRedoneOffTheGate(t *testing.T) {
	const ns = "app/data"
	srv := openUpsertTestRuntime(t, ns)
	id := mustRandomUUID(t)
	if _, err := srv.Upsert(ns, id, `{"seed":true}`, auth.Principal{}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var interfered atomic.Bool
	srv.SetAfterPremergeHookForTest(func(attempt int) {
		// The interfering Upsert runs this hook too; only the outer write's first attempt acts.
		if attempt == 0 && interfered.CompareAndSwap(false, true) {
			// A Replace: it builds on nothing in flight, so only the outer write is affected.
			if _, err := srv.Replace(ns, id, `{"seed":true,"between":1}`, auth.Principal{}); err != nil {
				t.Errorf("interfering write: %v", err)
			}
		}
	})
	before, commitsBefore := srv.PremergeOutcomes(), srv.dag.CommitCount()
	if _, err := srv.Upsert(ns, id, `{"mine":2}`, auth.Principal{}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	srv.SetAfterPremergeHookForTest(nil)

	after := srv.PremergeOutcomes()
	if got := after.Retried - before.Retried; got != 1 {
		t.Errorf("retried = %d, want 1 (the stale first attempt)", got)
	}
	if got := after.MergedAtGate - before.MergedAtGate; got != 0 {
		t.Errorf("merged at gate = %d, want 0 (the retry was current)", got)
	}
	if got := after.Current - before.Current; got != 1 {
		t.Errorf("current = %d, want 1 (the retry)", got)
	}
	// The stale attempt committed nothing: one commit each for the two writes.
	if got := srv.dag.CommitCount() - commitsBefore; got != 2 {
		t.Errorf("commits = %d, want 2", got)
	}
	got := readFields(t, srv, ns, id)
	for _, k := range []string{"seed", "between", "mine"} {
		if _, ok := got[k]; !ok {
			t.Errorf("field %q lost: %v", k, got)
		}
	}
}

// TestStaleUpsertPremergeFallsBackToTheGateAfterTheBound: a document written between every premerge
// and its gate turn sends the Upsert back exactly the retry bound's number of times, then the gate
// merges the patch itself - and every field still survives.
func TestStaleUpsertPremergeFallsBackToTheGateAfterTheBound(t *testing.T) {
	const ns = "app/data"
	for _, bound := range []int{0, 1, 3} {
		t.Run(fmt.Sprintf("retries=%d", bound), func(t *testing.T) {
			srv := openUpsertTestRuntime(t, ns)
			srv.SetPremergeRetriesForTest(bound)
			id := mustRandomUUID(t)
			if _, err := srv.Upsert(ns, id, `{"seed":true}`, auth.Principal{}); err != nil {
				t.Fatalf("seed: %v", err)
			}

			var attempts []int
			body := `{"seed":true`
			srv.SetAfterPremergeHookForTest(func(attempt int) {
				attempts = append(attempts, attempt)
				// Whole-document writes that keep every field so far, so a lost field can only
				// be the upsert's doing.
				body += fmt.Sprintf(`,"between%d":%d`, attempt, attempt)
				if _, err := srv.Replace(ns, id, body+"}", auth.Principal{}); err != nil {
					t.Errorf("interfering write: %v", err)
				}
			})
			before, commitsBefore := srv.PremergeOutcomes(), srv.dag.CommitCount()
			if _, err := srv.Upsert(ns, id, `{"mine":true}`, auth.Principal{}); err != nil {
				t.Fatalf("upsert: %v", err)
			}
			srv.SetAfterPremergeHookForTest(nil)

			if len(attempts) != bound+1 {
				t.Fatalf("premerged %d times (%v), want %d", len(attempts), attempts, bound+1)
			}
			after := srv.PremergeOutcomes()
			if got := after.Retried - before.Retried; got != int64(bound) {
				t.Errorf("retried = %d, want %d", got, bound)
			}
			if got := after.MergedAtGate - before.MergedAtGate; got != 1 {
				t.Errorf("merged at gate = %d, want 1 (the last attempt)", got)
			}
			if got := srv.dag.CommitCount() - commitsBefore; got != bound+2 {
				t.Errorf("commits = %d, want %d (each interfering write and one for the upsert)", got, bound+2)
			}
			got := readFields(t, srv, ns, id)
			want := []string{"seed", "mine"}
			for a := 0; a <= bound; a++ {
				want = append(want, fmt.Sprintf("between%d", a))
			}
			for _, k := range want {
				if _, ok := got[k]; !ok {
					t.Errorf("field %q lost: %v", k, got)
				}
			}
		})
	}
}

// TestUpsertBuildsOnTheUpsertQueuedAheadOfIt: two Upserts of one document queued at once. The
// second merges onto the first's result rather than the version at head, so when the first commits
// as built the second is current at the gate too - neither is redone, and both fields survive.
// Merged onto head, the second was stale every time.
func TestUpsertBuildsOnTheUpsertQueuedAheadOfIt(t *testing.T) {
	const ns = "app/data"
	srv := openUpsertTestRuntime(t, ns)
	id := mustRandomUUID(t)
	if _, err := srv.Upsert(ns, id, `{"seed":true}`, auth.Principal{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	before := srv.PremergeOutcomes()

	// Hold the gate so both writes queue behind it, the first ahead of the second.
	release, err := srv.AcquireWriteSlotForTest()
	if err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 2)
	upsert := func(body string) { _, err := srv.Upsert(ns, id, body, auth.Principal{}); errs <- err }
	go upsert(`{"first":1}`)
	waitForQueueDepth(t, srv, 2)
	go upsert(`{"second":2}`)
	waitForQueueDepth(t, srv, 3)
	release()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}

	after := srv.PremergeOutcomes()
	if got := after.Current - before.Current; got != 2 {
		t.Errorf("current = %d, want 2", got)
	}
	if got := after.Retried + after.MergedAtGate - before.Retried - before.MergedAtGate; got != 0 {
		t.Errorf("%d stale premerges, want none", got)
	}
	got := readFields(t, srv, ns, id)
	for _, k := range []string{"seed", "first", "second"} {
		if _, ok := got[k]; !ok {
			t.Errorf("field %q lost: %v", k, got)
		}
	}
}

// TestUpsertBuiltOnAWriteThatFailsIsRedone: the write queued ahead fails at the gate (here, the
// schema refuses it), so the version the second Upsert merged onto never exists. The second is
// stale at the gate, merges again on what is really there, and the refused write's field does
// not appear.
func TestUpsertBuiltOnAWriteThatFailsIsRedone(t *testing.T) {
	const ns = "app/data"
	f, err := schema.NewField("first", schema.StringType{}, false, false, false)
	if err != nil {
		t.Fatal(err)
	}
	sch, err := schema.Build([]schema.Field{f}, 1, codec.TimestampNow(), "")
	if err != nil {
		t.Fatal(err)
	}
	srv := openUpsertTestRuntimeWithSchema(t, ns, sch)
	id := mustRandomUUID(t)
	if _, err := srv.Upsert(ns, id, `{"seed":true}`, auth.Principal{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	before := srv.PremergeOutcomes()

	release, err := srv.AcquireWriteSlotForTest()
	if err != nil {
		t.Fatal(err)
	}
	refused, second := make(chan error, 1), make(chan error, 1)
	go func() { _, err := srv.Upsert(ns, id, `{"first":1}`, auth.Principal{}); refused <- err }()
	waitForQueueDepth(t, srv, 2)
	go func() { _, err := srv.Upsert(ns, id, `{"second":2}`, auth.Principal{}); second <- err }()
	waitForQueueDepth(t, srv, 3)
	release()
	if err := <-refused; err == nil {
		t.Fatal("a number in a string field was accepted")
	}
	if err := <-second; err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	after := srv.PremergeOutcomes()
	if got := after.Retried - before.Retried; got != 1 {
		t.Errorf("retried = %d, want 1 (the write built on the refused one)", got)
	}
	got := readFields(t, srv, ns, id)
	if _, ok := got["first"]; ok {
		t.Errorf("a refused write left its field: %v", got)
	}
	for _, k := range []string{"seed", "second"} {
		if _, ok := got[k]; !ok {
			t.Errorf("field %q lost: %v", k, got)
		}
	}
}

// waitForQueueDepth waits until depth writers are queued or running on the write gate.
func waitForQueueDepth(t *testing.T, srv *KdbServerRuntime, depth int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for srv.WriteQueueDepth() < depth {
		if time.Now().After(deadline) {
			t.Fatalf("queue depth %d, want %d", srv.WriteQueueDepth(), depth)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestUpsertBuiltOnAQueuedWriteThatItOvertookBuildsOnItAgain: B merges onto A's result, but reaches
// the gate first, where A has not committed - B is stale. B's retry must build on A again, which
// is still queued, not on head: on head it would be stale again behind A and end merging under
// the gate.
func TestUpsertBuiltOnAQueuedWriteThatItOvertookBuildsOnItAgain(t *testing.T) {
	const ns = "app/data"
	srv := openUpsertTestRuntime(t, ns)
	id := mustRandomUUID(t)
	if _, err := srv.Upsert(ns, id, `{"seed":true}`, auth.Principal{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	before := srv.PremergeOutcomes()

	release, err := srv.AcquireWriteSlotForTest()
	if err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 2)
	var overtake atomic.Bool
	overtake.Store(true)
	srv.SetAfterPremergeHookForTest(func(attempt int) {
		// A has premerged: B premerges onto it and queues before A does.
		if overtake.CompareAndSwap(true, false) {
			go func() { _, err := srv.Upsert(ns, id, `{"b":2}`, auth.Principal{}); errs <- err }()
			waitForQueueDepth(t, srv, 2)
		}
	})
	go func() { _, err := srv.Upsert(ns, id, `{"a":1}`, auth.Principal{}); errs <- err }()
	waitForQueueDepth(t, srv, 3)
	release()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}
	srv.SetAfterPremergeHookForTest(nil)

	after := srv.PremergeOutcomes()
	if got := after.Retried - before.Retried; got != 1 {
		t.Errorf("retried = %d, want 1 (B, overtaking A)", got)
	}
	if got := after.MergedAtGate - before.MergedAtGate; got != 0 {
		t.Errorf("merged at gate = %d, want 0 (B's retry built on A)", got)
	}
	got := readFields(t, srv, ns, id)
	for _, k := range []string{"seed", "a", "b"} {
		if _, ok := got[k]; !ok {
			t.Errorf("field %q lost: %v", k, got)
		}
	}
}

// TestAnUpsertThatPanicsLeavesNothingInFlight: however an Upsert ends, its premerge leaves the
// in-flight record. One left behind would be built on by every later Upsert of the document, each
// of them stale at the gate.
func TestAnUpsertThatPanicsLeavesNothingInFlight(t *testing.T) {
	const ns = "app/data"
	srv := openUpsertTestRuntime(t, ns)
	id := mustRandomUUID(t)
	if _, err := srv.Upsert(ns, id, `{"seed":true}`, auth.Principal{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	srv.SetAfterPremergeHookForTest(func(int) { panic(errRefusedForTest) })
	func() {
		defer func() {
			if r := recover(); r != errRefusedForTest {
				panic(r)
			}
		}()
		_, _ = srv.Upsert(ns, id, `{"a":1}`, auth.Principal{})
	}()
	srv.SetAfterPremergeHookForTest(nil)
	if p := srv.inFlight.newest(id); p != nil {
		t.Fatalf("the panicked upsert is still in flight")
	}
	before := srv.PremergeOutcomes()
	if _, err := srv.Upsert(ns, id, `{"b":2}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	if got := srv.PremergeOutcomes().Current - before.Current; got != 1 {
		t.Errorf("the next upsert was not current at the gate")
	}
}

var errRefusedForTest = fmt.Errorf("refused for test")
