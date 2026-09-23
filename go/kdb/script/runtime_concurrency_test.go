package script

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
)

// Runtime's own doc comment promises it is safe for concurrent use: one process-wide compiled-
// program cache (server.procRuntime is exactly this, shared across every namespace a process
// serves), a brand-new goja.Runtime per call. TestCallsCannotShareState (runtime_test.go) proves
// the no-leakage half of that sequentially, three calls in a row. These prove it under real
// concurrency, with go test -race able to catch what a sequential test cannot: a race on the
// program cache, or two calls quietly sharing one goja.Runtime under load.

// TestConcurrentCallsShareNoState: many goroutines call the same compiled procedure at once. If
// they shared a goja.Runtime - or even raced on setting one up - a document's resolution would
// depend on how many other conflicts happened to be resolving concurrently, which is exactly the
// kind of order-dependence an in-merge decision cannot tolerate (two nodes are never guaranteed to
// resolve conflicts in the same order, let alone the same interleaving).
func TestConcurrentCallsShareNoState(t *testing.T) {
	r := New(Limits{})
	const src = `
		var seen = (typeof seen === "undefined") ? 0 : seen;
		seen++;
		function main() { return { seen: seen }; }
	`
	// Compile once so every goroutine races the same cached *goja.Program, not just the call.
	if err := r.Compile(src); err != nil {
		t.Fatal(err)
	}

	const n = 200
	type outcome struct {
		seen int
		err  error
	}
	results := make([]outcome, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got, err := call(t, r, src, `{}`)
			if err != nil {
				results[i] = outcome{err: err}
				return
			}
			var parsed struct {
				Seen int `json:"seen"`
			}
			if uerr := json.Unmarshal([]byte(got.Value), &parsed); uerr != nil {
				results[i] = outcome{err: uerr}
				return
			}
			results[i] = outcome{seen: parsed.Seen}
		}(i)
	}
	wg.Wait()

	for i, o := range results {
		if o.err != nil {
			t.Fatalf("goroutine %d: %v", i, o.err)
		}
		if o.seen != 1 {
			t.Fatalf("goroutine %d saw seen=%d, want 1 - a global leaked across concurrent calls", i, o.seen)
		}
	}
}

// TestConcurrentResolveConflictCallsAreIndependentlyCorrect: the same runtime, the same compiled
// procedure, hundreds of goroutines each resolving a *different* conflict at once. Every result
// must match what that goroutine's own input implies - proof that concurrent calls never see, or
// decide from, one another's arguments.
func TestConcurrentResolveConflictCallsAreIndependentlyCorrect(t *testing.T) {
	const src = `function main(c) { return { take: c.local.qty >= c.remote.qty ? "local" : "remote" }; }`
	r := New(Limits{})
	if err := r.Compile(src); err != nil {
		t.Fatal(err)
	}

	const n = 300
	errs := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			localQty, remoteQty := i%7, (i*3)%11
			dec, err := r.ResolveConflict(context.Background(), src, ModePure, nil, ConflictInput{
				DocID:  fmt.Sprintf("doc-%d", i),
				Local:  ptr(fmt.Sprintf(`{"qty":%d}`, localQty)),
				Remote: ptr(fmt.Sprintf(`{"qty":%d}`, remoteQty)),
			})
			if err != nil {
				errs[i] = fmt.Sprintf("goroutine %d: %v", i, err)
				return
			}
			wantSide := 1
			if localQty >= remoteQty {
				wantSide = 0
			}
			if dec.Kind != DecideTake || dec.Side != wantSide {
				errs[i] = fmt.Sprintf("goroutine %d: local=%d remote=%d got %+v, want side %d",
					i, localQty, remoteQty, dec, wantSide)
			}
		}(i)
	}
	wg.Wait()

	for _, e := range errs {
		if e != "" {
			t.Error(e)
		}
	}
}

// TestConcurrentCompileAndCallRaceTheProgramCache: definition and execution are different code
// paths in production (a procedure is compiled once at SetProcedure time, then called on every
// later conflict), but nothing stops a redefinition from racing calls still using the old
// revision - server.KdbServerRuntime.procs is a copy-on-write atomic.Pointer for exactly this
// reason. This drives Runtime.Compile and Runtime.Call concurrently on two different sources that
// hash differently, so the cache itself - keyed by source hash - is what has to hold up under
// -race, independent of that outer copy-on-write table.
func TestConcurrentCompileAndCallRaceTheProgramCache(t *testing.T) {
	r := New(Limits{})
	const srcA = `function main() { return { v: "a" }; }`
	const srcB = `function main() { return { v: "b" }; }`

	var wg sync.WaitGroup
	errs := make(chan string, 400)
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			src, want := srcA, `{"v":"a"}`
			if i%2 == 1 {
				src, want = srcB, `{"v":"b"}`
			}
			if err := r.Compile(src); err != nil {
				errs <- fmt.Sprintf("compile %d: %v", i, err)
				return
			}
			got, err := call(t, r, src, `{}`)
			if err != nil {
				errs <- fmt.Sprintf("call %d: %v", i, err)
				return
			}
			if got.Value != want {
				errs <- fmt.Sprintf("call %d: got %s, want %s - the program cache mixed up two sources", i, got.Value, want)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}
