package server

import "testing"

// TestServerRuntimeRegistry_ReleasesAndReopensFresh is the regression test for the finding
// recorded in docs/kdb-finish-up-plan.md as 1-G5: GetOrOpen retained a fresh runtime twice on
// top of NewKdbServerRuntime's own initial refCount of 1 (so refCount started at 3, not 2), and
// Release never removed the registry's map entry even once a runtime's refCount actually
// reached zero and closed - so a single caller opening then releasing left a closed,
// zero-refCount runtime sitting in the map, silently handed back out to the next GetOrOpen for
// the same key instead of that key being reopened fresh.
func TestServerRuntimeRegistry_ReleasesAndReopensFresh(t *testing.T) {
	r := NewServerRuntimeRegistry()
	opens := 0
	open := func() (*KdbServerRuntime, error) {
		opens++
		return newTestRuntime(t), nil
	}

	rt1, err := r.GetOrOpen("ns", open)
	if err != nil {
		t.Fatal(err)
	}
	if opens != 1 {
		t.Fatalf("expected 1 open, got %d", opens)
	}

	r.Release("ns")

	r.mu.Lock()
	_, stillPresent := r.runtimes["ns"]
	r.mu.Unlock()
	if stillPresent {
		t.Fatal("expected the registry entry to be removed once the only caller released it")
	}

	rt2, err := r.GetOrOpen("ns", open)
	if err != nil {
		t.Fatal(err)
	}
	if opens != 2 {
		t.Fatalf("expected GetOrOpen to reopen fresh after a full release, got %d total opens", opens)
	}
	if rt1 == rt2 {
		t.Fatal("expected a distinct runtime instance after the first was fully released and closed")
	}
	r.Release("ns")
}

// TestServerRuntimeRegistry_SharedAcrossConcurrentCallers is 1-G5's other half: two callers
// sharing one registry entry must not have it closed out from under the second caller by the
// first one's Release, and must not reopen a fresh instance while any caller still holds a
// reference.
func TestServerRuntimeRegistry_SharedAcrossConcurrentCallers(t *testing.T) {
	r := NewServerRuntimeRegistry()
	opens := 0
	open := func() (*KdbServerRuntime, error) {
		opens++
		return newTestRuntime(t), nil
	}

	rtA, err := r.GetOrOpen("ns", open)
	if err != nil {
		t.Fatal(err)
	}
	rtB, err := r.GetOrOpen("ns", open)
	if err != nil {
		t.Fatal(err)
	}
	if opens != 1 {
		t.Fatalf("expected the second GetOrOpen to hit the cache, got %d opens", opens)
	}
	if rtA != rtB {
		t.Fatal("expected both callers to share the same runtime instance")
	}

	r.Release("ns") // A's checkout
	r.mu.Lock()
	_, stillPresent := r.runtimes["ns"]
	r.mu.Unlock()
	if !stillPresent {
		t.Fatal("expected the registry entry to survive while B still holds a reference")
	}

	rtC, err := r.GetOrOpen("ns", open)
	if err != nil {
		t.Fatal(err)
	}
	if opens != 1 {
		t.Fatalf("expected GetOrOpen to still hit the cache while a reference is outstanding, got %d opens", opens)
	}
	if rtC != rtA {
		t.Fatal("expected the same runtime instance while a reference is still outstanding")
	}

	r.Release("ns") // B's checkout
	r.Release("ns") // C's checkout - the last one, should fully close and remove
	r.mu.Lock()
	_, stillPresent = r.runtimes["ns"]
	r.mu.Unlock()
	if stillPresent {
		t.Fatal("expected the registry entry to be removed once every outstanding reference released")
	}
}
