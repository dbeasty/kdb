package peersync

import (
	"errors"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

// deepenFixture is the case Phase 14 exists for: A was bootstrapped from B by snapshot, so it holds
// B's head as a shallow root and nothing below it, while B still has the whole history. A's log is
// wired to a switch the tests fail the deepen through, and records every commit it is handed.
type deepenFixture struct {
	ns   string
	a, b *testNamespaces
	// root is A's shallow root: B's head at bootstrap time.
	root codec.Hash
	// ancestors are the commits below root that B holds and A does not, genesis excluded - what a
	// deepen has to store and log.
	ancestors []codec.Hash
	// failing makes every one of A's log writes fail while it is set.
	failing bool
	// failQueue fails under the serialization (persistAll never returns a wait) rather than on the
	// fsync waited for after it.
	failQueue bool
	// logged is every commit handed to A's log since resetLog, in order.
	logged []codec.Hash
	hub    string
}

var errLogFull = errors.New("log device is full")

func newDeepenFixture(t *testing.T, hub string, source *testNamespaces, sourceHub string) *deepenFixture {
	t.Helper()
	f := &deepenFixture{ns: "app/deepen", b: source, hub: hub}
	f.a = newTestNamespaces(t, f.ns)
	boot := syncV2(t, sourceHub, f.a, V2ClientConfig{Namespaces: []string{f.ns}, Mode: SyncPull, PreferSnapshot: true})
	if boot.Namespaces[0].Snapshot == "" {
		t.Fatal("A was not bootstrapped from a snapshot")
	}
	f.root, _ = codec.HashFromHex(boot.Namespaces[0].Snapshot)
	if !f.a.side(f.ns).dag.IsShallow(f.root) {
		t.Fatalf("A should hold %s as a shallow root", f.root.Hex())
	}
	// Every commit of the source below the root, which A has none of yet. Genesis is left out: it
	// is not logged, because every DAG makes its own when it opens.
	for _, e := range source.side(f.ns).dag.Walk(f.root, nil, 1_000) {
		entry, ok := e.(dag.FullEntry)
		if !ok {
			continue
		}
		full := entry.Commit
		if full.Hash == f.root || len(full.ParentHashes) == 0 {
			continue
		}
		f.ancestors = append(f.ancestors, full.Hash)
	}
	if len(f.ancestors) < 2 {
		t.Fatalf("the fixture needs history below the root, found %d commits", len(f.ancestors))
	}
	// Installed after the bootstrap, so only the deepen's own log writes are seen.
	f.a.persistAsync = func(c document.Commit) (func() error, error) {
		f.logged = append(f.logged, c.Hash)
		if f.failing {
			if f.failQueue {
				return nil, errLogFull
			}
			return func() error { return errLogFull }, nil
		}
		return func() error { return nil }, nil
	}
	v2Hub(t, hub, newHost(f.a), nil)
	return f
}

// newFullDeepenFixture sources A from a node holding history all the way down, so a completed
// deepen leaves no shallow root at all.
func newFullDeepenFixture(t *testing.T, prefix string) *deepenFixture {
	t.Helper()
	ns := "app/deepen"
	b := newTestNamespaces(t, ns)
	seedHistory(t, b, ns, 6)
	v2Hub(t, prefix+"-b", newHost(b), nil)
	return newDeepenFixture(t, prefix+"-a", b, prefix+"-b")
}

// deepen runs one Deepen of the root against the fixture's source over a fresh repair session.
func (f *deepenFixture) deepen(t *testing.T, sourceHub string) (DeepenResult, error) {
	t.Helper()
	s, err := OpenRepairSession(wire.NewCodec(wire.EncodingJSON), stream.NewInMemoryTransport(),
		V2ClientConfig{NodeID: "a", PeerURI: "memory://" + sourceHub, Namespaces: []string{f.ns}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	env, err := f.a.Env(f.ns, false)
	if err != nil {
		t.Fatal(err)
	}
	return Deepen(env, s, f.root)
}

func (f *deepenFixture) resetLog() { f.logged = nil }

// requireLogged: every one of want is in the order the log was handed, parents before children.
func (f *deepenFixture) requireLogged(t *testing.T, want []codec.Hash) {
	t.Helper()
	at := map[codec.Hash]int{}
	for i, h := range f.logged {
		if _, seen := at[h]; !seen {
			at[h] = i
		}
	}
	for _, h := range want {
		if _, ok := at[h]; !ok {
			t.Fatalf("commit %s is in A's DAG but was never logged (logged %d frames)", h.Hex(), len(f.logged))
		}
	}
	dag := f.a.side(f.ns).dag
	for _, h := range want {
		c, err := dag.GetCommitOrThrow(h)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range c.ParentHashes {
			pi, ok := at[p]
			if !ok {
				continue // below this namespace's history: a horizon's missing parent
			}
			if pi > at[h] {
				t.Fatalf("commit %s was logged before its parent %s", h.Hex(), p.Hex())
			}
		}
	}
}

// TestDeepenCanBeRetriedAfterAFailedPersist is the regression this exists for. The deepen stores
// every fetched commit in the DAG and then fails on the fsync behind the log write. Those commits
// stay resident - there is no taking them back, the DAG has no general commit delete and the log
// cannot be written first (see Deepen) - so the retry has to log them. Its store loop skips a
// commit the DAG already holds, and while what to log was taken from that loop the retry logged
// nothing but the root: a deepen that reported success over history no restart would come up with.
func TestDeepenCanBeRetriedAfterAFailedPersist(t *testing.T) {
	f := newFullDeepenFixture(t, "deepen-retry")
	f.failing = true

	res, err := f.deepen(t, "deepen-retry-b")
	if err == nil || !strings.Contains(err.Error(), errLogFull.Error()) {
		t.Fatalf("the first deepen should have failed on the log, got %v (%+v)", err, res)
	}
	if res.Unshallowed {
		t.Fatal("a deepen that could not log its history reported the root unshallowed")
	}
	dag := f.a.side(f.ns).dag
	if !dag.IsShallow(f.root) {
		t.Fatal("a failed deepen unshallowed the root")
	}
	// The history is resident, and not logged: exactly the state the retry has to finish.
	for _, h := range f.ancestors {
		if !dag.HasCommit(h) {
			t.Fatalf("the failed deepen did not leave %s resident", h.Hex())
		}
	}

	f.failing = false
	f.resetLog()
	res, err = f.deepen(t, "deepen-retry-b")
	if err != nil {
		t.Fatalf("the retry failed: %v", err)
	}
	if !res.Unshallowed || dag.IsShallow(f.root) {
		t.Fatalf("the retry did not unshallow the root: %+v", res)
	}
	if res.Fetched != 0 {
		t.Fatalf("the retry should not have re-fetched anything, fetched %d", res.Fetched)
	}
	// The point of the whole fix: the retry logged the history the first attempt left behind,
	// rather than passing over it because the DAG already held it.
	f.requireLogged(t, append(append([]codec.Hash(nil), f.ancestors...), f.root))
	if len(dag.ShallowRoots()) != 0 {
		t.Fatalf("deepening from a node with the whole history should leave no shallow roots: %v", dag.ShallowRoots())
	}
}

// TestDeepenCanBeRetriedWhenTheLogRefusesTheWrite is the same retry reached from inside the
// serialization, where persistAll never returns a wait to fail.
func TestDeepenCanBeRetriedWhenTheLogRefusesTheWrite(t *testing.T) {
	f := newFullDeepenFixture(t, "deepen-refuse")
	f.failing, f.failQueue = true, true

	if _, err := f.deepen(t, "deepen-refuse-b"); err == nil || !strings.Contains(err.Error(), errLogFull.Error()) {
		t.Fatalf("the first deepen should have failed on the log, got %v", err)
	}
	f.failing = false
	f.resetLog()
	res, err := f.deepen(t, "deepen-refuse-b")
	if err != nil {
		t.Fatalf("the retry failed: %v", err)
	}
	if !res.Unshallowed {
		t.Fatalf("the retry did not unshallow the root: %+v", res)
	}
	f.requireLogged(t, append(append([]codec.Hash(nil), f.ancestors...), f.root))
}

// TestDeepenRetryStillRecordsTheHorizon: the same retry for a partial deepen, from a peer whose own
// history is shallow. The first attempt admits the peer's root as this namespace's new horizon and
// records it in the marker, then fails to log it. The retry fetches nothing, so a horizon collected
// as commits were admitted would be empty - and the marker would go unwritten while the horizon's
// commit went into the log, which is a namespace that fails to open: a replay meeting a parentless
// commit the marker does not name cannot admit it. The horizon is therefore read back out of the
// DAG, not remembered.
func TestDeepenRetryStillRecordsTheHorizon(t *testing.T) {
	ns := "app/deepen"
	full := newTestNamespaces(t, ns)
	seedHistory(t, full, ns, 5)
	v2Hub(t, "deepen-horizon-full", newHost(full), nil)

	// mid is itself bootstrapped from full, so its history starts at midRoot.
	mid := newTestNamespaces(t, ns)
	bootMid := syncV2(t, "deepen-horizon-full", mid, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncPull, PreferSnapshot: true})
	midRoot, _ := codec.HashFromHex(bootMid.Namespaces[0].Snapshot)
	writeAtHead(t, mid.side(ns), ns, newUUID(t), `{"on":"mid"}`)
	writeAtHead(t, mid.side(ns), ns, newUUID(t), `{"on":"mid2"}`)
	v2Hub(t, "deepen-horizon-mid", newHost(mid), nil)

	f := newDeepenFixture(t, "deepen-horizon-a", mid, "deepen-horizon-mid")
	var markers [][]codec.Hash
	f.a.shallowRootsChanged = func(roots []codec.Hash) error {
		markers = append(markers, append([]codec.Hash(nil), roots...))
		return nil
	}
	f.failing = true

	if _, err := f.deepen(t, "deepen-horizon-mid"); err == nil || !strings.Contains(err.Error(), errLogFull.Error()) {
		t.Fatalf("the first deepen should have failed on the log, got %v", err)
	}
	if len(markers) == 0 || !containsHash(markers[len(markers)-1], midRoot) {
		t.Fatalf("the failed attempt should have recorded %s as a shallow root, markers %v", midRoot.Hex(), markers)
	}

	f.failing = false
	f.resetLog()
	markers = nil
	res, err := f.deepen(t, "deepen-horizon-mid")
	if err != nil {
		t.Fatalf("the retry failed: %v", err)
	}
	if len(res.Horizon) != 1 || res.Horizon[0] != midRoot {
		t.Fatalf("the retry should report %s as the horizon, got %v", midRoot.Hex(), res.Horizon)
	}
	if len(markers) == 0 || !containsHash(markers[len(markers)-1], midRoot) {
		t.Fatalf("the retry must record the horizon in the marker again, markers %v", markers)
	}
	// The horizon and the history above it are in the log, so a replay can admit them.
	f.requireLogged(t, append(append([]codec.Hash(nil), f.ancestors...), f.root))
	if !containsHash(f.logged, midRoot) {
		t.Fatalf("the horizon commit %s was never logged", midRoot.Hex())
	}
	if !f.a.side(f.ns).dag.IsShallow(midRoot) {
		t.Fatal("the peer's own root should be A's new shallow root")
	}
}

func containsHash(hs []codec.Hash, want codec.Hash) bool {
	for _, h := range hs {
		if h == want {
			return true
		}
	}
	return false
}
