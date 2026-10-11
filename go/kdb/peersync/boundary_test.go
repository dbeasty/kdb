package peersync

import (
	"errors"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

// boundaryScenario: local is bootstrapped from remote at root; remote then writes side on f, a
// commit behind root, and merges it into main. Returns f, side and remote's merged head.
func boundaryScenario(t *testing.T, hub string, local, remote *testNamespaces, ns string) (f, side, head document.Commit) {
	t.Helper()
	r := remote.side(ns)
	f = writeAtHead(t, r, ns, newUUID(t), `{"a":1}`)
	writeAtHead(t, r, ns, newUUID(t), `{"b":1}`)
	root := writeAtHead(t, r, ns, newUUID(t), `{"c":1}`)
	v2Hub(t, hub, newHost(remote), nil)
	syncV2(t, hub, local, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncPull, PreferSnapshot: true})

	side = writeDoc(t, r, ns, f.Hash, newUUID(t), `{"side":1}`)
	setHead(t, r, root.Hash) // writeDoc moved main to side; the remote's main is root's line
	env, err := remote.Env(ns, false)
	if err != nil {
		t.Fatal(err)
	}
	res, err := Adopt(env, side.Hash)
	if err != nil || res.Outcome.Kind != OutcomeMerged {
		t.Fatalf("remote should merge the side commit: %+v %v", res, err)
	}
	head, _ = r.dag.GetCommit(res.Head)
	return f, side, head
}

// TestTakingABranchBelowTheRootDoesNotReadoptTheForkPoint: the fork point a pull took in below
// this node's root is a shallow root, logged on its own. The fast-forward that follows adopts the
// side commit and the merge - not the fork point, whose operations are a change from parents this
// node does not hold: logging them as adopted, or announcing them to commit listeners, would be
// wrong.
func TestTakingABranchBelowTheRootDoesNotReadoptTheForkPoint(t *testing.T) {
	ns := "app/boundary-adopt"
	remote := newTestNamespaces(t, ns)
	local := newTestNamespaces(t, ns)
	var mu sync.Mutex
	var logged []codec.Hash
	local.persistAsync = func(c document.Commit) (func() error, error) {
		mu.Lock()
		logged = append(logged, c.Hash)
		mu.Unlock()
		return func() error { return nil }, nil
	}
	f, side, head := boundaryScenario(t, "hub-boundary-adopt", local, remote, ns)

	res := syncV2(t, "hub-boundary-adopt", local, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncPull})
	if err := res.Namespaces[0].Err; err != nil {
		t.Fatal(err)
	}
	if got := res.Namespaces[0].BoundaryExtended; len(got) != 1 || got[0] != f.Hash.Hex() {
		t.Fatalf("the fork point should have been taken in: %v", got)
	}
	if mustHead(t, local.side(ns)) != head.Hash {
		t.Fatal("local did not reach the remote's head")
	}
	counts := map[codec.Hash]int{}
	for _, h := range logged {
		counts[h]++
	}
	if counts[f.Hash] != 1 {
		t.Fatalf("the fork point should be logged once, by the extension itself, got %d", counts[f.Hash])
	}
	if counts[side.Hash] != 1 || counts[head.Hash] != 1 {
		t.Fatalf("the side commit and the merge should each be logged once as adopted: %v", counts)
	}
}

// TestBoundaryErrorIsTyped: when the fork point's state cannot be had from the peer, the pull fails
// with a ShallowBoundaryError naming the commit and the missing parent - not a bare "missing
// parent" - and main is left where it was.
func TestBoundaryErrorIsTyped(t *testing.T) {
	ns := "app/boundary-typed"
	remote := newTestNamespaces(t, ns)
	local := newTestNamespaces(t, ns)
	local.grafted = func(codec.Hash) error { return errors.New("disk full") }
	f, side, _ := boundaryScenario(t, "hub-boundary-typed", local, remote, ns)
	before := mustHead(t, local.side(ns))

	res, err := SyncV2(wire.NewCodec(wire.EncodingJSON), stream.NewInMemoryTransport(), V2ClientConfig{
		NodeID: "client", PeerURI: "memory://hub-boundary-typed", Local: local, Namespaces: []string{ns}, Mode: SyncPull,
	})
	if err != nil {
		t.Fatal(err)
	}
	var be *ShallowBoundaryError
	if !errors.As(res.Namespaces[0].Err, &be) {
		t.Fatalf("want a ShallowBoundaryError, got %v", res.Namespaces[0].Err)
	}
	if be.Missing != f.Hash || be.Commit != side.Hash {
		t.Fatalf("the error should name side and its parent f: %+v", be)
	}
	if mustHead(t, local.side(ns)) != before {
		t.Fatal("main moved although the pull failed")
	}
	if local.side(ns).dag.HasCommit(f.Hash) {
		t.Fatal("a fork point that could not be recorded must not stay resident")
	}

	// Once it can be recorded, the next sync takes it.
	local.grafted = nil
	syncV2(t, "hub-boundary-typed", local, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncPull})
}
