package engine_test

import (
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

func newEngine(t *testing.T, ns string) *engine.ServerEngine {
	t.Helper()
	h, err := engine.DefaultFactory{EngineTarget: engine.TargetInMemory}.Open(ns, storage.StorageEngineConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	e, ok := h.Adapter().(*engine.ServerEngine)
	if !ok {
		t.Fatalf("%s: not a ServerEngine", ns)
	}
	return e
}

func putAndCommit(t *testing.T, a storage.Adapter, ns, json string) codec.UUID {
	t.Helper()
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	doc, err := document.FromJSONWithID(id, json)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.PutDocument(ns, doc); err != nil {
		t.Fatalf("put into %s: %v", ns, err)
	}
	if _, err := a.CommitTree(ns, codec.Hash{}); err != nil {
		t.Fatalf("commit %s: %v", ns, err)
	}
	return id
}

// Writes through the multiplexer land in the namespace they name, and nowhere else. ServerEngine
// ignores its namespaceID parameter - the engine *is* the namespace - so if routing were wrong
// the write would silently go to the other engine and read back from the wrong one.
func TestMultiplexRoutesByNamespace(t *testing.T) {
	users, sessions := newEngine(t, "app/users"), newEngine(t, "app/sessions")
	m := engine.NewMultiplexAdapter()
	m.Register("app/users", users)
	m.Register("app/sessions", sessions)

	uid := putAndCommit(t, m, "app/users", `{"name":"ada"}`)
	sid := putAndCommit(t, m, "app/sessions", `{"token":"t-1"}`)

	uTree, err := m.CommitTree("app/users", codec.Hash{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := m.GetDocument("app/users", uid, uTree.TreeHash)
	if err != nil || got == nil {
		t.Fatalf("app/users lost its document: %v %v", got, err)
	}
	if other, err := m.GetDocument("app/users", sid, uTree.TreeHash); err == nil && other != nil {
		t.Fatal("app/users returned app/sessions' document - routing collapsed the two")
	}
	if got := m.Namespaces(); len(got) != 2 || got[0] != "app/sessions" || got[1] != "app/users" {
		t.Fatalf("Namespaces() = %v", got)
	}
}

// An unknown namespace is a typed error, never a fallback. Routing it to some other engine would
// write one namespace's documents into another's files: silent, permanent, and invisible until
// something read the wrong namespace back. The private predecessor of this type panicked here,
// which takes down every other connection too.
func TestMultiplexUnknownNamespaceIsATypedError(t *testing.T) {
	m := engine.NewMultiplexAdapter()
	m.Register("app/users", newEngine(t, "app/users"))

	_, err := m.GetDocument("app/missing", codec.UUID{}, codec.Hash{})
	var unknown *engine.ErrUnknownNamespace
	if !errors.As(err, &unknown) {
		t.Fatalf("got %v, want *ErrUnknownNamespace", err)
	}
	if unknown.NamespaceID != "app/missing" {
		t.Fatalf("error names %q, want app/missing", unknown.NamespaceID)
	}
	if err := m.PutDocument("app/missing", document.Document{}); !errors.As(err, &unknown) {
		t.Fatalf("PutDocument to an unrouted namespace returned %v", err)
	}
	if err := m.Flush("app/missing"); !errors.As(err, &unknown) {
		t.Fatalf("Flush on an unrouted namespace returned %v", err)
	}
}

// Unregister must make a namespace unreachable rather than leaving it resolvable through a stale
// route - a closed namespace's engine is being torn down behind it.
func TestMultiplexUnregisterStopsRouting(t *testing.T) {
	m := engine.NewMultiplexAdapter()
	m.Register("app/users", newEngine(t, "app/users"))
	if _, err := m.CommitTree("app/users", codec.Hash{}); err != nil {
		t.Fatal(err)
	}
	m.Unregister("app/users")

	var unknown *engine.ErrUnknownNamespace
	if _, err := m.CommitTree("app/users", codec.Hash{}); !errors.As(err, &unknown) {
		t.Fatalf("a namespace still routed after Unregister: %v", err)
	}
	if got := m.Namespaces(); len(got) != 0 {
		t.Fatalf("Namespaces() = %v after unregistering the only one", got)
	}
}

// A delta segment carries the namespace it belongs to, so it must route on that. The private
// predecessor sent these to an arbitrary member - safe only because its one caller never
// ingested anything; it would otherwise fold one namespace's commits into another's history.
func TestMultiplexIngestRoutesOnTheSegmentsNamespace(t *testing.T) {
	m := engine.NewMultiplexAdapter()
	m.Register("app/users", newEngine(t, "app/users"))

	err := m.IngestDeltaSegment(storage.DeltaSegmentRef{NamespaceID: "app/elsewhere"})
	var unknown *engine.ErrUnknownNamespace
	if !errors.As(err, &unknown) {
		t.Fatalf("ingest for an unrouted namespace returned %v, want *ErrUnknownNamespace", err)
	}
}

// Blobs are content-addressed and the interface carries no namespace, so a blob written through
// the multiplexer has to be findable through it again.
func TestMultiplexBlobRoundTrip(t *testing.T) {
	m := engine.NewMultiplexAdapter()
	m.Register("app/users", newEngine(t, "app/users"))
	m.Register("app/sessions", newEngine(t, "app/sessions"))

	h, err := m.WriteBlob([]byte("hello"))
	if err != nil {
		t.Fatalf("write blob: %v", err)
	}
	got, err := m.ReadBlob(h)
	if err != nil {
		t.Fatalf("read blob: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("read back %q, want %q", got, "hello")
	}
}

// Capabilities must not claim support one of the members lacks.
func TestMultiplexCapabilitiesAreTheIntersection(t *testing.T) {
	m := engine.NewMultiplexAdapter()
	if got := m.Capabilities(); got.PersistsDeltaLog {
		t.Fatal("an empty multiplexer claimed a capability")
	}
	m.Register("app/users", newEngine(t, "app/users"))
	m.Register("app/sessions", newEngine(t, "app/sessions"))
	if got := m.Capabilities(); got.PersistsAcrossReload {
		t.Fatal("in-memory engines reported as persisting across reload")
	}
}
