package server

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// hideDocs is an auth engine that allows everything except reading the listed documents.
type hideDocs struct{ hidden map[string]bool }

func (h hideDocs) Authenticator() auth.Authenticator { return auth.AllowAll.Authenticator() }
func (h hideDocs) Authorizer() auth.Authorizer       { return h }
func (h hideDocs) Authorize(_ context.Context, _ auth.Principal, a auth.Action) error {
	if r, ok := a.(auth.DocumentReadAction); ok && h.hidden[r.DocID] {
		return errors.New("hidden")
	}
	return nil
}

type projectionFixture struct {
	t       *testing.T
	source  *KdbServerRuntime
	replica *KdbServerRuntime
	addr    string
	filter  string
}

func newProjectionFixture(t *testing.T, filter string, engine ...auth.Engine) *projectionFixture {
	t.Helper()
	source := newTestRuntime(t)
	if len(engine) > 0 {
		source.AuthEngine = engine[0] // before the listener starts reading it
	}
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", source, source.Runtime.DefaultNamespace)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	rt, err := embed.OpenMemoryRuntime("app", peersync.ProjectionNamespace("app/data", filter), schema.None())
	if err != nil {
		t.Fatal(err)
	}
	replica := NewKdbServerRuntime(rt)
	replica.ProjectionOf = "app/data"
	return &projectionFixture{t: t, source: source, replica: replica, addr: "tcp://" + ln.Addr().String(), filter: filter}
}

func (f *projectionFixture) put(id codec.UUID, body string) {
	f.t.Helper()
	if _, err := f.source.Upsert("app/data", id, body, auth.Principal{}); err != nil {
		f.t.Fatal(err)
	}
}

func (f *projectionFixture) sync() peersync.ProjectionResult {
	f.t.Helper()
	res, err := peersync.SyncProjection(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.ProjectionConfig{
		NodeID: "replica", PeerURI: f.addr, Namespace: "app/data", Filter: f.filter, PageBytes: 60,
		Target: f.replica.ProjectionTarget(),
	})
	if err != nil {
		f.t.Fatalf("projection sync: %v", err)
	}
	return res
}

func (f *projectionFixture) held() map[codec.UUID]string {
	f.t.Helper()
	ids, err := f.replica.ProjectionTarget().DocIDs()
	if err != nil {
		f.t.Fatal(err)
	}
	out := map[codec.UUID]string{}
	for _, id := range ids {
		body, _, _, _ := f.replica.GetDocument(f.replica.Runtime.DefaultNamespace, id)
		out[id] = body
	}
	return out
}

func TestFilteredReplicaReceivesOnlyMatchingAndFollowsChanges(t *testing.T) {
	f := newProjectionFixture(t, `region = 'EU'`)
	eu1, eu2, eu3, us1 := mustRandomUUID(t), mustRandomUUID(t), mustRandomUUID(t), mustRandomUUID(t)
	f.put(eu1, `{"region":"EU","n":1}`)
	f.put(eu2, `{"region":"EU","n":2}`)
	f.put(eu3, `{"region":"EU","n":3}`)
	f.put(us1, `{"region":"US","n":4}`)

	res := f.sync()
	if !res.Reset {
		t.Fatal("a first sync should be a full snapshot of the filtered set")
	}
	got := f.held()
	if len(got) != 3 || got[us1] != "" {
		t.Fatalf("expected exactly the three EU documents, got %v", got)
	}

	// eu2 leaves the filter, eu3 is deleted at the source, a new EU document appears.
	f.put(eu2, `{"region":"US","n":2}`)
	if _, err := f.source.Commit("app/data", deleteTx(t, f.source, eu3), "", auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	eu4 := mustRandomUUID(t)
	f.put(eu4, `{"region":"EU","n":5}`)

	res = f.sync()
	if res.Reset {
		t.Fatal("a later sync should be a delta, not a new snapshot")
	}
	got = f.held()
	if len(got) != 2 || got[eu1] == "" || got[eu4] == "" {
		t.Fatalf("after the changes expected eu1 and eu4, got %v", got)
	}
	if src, _ := f.replica.ProjectionTarget().LastSource(); src != mustHeadHex(t, f.source) {
		t.Fatalf("projection records source %s, source head is %s", src, mustHeadHex(t, f.source))
	}
	// Nothing changed: nothing to do.
	if res = f.sync(); res.Writes != 0 || res.Deletes != 0 {
		t.Fatalf("an up-to-date projection received %+v", res)
	}
	// Only a document outside the filter changed: the projection records the new position
	// without touching its documents, so the next delta starts from here.
	f.put(us1, `{"region":"US","n":44}`)
	if res = f.sync(); res.Writes != 0 {
		t.Fatalf("an out-of-filter change wrote to the projection: %+v", res)
	}
	if src, _ := f.replica.ProjectionTarget().LastSource(); src != mustHeadHex(t, f.source) {
		t.Fatal("the projection did not record the source's new position")
	}
}

func TestRBACHiddenDocumentNeverSent(t *testing.T) {
	secret := mustRandomUUID(t)
	f := newProjectionFixture(t, `region = 'EU'`, hideDocs{hidden: map[string]bool{secret.String(): true}})
	visible := mustRandomUUID(t)
	f.put(visible, `{"region":"EU"}`)
	f.put(secret, `{"region":"EU","classified":true}`)
	f.sync()
	got := f.held()
	if len(got) != 1 || got[visible] == "" {
		t.Fatalf("expected only the readable document, got %v", got)
	}
}

func TestProjectionRefusesClientWrites(t *testing.T) {
	f := newProjectionFixture(t, `region = 'EU'`)
	_, err := f.replica.Upsert(f.replica.Runtime.DefaultNamespace, mustRandomUUID(t), `{"region":"EU"}`, auth.Principal{})
	var ro *ProjectionReadOnlyError
	if !errors.As(err, &ro) {
		t.Fatalf("expected ProjectionReadOnlyError, got %v", err)
	}
}

func mustHeadHex(t *testing.T, rt *KdbServerRuntime) string {
	t.Helper()
	h, err := rt.dag.Head()
	if err != nil {
		t.Fatal(err)
	}
	return h.Hex()
}

func deleteTx(t *testing.T, rt *KdbServerRuntime, id codec.UUID) document.Transaction {
	t.Helper()
	head, err := rt.dag.Head()
	if err != nil {
		t.Fatal(err)
	}
	return document.Transaction{
		ID: mustRandomUUID(t), BaseVersion: head, Timestamp: codec.TimestampNow(),
		Operations: []document.Op{document.DeleteOp{DocID: id}},
	}
}

// TestProjectionFollowsKeyRemoval: a source document that loses a key loses it in the
// projection too. Pages carry final states, so the projection must replace, not merge.
func TestProjectionFollowsKeyRemoval(t *testing.T) {
	f := newProjectionFixture(t, `region = 'EU'`)
	id := mustRandomUUID(t)
	f.put(id, `{"region":"EU","draft":true}`)
	f.sync()
	head, err := f.source.dag.Head()
	if err != nil {
		t.Fatal(err)
	}
	replace := document.Transaction{
		ID: mustRandomUUID(t), BaseVersion: head, Timestamp: codec.TimestampNow(),
		Operations: []document.Op{document.DeleteOp{DocID: id}, document.WriteOp{DocID: id, Patch: `{"region":"EU"}`}},
	}
	if _, err := f.source.Commit("app/data", replace, "", auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	src, _, _, _ := f.source.GetDocument("app/data", id)
	f.sync()
	if got := f.held()[id]; got != src {
		t.Fatalf("projection holds %s, source holds %s", got, src)
	}
}

// A projection delta carries a delete only for a document the replica can hold - one that matched
// the filter at the replica's position and has since left it. Changes to documents that never
// matched send nothing (Cimbiosys move-out; Phase 15 M2).
func TestProjectionDeltaSendsDeletesOnlyForDocumentsThatLeftTheFilter(t *testing.T) {
	f := newProjectionFixture(t, "region = 'EU'")
	eu, us, leaving := mustRandomUUID(t), mustRandomUUID(t), mustRandomUUID(t)
	f.put(eu, `{"region":"EU","v":0}`)
	f.put(us, `{"region":"US","v":0}`)
	f.put(leaving, `{"region":"EU","v":0}`)
	f.sync()
	for i := 1; i <= 5; i++ {
		f.put(us, fmt.Sprintf(`{"region":"US","v":%d}`, i)) // never matched: nothing to send
	}
	f.put(leaving, `{"region":"US","v":1}`) // leaves the filter: must be deleted
	f.put(eu, `{"region":"EU","v":1}`)
	res := f.sync()
	if res.Deletes != 1 || res.Writes != 1 {
		t.Fatalf("expected one write (eu) and one delete (leaving), got %d writes and %d deletes", res.Writes, res.Deletes)
	}
	held := f.held()
	if _, ok := held[leaving]; ok || len(held) != 1 {
		t.Fatalf("the document that left the filter must be gone: %v", held)
	}
}
