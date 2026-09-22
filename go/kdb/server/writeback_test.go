package server

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

func newWriteBackFixture(t *testing.T, filter string, engine ...auth.Engine) *projectionFixture {
	t.Helper()
	f := newProjectionFixture(t, filter, engine...)
	f.replica.ProjectionWriteBack, f.replica.ProjectionFilter = true, filter
	q, err := peersync.NewConflictQueue("")
	if err != nil {
		t.Fatal(err)
	}
	f.replica.Conflicts = q
	return f
}

// syncBack runs one write-back sync against addr: pending writes first, then the pull.
func (f *projectionFixture) syncBack(addr string) (peersync.ProjectionResult, error) {
	return peersync.SyncProjection(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.ProjectionConfig{
		NodeID: "replica", PeerURI: addr, Namespace: "app/data", Filter: f.filter, PageBytes: 60,
		Target: f.replica.ProjectionTarget(), WriteBack: true,
	})
}

func (f *projectionFixture) localPut(id codec.UUID, body string) {
	f.t.Helper()
	if _, err := f.replica.Upsert(f.replica.Runtime.DefaultNamespace, id, body, auth.Principal{}); err != nil {
		f.t.Fatal(err)
	}
}

func (f *projectionFixture) sourceDoc(id codec.UUID) string {
	f.t.Helper()
	body, _, _, err := f.source.GetDocument("app/data", id)
	if err != nil {
		f.t.Fatal(err)
	}
	return body
}

func (f *projectionFixture) pending() []peersync.PendingWrite {
	f.t.Helper()
	p, err := f.replica.ProjectionTarget().(peersync.WriteBackTarget).PendingWrites()
	if err != nil {
		f.t.Fatal(err)
	}
	return p
}

// TestWriteBackAppliesAtSource: local writes to a projection are readable at once and reach the
// source on the next sync - an update, a new document and a delete.
func TestWriteBackAppliesAtSource(t *testing.T) {
	f := newWriteBackFixture(t, `region = 'EU'`)
	a, gone := mustRandomUUID(t), mustRandomUUID(t)
	f.put(a, `{"region":"EU","n":1}`)
	f.put(gone, `{"region":"EU","n":2}`)
	f.sync()

	fresh := mustRandomUUID(t)
	f.localPut(a, `{"n":10}`)
	f.localPut(fresh, `{"region":"EU","n":3}`)
	if _, err := f.replica.Commit(f.replica.Runtime.DefaultNamespace, deleteTx(t, f.replica, gone), "", auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	if got := f.held()[a]; !strings.Contains(got, `"n":10`) {
		t.Fatalf("a local write is not readable locally: %s", got)
	}
	if n := len(f.pending()); n != 3 {
		t.Fatalf("expected 3 pending writes, got %d", n)
	}

	res, err := f.syncBack(f.addr)
	if err != nil {
		t.Fatal(err)
	}
	if res.Written != 3 || res.Rejected != 0 {
		t.Fatalf("expected 3 writes applied, got %+v", res)
	}
	if got := f.sourceDoc(a); got != f.held()[a] {
		t.Fatalf("source holds %s, projection %s", got, f.held()[a])
	}
	if f.sourceDoc(fresh) == "" || f.sourceDoc(gone) != "" {
		t.Fatal("the new document or the delete did not reach the source")
	}
	if n := len(f.pending()); n != 0 {
		t.Fatalf("%d writes still pending after they were applied", n)
	}
	if src, _ := f.replica.ProjectionTarget().LastSource(); src != mustHeadHex(t, f.source) {
		t.Fatal("the pull after the write-back did not reach the source's head")
	}
}

// TestWriteBackConflictPutsSourceStateBack: a document changed at the source since the
// projection's copy is not overwritten; the local write is queued as a conflict and the
// projection holds the source's value again - even though a pull in between skipped it.
func TestWriteBackConflictPutsSourceStateBack(t *testing.T) {
	f := newWriteBackFixture(t, `region = 'EU'`)
	x := mustRandomUUID(t)
	f.put(x, `{"region":"EU","v":"base"}`)
	f.sync()
	f.localPut(x, `{"v":"local"}`)
	f.put(x, `{"region":"EU","v":"source"}`)
	f.sync() // a pull while the write is pending must not overwrite it
	if got := f.held()[x]; !strings.Contains(got, `"local"`) {
		t.Fatalf("a pull overwrote a pending local write: %s", got)
	}

	res, err := f.syncBack(f.addr)
	if err != nil {
		t.Fatal(err)
	}
	if res.Rejected != 1 {
		t.Fatalf("expected the write to be rejected, got %+v", res)
	}
	if !strings.Contains(f.sourceDoc(x), `"source"`) {
		t.Fatalf("the source's newer value was overwritten: %s", f.sourceDoc(x))
	}
	if got := f.held()[x]; got != f.sourceDoc(x) {
		t.Fatalf("projection holds %s, source %s", got, f.sourceDoc(x))
	}
	entries := f.replica.Conflicts.List()
	if len(entries) != 1 || entries[0].Kind != peersync.ConflictWriteBack || entries[0].Items[0].LocalDoc == nil ||
		!strings.Contains(*entries[0].Items[0].LocalDoc, `"local"`) {
		t.Fatalf("expected one write-back conflict keeping the attempted value, got %+v", entries)
	}
	if n := len(f.pending()); n != 0 {
		t.Fatalf("a rejected write is still pending (%d)", n)
	}
}

// TestWriteBackOfflineThenReconnect: with the source unreachable, writes still commit locally and
// wait; the sync after reconnecting sends them in order, a later write building on an earlier.
func TestWriteBackOfflineThenReconnect(t *testing.T) {
	f := newWriteBackFixture(t, `region = 'EU'`)
	x := mustRandomUUID(t)
	f.put(x, `{"region":"EU","step":0}`)
	f.sync()

	f.localPut(x, `{"step":1}`)
	f.localPut(x, `{"step":2}`)
	if _, err := f.syncBack("tcp://127.0.0.1:1"); err == nil {
		t.Fatal("a sync against an unreachable source succeeded")
	}
	if n := len(f.pending()); n != 2 {
		t.Fatalf("expected both writes to wait, got %d pending", n)
	}
	res, err := f.syncBack(f.addr)
	if err != nil {
		t.Fatal(err)
	}
	if res.Written != 2 || !strings.Contains(f.sourceDoc(x), `"step":2`) {
		t.Fatalf("expected both writes applied in order, got %+v and %s", res, f.sourceDoc(x))
	}
}

// TestWriteBackResendIsIdempotent: a write the source applied but whose answer was lost is
// answered applied again, with the same commit, when it is sent again - not applied twice, and
// not called a conflict because the source now holds it.
func TestWriteBackResendIsIdempotent(t *testing.T) {
	f := newWriteBackFixture(t, `region = 'EU'`)
	x := mustRandomUUID(t)
	f.put(x, `{"region":"EU","v":0}`)
	f.sync()
	f.localPut(x, `{"v":1}`)
	w := f.pending()[0]
	m := wire.ProjectWriteMessage{Namespace: "app/data", TxID: w.TxID.String(), Docs: w.Docs}
	first, err := f.source.ApplyWriteBack(auth.Principal{}, m)
	if err != nil || first.Outcome != wire.WriteBackApplied {
		t.Fatalf("first send: %+v, %v", first, err)
	}
	f.put(x, `{"region":"EU","v":2}`) // the source moves on before the resend
	again, err := f.source.ApplyWriteBack(auth.Principal{}, m)
	if err != nil || again.Outcome != wire.WriteBackApplied || again.CommitHex != first.CommitHex {
		t.Fatalf("resend: %+v, %v (first was %s)", again, err, first.CommitHex)
	}
}

// TestPendingWritesRebuiltFromDAG: the pending list is what the DAG says - a write made while an
// earlier one was being decided stays pending after the decision, across a cache rebuild.
func TestPendingWritesRebuiltFromDAG(t *testing.T) {
	f := newWriteBackFixture(t, `region = 'EU'`)
	x, y, z := mustRandomUUID(t), mustRandomUUID(t), mustRandomUUID(t)
	f.localPut(x, `{"region":"EU"}`)
	w1 := f.pending()[0]
	f.localPut(y, `{"region":"EU"}`) // made while w1 is in flight
	if err := f.replica.ProjectionTarget().(peersync.WriteBackTarget).ResolveWrite(w1,
		wire.ProjectWriteResultMessage{Outcome: wire.WriteBackApplied}); err != nil {
		t.Fatal(err)
	}
	f.localPut(z, `{"region":"EU"}`)
	f.replica.writeBack.loaded = false // as after a restart
	got := f.pending()
	if len(got) != 2 || got[0].Docs[0].DocID != y.String() || got[1].Docs[0].DocID != z.String() {
		t.Fatalf("expected y then z pending, got %+v", got)
	}
}

// refuseWrites refuses every write once on; reads are always allowed.
type refuseWrites struct{ on atomic.Bool }

func (r *refuseWrites) Authenticator() auth.Authenticator { return auth.AllowAll.Authenticator() }
func (r *refuseWrites) Authorizer() auth.Authorizer       { return r }
func (r *refuseWrites) Authorize(_ context.Context, _ auth.Principal, a auth.Action) error {
	switch a.(type) {
	case auth.TxCommitAction, auth.DocumentWriteAction:
		if r.on.Load() {
			return errors.New("read only user")
		}
	}
	return nil
}

// TestWriteBackRefusedIsNotRetried: a write the source's principal may not make is refused
// once, queued, and put back - not resent forever.
func TestWriteBackRefusedIsNotRetried(t *testing.T) {
	refuse := &refuseWrites{}
	f := newWriteBackFixture(t, `region = 'EU'`, refuse)
	x := mustRandomUUID(t)
	f.put(x, `{"region":"EU","v":"source"}`)
	refuse.on.Store(true)
	f.sync()
	f.localPut(x, `{"v":"local"}`)
	res, err := f.syncBack(f.addr)
	if err != nil {
		t.Fatal(err)
	}
	if res.Rejected != 1 || len(f.pending()) != 0 {
		t.Fatalf("expected one refusal and nothing pending, got %+v", res)
	}
	if got := f.held()[x]; got != f.sourceDoc(x) {
		t.Fatalf("a refused write was not put back: %s vs %s", got, f.sourceDoc(x))
	}
	if e := f.replica.Conflicts.List(); len(e) != 1 || !strings.Contains(e[0].Detail, "refused") {
		t.Fatalf("expected the refusal queued, got %+v", e)
	}
}

// TestWriteBackLeavingFilterLeavesProjection: a local write that takes a document out of the
// filter is applied at the source, and the pull then removes it from the projection.
func TestWriteBackLeavingFilterLeavesProjection(t *testing.T) {
	f := newWriteBackFixture(t, `region = 'EU'`)
	x := mustRandomUUID(t)
	f.put(x, `{"region":"EU"}`)
	f.sync()
	f.localPut(x, `{"region":"US"}`)
	if _, err := f.syncBack(f.addr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.sourceDoc(x), `"US"`) {
		t.Fatalf("the write did not reach the source: %s", f.sourceDoc(x))
	}
	if _, held := f.held()[x]; held {
		t.Fatal("a document that left the filter is still in the projection")
	}
}

// TestWriteBackProjectionRefusesOtherOps: only document writes and deletes can be sent back.
func TestWriteBackProjectionRefusesOtherOps(t *testing.T) {
	f := newWriteBackFixture(t, `region = 'EU'`)
	head, _ := f.replica.dag.Head()
	tx := document.Transaction{ID: mustRandomUUID(t), BaseVersion: head, Timestamp: codec.TimestampNow(),
		Operations: []document.Op{document.FileWriteOp{Path: "x", BlobHash: codec.Hash{}}}}
	if _, err := f.replica.Commit(f.replica.Runtime.DefaultNamespace, tx, "", auth.Principal{}); !errors.Is(err, ErrProjectionWriteOp) {
		t.Fatalf("expected ErrProjectionWriteOp, got %v", err)
	}
}
