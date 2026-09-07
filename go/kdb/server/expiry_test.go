package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/policy"
	"github.com/limidus/kdb/go/kdb/wire"
)

const expiryNS = "app/data"

func upsertDoc(t *testing.T, rt *KdbServerRuntime, body string) codec.UUID {
	t.Helper()
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Upsert(expiryNS, id, body, auth.Principal{ID: "tester"}); err != nil {
		t.Fatalf("upsert %s: %v", body, err)
	}
	return id
}

func mustFound(t *testing.T, rt *KdbServerRuntime, id codec.UUID, want bool, why string) {
	t.Helper()
	_, _, found, err := rt.GetDocument(expiryNS, id)
	if err != nil {
		t.Fatal(err)
	}
	if found != want {
		t.Fatalf("%s: found=%v, want %v", why, found, want)
	}
}

// TestExpiryHidesExpiredDocumentsAtHead pins the read-side predicate (kdb-spec-layer16 §9.5):
// both accepted timestamp forms expire, a future timestamp does not, grace defers expiry, and
// anything that is not a timestamp never expires.
func TestExpiryHidesExpiredDocumentsAtHead(t *testing.T) {
	rt := newTestRuntime(t)
	defer rt.Release()
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	rt.SetClockForTest(func() time.Time { return now })

	past := now.Add(-time.Minute)
	future := now.Add(time.Minute)
	rfcPast := upsertDoc(t, rt, fmt.Sprintf(`{"expiresAt":%q}`, past.Format(time.RFC3339)))
	millisPast := upsertDoc(t, rt, fmt.Sprintf(`{"expiresAt":%d}`, past.UnixMilli()))
	exactlyNow := upsertDoc(t, rt, fmt.Sprintf(`{"expiresAt":%q}`, now.Format(time.RFC3339)))
	rfcFuture := upsertDoc(t, rt, fmt.Sprintf(`{"expiresAt":%q}`, future.Format(time.RFC3339Nano)))
	notATimestamp := upsertDoc(t, rt, `{"expiresAt":"tomorrow"}`)
	boolean := upsertDoc(t, rt, `{"expiresAt":true}`)
	null := upsertDoc(t, rt, `{"expiresAt":null}`)
	absent := upsertDoc(t, rt, `{"n":1}`)
	nested := upsertDoc(t, rt, fmt.Sprintf(`{"meta":{"ttl":%q}}`, past.Format(time.RFC3339)))

	// No policy: nothing expires.
	mustFound(t, rt, rfcPast, true, "no policy")

	rt.SetDocumentExpiry(&policy.DocumentExpiryPolicy{FieldPath: "expiresAt"})
	mustFound(t, rt, rfcPast, false, "RFC 3339 in the past")
	mustFound(t, rt, millisPast, false, "epoch millis in the past")
	mustFound(t, rt, exactlyNow, false, "timestamp == now is expired (<= now - grace)")
	mustFound(t, rt, rfcFuture, true, "future timestamp")
	mustFound(t, rt, notATimestamp, true, "non-timestamp string never expires")
	mustFound(t, rt, boolean, true, "boolean never expires")
	mustFound(t, rt, null, true, "null never expires")
	mustFound(t, rt, absent, true, "absent field never expires")
	mustFound(t, rt, nested, true, "a different path is not consulted")

	// Grace keeps a document readable past its timestamp.
	rt.SetDocumentExpiry(&policy.DocumentExpiryPolicy{FieldPath: "expiresAt", GraceMillis: (2 * time.Minute).Milliseconds()})
	mustFound(t, rt, rfcPast, true, "within grace")
	mustFound(t, rt, millisPast, true, "within grace (millis)")
	rt.SetDocumentExpiry(&policy.DocumentExpiryPolicy{FieldPath: "expiresAt", GraceMillis: (30 * time.Second).Milliseconds()})
	mustFound(t, rt, rfcPast, false, "past grace")

	// A dotted path reaches into the body.
	rt.SetDocumentExpiry(&policy.DocumentExpiryPolicy{FieldPath: "meta.ttl"})
	mustFound(t, rt, nested, false, "dotted path")
	mustFound(t, rt, rfcPast, true, "top-level field no longer the expiry field")

	// Disabling restores everything.
	rt.SetDocumentExpiry(nil)
	mustFound(t, rt, rfcPast, true, "disabled")
	if rt.DocumentExpiry() != nil {
		t.Fatal("expected no policy after SetDocumentExpiry(nil)")
	}
}

// TestExpiryHidingAdapterFiltersHeadOnly proves the storage wrapper the SQL engine reads through
// hides expired documents at the head tree only: a scan or point read at an older tree returns
// them, because historical reads never apply expiry.
func TestExpiryHidingAdapterFiltersHeadOnly(t *testing.T) {
	rt := newTestRuntime(t)
	defer rt.Release()
	now := time.Now()
	rt.SetClockForTest(func() time.Time { return now })
	expired := upsertDoc(t, rt, fmt.Sprintf(`{"expiresAt":%d,"k":"old"}`, now.Add(-time.Hour).UnixMilli()))
	oldTree, ok := rt.headTreeHash()
	if !ok {
		t.Fatal("no head")
	}
	live := upsertDoc(t, rt, `{"k":"live"}`)
	headTree, _ := rt.headTreeHash()
	rt.SetDocumentExpiry(&policy.DocumentExpiryPolicy{FieldPath: "expiresAt"})

	adapter := &expiryHidingAdapter{Adapter: rt.Runtime.Storage, runtime: rt}
	n := 0
	if err := adapter.ScanDocuments(expiryNS, headTree, 256, func(batch []document.Document) error {
		n += len(batch)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("scan at head saw %d documents, want 1 (expired hidden)", n)
	}
	n = 0
	if err := adapter.ScanDocuments(expiryNS, oldTree, 256, func(batch []document.Document) error {
		n += len(batch)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("scan at the old tree saw %d documents, want 1 (historical, unfiltered)", n)
	}
	if doc, err := adapter.GetDocument(expiryNS, expired, headTree); err != nil || doc != nil {
		t.Fatalf("point read at head: doc=%v err=%v, want hidden", doc, err)
	}
	if doc, err := adapter.GetDocument(expiryNS, expired, oldTree); err != nil || doc == nil {
		t.Fatalf("point read at the old tree: doc=%v err=%v, want visible", doc, err)
	}
	docs, err := adapter.GetDocuments(expiryNS, []codec.UUID{expired, live}, headTree)
	if err != nil || len(docs) != 2 || docs[0] != nil || docs[1] == nil {
		t.Fatalf("GetDocuments at head: %v err=%v", docs, err)
	}
	// The raw adapter underneath still has the document: hiding is not deleting.
	if doc, err := rt.Runtime.Storage.GetDocument(expiryNS, expired, headTree); err != nil || doc == nil {
		t.Fatalf("raw storage lost the document: doc=%v err=%v", doc, err)
	}
}

// TestExpirySqlSelectAtHeadSkipsExpired runs a real SELECT through the runtime's SQL engine: the
// expired row is absent at head and present when the query is pinned to the earlier commit.
func TestExpirySqlSelectAtHeadSkipsExpired(t *testing.T) {
	rt := newTestRuntime(t)
	addr := listenFor(t, rt)
	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, expiryNS)
	sess := c.sessionBegin(t, expiryNS, "READ_COMMITTED")
	if r := c.sqlExec(t, expiryNS, sess.SessionID, `CREATE TABLE t (k VARCHAR NOT NULL, expiresAt VARCHAR)`); r.Error != nil {
		t.Fatalf("create: %s", *r.Error)
	}
	now := time.Now()
	rt.SetClockForTest(func() time.Time { return now })
	past := now.Add(-time.Hour).UTC().Format(time.RFC3339)
	future := now.Add(time.Hour).UTC().Format(time.RFC3339)
	if r := c.sqlExec(t, expiryNS, sess.SessionID, fmt.Sprintf(`INSERT INTO t (k, expiresAt) VALUES ('old', '%s')`, past)); r.Error != nil {
		t.Fatalf("insert: %s", *r.Error)
	}
	if r, ok := c.txCommit(t, expiryNS, sess.SessionID).(wire.SqlResultMessage); !ok || r.Error != nil {
		t.Fatalf("commit: %+v", r)
	}
	oldHead := rt.mustHeadHex(t)
	if r := c.sqlExec(t, expiryNS, sess.SessionID, fmt.Sprintf(`INSERT INTO t (k, expiresAt) VALUES ('new', '%s')`, future)); r.Error != nil {
		t.Fatalf("insert: %s", *r.Error)
	}
	if r, ok := c.txCommit(t, expiryNS, sess.SessionID).(wire.SqlResultMessage); !ok || r.Error != nil {
		t.Fatalf("commit: %+v", r)
	}

	rt.SetDocumentExpiry(&policy.DocumentExpiryPolicy{FieldPath: "expiresAt"})
	res := c.sqlExec(t, expiryNS, sess.SessionID, `SELECT COUNT(*) AS n FROM t`)
	if res.Error != nil {
		t.Fatalf("select: %s", *res.Error)
	}
	if res.Rows[0][0] != "1" {
		t.Fatalf("count at head = %s, want 1 (expired row hidden)", res.Rows[0][0])
	}
	// A SNAPSHOT session pinned at the earlier commit reads history: expiry does not apply.
	pinned := c.sessionBeginAt(t, expiryNS, "SNAPSHOT", oldHead)
	res = c.sqlExec(t, expiryNS, pinned.SessionID, `SELECT COUNT(*) AS n FROM t`)
	if res.Error != nil {
		t.Fatalf("historical select: %s", *res.Error)
	}
	if res.Rows[0][0] != "1" {
		t.Fatalf("count at the old commit = %s, want 1 (the old row, unfiltered)", res.Rows[0][0])
	}
}

// TestExpirySweeperDeletesInBatches: SweepExpiredNow commits DeleteOps for every expired
// document in batches of at most ExpirySweepBatch, with the sweep message, leaving live
// documents alone; afterwards the raw store no longer holds them.
func TestExpirySweeperDeletesInBatches(t *testing.T) {
	rt := newTestRuntime(t)
	defer rt.Release()
	now := time.Now()
	rt.SetClockForTest(func() time.Time { return now })
	const expiredCount = ExpirySweepBatch + 3
	var expired []codec.UUID
	for i := 0; i < expiredCount; i++ {
		expired = append(expired, upsertDoc(t, rt, fmt.Sprintf(`{"i":%d,"expiresAt":%d}`, i, now.Add(-time.Second).UnixMilli())))
	}
	live := upsertDoc(t, rt, `{"expiresAt":"never"}`)
	before := rt.mustHeadHex(t)

	// Read-side only until the policy is set; a sweep without a policy is a no-op.
	if n, err := rt.SweepExpiredNow(); n != 0 || err != nil {
		t.Fatalf("sweep without a policy: n=%d err=%v", n, err)
	}
	rt.SetDocumentExpiry(&policy.DocumentExpiryPolicy{FieldPath: "expiresAt", SweepIntervalMillis: int64(time.Hour / time.Millisecond)})
	n, err := rt.SweepExpiredNow()
	if err != nil {
		t.Fatal(err)
	}
	if n != expiredCount {
		t.Fatalf("swept %d, want %d", n, expiredCount)
	}
	// Two commits: one full batch and one remainder, each carrying the sweep message.
	commits := 0
	head, err := rt.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	for head.Hex() != before {
		commit, ok := rt.Runtime.DAG.GetCommit(head)
		if !ok {
			t.Fatalf("commit %s missing", head.Hex())
		}
		if commit.Message != ExpirySweepMessage {
			t.Fatalf("sweep commit message %q, want %q", commit.Message, ExpirySweepMessage)
		}
		if len(commit.Operations) > ExpirySweepBatch {
			t.Fatalf("sweep commit carries %d ops, cap is %d", len(commit.Operations), ExpirySweepBatch)
		}
		commits++
		head = commit.ParentHashes[0]
	}
	if commits != 2 {
		t.Fatalf("expected 2 sweep commits, got %d", commits)
	}
	headTree, _ := rt.headTreeHash()
	for _, id := range expired[:3] {
		if doc, _ := rt.Runtime.Storage.GetDocument(expiryNS, id, headTree); doc != nil {
			t.Fatalf("expired document %s survived the sweep", id)
		}
	}
	mustFound(t, rt, live, true, "live document survives the sweep")
	if n, err := rt.SweepExpiredNow(); n != 0 || err != nil {
		t.Fatalf("second sweep: n=%d err=%v, want nothing left", n, err)
	}
}

// TestExpirySweeperRunsPeriodicallyAndStopsOnRelease: the goroutine started by
// SetDocumentExpiry deletes on its own schedule, and Release stops it.
func TestExpirySweeperRunsPeriodicallyAndStopsOnRelease(t *testing.T) {
	rt := newTestRuntime(t)
	now := time.Now()
	rt.SetClockForTest(func() time.Time { return now })
	id := upsertDoc(t, rt, fmt.Sprintf(`{"expiresAt":%d}`, now.Add(-time.Second).UnixMilli()))
	rt.SetDocumentExpiry(&policy.DocumentExpiryPolicy{FieldPath: "expiresAt", SweepIntervalMillis: 10})
	if !rt.SweepingForTest() {
		t.Fatal("expected a sweeper goroutine on a writable runtime")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		headTree, _ := rt.headTreeHash()
		if doc, _ := rt.Runtime.Storage.GetDocument(expiryNS, id, headTree); doc == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the sweeper never deleted the expired document")
		}
		time.Sleep(5 * time.Millisecond)
	}
	rt.Release()
	if rt.SweepingForTest() {
		t.Fatal("sweeper still running after Release")
	}
	// Reconfiguring replaces a running sweeper rather than leaking one.
	rt2 := newTestRuntime(t)
	rt2.SetDocumentExpiry(&policy.DocumentExpiryPolicy{FieldPath: "a", SweepIntervalMillis: 10})
	rt2.SetDocumentExpiry(&policy.DocumentExpiryPolicy{FieldPath: "b", SweepIntervalMillis: 10})
	rt2.SetDocumentExpiry(nil)
	if rt2.SweepingForTest() {
		t.Fatal("sweeper still running after SetDocumentExpiry(nil)")
	}
	rt2.Release()
}

// TestExpiryReadOnlyRuntimeHidesButNeverSweeps: a read-only runtime applies the read-side
// predicate and neither starts a sweeper nor deletes on an explicit sweep.
func TestExpiryReadOnlyRuntimeHidesButNeverSweeps(t *testing.T) {
	rt := newTestRuntime(t)
	defer rt.Release()
	now := time.Now()
	rt.SetClockForTest(func() time.Time { return now })
	id := upsertDoc(t, rt, fmt.Sprintf(`{"expiresAt":%d}`, now.Add(-time.Second).UnixMilli()))
	rt.Runtime.ReadOnly = true
	rt.SetDocumentExpiry(&policy.DocumentExpiryPolicy{FieldPath: "expiresAt", SweepIntervalMillis: 1})
	if rt.SweepingForTest() {
		t.Fatal("a read-only runtime must not start a sweeper")
	}
	mustFound(t, rt, id, false, "read-only runtime still hides expired documents")
	if n, err := rt.SweepExpiredNow(); n != 0 || err != nil {
		t.Fatalf("read-only sweep: n=%d err=%v, want a no-op", n, err)
	}
	headTree, _ := rt.headTreeHash()
	if doc, _ := rt.Runtime.Storage.GetDocument(expiryNS, id, headTree); doc == nil {
		t.Fatal("a read-only runtime deleted a document")
	}
}

// TestExpirySweepRunsAsSystemPrincipal: the sweep is the runtime enforcing its own policy, so an
// RBAC engine that denies every client still lets it delete.
func TestExpirySweepRunsAsSystemPrincipal(t *testing.T) {
	rt := newTestRuntime(t)
	defer rt.Release()
	now := time.Now()
	rt.SetClockForTest(func() time.Time { return now })
	id := upsertDoc(t, rt, fmt.Sprintf(`{"expiresAt":%d}`, now.Add(-time.Second).UnixMilli()))
	rt.AuthEngine = denyAllEngine{}
	if _, err := rt.Upsert(expiryNS, id, `{"x":1}`, auth.Principal{ID: "client"}); err == nil {
		t.Fatal("expected the deny-all engine to refuse a client write")
	}
	rt.SetDocumentExpiry(&policy.DocumentExpiryPolicy{FieldPath: "expiresAt", SweepIntervalMillis: int64(time.Hour / time.Millisecond)})
	n, err := rt.SweepExpiredNow()
	if err != nil || n != 1 {
		t.Fatalf("system sweep under deny-all RBAC: n=%d err=%v", n, err)
	}
}

func (s *KdbServerRuntime) mustHeadHex(t *testing.T) string {
	t.Helper()
	head, err := s.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	return head.Hex()
}
