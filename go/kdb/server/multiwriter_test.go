package server

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/wire"
)

// uniqueEmailSchema declares email as an optional unique field - the shape every "one account per
// address" application actually has.
// racerFailure carries the classified reason a losing writer was refused.
type racerFailure struct {
	code    wire.ErrorCode
	message string
}

func (e *racerFailure) Error() string { return string(e.code) + ": " + e.message }

func uniqueEmailSchema(t *testing.T) schema.KdbSchema {
	t.Helper()
	sch, err := schema.Build([]schema.Field{
		schema.MustField("email", schema.StringType{}, false, true, true),
	}, 1, codec.Timestamp{}, "unique email")
	if err != nil {
		t.Fatal(err)
	}
	return sch
}

func newTestRuntimeWithSchema(t *testing.T, sch schema.KdbSchema) *KdbServerRuntime {
	t.Helper()
	rt, err := embed.OpenMemoryRuntime("demo", "app/data", sch)
	if err != nil {
		t.Fatal(err)
	}
	return NewKdbServerRuntime(rt)
}

func listenFor(t *testing.T, rt *KdbServerRuntime) string {
	t.Helper()
	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", rt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	return fmt.Sprintf("tcp://%s", ln.Addr().String())
}

// TestConcurrentUniqueKeyRace is the multi-writer guarantee, end to end over real sockets: eight
// independent clients race to claim the same natural key, and exactly one wins.
//
// Before unique enforcement existed this test could not fail, because nothing checked: each
// client inserts a *different* document (SQL INSERT mints a fresh id), so KDB's content-addressed
// per-document conflict detection has nothing to compare - all eight writes are to documents that
// did not previously exist and cannot conflict with each other by that definition. That is
// precisely the hole that made "concurrent app instances against one service" unsafe.
func TestConcurrentUniqueKeyRace(t *testing.T) {
	rt := newTestRuntimeWithSchema(t, uniqueEmailSchema(t))
	addr := listenFor(t, rt)

	const racers = 8
	var wg sync.WaitGroup
	results := make([]error, racers)
	start := make(chan struct{})

	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c := dialRawWireClient(t, addr)
			c.handshake(t, wire.ClientSQL, "app/data")
			sess := c.sessionBegin(t, "app/data", "READ_COMMITTED")
			<-start
			r := c.sqlExec(t, "app/data", sess.SessionID, `INSERT INTO t (email) VALUES ('contended@example.com')`)
			if r.Error != nil {
				results[idx] = fmt.Errorf("insert: %s", *r.Error)
				return
			}
			reply := c.txCommit(t, "app/data", sess.SessionID)
			switch m := reply.(type) {
			case wire.SqlResultMessage:
				if m.Error != nil {
					code := wire.ErrorCode("")
					if m.ErrorCode != nil {
						code = *m.ErrorCode
					}
					results[idx] = &racerFailure{code: code, message: *m.Error}
				}
			default:
				results[idx] = fmt.Errorf("unexpected commit reply %T", reply)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	winners := 0
	for i, err := range results {
		if err == nil {
			winners++
			continue
		}
		var failure *racerFailure
		if !errors.As(err, &failure) {
			t.Fatalf("racer %d failed for an unexpected reason: %v", i, err)
		}
		// Asserted on the code, not on prose: a loser has to be able to tell "that address is
		// taken, pick another" from "your payload is malformed" without parsing an error string.
		if failure.code != wire.ErrorCodeUniqueViolation {
			t.Fatalf("racer %d was rejected as %s, want UNIQUE_VIOLATION: %s", i, failure.code, failure.message)
		}
	}
	if winners != 1 {
		t.Fatalf("expected exactly one writer to claim the key, got %d", winners)
	}
	if got := rt.UniqueKeys.Len(); got != 1 {
		t.Fatalf("expected exactly one claimed key in the registry, got %d", got)
	}
}

// TestUniqueConstraintHoldsAcrossDistinctValues is the negative control: enforcement must not
// reject writes that do not actually collide, or the constraint would be indistinguishable from
// a broken write path.
func TestUniqueConstraintHoldsAcrossDistinctValues(t *testing.T) {
	rt := newTestRuntimeWithSchema(t, uniqueEmailSchema(t))
	addr := listenFor(t, rt)

	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, "app/data")
	sess := c.sessionBegin(t, "app/data", "READ_COMMITTED")
	for _, email := range []string{"a@x.com", "b@x.com", "c@x.com"} {
		r := c.sqlExec(t, "app/data", sess.SessionID, fmt.Sprintf(`INSERT INTO t (email) VALUES ('%s')`, email))
		if r.Error != nil {
			t.Fatalf("insert %s: %s", email, *r.Error)
		}
		mustCommit(t, c, sess.SessionID)
	}
	if got := rt.UniqueKeys.Len(); got != 3 {
		t.Fatalf("expected 3 distinct claimed keys, got %d", got)
	}
}

// TestUniqueNullsDoNotCollide pins the SQL-shaped NULL semantics end to end: several documents
// may omit an optional unique field. Treating absence as a claimable value would let exactly one
// row in the namespace leave the column out.
func TestUniqueNullsDoNotCollide(t *testing.T) {
	rt := newTestRuntimeWithSchema(t, uniqueEmailSchema(t))
	addr := listenFor(t, rt)

	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, "app/data")
	sess := c.sessionBegin(t, "app/data", "READ_COMMITTED")
	for i := 0; i < 3; i++ {
		r := c.sqlExec(t, "app/data", sess.SessionID, `INSERT INTO t (name) VALUES ('no-email')`)
		if r.Error != nil {
			t.Fatalf("insert %d: %s", i, *r.Error)
		}
		mustCommit(t, c, sess.SessionID)
	}
	if got := rt.UniqueKeys.Len(); got != 0 {
		t.Fatalf("documents omitting the unique field claimed %d keys; expected none", got)
	}
}

// TestSessionsAndLocksReclaimedOnDisconnect covers the leak that made lock-based coordination
// unusable: a client that drops mid-transaction previously left its document locks held forever
// (nothing released them) and its session in the manager's map for the process lifetime.
func TestSessionsAndLocksReclaimedOnDisconnect(t *testing.T) {
	rt := newTestRuntimeWithSchema(t, schema.None())
	addr := listenFor(t, rt)

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}

	victim := dialRawWireClient(t, addr)
	victim.handshake(t, wire.ClientSQL, "app/data")
	victimSess := victim.sessionBegin(t, "app/data", "READ_COMMITTED")

	// Take an explicit lease, then drop the connection without releasing it.
	granted := victim.lockAcquire(t, "app/data", victimSess.SessionID, docID.String(), 60_000)
	if !granted.Granted {
		t.Fatalf("victim failed to acquire the lease: %+v", granted)
	}
	if rt.DocumentLocks.HeldCount() != 1 {
		t.Fatalf("expected the lease to be held, got %d", rt.DocumentLocks.HeldCount())
	}
	victim.conn.Close()

	// The server notices on its own read loop; poll rather than sleep a fixed amount.
	if !eventually(func() bool { return rt.DocumentLocks.HeldCount() == 0 }) {
		t.Fatalf("a dropped connection left %d lock(s) held", rt.DocumentLocks.HeldCount())
	}

	// And a fresh client can now take the document the dead one was holding.
	survivor := dialRawWireClient(t, addr)
	survivor.handshake(t, wire.ClientSQL, "app/data")
	survivorSess := survivor.sessionBegin(t, "app/data", "READ_COMMITTED")
	after := survivor.lockAcquire(t, "app/data", survivorSess.SessionID, docID.String(), 60_000)
	if !after.Granted {
		t.Fatalf("could not acquire a document the disconnected client had held: %+v", after)
	}
}

// TestLeaseBlocksAnotherSessionThenReleases is the basic lease contract over the wire.
func TestLeaseBlocksAnotherSessionThenReleases(t *testing.T) {
	rt := newTestRuntimeWithSchema(t, schema.None())
	addr := listenFor(t, rt)
	docID, _ := codec.RandomUUID()

	a := dialRawWireClient(t, addr)
	a.handshake(t, wire.ClientSQL, "app/data")
	aSess := a.sessionBegin(t, "app/data", "READ_COMMITTED")

	b := dialRawWireClient(t, addr)
	b.handshake(t, wire.ClientSQL, "app/data")
	bSess := b.sessionBegin(t, "app/data", "READ_COMMITTED")

	held := a.lockAcquire(t, "app/data", aSess.SessionID, docID.String(), 60_000)
	if !held.Granted || held.Fence == 0 {
		t.Fatalf("A should hold a fenced lease, got %+v", held)
	}

	denied := b.lockAcquire(t, "app/data", bSess.SessionID, docID.String(), 60_000)
	if denied.Granted {
		t.Fatal("B acquired a document A holds")
	}
	if denied.HolderSessionID == nil || *denied.HolderSessionID != aSess.SessionID {
		t.Fatalf("the refusal should name A as the holder, got %+v", denied.HolderSessionID)
	}

	a.lockRelease(t, "app/data", aSess.SessionID, docID.String())

	regained := b.lockAcquire(t, "app/data", bSess.SessionID, docID.String(), 60_000)
	if !regained.Granted {
		t.Fatalf("B should get the lease after A released it: %+v", regained)
	}
	if regained.Fence <= held.Fence {
		t.Fatalf("fence did not advance across holders: A=%d B=%d", held.Fence, regained.Fence)
	}
	_ = rt
}

func (c *rawWireClient) lockAcquire(t *testing.T, namespace, sessionID, docID string, ttlMillis int) wire.LockResultMessage {
	t.Helper()
	msg := wire.LockAcquireMessage{
		H:         wire.Header{MessageType: wire.MsgLockAcquire, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace: namespace,
		SessionID: sessionID,
		DocID:     docID,
		TTLMillis: ttlMillis,
	}
	reply := c.request(t, msg)
	result, ok := reply.(wire.LockResultMessage)
	if !ok {
		t.Fatalf("expected LockResultMessage, got %T", reply)
	}
	return result
}

func (c *rawWireClient) lockRelease(t *testing.T, namespace, sessionID, docID string) wire.LockResultMessage {
	t.Helper()
	msg := wire.LockReleaseMessage{
		H:         wire.Header{MessageType: wire.MsgLockRelease, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace: namespace,
		SessionID: sessionID,
		DocID:     docID,
	}
	reply := c.request(t, msg)
	result, ok := reply.(wire.LockResultMessage)
	if !ok {
		t.Fatalf("expected LockResultMessage, got %T", reply)
	}
	return result
}

// eventually polls cond for up to two seconds. Used where the thing under test is the server
// reacting to a closed socket, which has no synchronous completion signal to wait on.
func eventually(cond func() bool) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// TestUniqueConstraintSurvivesRestart proves the registry is genuinely derived from the stored
// documents rather than from in-process bookkeeping that a restart would silently discard. A
// constraint that only bound within one process lifetime would be worse than none: it would
// hold in every test and fail in production on the first redeploy.
func TestUniqueConstraintSurvivesRestart(t *testing.T) {
	dataRoot := t.TempDir()
	sch := uniqueEmailSchema(t)

	first, err := embed.OpenFileRuntime(dataRoot, "demo", "app/data", sch)
	if err != nil {
		t.Fatal(err)
	}
	rt := NewKdbServerRuntime(first)
	addr := listenFor(t, rt)
	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, "app/data")
	sess := c.sessionBegin(t, "app/data", "READ_COMMITTED")
	if r := c.sqlExec(t, "app/data", sess.SessionID, `INSERT INTO t (email) VALUES ('taken@example.com')`); r.Error != nil {
		t.Fatalf("insert: %s", *r.Error)
	}
	mustCommit(t, c, sess.SessionID)
	c.conn.Close()
	first.Close()

	// Reopen the same directory in a fresh runtime - the registry starts empty and must
	// repopulate from what is on disk.
	second, err := embed.OpenFileRuntime(dataRoot, "demo", "app/data", sch)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(second.Close)
	rt2 := NewKdbServerRuntime(second)
	if rt2.UniqueKeyRebuildError != nil {
		t.Fatalf("rebuild after restart failed: %v", rt2.UniqueKeyRebuildError)
	}
	if got := rt2.UniqueKeys.Len(); got != 1 {
		t.Fatalf("expected the stored claim to be rebuilt, got %d keys", got)
	}

	addr2 := listenFor(t, rt2)
	c2 := dialRawWireClient(t, addr2)
	c2.handshake(t, wire.ClientSQL, "app/data")
	sess2 := c2.sessionBegin(t, "app/data", "READ_COMMITTED")
	if r := c2.sqlExec(t, "app/data", sess2.SessionID, `INSERT INTO t (email) VALUES ('taken@example.com')`); r.Error != nil {
		t.Fatalf("insert should stage cleanly and fail at commit: %s", *r.Error)
	}
	reply := c2.txCommit(t, "app/data", sess2.SessionID)
	result, ok := reply.(wire.SqlResultMessage)
	if !ok {
		t.Fatalf("expected SqlResultMessage, got %T", reply)
	}
	if result.Error == nil {
		t.Fatal("a duplicate claim succeeded after restart: the constraint did not survive")
	}
	if result.ErrorCode == nil || *result.ErrorCode != wire.ErrorCodeUniqueViolation {
		t.Fatalf("expected UNIQUE_VIOLATION after restart, got %v: %s", result.ErrorCode, *result.Error)
	}
}

// TestUniqueViolationLeavesNoPartialState checks the losing transaction is a clean no-op: the
// rejected write must not appear in the namespace, and must not have taken a claim on its way
// out. A constraint that rejects the commit but leaves the document behind is not a constraint.
func TestUniqueViolationLeavesNoPartialState(t *testing.T) {
	rt := newTestRuntimeWithSchema(t, uniqueEmailSchema(t))
	addr := listenFor(t, rt)

	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, "app/data")
	sess := c.sessionBegin(t, "app/data", "READ_COMMITTED")

	if r := c.sqlExec(t, "app/data", sess.SessionID, `INSERT INTO t (email) VALUES ('first@example.com')`); r.Error != nil {
		t.Fatalf("first insert: %s", *r.Error)
	}
	mustCommit(t, c, sess.SessionID)

	// A second transaction inserting the same value in the same statement batch as a legitimate
	// one: the whole transaction must fail, taking the legitimate row with it.
	if r := c.sqlExec(t, "app/data", sess.SessionID, `INSERT INTO t (email) VALUES ('second@example.com')`); r.Error != nil {
		t.Fatalf("staging second: %s", *r.Error)
	}
	if r := c.sqlExec(t, "app/data", sess.SessionID, `INSERT INTO t (email) VALUES ('first@example.com')`); r.Error != nil {
		t.Fatalf("staging duplicate: %s", *r.Error)
	}
	reply := c.txCommit(t, "app/data", sess.SessionID)
	result := reply.(wire.SqlResultMessage)
	if result.Error == nil {
		t.Fatal("a transaction containing a duplicate claim committed")
	}

	sel := c.sqlExec(t, "app/data", sess.SessionID, `SELECT _doc FROM t`)
	if sel.Error != nil {
		t.Fatalf("select: %s", *sel.Error)
	}
	docs := flattenRows(sel)
	if containsSubstring(docs, "second@example.com") {
		t.Fatalf("the aborted transaction's other write was still applied: %v", docs)
	}
	if got := rt.UniqueKeys.Len(); got != 1 {
		t.Fatalf("expected only the first commit's claim to stand, got %d keys", got)
	}
}

// TestTwoOpsInOneTransactionCannotShareAKey: a transaction must not be able to launder a
// self-collision through atomicity. Both writes land at once, so neither "already exists" when
// the other is checked - the check has to consider the transaction's own claims.
func TestTwoOpsInOneTransactionCannotShareAKey(t *testing.T) {
	rt := newTestRuntimeWithSchema(t, uniqueEmailSchema(t))
	addr := listenFor(t, rt)

	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, "app/data")
	sess := c.sessionBegin(t, "app/data", "READ_COMMITTED")
	for i := 0; i < 2; i++ {
		if r := c.sqlExec(t, "app/data", sess.SessionID, `INSERT INTO t (email) VALUES ('same@example.com')`); r.Error != nil {
			t.Fatalf("staging %d: %s", i, *r.Error)
		}
	}
	reply := c.txCommit(t, "app/data", sess.SessionID)
	result := reply.(wire.SqlResultMessage)
	if result.Error == nil {
		t.Fatal("one transaction claimed the same unique value twice")
	}
	if result.ErrorCode == nil || *result.ErrorCode != wire.ErrorCodeUniqueViolation {
		t.Fatalf("expected UNIQUE_VIOLATION, got %v: %s", result.ErrorCode, *result.Error)
	}
	if got := rt.UniqueKeys.Len(); got != 0 {
		t.Fatalf("a rejected transaction left %d claim(s) behind", got)
	}
}

// TestSetSchemaCheckedRejectsDirtyMigration: turning a field unique when the stored data already
// violates it must be refused and must leave the previous schema in place. Applying it would
// leave the namespace permanently inconsistent with its own declared constraints.
func TestSetSchemaCheckedRejectsDirtyMigration(t *testing.T) {
	// Start schema-less, so duplicate emails are legal.
	rt := newTestRuntimeWithSchema(t, schema.None())
	addr := listenFor(t, rt)
	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, "app/data")
	sess := c.sessionBegin(t, "app/data", "READ_COMMITTED")
	for i := 0; i < 2; i++ {
		if r := c.sqlExec(t, "app/data", sess.SessionID, `INSERT INTO t (email) VALUES ('dup@example.com')`); r.Error != nil {
			t.Fatalf("insert %d: %s", i, *r.Error)
		}
		mustCommit(t, c, sess.SessionID)
	}

	before := rt.Schema()
	err := rt.SetSchemaChecked(uniqueEmailSchema(t))
	if err == nil {
		t.Fatal("a migration declaring email unique was accepted over data that already violates it")
	}
	if rt.Schema().SchemaHash != before.SchemaHash {
		t.Fatal("the rejected migration was left applied")
	}

	// And the negative control: the same migration is accepted once the data is clean.
	clean := newTestRuntimeWithSchema(t, schema.None())
	cleanAddr := listenFor(t, clean)
	cc := dialRawWireClient(t, cleanAddr)
	cc.handshake(t, wire.ClientSQL, "app/data")
	cleanSess := cc.sessionBegin(t, "app/data", "READ_COMMITTED")
	if r := cc.sqlExec(t, "app/data", cleanSess.SessionID, `INSERT INTO t (email) VALUES ('only@example.com')`); r.Error != nil {
		t.Fatalf("insert: %s", *r.Error)
	}
	mustCommit(t, cc, cleanSess.SessionID)
	if err := clean.SetSchemaChecked(uniqueEmailSchema(t)); err != nil {
		t.Fatalf("a migration over clean data was rejected: %v", err)
	}
	if got := clean.UniqueKeys.Len(); got != 1 {
		t.Fatalf("expected the migration to claim the existing value, got %d keys", got)
	}
}

// TestExplicitLeaseBlocksAnotherSessionsCommit is the pessimistic-lock contract that survives the
// commit path no longer taking its own locks: a document a client holds a lease on must not be
// written by anyone else, even though ordinary commits now queue at the write gate rather than
// contending for locks.
func TestExplicitLeaseBlocksAnotherSessionsCommit(t *testing.T) {
	rt := newTestRuntimeWithSchema(t, schema.None())
	addr := listenFor(t, rt)

	holder := dialRawWireClient(t, addr)
	holder.handshake(t, wire.ClientSQL, "app/data")
	holderSess := holder.sessionBegin(t, "app/data", "READ_COMMITTED")

	// Create a document so there is something concrete to lease.
	if r := holder.sqlExec(t, "app/data", holderSess.SessionID, `INSERT INTO t (name) VALUES ('under-edit')`); r.Error != nil {
		t.Fatalf("insert: %s", *r.Error)
	}
	mustCommit(t, holder, holderSess.SessionID)

	sel := holder.sqlExec(t, "app/data", holderSess.SessionID, `SELECT kdb_id FROM t`)
	if sel.Error != nil {
		t.Fatalf("select: %s", *sel.Error)
	}
	docID := firstCell(t, sel)

	if g := holder.lockAcquire(t, "app/data", holderSess.SessionID, docID, 60_000); !g.Granted {
		t.Fatalf("holder could not lease its own document: %+v", g)
	}

	// Another session tries to overwrite the leased document. Upsert is the sharpest case: it is
	// the unconditional write verb, so if a lease does not bind it, the lease binds nothing.
	other := dialRawWireClient(t, addr)
	other.handshake(t, wire.ClientSQL, "app/data")
	otherSess := other.sessionBegin(t, "app/data", "READ_COMMITTED")
	blocked := other.upsert(t, "app/data", otherSess.SessionID, docID, `{"name":"stolen"}`)
	if blocked.Error == nil {
		t.Fatal("another session overwrote a leased document")
	}

	// The holder itself is not locked out of its own document.
	mine := holder.upsert(t, "app/data", holderSess.SessionID, docID, `{"name":"edited-by-holder"}`)
	if mine.Error != nil {
		t.Fatalf("the lease holder was refused its own document: %s", *mine.Error)
	}

	// Once the lease is released, the other session's write goes through.
	holder.lockRelease(t, "app/data", holderSess.SessionID, docID)
	allowed := other.upsert(t, "app/data", otherSess.SessionID, docID, `{"name":"allowed"}`)
	if allowed.Error != nil {
		t.Fatalf("the write should succeed once the lease is gone: %s", *allowed.Error)
	}
}

func (c *rawWireClient) upsert(t *testing.T, namespace, sessionID, docID, jsonBody string) wire.UpsertResultMessage {
	t.Helper()
	msg := wire.UpsertMessage{
		H:         wire.Header{MessageType: wire.MsgUpsert, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace: namespace,
		SessionID: sessionID,
		DocID:     docID,
		JSON:      jsonBody,
	}
	reply := c.request(t, msg)
	result, ok := reply.(wire.UpsertResultMessage)
	if !ok {
		t.Fatalf("expected UpsertResultMessage, got %T", reply)
	}
	return result
}

// TestConcurrentWritersToDistinctDocumentsAllSucceed guards the regression the lock change was
// made to prevent from coming back in the other direction: independent writers must queue at the
// write gate, not refuse each other.
func TestConcurrentWritersToDistinctDocumentsAllSucceed(t *testing.T) {
	rt := newTestRuntimeWithSchema(t, schema.None())
	addr := listenFor(t, rt)

	const writers = 8
	var wg sync.WaitGroup
	errs := make([]error, writers)
	start := make(chan struct{})
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c := dialRawWireClient(t, addr)
			c.handshake(t, wire.ClientSQL, "app/data")
			sess := c.sessionBegin(t, "app/data", "READ_COMMITTED")
			<-start
			if r := c.sqlExec(t, "app/data", sess.SessionID, fmt.Sprintf(`INSERT INTO t (name) VALUES ('w%d')`, idx)); r.Error != nil {
				errs[idx] = fmt.Errorf("insert: %s", *r.Error)
				return
			}
			reply := c.txCommit(t, "app/data", sess.SessionID)
			if m, ok := reply.(wire.SqlResultMessage); ok && m.Error != nil {
				errs[idx] = fmt.Errorf("commit: %s", *m.Error)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d to a distinct document was refused: %v", i, err)
		}
	}
	_ = rt
}

func firstCell(t *testing.T, r wire.SqlResultMessage) string {
	t.Helper()
	if len(r.Rows) == 0 || len(r.Rows[0]) == 0 {
		t.Fatalf("expected at least one cell, got %v", r.Rows)
	}
	return r.Rows[0][0]
}
