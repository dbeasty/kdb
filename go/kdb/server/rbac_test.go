package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/wire"
)

func newTestRegistryAuthEngine(t *testing.T) (auth.Engine, *auth.RegistryAuthStore) {
	t.Helper()
	usersDag, err := dag.NewInMemoryCommitDag(auth.UsersNamespace)
	if err != nil {
		t.Fatal(err)
	}
	rolesDag, err := dag.NewInMemoryCommitDag(auth.RolesNamespace)
	if err != nil {
		t.Fatal(err)
	}
	store, err := auth.NewRegistryAuthStore(usersDag, rolesDag, mem.NewInMemoryStorageAdapter())
	if err != nil {
		t.Fatal(err)
	}
	return auth.NewRegistryAuthEngine(store), store
}

// TestKdbServerRuntimeCommitDeniesUnauthorizedWrite is component 38 spec §7 test 4's core claim:
// a principal authenticated successfully but lacking a grant for the write being committed is
// rejected at Commit - not just at handshake/session-begin, closing the gap the Kotlin reference
// (AuthorizingTransactionEngine.kt) exists to close. Driven directly against
// KdbServerRuntime.Commit, bypassing the wire layer's own SqlExec-level check entirely, to prove
// this is real defense in depth and not something only the wire dispatcher happens to enforce.
func TestKdbServerRuntimeCommitDeniesUnauthorizedWrite(t *testing.T) {
	rt := newTestRuntime(t)
	engine, store := newTestRegistryAuthEngine(t)
	rt.AuthEngine = engine

	if err := store.CreateRole("reader", []string{"read:app/data"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateUser("reader-user", "pw", []string{"reader"}); err != nil {
		t.Fatal(err)
	}
	principal, err := rt.AuthEngine.Authenticator().Authenticate(
		context.Background(),
		auth.Credentials{User: ptr("reader-user"), Password: ptr("pw")},
	)
	if err != nil {
		t.Fatal(err)
	}

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	head, err := rt.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	tx := document.Transaction{
		ID:          docID, // reused as a stand-in unique id, value irrelevant to the check
		BaseVersion: head,
		Operations:  []document.Op{document.WriteOp{DocID: docID, Patch: `{"v":"x"}`}},
		Timestamp:   codec.TimestampNow(),
	}

	_, err = rt.Commit("app/data", tx, "sess-1", principal)
	if err == nil {
		t.Fatal("expected commit to be denied for a principal with only a read grant")
	}
	var authErr *AuthorizationError
	if !asError(err, &authErr) {
		t.Fatalf("expected *AuthorizationError, got %T: %v", err, err)
	}

	// The write must not have landed: head unchanged.
	newHead, err := rt.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	if newHead != head {
		t.Fatalf("head moved despite denied commit: was %s now %s", head.Hex(), newHead.Hex())
	}
}

// TestKdbServerRuntimeCommitAllowsAuthorizedWrite is the positive counterpart: a principal with
// a matching write grant commits successfully.
func TestKdbServerRuntimeCommitAllowsAuthorizedWrite(t *testing.T) {
	rt := newTestRuntime(t)
	engine, store := newTestRegistryAuthEngine(t)
	rt.AuthEngine = engine

	if err := store.CreateRole("writer", []string{"write:app/data"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateUser("writer-user", "pw", []string{"writer"}); err != nil {
		t.Fatal(err)
	}
	principal, err := rt.AuthEngine.Authenticator().Authenticate(
		context.Background(),
		auth.Credentials{User: ptr("writer-user"), Password: ptr("pw")},
	)
	if err != nil {
		t.Fatal(err)
	}

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	head, err := rt.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	tx := document.Transaction{
		ID:          docID,
		BaseVersion: head,
		Operations:  []document.Op{document.WriteOp{DocID: docID, Patch: `{"v":"x"}`}},
		Timestamp:   codec.TimestampNow(),
	}
	commit, err := rt.Commit("app/data", tx, "sess-1", principal)
	if err != nil {
		t.Fatalf("expected commit to succeed for an authorized writer: %v", err)
	}
	if commit.Hash == head {
		t.Fatal("expected a new commit hash")
	}
}

// TestListenSqlWireRbacEndToEnd exercises the full stack over a real TCP socket: a client
// authenticates with real credentials (wire.HandshakePayload.User/Password - the credential
// transport this component adds, since raw TCP has no ConnectionContext/header side channel the
// way WebSocket does), a properly-privileged user creates a table, inserts, and commits
// successfully, and an under-privileged user's write attempt is rejected.
func TestListenSqlWireRbacEndToEnd(t *testing.T) {
	rt := newTestRuntime(t)
	engine, store := newTestRegistryAuthEngine(t)
	rt.AuthEngine = engine
	if err := store.CreateRole("writer", []string{"write:app/data", "read:app/data"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRole("reader", []string{"read:app/data"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateUser("writer-user", "writer-pw", []string{"writer"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateUser("reader-user", "reader-pw", []string{"reader"}); err != nil {
		t.Fatal(err)
	}

	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", rt)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := fmt.Sprintf("tcp://%s", ln.Addr().String())

	writer := dialRawWireClient(t, addr)
	writerAck := writer.handshakeWithCredentials(t, "writer-user", "writer-pw")
	if !writerAck.Response.Accepted {
		t.Fatalf("writer handshake rejected: %+v", writerAck.Response.RejectionReason)
	}
	writerSess := writer.sessionBegin(t, "app/data", "READ_COMMITTED")
	if r := writer.sqlExec(t, "app/data", writerSess.SessionID, `CREATE TABLE t (id VARCHAR NOT NULL)`); r.Error != nil {
		t.Fatalf("writer create table: %s", *r.Error)
	}
	if r := writer.sqlExec(t, "app/data", writerSess.SessionID, `INSERT INTO t (id) VALUES ('a')`); r.Error != nil {
		t.Fatalf("writer insert: %s", *r.Error)
	}
	commitReply := writer.txCommit(t, "app/data", writerSess.SessionID)
	commitResult, ok := commitReply.(wire.SqlResultMessage)
	if !ok {
		t.Fatalf("expected SqlResultMessage from commit, got %T", commitReply)
	}
	if commitResult.Error != nil {
		t.Fatalf("writer commit: %s", *commitResult.Error)
	}

	reader := dialRawWireClient(t, addr)
	readerAck := reader.handshakeWithCredentials(t, "reader-user", "reader-pw")
	if !readerAck.Response.Accepted {
		t.Fatalf("reader handshake rejected: %+v", readerAck.Response.RejectionReason)
	}
	readerSess := reader.sessionBegin(t, "app/data", "READ_COMMITTED")
	insertResult := reader.sqlExec(t, "app/data", readerSess.SessionID, `INSERT INTO t (id) VALUES ('b')`)
	if insertResult.Error == nil {
		t.Fatal("expected reader's INSERT to be denied")
	}

	badAuth := dialRawWireClient(t, addr)
	badAck := badAuth.handshakeWithCredentials(t, "writer-user", "wrong-password")
	if badAck.Response.Accepted {
		t.Fatal("expected handshake with wrong password to be rejected")
	}

	unknownUser := dialRawWireClient(t, addr)
	unknownAck := unknownUser.handshakeWithCredentials(t, "no-such-user", "whatever")
	if unknownAck.Response.Accepted {
		t.Fatal("expected handshake for an unknown user to be rejected")
	}
}

// TestListenSqlWireDeniesCreateTableForReadOnlyPrincipal is the regression test for the finding
// recorded in docs/kdb-finish-up-plan.md as 1-G6: handleSqlExec authorized any non-INSERT
// statement as ReadOnly:true, so CREATE TABLE - which is neither StmtInsert nor StmtSelect - was
// checked as a read and a principal with only a read grant could rewrite the namespace's schema.
func TestListenSqlWireDeniesCreateTableForReadOnlyPrincipal(t *testing.T) {
	rt := newTestRuntime(t)
	engine, store := newTestRegistryAuthEngine(t)
	rt.AuthEngine = engine
	if err := store.CreateRole("reader", []string{"read:app/data"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateUser("reader-user", "reader-pw", []string{"reader"}); err != nil {
		t.Fatal(err)
	}

	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", rt)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := fmt.Sprintf("tcp://%s", ln.Addr().String())

	reader := dialRawWireClient(t, addr)
	readerAck := reader.handshakeWithCredentials(t, "reader-user", "reader-pw")
	if !readerAck.Response.Accepted {
		t.Fatalf("reader handshake rejected: %+v", readerAck.Response.RejectionReason)
	}
	readerSess := reader.sessionBegin(t, "app/data", "READ_COMMITTED")
	result := reader.sqlExec(t, "app/data", readerSess.SessionID, `CREATE TABLE t (id VARCHAR NOT NULL)`)
	if result.Error == nil {
		t.Fatal("expected CREATE TABLE to be denied for a principal with only a read grant")
	}
}

func ptr(s string) *string { return &s }
