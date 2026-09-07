package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transaction"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// ListenSqlWire starts a TCP wire listener bound to addr (a tcp://host:port?bind=true URI,
// e.g. "tcp://127.0.0.1:0?bind=true" to let the OS pick a port). Each accepted connection is
// dispatched to runtime, authenticating/authorizing via runtime.AuthEngine (auth.AllowAll by
// default - set runtime.AuthEngine = auth.NewRegistryAuthEngine(store) to enable RBAC). Modeled
// on kdb-server's SqlWireListen.kt/SqlWireHost.kt: same wire message types, same handshake, same
// framing.
//
// Layer 12 Component 38 status: Phase 1 sub-phase A (listener/framing), sub-phase B
// (Commit/Query wired to the real TransactionEngine), and sub-phase C (RBAC: authentication at
// handshake, authorization at both handshake and commit time) are all in - every connection gets
// its own SessionManager (matching sqlWireHostFactory's per-ConnectionContext SqlWireHost in the
// Kotlin reference). Still missing: committing a client-encoded transaction via TxCommit's
// transactionBytes (no Go wire codec for document.Transaction exists yet - only the session's
// server-side pending builder is supported), and the RBAC admin SQL surface (CREATE/DROP
// ROLE/USER, GRANT/REVOKE) - go/kdb/sql's parser doesn't parse those statements yet, so
// RegistryAuthStore's CRUD is Go-API-only for now, not reachable over SqlExec.
func ListenSqlWire(addr string, runtime *KdbServerRuntime) (*Listener, error) {
	return ListenSqlWireTLS(addr, runtime, nil)
}

// ListenSqlWireTLS is ListenSqlWire with TLS settings for a tcps:// addr - see
// core.TransportTlsSettings. Pass nil for plaintext (equivalent to ListenSqlWire).
func ListenSqlWireTLS(addr string, runtime *KdbServerRuntime, tlsSettings *core.TransportTlsSettings) (*Listener, error) {
	codec := wire.NewCodec(wire.EncodingJSON)
	opts := core.DefaultConnectOptions()
	opts.TLS = tlsSettings
	opts.MaxConnections = runtime.MaxConnections
	opts.Admitter = runtime.frameAdmitter(codec)
	transport := tcp.NewTransport(opts)
	ln, err := transport.ListenBound(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	l := &Listener{ln: ln, cancel: cancel, done: done}
	go func() {
		defer close(done)
		_ = transport.Serve(ctx, ln, func(conn stream.ConnectionHandle) {
			newSqlWireConnHandler(codec, runtime).run(conn)
		})
	}()
	return l, nil
}

// Listener is a running SQL-wire TCP listener; Close stops accepting and waits for the accept
// loop to exit.
type Listener struct {
	ln     net.Listener
	cancel context.CancelFunc
	done   chan struct{}
}

// Addr returns the bound local address (useful when the listen URI's port was 0).
func (l *Listener) Addr() net.Addr { return l.ln.Addr() }

func (l *Listener) Close() error {
	l.cancel()
	<-l.done
	return nil
}

// MaxInFlightFrames bounds how many frames one connection may have dispatched at once
// (kdb-spec-layer16 §12). Past it the reader stops pulling frames off the socket, so a client
// pipelining faster than the server can answer is paced by TCP backpressure rather than by an
// unbounded goroutine pile.
const MaxInFlightFrames = 64

// sqlWireConnHandler decodes and dispatches frames for one connection. Its SessionManager is
// scoped to this connection only, matching the Kotlin reference's per-ConnectionContext
// SqlWireHost.
//
// Frames are handled concurrently (§12, Component 73): each decoded frame after the handshake
// runs on its own goroutine, replies are serialized through sendMu, and frames that name a
// session are ordered per session by a FIFO ticket taken on the reader goroutine, so two
// SqlExecs on one session are still processed in the order they were sent. Sessionless frames (DocumentGet, Upsert, Search,
// TransactionReplay) run freely. Nothing is dispatched before the handshake reply is written:
// frames arriving before a successful handshake are answered inline, in order, with the
// existing "not authenticated" errors.
type sqlWireConnHandler struct {
	codec    wire.Codec
	runtime  *KdbServerRuntime
	sessions *SessionManager
	parser   sql.Parser

	// principal is set once, by a successful Handshake, and reused for every SessionBegin on
	// this connection - there is no per-connection ConnectionContext side channel to
	// re-authenticate against for TCP the way the Kotlin/WebSocket reference has (see
	// HandshakePayload's User/Password/Token doc comment), so credentials are supplied once,
	// at handshake time, for the life of the connection. Guarded by authMu: a repeated
	// Handshake frame is dispatched concurrently with everything else once the first succeeded.
	authMu        sync.RWMutex
	principal     auth.Principal
	authenticated bool

	// sendMu serializes replies onto the connection.
	sendMu sync.Mutex
	// sessionMu guards sessionTails: per session, the completion channel of the most recently
	// enqueued frame naming it. Tickets are taken on the reader goroutine, in arrival order, so
	// two frames on one session run strictly in the order they were sent - a plain mutex would
	// let two already-spawned goroutines race for it.
	sessionMu    sync.Mutex
	sessionTails map[string]chan struct{}
}

func newSqlWireConnHandler(codec wire.Codec, runtime *KdbServerRuntime) *sqlWireConnHandler {
	return &sqlWireConnHandler{
		codec:        codec,
		runtime:      runtime,
		sessions:     NewSessionManager(runtime),
		parser:       sql.DefaultParser{},
		sessionTails: make(map[string]chan struct{}),
	}
}

// principalSnapshot returns the connection's authenticated principal, if any.
func (h *sqlWireConnHandler) principalSnapshot() (auth.Principal, bool) {
	h.authMu.RLock()
	defer h.authMu.RUnlock()
	return h.principal, h.authenticated
}

func (h *sqlWireConnHandler) isAuthenticated() bool {
	_, ok := h.principalSnapshot()
	return ok
}

func (h *sqlWireConnHandler) run(conn stream.ConnectionHandle) {
	// Every session on this connection dies with it. Without this, a client that dropped
	// mid-transaction left its document locks held forever (nothing else released them) and its
	// session in the manager's map for the process lifetime - the map only ever grew. Deferred
	// so it runs after the in-flight wait below: releasing a session's leases while a frame on
	// that session is still executing would let a stranger take the document mid-commit.
	defer h.closeAllSessions()
	inFlight := make(chan struct{}, MaxInFlightFrames)
	var wg sync.WaitGroup
	defer wg.Wait()
	for frame := range conn.Incoming() {
		message, err := h.codec.Decode(frame)
		if err != nil {
			continue
		}
		if !h.isAuthenticated() {
			// Before the handshake reply is written nothing runs concurrently: the handshake
			// itself, and any frame that jumped the gun, is handled inline and in order.
			h.dispatchAndSend(conn, message)
			continue
		}
		// The ticket is taken here, on the reader goroutine, so session order is arrival order.
		wait, done := h.sessionTicket(message)
		inFlight <- struct{}{}
		wg.Add(1)
		go func(message wire.Message) {
			defer wg.Done()
			defer func() { <-inFlight }()
			<-wait
			defer done()
			h.dispatchAndSend(conn, message)
		}(message)
	}
}

// dispatchAndSend dispatches one decoded frame and writes its reply under sendMu.
func (h *sqlWireConnHandler) dispatchAndSend(conn stream.ConnectionHandle, message wire.Message) {
	reply := h.dispatch(message)
	if reply == nil {
		return
	}
	response, err := h.codec.Encode(reply)
	if err != nil {
		return
	}
	h.sendMu.Lock()
	defer h.sendMu.Unlock()
	_ = conn.Send(response)
}

// sessionTicket enqueues message behind every earlier frame naming the same session: wait is
// closed once the previous frame on that session has finished, done must be called when this one
// has. A frame naming no session gets an already-closed wait and a no-op done.
func (h *sqlWireConnHandler) sessionTicket(message wire.Message) (wait <-chan struct{}, done func()) {
	sessionID, ok := sessionIDOf(message)
	if !ok || sessionID == "" {
		return closedChan, func() {}
	}
	mine := make(chan struct{})
	h.sessionMu.Lock()
	prev, queued := h.sessionTails[sessionID]
	h.sessionTails[sessionID] = mine
	h.sessionMu.Unlock()
	if !queued {
		prev = closedChan
	}
	return prev, func() {
		close(mine)
		h.sessionMu.Lock()
		if h.sessionTails[sessionID] == mine {
			delete(h.sessionTails, sessionID)
		}
		h.sessionMu.Unlock()
	}
}

var closedChan = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// sessionIDOf names the session a frame is ordered by. Only frames whose semantics depend on
// session state are ordered: SqlExec (pending builder, read pin), TxCommit/TxRollback (the
// transaction boundary), and the lock verbs (the session's leases). Upsert carries an optional
// session id purely for the lease check and is sessionless for ordering purposes, as §12 says.
func sessionIDOf(message wire.Message) (string, bool) {
	switch msg := message.(type) {
	case wire.SqlExecMessage:
		return msg.SessionID, true
	case wire.TxCommitMessage:
		return msg.SessionID, true
	case wire.TxRollbackMessage:
		return msg.SessionID, true
	case wire.LockAcquireMessage:
		return msg.SessionID, true
	case wire.LockRenewMessage:
		return msg.SessionID, true
	case wire.LockReleaseMessage:
		return msg.SessionID, true
	default:
		return "", false
	}
}

// closeAllSessions releases every lock and drops every session belonging to this connection.
// Safe to call more than once; a second call finds nothing left to do.
func (h *sqlWireConnHandler) closeAllSessions() {
	for _, sess := range h.sessions.All() {
		h.runtime.DocumentLocks.ReleaseAll(sess.ID.Value)
		sess.ClearLeases()
		h.sessions.End(sess.ID.Value)
	}
}

// handleFrame decodes, dispatches, and encodes one frame synchronously - the in-process path
// tests and StreamHub use; the listener's connection loop goes through handleAndSend.
func (h *sqlWireConnHandler) handleFrame(frame []byte) ([]byte, error) {
	message, err := h.codec.Decode(frame)
	if err != nil {
		return nil, err
	}
	reply := h.dispatch(message)
	if reply == nil {
		return nil, nil
	}
	return h.codec.Encode(reply)
}

func (h *sqlWireConnHandler) dispatch(message wire.Message) wire.Message {
	switch msg := message.(type) {
	case wire.HandshakeMessage:
		return h.handleHandshake(msg)
	case wire.SessionBeginMessage:
		return h.handleSessionBegin(msg)
	case wire.SqlExecMessage:
		return h.handleSqlExec(msg)
	case wire.TxCommitMessage:
		return h.handleTxCommit(msg)
	case wire.TxRollbackMessage:
		return h.handleTxRollback(msg)
	case wire.DocumentGetMessage:
		return h.handleDocumentGet(msg)
	case wire.UpsertMessage:
		return h.handleUpsert(msg)
	case wire.TransactionReplayMessage:
		return h.handleTransactionReplay(msg)
	case wire.LockAcquireMessage:
		return h.handleLockAcquire(msg)
	case wire.LockRenewMessage:
		return h.handleLockRenew(msg)
	case wire.LockReleaseMessage:
		return h.handleLockRelease(msg)
	case wire.SearchMessage:
		return h.handleSearch(msg)
	default:
		return nil
	}
}

func (h *sqlWireConnHandler) handleHandshake(msg wire.HandshakeMessage) wire.Message {
	if msg.Request.ClientMode != wire.ClientSQL {
		reason := "SQL_CLIENT mode required"
		return handshakeAck(msg, false, "", &reason)
	}
	creds := auth.Credentials{User: msg.Request.User, Password: msg.Request.Password, Token: msg.Request.Token}
	principal, err := h.runtime.AuthEngine.Authenticator().Authenticate(context.Background(), creds)
	if err != nil {
		reason := err.Error()
		return handshakeAck(msg, false, "", &reason)
	}
	ns := defaultNamespaceFrom(msg.Request.Namespaces)
	if ns == "" {
		// A client with no single target namespace at handshake time (e.g. client.Connect,
		// which opens per-namespace sessions lazily) is authorized against the server's
		// default namespace - matching Kotlin's SqlWireHost.handleHandshake, and closing the
		// hole where the check ran against the namespace "" (which no real grant names).
		// Per-namespace authorization still happens at every SessionBegin.
		ns = h.runtime.Runtime.DefaultNamespace
	}
	if err := h.runtime.AuthEngine.Authorizer().Authorize(context.Background(), principal, auth.SessionBeginAction{Namespace: ns}); err != nil {
		reason := err.Error()
		return handshakeAck(msg, false, "", &reason)
	}
	head, err := h.runtime.Runtime.DAG.Head()
	if err != nil {
		reason := err.Error()
		return handshakeAck(msg, false, "", &reason)
	}
	h.authMu.Lock()
	h.principal = principal
	h.authenticated = true
	h.authMu.Unlock()
	return handshakeAck(msg, true, head.Hex(), nil)
}

// sessionBeginError is a rejected SessionBeginAck (empty SessionID) carrying an explicit error
// string - Phase 2.7's explicit auth-failure frame, mirroring Kotlin's sessionBeginAuthError.
func sessionBeginError(msg wire.SessionBeginMessage, errText string) wire.SessionBeginAckMessage {
	return wire.SessionBeginAckMessage{
		H:               header(msg.H.CorrelationID, wire.MsgSessionBeginAck),
		Namespace:       msg.Namespace,
		SessionID:       "",
		HeadHex:         "",
		ReadConsistency: msg.ReadConsistency,
		Error:           &errText,
	}
}

func defaultNamespaceFrom(namespaces []string) string {
	if len(namespaces) > 0 {
		return namespaces[0]
	}
	return ""
}

func handshakeAck(msg wire.HandshakeMessage, accepted bool, headHex string, rejectionReason *string) wire.HandshakeAckMessage {
	remoteHeads := map[string]string{}
	if accepted {
		ns := defaultNamespaceFrom(msg.Request.Namespaces)
		remoteHeads[ns] = headHex
	}
	return wire.HandshakeAckMessage{
		H: wire.Header{
			MessageType:     wire.MsgHandshake,
			ProtocolVersion: wire.KdbWireProtocolVersion,
			CorrelationID:   msg.H.CorrelationID,
		},
		Response: wire.HandshakeAckPayload{
			Accepted:           accepted,
			NegotiatedEncoding: wire.EncodingJSON,
			ProtocolVersion:    wire.KdbWireProtocolVersion,
			RemoteHeads:        remoteHeads,
			RejectionReason:    rejectionReason,
		},
	}
}

func (h *sqlWireConnHandler) handleSessionBegin(msg wire.SessionBeginMessage) wire.Message {
	principal, authenticated := h.principalSnapshot()
	if !authenticated {
		return sessionBeginError(msg, "not authenticated: handshake required before session begin")
	}
	if err := h.runtime.AuthEngine.Authorizer().Authorize(context.Background(), principal, auth.SessionBeginAction{Namespace: msg.Namespace}); err != nil {
		return sessionBeginError(msg, err.Error())
	}
	sessionID := ""
	if msg.SessionID != nil {
		sessionID = *msg.SessionID
	}
	baseVersionHex := ""
	if msg.BaseVersionHex != nil {
		baseVersionHex = *msg.BaseVersionHex
	}
	sess, err := h.sessions.Begin(msg.Namespace, parseReadConsistency(msg.ReadConsistency), baseVersionHex, sessionID, principal)
	if err != nil {
		return wire.SessionBeginAckMessage{
			H:               header(msg.H.CorrelationID, wire.MsgSessionBeginAck),
			Namespace:       msg.Namespace,
			SessionID:       "",
			HeadHex:         "",
			ReadConsistency: msg.ReadConsistency,
		}
	}
	return wire.SessionBeginAckMessage{
		H:               header(msg.H.CorrelationID, wire.MsgSessionBeginAck),
		Namespace:       msg.Namespace,
		SessionID:       sess.ID.Value,
		HeadHex:         sess.BaseVersion().Hex(),
		ReadConsistency: sess.ReadConsistency.String(),
	}
}

// handleSqlExec runs SELECT immediately (read-only, at the session's read head), applies DDL
// (CREATE TABLE, CREATE/DROP INDEX) immediately, and buffers DML - INSERT, UPDATE, DELETE, and
// any statement kind that is neither a SELECT nor DDL - as document ops on the session's pending
// transaction builder; it does not commit. A client must send TxCommit to persist buffered
// writes. (No BEGIN/COMMIT SQL-text transaction control exists yet - go/kdb/sql's parser doesn't
// parse those statements - so TxCommit/TxRollback are the only way to flush or discard buffered
// writes in this phase.)
func (h *sqlWireConnHandler) handleSqlExec(msg wire.SqlExecMessage) wire.Message {
	if hook := h.runtime.beforeSqlExecHook(); hook != nil {
		hook(msg)
	}
	sess, ok := h.sessions.Get(msg.SessionID)
	if !ok {
		return sqlResultError(msg.H.CorrelationID, msg.Namespace, msg.SessionID, "unknown session: "+msg.SessionID)
	}
	stmt, err := h.parser.Parse(strings.TrimSpace(msg.SQL))
	if err != nil {
		return sqlResultError(msg.H.CorrelationID, msg.Namespace, msg.SessionID, err.Error())
	}
	// ReadOnly must be false for anything that isn't actually a read - a read-only principal
	// must be able neither to buffer writes (DML) nor to rewrite the namespace's schema (DDL,
	// kdb-finish-up-plan.md's 1-G6).
	isSelect, isDDL := classifyStatement(stmt)
	action := auth.SqlExecAction{Namespace: msg.Namespace, ReadOnly: isSelect}
	if err := h.runtime.AuthEngine.Authorizer().Authorize(context.Background(), sess.Principal, action); err != nil {
		return sqlResultError(msg.H.CorrelationID, msg.Namespace, msg.SessionID, (&AuthorizationError{Cause: err}).Error())
	}
	params, err := decodeParametersJSON(msg.ParametersJSON)
	if err != nil {
		return sqlResultError(msg.H.CorrelationID, msg.Namespace, msg.SessionID, "invalid parameters: "+err.Error())
	}
	if isSelect || isDDL {
		return h.execRead(msg, sess, params, stmt)
	}
	return h.execDML(msg, sess, params)
}

// classifyStatement sorts a parsed statement into the three paths handleSqlExec has: a SELECT
// (read), DDL (applied immediately through Engine.Execute), or - everything else - DML buffered
// on the session. Unknown statement kinds are DML on purpose: a new mutating statement added to
// package sql then works over the wire without this switch having to learn its name, and the
// worst case for a new read-like kind is a "not a DML statement" error rather than a write
// slipping through as a read.
func classifyStatement(stmt sql.Statement) (isSelect, isDDL bool) {
	switch stmt.(type) {
	case sql.StmtSelect:
		return true, false
	case sql.StmtCreateTable, sql.StmtCreateIndex, sql.StmtDropIndex:
		return false, true
	default:
		return false, false
	}
}

// readAcquireTimeout bounds how long a read waits for grant capacity before being told Busy.
// Short on purpose: a read queued behind a full grant table is a read the client is still
// synchronously waiting on, and a fast typed Busy beats a slow success at the tail.
const readAcquireTimeout = 2 * time.Second

func (h *sqlWireConnHandler) execRead(msg wire.SqlExecMessage, sess *KdbSession, params []sql.Parameter, stmt sql.Statement) wire.Message {
	// A SNAPSHOT session reads at the pin its current transaction started from;
	// READ_COMMITTED/READ_YOUR_WRITES follow the live head. See KdbSession.ReadHead for the
	// two bugs this has had - a pin that was computed and never read, then a pin that was read
	// but never advanced.
	resolved, err := sess.ReadHead(h.runtime.Runtime.DAG.Head)
	if err != nil {
		return sqlResultError(msg.H.CorrelationID, msg.Namespace, msg.SessionID, err.Error())
	}
	head := &resolved

	// A SELECT reserves the memory it is expected to materialize before it runs - sized from
	// the namespace's O(1) cardinality at the read head, the observed document sizes, and the
	// query's shape, not from the SQL string's length, which predicts nothing (kdb-spec-layer13
	// Component 48 §5.2's "hard case"). Non-SELECT statements reaching this path (CREATE TABLE)
	// materialize nothing worth reserving for.
	var (
		scanIn   ScanEstimateInput
		estimate int64
		grant    *Grant
	)
	adm := h.runtime.admission
	if sel, ok := stmt.(sql.StmtSelect); ok && adm != nil {
		scanIn = ScanEstimateInput{
			Namespace: sess.NamespaceID,
			Shape:     sql.ShapeOfSelect(sel.Query),
			TreeSize:  h.runtime.treeSizeAt(*head),
			MaxRows:   10_000,
			RowBudget: int(adm.ScanRowBudget()),
		}
		estimate = adm.Costs().EstimateScan(scanIn)
		actx, cancel := context.WithTimeout(context.Background(), readAcquireTimeout)
		g, err := adm.AcquireBytes(actx, ClassScan, estimate)
		cancel()
		if err != nil {
			return sqlResultErrorClassified(msg.H.CorrelationID, msg.Namespace, msg.SessionID, err)
		}
		grant = g
		defer grant.Release()
	}

	stats := &sql.ExecStats{}
	ctx := sql.QueryContext{
		NamespaceID: sess.NamespaceID,
		Schema:      h.runtime.Schema(),
		AtCommit:    head,
		Parameters:  params,
		MaxRows:     10_000,
		// Bounds rows *examined*, and shrinks automatically as memory pressure rises - a scan
		// that reads the whole namespace to return nothing is otherwise unbounded work no
		// admission decision can see coming (kdb-spec-layer13 §2.8).
		RowBudget: int(h.runtime.admission.ScanRowBudget()),
		Stats:     stats,
	}
	result, err := h.runtime.SQLEngine().Execute(msg.SQL, ctx)
	if err != nil {
		// Classified, not a bare string: a scan aborted for exceeding its row budget has to reach
		// the client as RESOURCE_EXHAUSTED, or the one signal telling them to narrow the query
		// is lost in prose.
		return sqlResultErrorClassified(msg.H.CorrelationID, msg.Namespace, msg.SessionID, err)
	}
	if result.AppliedSchema != nil {
		// Checked, not blind: a schema that turns a field unique must be rejected outright when
		// the data already there violates it, rather than applied and left permanently at odds
		// with its own namespace.
		if err := h.runtime.SetSchemaChecked(*result.AppliedSchema); err != nil {
			return sqlResultErrorClassified(msg.H.CorrelationID, msg.Namespace, msg.SessionID, err)
		}
	}
	rows := rowsToStrings(result.Rows)
	if grant != nil {
		// Feed back what the query really cost: the executor's exactly-attributed retained
		// bytes plus the wire copy built above. This is the P2 loop ("cost is estimated, then
		// measured") running on real numbers instead of a process-wide allocation counter.
		actual := stats.RetainedBytes + stringsBytes(rows)
		adm.Costs().ObserveScanActual(scanIn, estimate, actual)
		if stats.DocsRead > 0 {
			adm.Costs().ObserveDocSize(sess.NamespaceID, int(stats.DocBytesRead/int64(stats.DocsRead)))
		}
	}
	return wire.SqlResultMessage{
		H:                 header(msg.H.CorrelationID, wire.MsgSqlResult),
		Namespace:         msg.Namespace,
		SessionID:         msg.SessionID,
		Columns:           columnNames(result.Columns),
		Rows:              rows,
		RowsAffected:      result.RowsAffected,
		ResolvedCommitHex: head.Hex(),
		ReadOnly:          true,
		GeneratedIDs:      result.GeneratedIDs,
	}
}

// stringsBytes sizes the wire row copy: cell content plus per-string header overhead.
func stringsBytes(rows [][]string) int64 {
	total := int64(0)
	for _, row := range rows {
		for _, cell := range row {
			total += int64(len(cell)) + 16
		}
	}
	return total
}

// execDML resolves a mutating statement into document ops (INSERT mints or derives ids; UPDATE
// and DELETE scan for their targets at the session's read head) and buffers them on the
// session's pending transaction.
func (h *sqlWireConnHandler) execDML(msg wire.SqlExecMessage, sess *KdbSession, params []sql.Parameter) wire.Message {
	readHead, err := sess.ReadHead(h.runtime.Runtime.DAG.Head)
	if err != nil {
		return sqlResultError(msg.H.CorrelationID, msg.Namespace, msg.SessionID, err.Error())
	}
	ctx := sql.QueryContext{
		AtCommit:    &readHead,
		NamespaceID: sess.NamespaceID,
		Schema:      h.runtime.Schema(),
		Parameters:  params,
		// UPDATE/DELETE resolve their target rows by scanning too, so the same bound applies -
		// an unbounded DML predicate is unbounded work for exactly the same reason a SELECT's is.
		RowBudget: int(h.runtime.admission.ScanRowBudget()),
	}
	dmlResult, err := h.runtime.SQLEngine().ExecuteDML(msg.SQL, ctx)
	if err != nil {
		return sqlResultErrorClassified(msg.H.CorrelationID, msg.Namespace, msg.SessionID, err)
	}
	builder := h.sessions.PendingBuilder(sess)
	for _, op := range dmlResult.Operations {
		switch o := op.(type) {
		case document.WriteOp:
			builder.Write(o.DocID, o.Patch)
		case document.DeleteOp:
			builder.Delete(o.DocID)
		}
	}
	return wire.SqlResultMessage{
		H:                 header(msg.H.CorrelationID, wire.MsgSqlResult),
		Namespace:         msg.Namespace,
		SessionID:         msg.SessionID,
		RowsAffected:      dmlResult.RowsAffected,
		ResolvedCommitHex: sess.BaseVersion().Hex(),
		ReadOnly:          false,
		GeneratedIDs:      dmlResult.GeneratedIDs,
	}
}

// handleTxCommit commits the session's buffered pending transaction. msg.TransactionBytes
// (a client-pre-built, already-encoded transaction) is not yet supported - go/kdb/wire has no
// document.Transaction codec (that's Component 40's Go client SDK territory) - and is rejected
// with a clean, named error rather than silently ignored.
func (h *sqlWireConnHandler) handleTxCommit(msg wire.TxCommitMessage) wire.Message {
	sess, ok := h.sessions.Get(msg.SessionID)
	if !ok {
		return sqlResultError(msg.H.CorrelationID, msg.Namespace, msg.SessionID, "unknown session: "+msg.SessionID)
	}
	var tx document.Transaction
	if len(msg.TransactionBytes) > 0 {
		// A client-pre-built, already-encoded transaction (component 40's Go Client SDK path:
		// PutJSON/Commit build one directly with wire.EncodeTransaction, since it's the only
		// way to write at a caller-chosen document id - SqlExec's INSERT always mints a fresh
		// random UUID). Takes priority over the session's pending builder, matching the Kotlin
		// reference's commitEncodedTransaction.
		decoded, err := wire.DecodeTransaction(msg.TransactionBytes)
		if err != nil {
			return sqlResultError(msg.H.CorrelationID, msg.Namespace, msg.SessionID, "invalid transactionBytes: "+err.Error())
		}
		tx = decoded
	} else {
		if sess.Pending == nil {
			return sqlResultError(msg.H.CorrelationID, msg.Namespace, msg.SessionID, "no pending transaction to commit")
		}
		built, err := sess.Pending.Build(codec.Timestamp{})
		if err != nil {
			return sqlResultError(msg.H.CorrelationID, msg.Namespace, msg.SessionID, err.Error())
		}
		tx = built
	}
	// Any lease this session took explicitly must still be current before the commit runs.
	// Acquiring below would happily re-grant a document whose lease had lapsed and been picked
	// up by nobody - under a brand-new fence token - which would let a writer that had already
	// lost its claim land the write anyway. Checking the fences first is what closes that.
	held := sess.LeasesFor(transaction.DocumentIDsIn(tx.Operations))
	if err := h.runtime.DocumentLocks.ValidateFences(held); err != nil {
		return sqlResultErrorClassified(msg.H.CorrelationID, msg.Namespace, msg.SessionID, err)
	}
	// Refuse only what someone else is actually holding. The commit does not take locks of its
	// own: writes into a runtime are already serialized by the write gate, and taking fail-fast
	// locks on top of that meant a writer waiting its turn in the gate refused every other
	// writer to the same document instead of letting them queue behind it.
	if err := h.runtime.DocumentLocks.AssertUnheldByOthers(sess.NamespaceID, sess.ID.Value, tx); err != nil {
		return sqlResultErrorClassified(msg.H.CorrelationID, msg.Namespace, msg.SessionID, err)
	}
	commit, err := h.runtime.Commit(sess.NamespaceID, tx, sess.ID.Value, sess.Principal)
	h.sessions.ClearPending(sess)
	if err != nil {
		var conflictErr *ConflictError
		if asError(err, &conflictErr) {
			return conflictReport(msg.H.CorrelationID, msg.Namespace, conflictErr)
		}
		return sqlResultErrorClassified(msg.H.CorrelationID, msg.Namespace, msg.SessionID, err)
	}
	// The transaction is over: the next one anchors on - and, for a SNAPSHOT session, reads at -
	// the commit just produced. Without re-pinning here a SNAPSHOT session kept reading at the
	// version it opened with and could not see its own committed writes.
	sess.startTransactionAt(commit.Hash)
	return wire.SqlResultMessage{
		H:                 header(msg.H.CorrelationID, wire.MsgSqlResult),
		Namespace:         msg.Namespace,
		SessionID:         msg.SessionID,
		RowsAffected:      len(tx.Operations),
		ResolvedCommitHex: commit.Hash.Hex(),
		ReadOnly:          false,
	}
}

func (h *sqlWireConnHandler) handleTxRollback(msg wire.TxRollbackMessage) wire.Message {
	sess, ok := h.sessions.Get(msg.SessionID)
	head, headErr := h.runtime.Runtime.DAG.Head()
	headHex := ""
	if headErr == nil {
		headHex = head.Hex()
	}
	if !ok {
		return wire.SqlResultMessage{
			H:                 header(msg.H.CorrelationID, wire.MsgSqlResult),
			Namespace:         msg.Namespace,
			SessionID:         msg.SessionID,
			ResolvedCommitHex: headHex,
			ReadOnly:          false,
		}
	}
	// Rollback abandons this session's write state wholesale, explicitly-held leases included -
	// a client that rolled back is done with the work those leases were protecting. Clearing the
	// session's own tracking keeps its view from outliving the manager's.
	h.runtime.DocumentLocks.ReleaseAll(sess.ID.Value)
	sess.ClearLeases()
	h.sessions.ClearPending(sess)
	// Rollback ends the transaction too, so the next one starts from the current head rather
	// than from the abandoned transaction's snapshot.
	if headErr == nil {
		sess.startTransactionAt(head)
	}
	return wire.SqlResultMessage{
		H:                 header(msg.H.CorrelationID, wire.MsgSqlResult),
		Namespace:         msg.Namespace,
		SessionID:         msg.SessionID,
		ResolvedCommitHex: headHex,
		ReadOnly:          false,
	}
}

// handleTransactionReplay applies a Mode 2 (write-back stream) client's already-built
// transaction directly onto the current head - the wire counterpart to
// KdbServerRuntime.Replay, mirroring Kotlin's SqlWireHost.handleTransactionReplay exactly
// (including ignoring msg.BaseVersion server-side and computing replayTarget independently from
// this node's own current head - the Kotlin reference never reads that field either). No
// session: unlike TxCommit, this isn't building on a session's pending statement builder, it's
// one self-contained transaction the caller already fully built (go/kdb/stream's write-back
// subscriber, once wired - component 40's Go client SDK doesn't need this path, it always has a
// real BaseVersion and uses Commit/TxCommit instead).
func (h *sqlWireConnHandler) handleTransactionReplay(msg wire.TransactionReplayMessage) wire.Message {
	principal, authenticated := h.principalSnapshot()
	if !authenticated {
		return sqlResultError(msg.H.CorrelationID, msg.Namespace, "", "not authenticated")
	}
	action := auth.TxCommitAction{Namespace: msg.Namespace}
	if err := h.runtime.AuthEngine.Authorizer().Authorize(context.Background(), principal, action); err != nil {
		return sqlResultError(msg.H.CorrelationID, msg.Namespace, "", (&AuthorizationError{Cause: err}).Error())
	}
	return replayTransaction(h.runtime, principal, msg)
}

// replayTransaction is handleTransactionReplay's shared core, reused by both entry points that
// can reach TransactionReplay: SQL_CLIENT connections (above) and StreamHub's write-back
// subscribers (stream_listen.go) - identical response shaping either way, only auth differs
// (StreamHub has no per-connection authenticated principal the way a SQL_CLIENT handshake
// establishes one, matching Kotlin's StreamBroadcastHub.handleHandshake, which never
// authenticates stream connections at all).
func replayTransaction(runtime *KdbServerRuntime, principal auth.Principal, msg wire.TransactionReplayMessage) wire.Message {
	tx, err := wire.DecodeTransaction(msg.TransactionBytes)
	if err != nil {
		return sqlResultError(msg.H.CorrelationID, msg.Namespace, "", "invalid transactionBytes: "+err.Error())
	}
	head, err := runtime.Runtime.DAG.Head()
	if err != nil {
		return sqlResultErrorClassified(msg.H.CorrelationID, msg.Namespace, "", err)
	}
	commit, err := runtime.Replay(msg.Namespace, tx, head, principal)
	if err != nil {
		var conflictErr *ConflictError
		if asError(err, &conflictErr) {
			return conflictReport(msg.H.CorrelationID, msg.Namespace, conflictErr)
		}
		return sqlResultErrorClassified(msg.H.CorrelationID, msg.Namespace, "", err)
	}
	return wire.SqlResultMessage{
		H:                 header(msg.H.CorrelationID, wire.MsgSqlResult),
		Namespace:         msg.Namespace,
		RowsAffected:      len(tx.Operations),
		ResolvedCommitHex: commit.Hash.Hex(),
		ReadOnly:          false,
	}
}

// handleDocumentGet is a direct point lookup by document id (component 40's GetJSON) - unlike
// SqlExec's SELECT, this doesn't scan the namespace, and doesn't require a session (no
// transactional/read-consistency semantics to track for a single unconditional read of current
// head). Still gated on authentication/authorization like every other op.
func (h *sqlWireConnHandler) handleDocumentGet(msg wire.DocumentGetMessage) wire.Message {
	principal, authenticated := h.principalSnapshot()
	if !authenticated {
		return documentGetError(msg, "not authenticated")
	}
	docID, err := codec.UUIDFromString(msg.DocID)
	if err != nil {
		return documentGetError(msg, "invalid docId: "+err.Error())
	}
	action := auth.DocumentReadAction{Namespace: msg.Namespace, DocID: msg.DocID}
	if err := h.runtime.AuthEngine.Authorizer().Authorize(context.Background(), principal, action); err != nil {
		return documentGetError(msg, (&AuthorizationError{Cause: err}).Error())
	}
	// Point reads take a small grant too - not because one is dangerous, but because a flood
	// of them against a namespace of large documents is real memory the sampler would otherwise
	// meet only after the fact. Estimated from observed document sizes; the class is never
	// zone-shed (admitInZone admits point reads everywhere), so this only queues when grant
	// capacity is genuinely exhausted.
	var (
		grant    *Grant
		estimate int64
	)
	if adm := h.runtime.admission; adm != nil {
		estimate = adm.Costs().EstimatePointRead(msg.Namespace)
		actx, cancel := context.WithTimeout(context.Background(), readAcquireTimeout)
		g, err := adm.AcquireBytes(actx, ClassPointRead, estimate)
		cancel()
		if err != nil {
			return documentGetErrorClassified(msg, err)
		}
		grant = g
		defer grant.Release()
	}
	jsonBody, commitHex, found, err := h.runtime.GetDocument(msg.Namespace, docID)
	if err != nil {
		return documentGetErrorClassified(msg, err)
	}
	if adm := h.runtime.admission; adm != nil && found {
		adm.Costs().ObserveDocSize(msg.Namespace, len(jsonBody))
		// Actual: the document plus its response copy - the same two terms the estimate models.
		adm.Costs().ObservePointReadActual(msg.Namespace, estimate, int64(len(jsonBody))*2)
	}
	if !found {
		return wire.DocumentGetResultMessage{
			H:         header(msg.H.CorrelationID, wire.MsgDocumentGetResult),
			Namespace: msg.Namespace,
			DocID:     msg.DocID,
			CommitHex: commitHex,
		}
	}
	return wire.DocumentGetResultMessage{
		H:         header(msg.H.CorrelationID, wire.MsgDocumentGetResult),
		Namespace: msg.Namespace,
		DocID:     msg.DocID,
		JSON:      &jsonBody,
		CommitHex: commitHex,
	}
}

// conflictReport shapes a lost optimistic-concurrency race, carrying the structured report the
// client needs to decide *what* to do and the code/retry-after it needs to decide *when*. Before
// these fields existed a conflict was the one refusal the server could not pace: BUSY and
// memory-pressure sheds have carried a retry-after since Component 51, but a conflict - the
// refusal a contended workload produces by far the most of - arrived with nothing, so every
// client retried instantly and collided again. See KdbServerRuntime.conflictRetryAfterMs.
func conflictReport(correlationID int, namespace string, conflictErr *ConflictError) wire.Message {
	reportBytes, _ := json.Marshal(conflictErr.Report)
	code, retryAfterMs := classifyError(conflictErr)
	return wire.ConflictReportMessage{
		H:            header(correlationID, wire.MsgConflictReport),
		Namespace:    namespace,
		ReportBytes:  reportBytes,
		ErrorCode:    &code,
		RetryAfterMs: retryAfterMs,
	}
}

func documentGetError(msg wire.DocumentGetMessage, errMsg string) wire.Message {
	return wire.DocumentGetResultMessage{
		H:         header(msg.H.CorrelationID, wire.MsgDocumentGetResult),
		Namespace: msg.Namespace,
		DocID:     msg.DocID,
		Error:     &errMsg,
	}
}

// documentGetErrorClassified is documentGetError plus ErrorCode/RetryAfterMs from err's concrete
// type - see upsertErrorClassified's doc comment. Used at the two call sites carrying a real
// typed error: the admission grant (which refuses with BusyError or ResourceExhaustedError under
// load) and the read itself. Reads reaching for a grant can be shed exactly like writes, so a
// reading client needs the same "whether and when to retry" answer a writing one already got;
// until DocumentGetResultMessage carried these fields it got prose.
func documentGetErrorClassified(msg wire.DocumentGetMessage, err error) wire.Message {
	errMsg := err.Error()
	code, retryAfterMs := classifyError(err)
	return wire.DocumentGetResultMessage{
		H:            header(msg.H.CorrelationID, wire.MsgDocumentGetResult),
		Namespace:    msg.Namespace,
		DocID:        msg.DocID,
		Error:        &errMsg,
		ErrorCode:    &code,
		RetryAfterMs: retryAfterMs,
	}
}

// handleUpsert writes msg.JSON at msg.DocID unconditionally (component 40's Upsert) via
// KdbServerRuntime.Upsert's LAST_WRITE-policy engine - no session, no BaseVersion, per spec §5.
// Creating stores the body verbatim; updating merges it onto the stored one at the root level.
func (h *sqlWireConnHandler) handleUpsert(msg wire.UpsertMessage) wire.Message {
	principal, authenticated := h.principalSnapshot()
	if !authenticated {
		return upsertError(msg, "not authenticated")
	}
	docID, err := codec.UUIDFromString(msg.DocID)
	if err != nil {
		return upsertError(msg, "invalid docId: "+err.Error())
	}
	// A lease binds every write path, not only TxCommit. Upsert is the one that most needs
	// saying so: it is the unconditional verb, so a client holding a document while it edits
	// would otherwise watch a stranger's Upsert land on top of it.
	leaseCheck := document.Transaction{Operations: []document.Op{document.WriteOp{DocID: docID}}}
	if err := h.runtime.DocumentLocks.AssertUnheldByOthers(msg.Namespace, msg.SessionID, leaseCheck); err != nil {
		return upsertErrorClassified(msg, err)
	}
	commit, err := h.runtime.Upsert(msg.Namespace, docID, msg.JSON, principal)
	if err != nil {
		return upsertErrorClassified(msg, err)
	}
	return wire.UpsertResultMessage{
		H:         header(msg.H.CorrelationID, wire.MsgUpsertResult),
		Namespace: msg.Namespace,
		CommitHex: commit.Hash.Hex(),
	}
}

func upsertError(msg wire.UpsertMessage, errMsg string) wire.Message {
	return wire.UpsertResultMessage{
		H:         header(msg.H.CorrelationID, wire.MsgUpsertResult),
		Namespace: msg.Namespace,
		Error:     &errMsg,
	}
}

// upsertErrorClassified builds the same error response as upsertError, additionally populating
// ErrorCode/RetryAfterMs from err's concrete type via classifyError - see that function's doc
// comment (kdb-spec-layer13 Component 51 §8.1). Used at the one call site where err is a real
// error value from KdbServerRuntime.Upsert, as opposed to the plain-string cases elsewhere in
// this file (invalid docId, not authenticated) that have no typed error to classify.
func upsertErrorClassified(msg wire.UpsertMessage, err error) wire.Message {
	errMsg := err.Error()
	code, retryAfterMs := classifyError(err)
	return wire.UpsertResultMessage{
		H:            header(msg.H.CorrelationID, wire.MsgUpsertResult),
		Namespace:    msg.Namespace,
		Error:        &errMsg,
		ErrorCode:    &code,
		RetryAfterMs: retryAfterMs,
	}
}

func header(correlationID int, msgType wire.MessageType) wire.Header {
	return wire.Header{MessageType: msgType, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: correlationID}
}

func sqlResultError(correlationID int, namespace, sessionID, errMsg string) wire.Message {
	return wire.SqlResultMessage{
		H:         header(correlationID, wire.MsgSqlResult),
		Namespace: namespace,
		SessionID: sessionID,
		ReadOnly:  true,
		Error:     &errMsg,
	}
}

// sqlResultErrorClassified builds the same error response as sqlResultError, additionally
// populating ErrorCode/RetryAfterMs from err's concrete type - see upsertErrorClassified's doc
// comment; same reasoning, TxCommit's call site instead of Upsert's.
func sqlResultErrorClassified(correlationID int, namespace, sessionID string, err error) wire.Message {
	errMsg := err.Error()
	code, retryAfterMs := classifyError(err)
	return wire.SqlResultMessage{
		H:            header(correlationID, wire.MsgSqlResult),
		Namespace:    namespace,
		SessionID:    sessionID,
		ReadOnly:     true,
		Error:        &errMsg,
		ErrorCode:    &code,
		RetryAfterMs: retryAfterMs,
	}
}

// asError reports whether err (or something in its chain) is a *T, setting target if so - a
// thin generic wrapper around errors.As (which can't be called directly with a type parameter
// as its target type). Previously this did a plain err.(T) type assertion with no chain
// unwrapping, contradicting its own doc comment: any *ConflictError wrapped in the future (e.g.
// via fmt.Errorf("%w", ...)) would have been silently reclassified as a generic SQL error at
// every call site instead of surfacing as a ConflictReportMessage (kdb-finish-up-plan.md's 1-G7).
func asError[T error](err error, target *T) bool {
	if err == nil {
		return false
	}
	return errors.As(err, target)
}

func columnNames(cols []sql.ResultColumn) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}

func rowsToStrings(rows []sql.QueryRow) [][]string {
	out := make([][]string, len(rows))
	for i, row := range rows {
		cells := make([]string, len(row.Values))
		for j, v := range row.Values {
			cells[j] = cellToString(v)
		}
		out[i] = cells
	}
	return out
}

func cellToString(c sql.Cell) string {
	switch v := c.(type) {
	case sql.CellNull:
		return ""
	case sql.CellString:
		return v.Value
	case sql.CellLong:
		return strconv.FormatInt(v.Value, 10)
	case sql.CellDouble:
		return strconv.FormatFloat(v.Value, 'g', -1, 64)
	case sql.CellBool:
		// "1"/"0", not "true"/"false": the wire has no typed booleans, and CellBool compares
		// equal to CellLong 0/1 inside the engine, so a client sorting or comparing the string
		// form sees the same values either way.
		if v.Value {
			return "1"
		}
		return "0"
	case sql.CellJSON:
		return v.JSON
	default:
		return fmt.Sprintf("%v", c)
	}
}

// decodeParametersJSON decodes a wire SqlExec ParametersJSON string (a JSON array of scalars,
// matching Kotlin's decodeSqlParameters) into []sql.Parameter.
func decodeParametersJSON(raw *string) ([]sql.Parameter, error) {
	if raw == nil || *raw == "" {
		return nil, nil
	}
	var values []any
	if err := json.Unmarshal([]byte(*raw), &values); err != nil {
		return nil, err
	}
	params := make([]sql.Parameter, len(values))
	for i, v := range values {
		switch t := v.(type) {
		case nil:
			params[i] = sql.ParamNull{}
		case string:
			params[i] = sql.ParamString{Value: t}
		case bool:
			params[i] = sql.ParamBool{Value: t}
		case float64:
			if t == float64(int64(t)) {
				params[i] = sql.ParamInt{Value: int64(t)}
			} else {
				params[i] = sql.ParamDouble{Value: t}
			}
		case []any:
			// A JSON array of numbers is the vector parameter type (kdb-spec-layer16 §9.1),
			// which SIMILARITY takes. An array holding anything else is a client mistake worth
			// naming rather than a vector of zeros.
			vec, err := vectorParam(t, i)
			if err != nil {
				return nil, err
			}
			params[i] = vec
		default:
			return nil, fmt.Errorf("unsupported parameter type %T at index %d", v, i)
		}
	}
	return params, nil
}

// vectorParam converts a decoded JSON array into sql.ParamVector.
func vectorParam(values []any, index int) (sql.ParamVector, error) {
	out := make([]float32, len(values))
	for i, v := range values {
		n, ok := v.(float64)
		if !ok {
			return sql.ParamVector{}, fmt.Errorf("vector parameter at index %d: element %d is %T, want a number", index, i, v)
		}
		out[i] = float32(n)
	}
	return sql.ParamVector{Value: out}, nil
}
