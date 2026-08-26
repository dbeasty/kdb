package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/stream"
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
	transport := tcp.NewTransport(core.DefaultConnectOptions())
	ln, err := transport.ListenBound(addr)
	if err != nil {
		return nil, err
	}
	codec := wire.NewCodec(wire.EncodingJSON)
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

// sqlWireConnHandler decodes and dispatches frames for one connection. Its SessionManager is
// scoped to this connection only, matching the Kotlin reference's per-ConnectionContext
// SqlWireHost.
type sqlWireConnHandler struct {
	codec    wire.Codec
	runtime  *KdbServerRuntime
	sessions *SessionManager
	parser   sql.Parser

	// principal is set once, by a successful Handshake, and reused for every SessionBegin on
	// this connection - there is no per-connection ConnectionContext side channel to
	// re-authenticate against for TCP the way the Kotlin/WebSocket reference has (see
	// HandshakePayload's User/Password/Token doc comment), so credentials are supplied once,
	// at handshake time, for the life of the connection.
	principal     auth.Principal
	authenticated bool
}

func newSqlWireConnHandler(codec wire.Codec, runtime *KdbServerRuntime) *sqlWireConnHandler {
	return &sqlWireConnHandler{
		codec:    codec,
		runtime:  runtime,
		sessions: NewSessionManager(runtime),
		parser:   sql.DefaultParser{},
	}
}

func (h *sqlWireConnHandler) run(conn stream.ConnectionHandle) {
	for frame := range conn.Incoming() {
		response, err := h.handleFrame(frame)
		if err != nil || response == nil {
			continue
		}
		if err := conn.Send(response); err != nil {
			return
		}
	}
}

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
	if err := h.runtime.AuthEngine.Authorizer().Authorize(context.Background(), principal, auth.SessionBeginAction{Namespace: ns}); err != nil {
		reason := err.Error()
		return handshakeAck(msg, false, "", &reason)
	}
	head, err := h.runtime.Runtime.DAG.Head()
	if err != nil {
		reason := err.Error()
		return handshakeAck(msg, false, "", &reason)
	}
	h.principal = principal
	h.authenticated = true
	return handshakeAck(msg, true, head.Hex(), nil)
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
	if !h.authenticated {
		return wire.SessionBeginAckMessage{
			H:               header(msg.H.CorrelationID, wire.MsgSessionBeginAck),
			Namespace:       msg.Namespace,
			SessionID:       "",
			HeadHex:         "",
			ReadConsistency: msg.ReadConsistency,
		}
	}
	if err := h.runtime.AuthEngine.Authorizer().Authorize(context.Background(), h.principal, auth.SessionBeginAction{Namespace: msg.Namespace}); err != nil {
		return wire.SessionBeginAckMessage{
			H:               header(msg.H.CorrelationID, wire.MsgSessionBeginAck),
			Namespace:       msg.Namespace,
			SessionID:       "",
			HeadHex:         "",
			ReadConsistency: msg.ReadConsistency,
		}
	}
	sessionID := ""
	if msg.SessionID != nil {
		sessionID = *msg.SessionID
	}
	baseVersionHex := ""
	if msg.BaseVersionHex != nil {
		baseVersionHex = *msg.BaseVersionHex
	}
	sess, err := h.sessions.Begin(msg.Namespace, parseReadConsistency(msg.ReadConsistency), baseVersionHex, sessionID, h.principal)
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
		HeadHex:         sess.BaseVersion.Hex(),
		ReadConsistency: sess.ReadConsistency.String(),
	}
}

// handleSqlExec runs SELECT immediately (read-only, at the current DAG head) and buffers INSERT
// as document ops on the session's pending transaction builder - it does not commit. A client
// must send TxCommit to persist buffered writes. (No BEGIN/COMMIT SQL-text transaction control
// exists yet - go/kdb/sql's parser doesn't parse those statements - so TxCommit/TxRollback are
// the only way to flush or discard buffered writes in this phase.)
func (h *sqlWireConnHandler) handleSqlExec(msg wire.SqlExecMessage) wire.Message {
	sess, ok := h.sessions.Get(msg.SessionID)
	if !ok {
		return sqlResultError(msg.H.CorrelationID, msg.Namespace, msg.SessionID, "unknown session: "+msg.SessionID)
	}
	stmt, err := h.parser.Parse(strings.TrimSpace(msg.SQL))
	if err != nil {
		return sqlResultError(msg.H.CorrelationID, msg.Namespace, msg.SessionID, err.Error())
	}
	_, isInsert := stmt.(sql.StmtInsert)
	action := auth.SqlExecAction{Namespace: msg.Namespace, ReadOnly: !isInsert}
	if err := h.runtime.AuthEngine.Authorizer().Authorize(context.Background(), sess.Principal, action); err != nil {
		return sqlResultError(msg.H.CorrelationID, msg.Namespace, msg.SessionID, (&AuthorizationError{Cause: err}).Error())
	}
	params, err := decodeParametersJSON(msg.ParametersJSON)
	if err != nil {
		return sqlResultError(msg.H.CorrelationID, msg.Namespace, msg.SessionID, "invalid parameters: "+err.Error())
	}
	if isInsert {
		return h.execInsert(msg, sess, params)
	}
	return h.execRead(msg, sess, params)
}

func (h *sqlWireConnHandler) execRead(msg wire.SqlExecMessage, sess *KdbSession, params []sql.Parameter) wire.Message {
	head, err := h.runtime.Runtime.DAG.Head()
	if err != nil {
		return sqlResultError(msg.H.CorrelationID, msg.Namespace, msg.SessionID, err.Error())
	}
	ctx := sql.QueryContext{
		NamespaceID: sess.NamespaceID,
		Schema:      h.runtime.Schema(),
		AtCommit:    &head,
		Parameters:  params,
		MaxRows:     10_000,
	}
	result, err := h.runtime.SQLEngine.Execute(msg.SQL, ctx)
	if err != nil {
		return sqlResultError(msg.H.CorrelationID, msg.Namespace, msg.SessionID, err.Error())
	}
	if result.AppliedSchema != nil {
		h.runtime.SetSchema(*result.AppliedSchema)
	}
	return wire.SqlResultMessage{
		H:                 header(msg.H.CorrelationID, wire.MsgSqlResult),
		Namespace:         msg.Namespace,
		SessionID:         msg.SessionID,
		Columns:           columnNames(result.Columns),
		Rows:              rowsToStrings(result.Rows),
		RowsAffected:      result.RowsAffected,
		ResolvedCommitHex: head.Hex(),
		ReadOnly:          true,
		GeneratedIDs:      result.GeneratedIDs,
	}
}

func (h *sqlWireConnHandler) execInsert(msg wire.SqlExecMessage, sess *KdbSession, params []sql.Parameter) wire.Message {
	ctx := sql.QueryContext{
		NamespaceID: sess.NamespaceID,
		Schema:      h.runtime.Schema(),
		Parameters:  params,
	}
	dmlResult, err := h.runtime.SQLEngine.ExecuteDML(msg.SQL, ctx)
	if err != nil {
		return sqlResultError(msg.H.CorrelationID, msg.Namespace, msg.SessionID, err.Error())
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
		ResolvedCommitHex: sess.BaseVersion.Hex(),
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
	if err := h.runtime.DocumentLocks.AcquireAllForTransaction(sess.NamespaceID, sess.ID.Value, tx); err != nil {
		return sqlResultError(msg.H.CorrelationID, msg.Namespace, msg.SessionID, err.Error())
	}
	commit, err := h.runtime.Commit(sess.NamespaceID, tx, sess.ID.Value, sess.Principal)
	h.runtime.DocumentLocks.ReleaseAll(sess.ID.Value)
	h.sessions.ClearPending(sess)
	if err != nil {
		var conflictErr *ConflictError
		if asError(err, &conflictErr) {
			reportBytes, _ := json.Marshal(conflictErr.Report)
			return wire.ConflictReportMessage{
				H:           header(msg.H.CorrelationID, wire.MsgConflictReport),
				Namespace:   msg.Namespace,
				ReportBytes: reportBytes,
			}
		}
		return sqlResultErrorClassified(msg.H.CorrelationID, msg.Namespace, msg.SessionID, err)
	}
	sess.BaseVersion = commit.Hash
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
	h.runtime.DocumentLocks.ReleaseAll(sess.ID.Value)
	h.sessions.ClearPending(sess)
	return wire.SqlResultMessage{
		H:                 header(msg.H.CorrelationID, wire.MsgSqlResult),
		Namespace:         msg.Namespace,
		SessionID:         msg.SessionID,
		ResolvedCommitHex: headHex,
		ReadOnly:          false,
	}
}

// handleDocumentGet is a direct point lookup by document id (component 40's GetJSON) - unlike
// SqlExec's SELECT, this doesn't scan the namespace, and doesn't require a session (no
// transactional/read-consistency semantics to track for a single unconditional read of current
// head). Still gated on authentication/authorization like every other op.
func (h *sqlWireConnHandler) handleDocumentGet(msg wire.DocumentGetMessage) wire.Message {
	if !h.authenticated {
		return documentGetError(msg, "not authenticated")
	}
	docID, err := codec.UUIDFromString(msg.DocID)
	if err != nil {
		return documentGetError(msg, "invalid docId: "+err.Error())
	}
	action := auth.DocumentReadAction{Namespace: msg.Namespace, DocID: msg.DocID}
	if err := h.runtime.AuthEngine.Authorizer().Authorize(context.Background(), h.principal, action); err != nil {
		return documentGetError(msg, (&AuthorizationError{Cause: err}).Error())
	}
	jsonBody, commitHex, found, err := h.runtime.GetDocument(msg.Namespace, docID)
	if err != nil {
		return documentGetError(msg, err.Error())
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

func documentGetError(msg wire.DocumentGetMessage, errMsg string) wire.Message {
	return wire.DocumentGetResultMessage{
		H:         header(msg.H.CorrelationID, wire.MsgDocumentGetResult),
		Namespace: msg.Namespace,
		DocID:     msg.DocID,
		Error:     &errMsg,
	}
}

// handleUpsert writes msg.JSON at msg.DocID unconditionally (component 40's Upsert) via
// KdbServerRuntime.Upsert's LAST_WRITE-policy engine - no session, no BaseVersion, per spec §5.
func (h *sqlWireConnHandler) handleUpsert(msg wire.UpsertMessage) wire.Message {
	if !h.authenticated {
		return upsertError(msg, "not authenticated")
	}
	docID, err := codec.UUIDFromString(msg.DocID)
	if err != nil {
		return upsertError(msg, "invalid docId: "+err.Error())
	}
	commit, err := h.runtime.Upsert(msg.Namespace, docID, msg.JSON, h.principal)
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
// small stand-in for errors.As so callers don't need to import "errors" just for this one check.
func asError[T error](err error, target *T) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(T); ok {
		*target = e
		return true
	}
	return false
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
		default:
			return nil, fmt.Errorf("unsupported parameter type %T at index %d", v, i)
		}
	}
	return params, nil
}
