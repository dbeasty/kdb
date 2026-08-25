// Package client is the Go client SDK for KDB's SQL wire protocol (Layer 12 Component 40),
// purpose-built for Zolik's game/match/stats/session/user repository pattern: connect, read or
// write one document by id, commit a transaction with optimistic-concurrency semantics, upsert
// unconditionally, and run the occasional SQL statement - not a general database/sql driver.
//
// One *Client is one TCP connection, one KDB session per namespace touched. Reuses
// go/kdb/wire and go/kdb/transport/tcp directly per the component spec's explicit reuse
// decision - this package is request/response semantics and ergonomics on top of them, not a
// second wire implementation.
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// ErrConflict is returned by Commit when something else committed against the same BaseVersion
// first - the direct analogue of Zolik's match.ErrVersionConflict. Use errors.Is; unwrap a
// *ConflictError from it with errors.As for detail.
var ErrConflict = errors.New("kdb: version conflict")

// ErrNotFound is returned by GetJSON when no document exists at the given id.
var ErrNotFound = errors.New("kdb: not found")

// ErrUnauthenticated is returned by Connect when the handshake's auth step fails.
var ErrUnauthenticated = errors.New("kdb: unauthenticated")

// ErrClosed is returned by any call made after Close, or one in flight when the connection
// drops.
var ErrClosed = errors.New("kdb: connection closed")

// ConflictDetail carries enough of the server's ConflictReport for a caller's error handling to
// log something actionable, without modeling KDB's full internal conflict-classification types.
type ConflictDetail struct {
	DocumentID    string
	OperationType string // "CONCURRENT_WRITE" | "DELETE_WRITE" | "WRITE_DELETE"
}

// ConflictError wraps ErrConflict with the server's reported detail. errors.Is(err, ErrConflict)
// still works via Unwrap.
type ConflictError struct {
	TransactionID string
	BaseHash      string
	TargetHash    string
	Conflicts     []ConflictDetail
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("kdb: version conflict (%d document(s))", len(e.Conflicts))
}

func (e *ConflictError) Unwrap() error { return ErrConflict }

// TransportError wraps an underlying go/kdb/transport error - never swallowed or re-typed as a
// protocol error, per component 40 spec §6.
type TransportError struct{ Cause error }

func (e *TransportError) Error() string { return "kdb: transport error: " + e.Cause.Error() }
func (e *TransportError) Unwrap() error { return e.Cause }

// DocWrite is one document write within a Transaction.
type DocWrite struct {
	DocID string
	JSON  []byte
}

// Transaction mirrors the wire Transaction object - the operation Commit needs: "write iff
// nothing else committed since I read BaseVersion." All Writes must share one namespace (see
// Commit's doc comment) - a single KdbServerRuntime is scoped to one namespace, so a
// transaction spanning namespaces isn't something the current server can execute atomically.
type Transaction struct {
	Namespace   string
	BaseVersion string // commit hash this write is anchored on; from a prior GetJSON/PutJSON/Commit
	Writes      []DocWrite
}

// Client is one TCP connection and one KDB session per namespace touched. Safe for concurrent
// use: every request carries its own correlation id, so multiple goroutines can have calls in
// flight on one Client at once (component 40 spec §5).
type Client struct {
	conn         stream.ConnectionHandle
	codec        wire.Codec
	authorNodeID codec.UUID

	mu          sync.Mutex
	correlation int
	pending     map[int]chan wire.Message
	closed      bool

	nsMu       sync.Mutex
	namespaces map[string]*namespaceState
}

type namespaceState struct {
	mu        sync.Mutex
	sessionID string
	head      codec.Hash
}

// Connect dials addr (host:port, or a tcp://... wire URI) and performs the wire handshake.
// token, if non-empty, authenticates as "user:secret" (matching wire.HandshakePayload.Token) -
// pass "" against a server with no RBAC configured (auth.AllowAll). Blocks until the handshake
// completes or ctx is cancelled.
func Connect(ctx context.Context, addr string, token string) (*Client, error) {
	uri := addr
	if !hasScheme(uri) {
		uri = "tcp://" + uri
	}
	transport := tcp.NewTransport(core.DefaultConnectOptions())
	conn, err := transport.Connect(uri)
	if err != nil {
		return nil, &TransportError{Cause: err}
	}
	authorNodeID, err := codec.RandomUUID()
	if err != nil {
		conn.Close()
		return nil, err
	}
	c := &Client{
		conn:         conn,
		codec:        wire.NewCodec(wire.EncodingJSON),
		authorNodeID: authorNodeID,
		pending:      make(map[int]chan wire.Message),
		namespaces:   make(map[string]*namespaceState),
		correlation:  1,
	}
	go c.readLoop()

	req := wire.HandshakePayload{NodeID: "kdb-client-go", ClientMode: wire.ClientSQL}
	if token != "" {
		req.Token = &token
	}
	hs := wire.HandshakeMessage{H: wire.Header{MessageType: wire.MsgHandshake, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()}, Request: req}
	reply, err := c.request(ctx, hs)
	if err != nil {
		conn.Close()
		return nil, err
	}
	ack, ok := reply.(wire.HandshakeAckMessage)
	if !ok {
		conn.Close()
		return nil, fmt.Errorf("kdb: expected HandshakeAck, got %T", reply)
	}
	if !ack.Response.Accepted {
		conn.Close()
		reason := "handshake rejected"
		if ack.Response.RejectionReason != nil {
			reason = *ack.Response.RejectionReason
		}
		return nil, fmt.Errorf("%w: %s", ErrUnauthenticated, reason)
	}
	return c, nil
}

// hasScheme reports whether uri already names a transport scheme (e.g. "tcp://host:port") as
// opposed to a bare "host:port" or "host:port?query" Connect is also documented to accept.
// Looking only for the first ':' is not enough: a bare IPv4 host:port (e.g. "127.0.0.1:9090")
// or an IPv6 literal both contain a ':' before any '/', which would otherwise be misread as a
// scheme separator and left unprefixed - matching "://" is what a scheme actually looks like.
func hasScheme(uri string) bool {
	return strings.Contains(uri, "://")
}

// Close closes the underlying connection. Safe to call more than once.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	return c.conn.Close()
}

func (c *Client) readLoop() {
	for frame := range c.conn.Incoming() {
		msg, err := c.codec.Decode(frame)
		if err != nil {
			continue
		}
		cid := msg.Header().CorrelationID
		c.mu.Lock()
		ch, ok := c.pending[cid]
		if ok {
			delete(c.pending, cid)
		}
		c.mu.Unlock()
		if ok {
			ch <- msg
		}
	}
	c.mu.Lock()
	c.closed = true
	for cid, ch := range c.pending {
		close(ch)
		delete(c.pending, cid)
	}
	c.mu.Unlock()
}

func (c *Client) nextCorrelation() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.correlation
	c.correlation++
	return id
}

// request sends msg and waits for the correlated response, respecting ctx cancellation - a
// cancelled context aborts this call and returns ctx.Err(), leaving the connection itself
// reusable for the next call (component 40 spec §5).
func (c *Client) request(ctx context.Context, msg wire.Message) (wire.Message, error) {
	frame, err := c.codec.Encode(msg)
	if err != nil {
		return nil, err
	}
	cid := msg.Header().CorrelationID
	replyCh := make(chan wire.Message, 1)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	c.pending[cid] = replyCh
	c.mu.Unlock()

	if err := c.conn.Send(frame); err != nil {
		c.mu.Lock()
		delete(c.pending, cid)
		c.mu.Unlock()
		return nil, &TransportError{Cause: err}
	}

	select {
	case reply, ok := <-replyCh:
		if !ok {
			return nil, ErrClosed
		}
		return reply, nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, cid)
		c.mu.Unlock()
		return nil, ctx.Err()
	}
}

// ensureNamespace begins (or reuses) this connection's session for ns, returning its current
// tracked head - used as every subsequent write's BaseVersion anchor. Staleness is fine: the
// server's conflict detection compares per-document state between BaseVersion and the current
// head for the documents actually touched, not raw head equality, so a head cached once at
// first use and only refreshed on this client's own successful writes is sufficient - see
// go/kdb/transaction's Commit for why (conflict-detection is content-based, not head-based).
func (c *Client) ensureNamespace(ctx context.Context, ns string) (*namespaceState, error) {
	c.nsMu.Lock()
	st, ok := c.namespaces[ns]
	c.nsMu.Unlock()
	if ok {
		return st, nil
	}
	msg := wire.SessionBeginMessage{
		H:               wire.Header{MessageType: wire.MsgSessionBegin, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace:       ns,
		ReadConsistency: "READ_COMMITTED",
	}
	reply, err := c.request(ctx, msg)
	if err != nil {
		return nil, err
	}
	ack, ok := reply.(wire.SessionBeginAckMessage)
	if !ok {
		return nil, fmt.Errorf("kdb: expected SessionBeginAck, got %T", reply)
	}
	if ack.SessionID == "" {
		return nil, fmt.Errorf("kdb: session begin rejected for namespace %s", ns)
	}
	head, err := codec.HashFromHex(ack.HeadHex)
	if err != nil {
		return nil, err
	}
	st = &namespaceState{sessionID: ack.SessionID, head: head}
	c.nsMu.Lock()
	c.namespaces[ns] = st
	c.nsMu.Unlock()
	return st, nil
}

// PutJSON writes one document as a new commit in namespace ns - used for the create-only /
// insert-then-later-CAS-update lifecycle. Returns the resulting commit hash, retained as the
// BaseVersion anchor for a later Commit call.
func (c *Client) PutJSON(ctx context.Context, ns string, docID string, jsonBody []byte) (string, error) {
	id, err := codec.UUIDFromString(docID)
	if err != nil {
		return "", fmt.Errorf("kdb: invalid docID: %w", err)
	}
	st, err := c.ensureNamespace(ctx, ns)
	if err != nil {
		return "", err
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		return "", err
	}
	st.mu.Lock()
	base := st.head
	st.mu.Unlock()
	tx := document.Transaction{
		ID:           txID,
		BaseVersion:  base,
		Operations:   []document.Op{document.WriteOp{DocID: id, Patch: string(jsonBody)}},
		Timestamp:    codec.TimestampNow(),
		AuthorNodeID: c.authorNodeID,
	}
	return c.commitTransaction(ctx, ns, st, tx)
}

// GetJSON reads one document's current JSON by id, plus the commit hash it was read at.
// Returns ErrNotFound if no document exists at docID.
func (c *Client) GetJSON(ctx context.Context, ns string, docID string) ([]byte, string, error) {
	msg := wire.DocumentGetMessage{
		H:         wire.Header{MessageType: wire.MsgDocumentGet, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace: ns,
		DocID:     docID,
	}
	reply, err := c.request(ctx, msg)
	if err != nil {
		return nil, "", err
	}
	result, ok := reply.(wire.DocumentGetResultMessage)
	if !ok {
		return nil, "", fmt.Errorf("kdb: expected DocumentGetResult, got %T", reply)
	}
	if result.Error != nil {
		return nil, "", fmt.Errorf("kdb: %s", *result.Error)
	}
	if result.JSON == nil {
		return nil, result.CommitHex, ErrNotFound
	}
	return []byte(*result.JSON), result.CommitHex, nil
}

// Upsert writes a document unconditionally - create it if it doesn't exist, replace it if it
// does, no BaseVersion, no conflict possible. Targets a namespace whose server-side conflict
// policy is LAST_WRITE (component 40 spec §5) - the server enforces this, not the client.
func (c *Client) Upsert(ctx context.Context, ns string, docID string, jsonBody []byte) (string, error) {
	msg := wire.UpsertMessage{
		H:         wire.Header{MessageType: wire.MsgUpsert, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace: ns,
		DocID:     docID,
		JSON:      string(jsonBody),
	}
	reply, err := c.request(ctx, msg)
	if err != nil {
		return "", err
	}
	result, ok := reply.(wire.UpsertResultMessage)
	if !ok {
		return "", fmt.Errorf("kdb: expected UpsertResult, got %T", reply)
	}
	if result.Error != nil {
		return "", fmt.Errorf("kdb: %s", *result.Error)
	}
	return result.CommitHex, nil
}

// Commit submits tx. On success returns the new commit hash. On a conflict (something else
// committed against the same BaseVersion first) returns a *ConflictError satisfying
// errors.Is(err, ErrConflict); no partial write happens either way.
//
// All tx.Writes must share tx.Namespace - a KdbServerRuntime is scoped to one namespace, so a
// transaction spanning namespaces can't be executed atomically by the current server; Commit
// returns an error rather than silently splitting it into several commits.
func (c *Client) Commit(ctx context.Context, tx Transaction) (string, error) {
	if len(tx.Writes) == 0 {
		return "", fmt.Errorf("kdb: transaction has no writes")
	}
	base, err := codec.HashFromHex(tx.BaseVersion)
	if err != nil {
		return "", fmt.Errorf("kdb: invalid BaseVersion: %w", err)
	}
	ops := make([]document.Op, len(tx.Writes))
	for i, w := range tx.Writes {
		id, err := codec.UUIDFromString(w.DocID)
		if err != nil {
			return "", fmt.Errorf("kdb: invalid docID %q: %w", w.DocID, err)
		}
		ops[i] = document.WriteOp{DocID: id, Patch: string(w.JSON)}
	}
	st, err := c.ensureNamespace(ctx, tx.Namespace)
	if err != nil {
		return "", err
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		return "", err
	}
	docTx := document.Transaction{
		ID:           txID,
		BaseVersion:  base,
		Operations:   ops,
		Timestamp:    codec.TimestampNow(),
		AuthorNodeID: c.authorNodeID,
	}
	return c.commitTransaction(ctx, tx.Namespace, st, docTx)
}

func (c *Client) commitTransaction(ctx context.Context, ns string, st *namespaceState, tx document.Transaction) (string, error) {
	encoded, err := wire.EncodeTransaction(tx)
	if err != nil {
		return "", err
	}
	msg := wire.TxCommitMessage{
		H:                wire.Header{MessageType: wire.MsgTxCommit, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace:        ns,
		SessionID:        st.sessionID,
		TransactionBytes: encoded,
	}
	reply, err := c.request(ctx, msg)
	if err != nil {
		return "", err
	}
	switch r := reply.(type) {
	case wire.ConflictReportMessage:
		return "", decodeConflictError(r.ReportBytes)
	case wire.SqlResultMessage:
		if r.Error != nil {
			return "", fmt.Errorf("kdb: %s", *r.Error)
		}
		newHead, err := codec.HashFromHex(r.ResolvedCommitHex)
		if err != nil {
			return "", err
		}
		st.mu.Lock()
		st.head = newHead
		st.mu.Unlock()
		return r.ResolvedCommitHex, nil
	default:
		return "", fmt.Errorf("kdb: unexpected commit response %T", reply)
	}
}

func decodeConflictError(reportBytes []byte) error {
	var raw struct {
		TransactionID string `json:"transactionId"`
		BaseHash      string `json:"baseHash"`
		TargetHash    string `json:"targetHash"`
		Conflicts     []struct {
			DocumentID    string `json:"documentId"`
			OperationType string `json:"operationType"`
		} `json:"conflicts"`
	}
	if err := json.Unmarshal(reportBytes, &raw); err != nil {
		return ErrConflict
	}
	details := make([]ConflictDetail, len(raw.Conflicts))
	for i, c := range raw.Conflicts {
		details[i] = ConflictDetail{DocumentID: c.DocumentID, OperationType: c.OperationType}
	}
	return &ConflictError{TransactionID: raw.TransactionID, BaseHash: raw.BaseHash, TargetHash: raw.TargetHash, Conflicts: details}
}

// AppendEvent writes one entry to an APPEND_ONLY namespace - always succeeds under that
// namespace's conflict policy, no BaseVersion needed, matching Upsert's transport but for a
// namespace where every write is a new independent record rather than a wholesale replacement
// of a given docID (component 40 spec §3).
func (c *Client) AppendEvent(ctx context.Context, ns string, docID string, jsonBody []byte) error {
	_, err := c.Upsert(ctx, ns, docID, jsonBody)
	return err
}
