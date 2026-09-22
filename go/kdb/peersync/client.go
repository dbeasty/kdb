package peersync

import (
	"encoding/json"
	"math"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transaction"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/ws"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Client connects to a peer and synchronizes commits.
type Client interface {
	Connect(config ClientConfig) (Session, error)
	Disconnect() error
}

// Session is an active peer sync session.
type Session interface {
	NamespaceID() string
	RemoteHead() codec.Hash
	PullMissing() (Result, error)
	PushCommits(commits []document.Commit) (int, error)
	SyncBidirectional() (Result, error)
	FetchCommitsSince(sinceHash *codec.Hash) ([]document.Commit, error)
}

type defaultClient struct {
	wire        wire.Codec
	transport   stream.Transport
	dag         *dag.InMemoryCommitDag
	storage     storage.Adapter
	correlation int
	conn        stream.ConnectionHandle
	mu          sync.Mutex
}

// NewClient creates a peer sync client. store is used for the document-level writes/deletes a
// non-conflicting auto-merge (see ResolveDivergence) stages when local and remote history has
// diverged - required, not optional, since PullMissing can hit that path on any real sync.
func NewClient(w wire.Codec, transport stream.Transport, dagInst *dag.InMemoryCommitDag, store storage.Adapter) Client {
	return &defaultClient{wire: w, transport: transport, dag: dagInst, storage: store, correlation: 2000}
}

func (c *defaultClient) Connect(config ClientConfig) (Session, error) {
	var conn stream.ConnectionHandle
	var err error
	if wsTransport, ok := c.transport.(ws.Transport); ok {
		opts := coreConnectOptions(config)
		conn, err = wsTransport.ConnectWithOptions(config.PeerURI, opts)
	} else {
		conn, err = c.transport.Connect(config.PeerURI)
	}
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	localHead, err := c.dag.Head()
	if err != nil {
		return nil, err
	}
	hs := wire.HandshakeMessage{
		H: wire.Header{
			MessageType:     wire.MsgHandshake,
			ProtocolVersion: wire.KdbWireProtocolVersion,
			CorrelationID:   c.nextCorrelation(),
		},
		Request: wire.HandshakePayload{
			NodeID:     config.NodeID,
			Namespaces: []string{config.NamespaceID},
			LocalHeads: map[string]string{config.NamespaceID: localHead.Hex()},
			ClientMode: wire.ClientFullPeer,
			// config.ConnectionContext was accepted but never actually used here - every peer
			// handshake went out with no credentials regardless of what a caller configured,
			// which would have made the host's new PeerSyncAction enforcement (host.go) reject
			// every real client outright, not just an unauthenticated one.
			User:     config.ConnectionContext.User,
			Password: config.ConnectionContext.Password,
			Token:    config.ConnectionContext.Token,
		},
	}
	ackMsg, err := c.request(conn, hs)
	if err != nil {
		return nil, err
	}
	ack, ok := ackMsg.(wire.HandshakeAckMessage)
	if !ok {
		return nil, NewError("expected HandshakeAck", nil)
	}
	if !ack.Response.Accepted {
		reason := "handshake rejected"
		if ack.Response.RejectionReason != nil {
			reason = *ack.Response.RejectionReason
		}
		return nil, NewError(reason, nil)
	}
	remoteHex, ok := ack.Response.RemoteHeads[config.NamespaceID]
	if !ok {
		return nil, NewError("remote head missing for "+config.NamespaceID, nil)
	}
	remoteHead, err := codec.HashFromHex(remoteHex)
	if err != nil {
		return nil, err
	}
	return &defaultSession{
		client: c, dag: c.dag, storage: c.storage, namespaceID: config.NamespaceID, remoteHead: remoteHead, conn: conn,
		materialize: config.MaterializeCommit, persist: config.Persist, persistAsync: config.PersistAsync,
		conflictPolicy: config.ConflictPolicy, conflictResolver: config.ConflictResolver,
		node: config.Node, applyToStorage: config.ApplyToStorage, pageSize: config.PageCommits,
	}, nil
}

func (c *defaultClient) Disconnect() error {
	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (c *defaultClient) nextCorrelation() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.correlation
	c.correlation++
	return id
}

func (c *defaultClient) request(conn stream.ConnectionHandle, message wire.Message) (wire.Message, error) {
	frame, err := c.wire.Encode(message)
	if err != nil {
		return nil, err
	}
	cid := message.Header().CorrelationID
	if err := conn.Send(frame); err != nil {
		return nil, err
	}
	for i := 0; i < 4000; i++ {
		if frame := conn.TryPoll(); frame != nil {
			decoded, err := c.wire.Decode(frame)
			if err != nil {
				return nil, err
			}
			if decoded.Header().CorrelationID == cid {
				if pe, ok := decoded.(wire.PeerErrorMessage); ok {
					return nil, &RemoteError{Code: pe.Code, Message: pe.Message}
				}
				return decoded, nil
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil, NewError("no response for correlation", nil)
}

func (c *defaultClient) fetchRemote(conn stream.ConnectionHandle, namespaceID string, sinceHash *codec.Hash, haves []codec.Hash, maxCommits int) ([]document.Commit, []document.CommitStub, error) {
	fetch := wire.CommitFetchMessage{
		H: wire.Header{
			MessageType:     wire.MsgCommitFetch,
			ProtocolVersion: wire.KdbWireProtocolVersion,
			CorrelationID:   c.nextCorrelation(),
		},
		Namespace:  namespaceID,
		SinceHash:  sinceHash,
		MaxCommits: maxCommits,
		Haves:      haves,
	}
	resp, err := c.request(conn, fetch)
	if err != nil {
		return nil, nil, err
	}
	push, ok := resp.(wire.CommitPushMessage)
	if !ok {
		return nil, nil, NewError("expected CommitPush response to CommitFetch", nil)
	}
	return push.Commits, push.Stubs, nil
}

func (c *defaultClient) pushToRemote(conn stream.ConnectionHandle, namespaceID string, commits []document.Commit) (int, error) {
	return c.pushPage(conn, namespaceID, commits, nil, false)
}

func (c *defaultClient) pushPage(conn stream.ConnectionHandle, namespaceID string, commits []document.Commit, stubs []document.CommitStub, more bool) (int, error) {
	if len(commits) == 0 {
		return 0, nil
	}
	push := wire.CommitPushMessage{
		H: wire.Header{
			MessageType:     wire.MsgCommitPush,
			ProtocolVersion: wire.KdbWireProtocolVersion,
			CorrelationID:   c.nextCorrelation(),
		},
		Namespace: namespaceID,
		Commits:   commits,
		Stubs:     stubs,
		More:      more,
	}
	resp, err := c.request(conn, push)
	if err != nil {
		return 0, err
	}
	switch r := resp.(type) {
	case wire.CommitPushAckMessage:
		return r.AppliedCommits, nil
	case wire.ConflictReportMessage:
		// Previously the response went unread, so a peer that rejected the push for a genuine
		// same-document divergence still reported len(commits) pushed - the caller had no way to
		// learn its commits were stored but never adopted as the peer's head.
		var report kdberr.ConflictReport
		if err := json.Unmarshal(r.ReportBytes, &report); err != nil {
			return 0, NewError("peer reported a push conflict with an undecodable report", err)
		}
		return 0, kdberr.NewConflictError("peer rejected push: divergent history in "+r.Namespace, report)
	default:
		return 0, NewError("expected CommitPushAck response to CommitPush", nil)
	}
}

type defaultSession struct {
	client      *defaultClient
	dag         *dag.InMemoryCommitDag
	storage     storage.Adapter
	namespaceID string
	remoteHead  codec.Hash
	conn        stream.ConnectionHandle
	// materialize replays a fetched commit's document ops into local storage - see
	// ClientConfig.MaterializeCommit's doc comment. May be nil (a pulled commit is then reachable
	// from the DAG but invisible to any query that reads through storage, matching the behavior
	// before this field existed).
	materialize func(document.Commit) error
	// persist durably logs a commit pulled from a peer - see ClientConfig.Persist's doc
	// comment. May be nil (peer sync then has no local durability of its own, matching the
	// behavior before this field existed).
	persist      func(document.Commit) error
	persistAsync func(document.Commit) (func() error, error)
	// conflictPolicy/conflictResolver - see ClientConfig's doc comment.
	conflictPolicy   transaction.ConflictPolicy
	conflictResolver transaction.ConflictResolver
	node             LocalNode
	applyToStorage   bool
	pageSize         int
}

func (s *defaultSession) NamespaceID() string    { return s.namespaceID }
func (s *defaultSession) RemoteHead() codec.Hash { return s.remoteHead }

func (s *defaultSession) PullMissing() (Result, error) {
	localHead, err := s.dag.Head()
	if err != nil {
		return Result{}, err
	}
	if localHead == s.remoteHead {
		plan, _ := ComputeSyncPlan(s.dag, localHead, s.remoteHead)
		return Result{FinalHead: localHead, Plan: plan}, nil
	}
	// Page until the host has nothing this node lacks, storing each page as it arrives and
	// deciding nothing: deciding on a partial fetch of a divergent branch would merge half of
	// it, then merge again for every later page. The head decision is made once, below.
	haves := spreadAncestors(s.dag, localHead)
	env := s.ingestEnv()
	applied := 0
	incomingHead := s.remoteHead
	for {
		page, stubs, err := s.client.fetchRemote(s.conn, s.namespaceID, &localHead, haves, s.pageCommits())
		if err != nil {
			return Result{}, err
		}
		if len(page) == 0 {
			break
		}
		n, err := StoreCommits(env, page, stubs)
		applied += n
		if err != nil {
			return Result{AppliedCommits: applied}, err
		}
		// Parents first means the page's last commit is the host's head when this was the final
		// page, and in every case a commit whose ancestry covers everything received so far is
		// among the page's childless commits - which are what the next fetch names as haves.
		incomingHead = page[len(page)-1].Hash
		haves = append(haves, pageTips(page)...)
	}
	ingested, err := Adopt(env, incomingHead)
	if err != nil {
		return Result{AppliedCommits: applied}, err
	}
	outcome := ingested.Outcome
	finalHead, err := s.dag.Head()
	if err != nil {
		return Result{}, err
	}
	plan, _ := ComputeSyncPlan(s.dag, finalHead, s.remoteHead)
	// Non-nil only on a genuine same-document divergence: finalHead was deliberately left
	// unmoved from what it was before the pull - the caller must resolve this before retrying.
	return Result{AppliedCommits: applied, FinalHead: finalHead, Plan: plan, Conflict: outcome.Report}, nil
}

func (s *defaultSession) pageCommits() int {
	if s.pageSize > 0 {
		return s.pageSize
	}
	return DefaultPageCommits
}

// spreadAncestors names head plus first-parent ancestors at exponentially growing distances
// (1, 2, 4, ...), and every local branch head: enough for the host to find a recent common
// ancestor when this node's head is one it has never seen, without sending all of history.
func spreadAncestors(d *dag.InMemoryCommitDag, head codec.Hash) []codec.Hash {
	out := []codec.Hash{head}
	cur, step, walked := head, 1, 0
	for len(out) < 32 {
		c, ok := d.GetCommit(cur)
		if !ok || len(c.ParentHashes) == 0 {
			break
		}
		cur = c.ParentHashes[0]
		walked++
		if walked == step {
			out = append(out, cur)
			step *= 2
		}
	}
	for _, b := range d.ListBranches() {
		out = append(out, b.HeadHash)
	}
	return out
}

// pageTips returns the commits in page no other commit in page names as a parent.
func pageTips(page []document.Commit) []codec.Hash {
	parent := map[codec.Hash]bool{}
	for _, c := range page {
		for _, p := range c.ParentHashes {
			parent[p] = true
		}
	}
	var out []codec.Hash
	for _, c := range page {
		if !parent[c.Hash] {
			out = append(out, c.Hash)
		}
	}
	return out
}

// RemoteError is a PeerErrorMessage the remote peer replied with.
type RemoteError struct {
	Code    wire.ErrorCode
	Message string
}

func (e *RemoteError) Error() string { return "peer: " + string(e.Code) + ": " + e.Message }

// ingestEnv describes this session's local namespace to Ingest.
func (s *defaultSession) ingestEnv() IngestEnv {
	return IngestEnv{
		DAG:            s.dag,
		Storage:        s.storage,
		NamespaceID:    s.namespaceID,
		Node:           s.node,
		Persist:        s.persist,
		PersistAsync:   s.persistAsync,
		ApplyToStorage: s.materialize != nil || s.applyToStorage,
		Resolution:     ResolutionOptions{Policy: s.conflictPolicy, Resolver: s.conflictResolver},
	}
}

func (s *defaultSession) PushCommits(commits []document.Commit) (int, error) {
	return s.client.pushToRemote(s.conn, s.namespaceID, commits)
}

func (s *defaultSession) SyncBidirectional() (Result, error) {
	pull, err := s.PullMissing()
	if err != nil {
		return Result{}, err
	}
	localHead, err := s.dag.Head()
	if err != nil {
		return Result{}, err
	}
	pushed, err := s.pushMissing(localHead)
	if err != nil {
		return Result{}, err
	}
	finalHead, err := s.dag.Head()
	if err != nil {
		return Result{}, err
	}
	pull.PushedCommits = pushed
	pull.FinalHead = finalHead
	return pull, nil
}

// pushMissing sends everything reachable from localHead that the remote lacks, in pages; only
// the last page asks the remote to decide its head.
func (s *defaultSession) pushMissing(localHead codec.Hash) (int, error) {
	all, stubs, err := MissingCommits(s.dag, localHead, []codec.Hash{s.remoteHead}, math.MaxInt)
	if err != nil {
		return 0, err
	}
	pushed := 0
	size := s.pageCommits()
	for start := 0; start < len(all); start += size {
		end := min(start+size, len(all))
		var pageStubs []document.CommitStub
		if start == 0 {
			pageStubs = stubs
		}
		n, err := s.client.pushPage(s.conn, s.namespaceID, all[start:end], pageStubs, end < len(all))
		pushed += n
		if err != nil {
			return pushed, err
		}
	}
	return pushed, nil
}

func (s *defaultSession) FetchCommitsSince(sinceHash *codec.Hash) ([]document.Commit, error) {
	commits, _, err := s.client.fetchRemote(s.conn, s.namespaceID, sinceHash, nil, s.pageCommits())
	return commits, err
}

func coreConnectOptions(config ClientConfig) core.TransportConnectOptions {
	opts := core.DefaultConnectOptions()
	opts.TLS = config.TLS
	if config.ConnectionContext.Headers != nil {
		opts.ConnectHeaders = config.ConnectionContext.Headers
	}
	return opts
}
