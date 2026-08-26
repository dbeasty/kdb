package peersync

import (
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/stream"
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
	return &defaultSession{client: c, dag: c.dag, storage: c.storage, namespaceID: config.NamespaceID, remoteHead: remoteHead, conn: conn, persist: config.Persist}, nil
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
				return decoded, nil
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil, NewError("no response for correlation", nil)
}

func (c *defaultClient) fetchRemote(conn stream.ConnectionHandle, namespaceID string, sinceHash *codec.Hash, maxCommits int) ([]document.Commit, error) {
	fetch := wire.CommitFetchMessage{
		H: wire.Header{
			MessageType:     wire.MsgCommitFetch,
			ProtocolVersion: wire.KdbWireProtocolVersion,
			CorrelationID:   c.nextCorrelation(),
		},
		Namespace:  namespaceID,
		SinceHash:  sinceHash,
		MaxCommits: maxCommits,
	}
	resp, err := c.request(conn, fetch)
	if err != nil {
		return nil, err
	}
	push, ok := resp.(wire.CommitPushMessage)
	if !ok {
		return nil, NewError("expected CommitPush response to CommitFetch", nil)
	}
	return push.Commits, nil
}

func (c *defaultClient) pushToRemote(conn stream.ConnectionHandle, namespaceID string, commits []document.Commit) (int, error) {
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
	}
	if _, err := c.request(conn, push); err != nil {
		return 0, err
	}
	return len(commits), nil
}

type defaultSession struct {
	client      *defaultClient
	dag         *dag.InMemoryCommitDag
	storage     storage.Adapter
	namespaceID string
	remoteHead  codec.Hash
	conn        stream.ConnectionHandle
	// persist durably logs a commit pulled from a peer - see ClientConfig.Persist's doc
	// comment. May be nil (peer sync then has no local durability of its own, matching the
	// behavior before this field existed).
	persist func(document.Commit) error
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
	fetched, err := s.client.fetchRemote(s.conn, s.namespaceID, &localHead, 100)
	if err != nil {
		return Result{}, err
	}
	// putCommit always stores every fetched commit, same as the push-receiving side - only the
	// branch-pointer decision below is gated.
	applied := 0
	for _, commit := range fetched {
		if _, ok := s.dag.GetCommit(commit.Hash); ok {
			continue
		}
		if err := s.dag.PutCommit(commit, true); err != nil {
			return Result{}, err
		}
		// Fixes kdb-spec-layer13 §2.2 client-side: without this, a commit pulled from a peer
		// lived only in memory and vanished on restart of a file-backed node.
		if s.persist != nil {
			if err := s.persist(commit); err != nil {
				return Result{}, err
			}
		}
		applied++
	}
	incomingHead := s.remoteHead
	if len(fetched) > 0 {
		incomingHead = fetched[len(fetched)-1].Hash
	}
	// Component 39-equivalent fix: the remote head is not automatically "ahead" just because we
	// fetched commits leading to it - local history may have diverged from remote since the last
	// sync (see ResolveDivergence's own doc comment). Blindly moving main to the last fetched
	// commit here would silently orphan any local-only commits from main, exactly the bug fixed
	// on the Kotlin side for Component 39 - same shared decision function as the host's
	// CommitPush handler, not two independently maintained copies.
	outcome, err := ResolveDivergence(s.dag, s.storage, s.namespaceID, localHead, incomingHead)
	if err != nil {
		return Result{}, err
	}
	// See host.go's identical comment: the auto-merge case creates a brand new commit that
	// exists nowhere but this node and needs the same persistence as anything pulled over the
	// wire above (kdb-spec-layer13 §2.2).
	if outcome.MergeCommit != nil && s.persist != nil {
		if err := s.persist(*outcome.MergeCommit); err != nil {
			return Result{}, err
		}
	}
	finalHead, err := s.dag.Head()
	if err != nil {
		return Result{}, err
	}
	plan, _ := ComputeSyncPlan(s.dag, finalHead, s.remoteHead)
	// Non-nil only on a genuine same-document divergence (§7 test 2/3 equivalent): finalHead was
	// deliberately left unmoved from what it was before the pull - the caller must resolve this
	// before retrying, not just ignore it.
	return Result{AppliedCommits: applied, FinalHead: finalHead, Plan: plan, Conflict: outcome.Report}, nil
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
	toPush, err := CommitsToPush(s.dag, localHead, s.remoteHead, 100)
	if err != nil {
		return Result{}, err
	}
	pushed, err := s.PushCommits(toPush)
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

func (s *defaultSession) FetchCommitsSince(sinceHash *codec.Hash) ([]document.Commit, error) {
	return s.client.fetchRemote(s.conn, s.namespaceID, sinceHash, 100)
}

func coreConnectOptions(config ClientConfig) core.TransportConnectOptions {
	opts := core.DefaultConnectOptions()
	opts.TLS = config.TLS
	if config.ConnectionContext.Headers != nil {
		opts.ConnectHeaders = config.ConnectionContext.Headers
	}
	return opts
}
