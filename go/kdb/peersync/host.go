package peersync

import (
	"encoding/json"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Host serves peer sync wire frames.
type Host interface {
	Start(config HostConfig) error
	Stop() error
	HandleFrame(frame []byte) ([]byte, error)
}

type defaultHost struct {
	wire    wire.Codec
	dag     *dag.InMemoryCommitDag
	storage storage.Adapter
	auth    auth.Engine
	handler *frameHandler
	config  *HostConfig
}

// NewHost creates an in-memory peer sync host. store is used for the document-level writes/
// deletes a non-conflicting auto-merge (see ResolveDivergence) stages when an incoming push
// diverges from local history - required, not optional, since a real push can hit that path.
func NewHost(w wire.Codec, dagInst *dag.InMemoryCommitDag, store storage.Adapter, engine auth.Engine, ctx auth.ConnectionContext) Host {
	if engine == nil {
		engine = auth.AllowAll
	}
	return &defaultHost{wire: w, dag: dagInst, storage: store, auth: engine}
}

func (h *defaultHost) Start(config HostConfig) error {
	h.config = &config
	h.handler = newFrameHandler(h.wire, h.dag, h.storage, config, h.auth, auth.EmptyContext)
	hub := stream.HubFor(config.TransportHub)
	hub.ServerHandler = func(frame []byte) {
		if response, err := h.handler.handleFrame(frame); err == nil && response != nil {
			hub.ServerSend(response)
		}
	}
	return nil
}

func (h *defaultHost) Stop() error {
	if h.config == nil {
		return nil
	}
	stream.HubFor(h.config.TransportHub).ServerHandler = nil
	h.config = nil
	h.handler = nil
	return nil
}

func (h *defaultHost) HandleFrame(frame []byte) ([]byte, error) {
	if h.handler == nil {
		return nil, NewError("PeerSyncHost not started", nil)
	}
	return h.handler.handleFrame(frame)
}

// ConnectionHost wraps a per-connection frame handler.
type ConnectionHost struct {
	handler *frameHandler
}

// NewConnectionHost builds a host for one connection context.
func NewConnectionHost(w wire.Codec, dagInst *dag.InMemoryCommitDag, store storage.Adapter, config HostConfig, engine auth.Engine, ctx auth.ConnectionContext) *ConnectionHost {
	if engine == nil {
		engine = auth.AllowAll
	}
	return &ConnectionHost{handler: newFrameHandler(w, dagInst, store, config, engine, ctx)}
}

func (h *ConnectionHost) Start(HostConfig) error {
	return NewError("ConnectionHost does not support in-memory hub start", nil)
}

func (h *ConnectionHost) Stop() error { return nil }

func (h *ConnectionHost) HandleFrame(frame []byte) ([]byte, error) {
	return h.handler.handleFrame(frame)
}

type frameHandler struct {
	wire    wire.Codec
	dag     *dag.InMemoryCommitDag
	storage storage.Adapter
	cfg     HostConfig
	auth    auth.Engine
	ctx     auth.ConnectionContext
}

func newFrameHandler(w wire.Codec, dagInst *dag.InMemoryCommitDag, store storage.Adapter, cfg HostConfig, engine auth.Engine, ctx auth.ConnectionContext) *frameHandler {
	return &frameHandler{wire: w, dag: dagInst, storage: store, cfg: cfg, auth: engine, ctx: ctx}
}

func (h *frameHandler) handleFrame(frame []byte) ([]byte, error) {
	msg, err := h.wire.Decode(frame)
	if err != nil {
		return nil, err
	}
	switch m := msg.(type) {
	case wire.HandshakeMessage:
		ack := wire.HandshakeAckPayload{
			Accepted:           true,
			NegotiatedEncoding: wire.EncodingKdbBinary,
			ProtocolVersion:    wire.KdbWireProtocolVersion,
			RemoteHeads:        map[string]string{h.cfg.NamespaceID: mustHeadHex(h.dag)},
		}
		ackMsg := wire.HandshakeAckMessage{
			H: wire.Header{
				MessageType:     wire.MsgHandshake,
				ProtocolVersion: wire.KdbWireProtocolVersion,
				CorrelationID:   m.H.CorrelationID,
			},
			Response: ack,
		}
		return h.wire.Encode(ackMsg)
	case wire.CommitFetchMessage:
		commits, err := h.fetchCommits(m.SinceHash, m.MaxCommits)
		if err != nil {
			return nil, err
		}
		push := wire.CommitPushMessage{
			H: wire.Header{
				MessageType:     wire.MsgCommitPush,
				ProtocolVersion: wire.KdbWireProtocolVersion,
				CorrelationID:   m.H.CorrelationID,
			},
			Namespace: m.Namespace,
			Commits:   commits,
		}
		return h.wire.Encode(push)
	case wire.CommitPushMessage:
		// putCommit always stores, regardless of what happens to "main" below (component 39
		// spec §5: history must never be lost, only the branch-pointer decision is gated).
		applied := 0
		for _, commit := range m.Commits {
			if h.dag.HasCommit(commit.Hash) {
				continue
			}
			if err := h.dag.PutCommit(commit, true); err != nil {
				return nil, err
			}
			if h.cfg.MaterializeCommit != nil {
				_ = h.cfg.MaterializeCommit(commit)
			}
			// Fixes kdb-spec-layer13 §2.2: dag.PutCommit only mutates the in-memory DAG - without
			// this, a commit received from a peer lived only in memory and vanished on restart of
			// a file-backed node, even though the node re-fetching it from peers on next connect
			// would eventually paper over it cluster-wide (this is about *this* node's local
			// durability, not data loss overall).
			if h.cfg.Persist != nil {
				if err := h.cfg.Persist(commit); err != nil {
					return nil, err
				}
			}
			applied++
		}
		if len(m.Commits) > 0 {
			incomingHead := m.Commits[len(m.Commits)-1].Hash
			localHead, err := h.dag.Head()
			if err != nil {
				return nil, err
			}
			// Component 39-equivalent fix: an incoming push is not automatically "ahead" of
			// main just because it was pushed - this host's own history may have diverged
			// (e.g. local writes since the last sync). Same shared decision function as the
			// client's PullMissing, not two independently maintained copies - that's exactly
			// how the original blind dag.SetHead("main", ...) bug went unnoticed on one side
			// while looking "fine" on the other.
			outcome, err := ResolveDivergence(h.dag, h.storage, m.Namespace, localHead, incomingHead)
			if err != nil {
				return nil, err
			}
			// The auto-merge case (OutcomeMerged) creates a brand new commit (via
			// AppendMergeCommit) that exists nowhere but this node - it needs the same
			// persistence as any other newly-created commit, not just the commits that arrived
			// over the wire above (kdb-spec-layer13 §2.2).
			if outcome.MergeCommit != nil && h.cfg.Persist != nil {
				if err := h.cfg.Persist(*outcome.MergeCommit); err != nil {
					return nil, err
				}
			}
			if outcome.Kind == OutcomeConflict {
				reportBytes, err := json.Marshal(outcome.Report)
				if err != nil {
					return nil, err
				}
				return h.wire.Encode(wire.ConflictReportMessage{
					H:           wire.Header{MessageType: wire.MsgConflictReport, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: m.H.CorrelationID},
					Namespace:   m.Namespace,
					ReportBytes: reportBytes,
				})
			}
			// NoOp/FastForwarded/Merged all succeed - fall through to the ack below.
		}
		// CommitPush is a request/response pair, not fire-and-forget (component 23 spec §5): the
		// client blocks on a correlated reply, so every non-conflicting outcome owes it one.
		// Returning nil here instead left a clean push with no reply at all and hung the caller
		// until its correlation wait expired.
		head, err := h.dag.Head()
		if err != nil {
			return nil, err
		}
		return h.wire.Encode(wire.CommitPushAckMessage{
			H:              wire.Header{MessageType: wire.MsgCommitPushAck, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: m.H.CorrelationID},
			Namespace:      m.Namespace,
			AppliedCommits: applied,
			HeadHex:        head.Hex(),
		})
	default:
		return nil, nil
	}
}

func (h *frameHandler) fetchCommits(sinceHash *codec.Hash, maxCommits int) ([]document.Commit, error) {
	head, err := h.dag.Head()
	if err != nil {
		return nil, err
	}
	if sinceHash != nil && *sinceHash == head {
		return nil, nil
	}
	walked := h.dag.Walk(head, sinceHash, maxCommits)
	out := make([]document.Commit, 0, len(walked))
	for i := len(walked) - 1; i >= 0; i-- {
		if full, ok := walked[i].(dag.FullEntry); ok {
			out = append(out, full.Commit)
		}
	}
	return out, nil
}

func mustHeadHex(d *dag.InMemoryCommitDag) string {
	head, err := d.Head()
	if err != nil {
		return ""
	}
	return head.Hex()
}
