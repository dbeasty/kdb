package peersync

import (
	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
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
	auth    auth.Engine
	handler *frameHandler
	config  *HostConfig
}

// NewHost creates an in-memory peer sync host.
func NewHost(w wire.Codec, dagInst *dag.InMemoryCommitDag, engine auth.Engine, ctx auth.ConnectionContext) Host {
	if engine == nil {
		engine = auth.AllowAll
	}
	return &defaultHost{wire: w, dag: dagInst, auth: engine}
}

func (h *defaultHost) Start(config HostConfig) error {
	h.config = &config
	h.handler = newFrameHandler(h.wire, h.dag, config, h.auth, auth.EmptyContext)
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
func NewConnectionHost(w wire.Codec, dagInst *dag.InMemoryCommitDag, config HostConfig, engine auth.Engine, ctx auth.ConnectionContext) *ConnectionHost {
	if engine == nil {
		engine = auth.AllowAll
	}
	return &ConnectionHost{handler: newFrameHandler(w, dagInst, config, engine, ctx)}
}

func (h *ConnectionHost) Start(HostConfig) error {
	return NewError("ConnectionHost does not support in-memory hub start", nil)
}

func (h *ConnectionHost) Stop() error { return nil }

func (h *ConnectionHost) HandleFrame(frame []byte) ([]byte, error) {
	return h.handler.handleFrame(frame)
}

type frameHandler struct {
	wire   wire.Codec
	dag    *dag.InMemoryCommitDag
	cfg    HostConfig
	auth   auth.Engine
	ctx    auth.ConnectionContext
}

func newFrameHandler(w wire.Codec, dagInst *dag.InMemoryCommitDag, cfg HostConfig, engine auth.Engine, ctx auth.ConnectionContext) *frameHandler {
	return &frameHandler{wire: w, dag: dagInst, cfg: cfg, auth: engine, ctx: ctx}
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
		for _, commit := range m.Commits {
			if err := h.dag.PutCommit(commit, true); err != nil {
				return nil, err
			}
			if h.cfg.MaterializeCommit != nil {
				_ = h.cfg.MaterializeCommit(commit)
			}
		}
		return nil, nil
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
