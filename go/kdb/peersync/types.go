package peersync

import (
	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/transport/core"
)

// HostConfig configures an in-memory peer sync host hub.
type HostConfig struct {
	NamespaceID       string
	NodeID            string
	TransportHub      string
	MaterializeCommit func(document.Commit) error
}

// ClientConfig configures a peer sync client connection.
type ClientConfig struct {
	NamespaceID       string
	NodeID            string
	PeerURI           string
	ConnectionContext auth.ConnectionContext
	TLS               *core.TransportTlsSettings
}

// DagSyncPlan describes commits unique to each side of a sync.
type DagSyncPlan struct {
	CommonAncestor *codec.Hash
	LocalOnly      []codec.Hash
	RemoteOnly     []codec.Hash
}

// Result summarizes a peer sync operation.
type Result struct {
	AppliedCommits int
	PushedCommits  int
	FinalHead      codec.Hash
	Plan           *DagSyncPlan
	// Conflict is non-nil only on a genuine same-document divergence (see ResolveDivergence):
	// FinalHead was deliberately left unmoved from what it was before the pull - the caller must
	// resolve this before retrying, not just ignore it.
	Conflict *kdberr.ConflictReport
}
