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
	// Persist durably logs a commit ingested from a peer (via CommitPush, or a
	// ResolveDivergence-created auto-merge commit) - separate from MaterializeCommit, which is
	// about tree reconstruction, not durability. dag.PutCommit only ever mutates the in-memory
	// DAG; without Persist wired to the runtime's actual delta-log writer (e.g.
	// embed.PersistingCommitDAG.Persist), a commit received from a peer lives only in memory and
	// is silently lost on restart - see kdb-spec-layer13 Component 47 §2.2/Component 52 §9.2.
	// Left nil (the default), peer sync is exactly as durable as it was before this field
	// existed: not durable at all. A caller wiring peer sync into a file-backed runtime must set
	// this to get commits from peers to survive a restart.
	Persist func(document.Commit) error
}

// ClientConfig configures a peer sync client connection.
type ClientConfig struct {
	NamespaceID       string
	NodeID            string
	PeerURI           string
	ConnectionContext auth.ConnectionContext
	TLS               *core.TransportTlsSettings
	// MaterializeCommit replays a commit fetched from the remote peer into local storage - see
	// HostConfig.MaterializeCommit's doc comment; same contract, client side. Only needed for
	// commits fetched over the wire (PutMissing's CommitFetch results): a ResolveDivergence
	// auto-merge commit already writes its own documents into storage as it builds the merge
	// tree (see mergeNonConflicting), so it is deliberately not passed through this callback.
	MaterializeCommit func(document.Commit) error
	// Persist durably logs a commit pulled from a peer - see HostConfig.Persist's doc comment;
	// same contract, client side.
	Persist func(document.Commit) error
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
