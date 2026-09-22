package peersync

import (
	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/transaction"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/wire"
)

// HostConfig configures an in-memory peer sync host hub.
type HostConfig struct {
	NamespaceID       string
	NodeID            string
	TransportHub      string
	MaterializeCommit func(document.Commit) error
	// PersistAsync is Persist split into queue and wait - see IngestEnv.PersistAsync. Preferred
	// over Persist when set.
	PersistAsync func(document.Commit) (wait func() error, err error)
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
	// ConflictPolicy/ConflictResolver control how a genuine same-document divergence is resolved
	// - see ResolutionOptions' doc comment (conflict_detection.go) for the full contract. The
	// zero value (ConflictPolicyAppendOnly) means "report, don't resolve", unchanged from before
	// these fields existed.
	ConflictPolicy   transaction.ConflictPolicy
	ConflictResolver transaction.ConflictResolver
	// ChainOf, when set, returns the namespace's resolution chain. v1 carries no chain hash, so
	// a v1 peer cannot be known to decide conflicts alike: a namespace with a chain never
	// auto-merges over v1 - its divergences are queued instead.
	ChainOf func() *ResolutionChain
	// Node serializes ingest against the runtime's own writers and runs its post-commit hooks -
	// see LocalNode. nil serializes only against other ingests in this process.
	Node LocalNode
	// Conflicts records refused updates durably - see IngestEnv.Conflicts.
	Conflicts *ConflictQueue
	// ClassifyError maps a failure to the wire error code sent back in a PeerErrorMessage; the
	// server supplies its own classifier so a busy or draining node says so. Unrecognized errors
	// fall back to peer sync's own classification.
	ClassifyError func(error) (wire.ErrorCode, bool)
	// ApplyToStorage writes ingested documents into the storage adapter. Setting
	// MaterializeCommit (the older way to ask for this) implies it; the function itself is no
	// longer called, because Ingest applies a head move's net effect in one verified step.
	ApplyToStorage bool
}

// DefaultPageCommits caps one page of a commit transfer when the requester names no cap.
const DefaultPageCommits = 500

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
	// PersistAsync - see HostConfig.PersistAsync; same contract, client side.
	PersistAsync func(document.Commit) (wait func() error, err error)
	// Persist durably logs a commit pulled from a peer - see HostConfig.Persist's doc comment;
	// same contract, client side.
	Persist func(document.Commit) error
	// ConflictPolicy/ConflictResolver - see HostConfig's doc comment; same contract, client side.
	ConflictPolicy   transaction.ConflictPolicy
	ConflictResolver transaction.ConflictResolver
	// FastForwardPushOnly pushes only when the peer's head is already in this node's history, so
	// the push fast-forwards it and the peer never has to merge. Set for a namespace with a
	// resolution chain: a v1 peer cannot be known to settle conflicts the same way.
	FastForwardPushOnly bool
	// Node / ApplyToStorage - see HostConfig's doc comments; same contract, client side.
	Node           LocalNode
	ApplyToStorage bool
	// PageCommits caps each page of a pull or push; 0 means DefaultPageCommits.
	PageCommits int
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
