package compaction

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/policy"
)

// Request triggers one compaction cycle.
type Request struct {
	NamespaceID      string
	Force            bool
	MaxSquashCommits int
}

// Result summarizes a compaction cycle.
type Result struct {
	SquashedCount       int
	SyntheticRoot       *codec.Hash
	GcReclaimedBytes    int64
	StorageJobsEnqueued int
}

// Plan is a planned compaction with peer safety metadata.
type Plan struct {
	Boundaries []PlannedSquash
	PeerSafe   bool
	Blockers   []Blocker
}

// PlannedSquash is one squash operation.
type PlannedSquash struct {
	Boundary     codec.Hash
	SquashHashes []codec.Hash
	Strategy     policy.RetainStrategy
}

// Blocker prevents compaction from proceeding.
type Blocker interface {
	blocker()
}

// ProtectedTag blocks compaction for a tagged commit.
type ProtectedTag struct {
	Tag  string
	Hash codec.Hash
}

func (ProtectedTag) blocker() {}

// ProtectedBranch blocks compaction for a branch head.
type ProtectedBranch struct {
	Branch string
	Hash   codec.Hash
}

func (ProtectedBranch) blocker() {}

// PeerBelowBoundary blocks when a peer is below the boundary.
type PeerBelowBoundary struct {
	PeerID string
	Head   codec.Hash
}

func (PeerBelowBoundary) blocker() {}

// PolicyDisabled blocks when policy disables squash.
type PolicyDisabled struct {
	Reason string
}

func (PolicyDisabled) blocker() {}

// Intent records a compaction intent.
type Intent struct {
	NamespaceID    string
	Boundary       codec.Hash
	IssuedAtMillis int64
}

// AckSet records peer acknowledgements.
type AckSet struct {
	AckedPeers []string
	Rejected   map[string]codec.Hash
}
