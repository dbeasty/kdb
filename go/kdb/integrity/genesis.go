package integrity

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// GenesisCommitHash reconstructs the well-known genesis commit hash for a
// namespace - the same fixed transaction/author/timestamp/message
// dag.NewInMemoryCommitDag builds on every open (in_memory_commit_dag.go).
// Genesis is namespace-scoped (NamespaceID is part of the hashed payload)
// but otherwise identical every time, and by design is never written to
// the delta log - a real commit's first parent legitimately points to it,
// and L2 verification must not report that as a missing_parent.
func GenesisCommitHash(namespaceID string) (codec.Hash, error) {
	genesisTx, err := codec.UUIDFromString("00000000-0000-4000-8000-000000000001")
	if err != nil {
		return codec.Hash{}, err
	}
	genesisAuthor, err := codec.UUIDFromString("00000000-0000-4000-8000-000000000002")
	if err != nil {
		return codec.Hash{}, err
	}
	genesis, err := document.BuildCommit(
		nil, namespaceID, genesisTx, codec.Timestamp{}, genesisAuthor,
		nil, document.EmptyDocumentTree().TreeHash, nil, "genesis",
	)
	if err != nil {
		return codec.Hash{}, err
	}
	return genesis.Hash, nil
}
