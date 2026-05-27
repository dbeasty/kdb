package hybrid

import (
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
)

// VersionResolver resolves version clauses to commit hashes.
type VersionResolver interface {
	Resolve(d dag.CommitDAG, clause VersionClause, activeCheckout *CheckoutHandle) (codec.Hash, error)
}

// DefaultVersionResolver resolves via the commit DAG head and hash literals.
type DefaultVersionResolver struct{}

// NewDefaultVersionResolver returns the default resolver.
func NewDefaultVersionResolver() *DefaultVersionResolver {
	return &DefaultVersionResolver{}
}

func (DefaultVersionResolver) Resolve(d dag.CommitDAG, clause VersionClause, activeCheckout *CheckoutHandle) (codec.Hash, error) {
	if clause == nil {
		if activeCheckout != nil {
			return activeCheckout.CommitHash, nil
		}
		return d.Head()
	}
	switch c := clause.(type) {
	case AtCommit:
		return codec.HashFromHex(strings.TrimSpace(c.Hex))
	case AtTag, AtTime:
		// Tag/time resolution requires extended DAG APIs; use head until wired.
		return d.Head()
	default:
		return d.Head()
	}
}
