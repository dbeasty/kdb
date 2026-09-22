package server

import (
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
)

// Conflict resolution chains (docs/kdb-distributed-self-healing-research.md, Phase 10.5): a
// namespace's ordered rules for settling a same-document conflict when peer sync merges. The chain
// is a replicated definition in the metadata namespace, because merges are deterministic only when
// every node decides them alike.

// SetResolutionChain records ns's chain on this runtime. Called by the metadata store as the
// definition arrives or changes; nil clears it.
func (s *KdbServerRuntime) SetResolutionChain(c *peersync.ResolutionChain) {
	if c != nil && len(c.Rules) == 0 {
		c = nil
	}
	s.resolution.Store(c)
}

// ResolutionChainOf returns this namespace's chain, or nil.
func (s *KdbServerRuntime) ResolutionChainOf() *peersync.ResolutionChain { return s.resolution.Load() }

// peerResolution is the resolution options peer sync merges this namespace with.
func (s *KdbServerRuntime) peerResolution() peersync.ResolutionOptions {
	return peersync.ResolutionOptions{
		Policy: s.PeerSyncConflictPolicy,
		Chain:  s.ResolutionChainOf(),
		Valid: func(body string) bool {
			// Every node validates against the namespace's replicated schema. Until a schema
			// change has reached both nodes they can judge differently - the chain hash does not
			// cover the schema - which is why the rule only ever picks between two present values.
			return schema.Validate(document.Document{JSON: body}, s.Schema()).IsSuccess()
		},
	}
}
