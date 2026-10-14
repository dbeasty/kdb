package server

import "fmt"

// CatchUpPolicy is how a namespace lets its replicas catch up (docs/kdb-memory-cassandra-plan.md
// C7): a replicated definition in the metadata namespace (kind "catchup"), like a resolution
// chain, because the history a replica keeps is the whole cluster's concern - a namespace that
// must keep full history everywhere has to refuse a squash on every node that could serve one.
type CatchUpPolicy struct {
	// RefuseSquash makes every node refuse to serve this namespace's squash catch-ups (a peer
	// that asks replays instead, which keeps every commit), and refuse to take one from a peer
	// that does not know the policy yet.
	RefuseSquash bool `json:"refuseSquash,omitempty"`
	// Reason is what a refused peer is told; empty gives a generic one.
	Reason string `json:"reason,omitempty"`
}

// SetCatchUpPolicy records ns's catch-up policy on this runtime. Called by the metadata store as
// the definition arrives or changes; nil (or a policy refusing nothing) clears it.
func (s *KdbServerRuntime) SetCatchUpPolicy(p *CatchUpPolicy) {
	if p != nil && !p.RefuseSquash {
		p = nil
	}
	s.catchUp.Store(p)
}

// CatchUpPolicyOf returns this namespace's catch-up policy; the zero policy refuses nothing.
func (s *KdbServerRuntime) CatchUpPolicyOf() CatchUpPolicy {
	if p := s.catchUp.Load(); p != nil {
		return *p
	}
	return CatchUpPolicy{}
}

// squashRefusal is peersync.IngestEnv.SquashRefused for this namespace: why it refuses squash
// catch-ups, or empty when it allows them.
func (s *KdbServerRuntime) squashRefusal() string {
	p := s.catchUp.Load()
	if p == nil || !p.RefuseSquash {
		return ""
	}
	if p.Reason != "" {
		return p.Reason
	}
	return fmt.Sprintf("namespace %s keeps full history on every replica (its catch-up policy refuses squash)", s.Runtime.DefaultNamespace)
}
