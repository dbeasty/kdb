package compaction

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/policy"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Engine orchestrates DAG compaction for a namespace.
type Engine interface {
	RunCycle(request Request) (Result, error)
	Plan(request Request) (Plan, error)
	UpdatePeerHeads(namespaceID string, heads map[string]codec.Hash)
}

// Config wires a compaction engine.
type Config struct {
	NamespaceID    string
	DAG            dag.CommitDAG
	Storage        storage.Adapter
	PolicyRegistry policy.Registry
	Evaluator      policy.Evaluator
	Coordinator    Coordinator
}

// Coordinator coordinates multi-peer compaction (in-process default).
type Coordinator interface {
	RegisterPeer(namespaceID, peerID string)
}

// InProcessCoordinator is a no-op in-process coordinator skeleton.
type InProcessCoordinator struct{}

func (InProcessCoordinator) RegisterPeer(namespaceID, peerID string) {}

// NewEngine returns a default compaction engine.
func NewEngine(cfg Config) Engine {
	if cfg.Evaluator == nil {
		cfg.Evaluator = policy.DefaultEvaluator
	}
	if cfg.Coordinator == nil {
		cfg.Coordinator = InProcessCoordinator{}
	}
	return &defaultEngine{cfg: cfg}
}

type defaultEngine struct {
	cfg Config
}

func (e *defaultEngine) UpdatePeerHeads(namespaceID string, heads map[string]codec.Hash) {}

func (e *defaultEngine) Plan(request Request) (Plan, error) {
	if request.NamespaceID != e.cfg.NamespaceID {
		return Plan{}, errNamespaceMismatch
	}
	pol, err := e.cfg.PolicyRegistry.Get(request.NamespaceID)
	if err != nil {
		return Plan{}, err
	}
	if pol.Compaction.SquashAfter == policy.SquashModeNever {
		return Plan{
			PeerSafe: true,
			Blockers: []Blocker{PolicyDisabled{Reason: "squashAfter=NEVER"}},
		}, nil
	}
	head, err := e.cfg.DAG.Head()
	if err != nil {
		return Plan{}, err
	}
	// Skeleton: full plan requires timestamp collection from DAG walks.
	_ = head
	return Plan{PeerSafe: true}, nil
}

func (e *defaultEngine) RunCycle(request Request) (Result, error) {
	plan, err := e.Plan(request)
	if err != nil {
		return Result{}, err
	}
	if len(plan.Blockers) > 0 {
		return Result{}, nil
	}
	return Result{}, nil
}

var errNamespaceMismatch = &namespaceMismatchError{}

type namespaceMismatchError struct{}

func (e *namespaceMismatchError) Error() string { return "namespace mismatch" }
