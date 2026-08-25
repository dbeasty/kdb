package embed

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// EmbeddedKdbRuntime composes DAG + storage for embedded use.
type EmbeddedKdbRuntime struct {
	Catalog          string
	DAG              dag.CommitDAG
	Storage          storage.Adapter
	Schema           schema.KdbSchema
	DefaultNamespace string
	WriteBaseVersion *codec.Hash
	// DataRoot is set for file-backed runtimes (empty for pure memory).
	DataRoot string
	release  func()
}

// Close frees any file-runtime resources (e.g. directory lock).
//
// Named Close rather than Release: gomobile's iOS binding generates an Objective-C class per
// exported type, and Release collides with Objective-C ARC's reserved -release selector, which
// breaks the iOS bind entirely (see docs/kdb-spec-layer12-execution-plan.md's Phase 0 spike).
func (r *EmbeddedKdbRuntime) Close() {
	if r == nil {
		return
	}
	if r.release != nil {
		r.release()
		r.release = nil
	}
}
