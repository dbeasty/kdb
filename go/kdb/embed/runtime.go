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

// Release frees any file-runtime resources (e.g. directory lock).
func (r *EmbeddedKdbRuntime) Release() {
	if r == nil {
		return
	}
	if r.release != nil {
		r.release()
		r.release = nil
	}
}
