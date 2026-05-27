package embed

import (
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/schema"
	storagemem "github.com/limidus/kdb/go/kdb/storage/mem"
)

// OpenMemoryRuntime opens an in-memory embedded runtime (DAG + storage).
func OpenMemoryRuntime(catalog, namespaceID string, sch schema.KdbSchema) (*EmbeddedKdbRuntime, error) {
	d, err := dag.NewInMemoryCommitDag(namespaceID)
	if err != nil {
		return nil, err
	}
	s := storagemem.NewInMemoryStorageAdapter()
	rt := &EmbeddedKdbRuntime{
		Catalog:          catalog,
		DAG:              d,
		Storage:          s,
		Schema:           sch,
		DefaultNamespace: namespaceID,
	}
	if !sch.IsNone() {
		if err := syncEmbedSchema(rt, namespaceID, sch); err != nil {
			return nil, err
		}
	}
	return rt, nil
}

// syncEmbedSchema is a minimal hook for schema registration (index layer not wired in Go yet).
func syncEmbedSchema(rt *EmbeddedKdbRuntime, namespaceID string, sch schema.KdbSchema) error {
	if sch.IsNone() {
		return nil
	}
	_ = namespaceID
	_ = rt
	return nil
}
