package embed

import (
	"os"
	"path/filepath"

	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/schema"
	storagemem "github.com/limidus/kdb/go/kdb/storage/mem"
)

// OpenFileRuntime opens an embedded runtime backed by a data directory.
// v1 Go port: ensures namespace layout on disk and uses in-memory DAG/storage
// until the file storage engine is ported from Kotlin.
func OpenFileRuntime(dataRoot, catalog, namespaceID string, sch schema.KdbSchema) (*EmbeddedKdbRuntime, error) {
	if err := ensureNamespaceDirs(dataRoot, namespaceID); err != nil {
		return nil, err
	}
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
		DataRoot:         dataRoot,
	}
	if !sch.IsNone() {
		if err := syncEmbedSchema(rt, namespaceID, sch); err != nil {
			return nil, err
		}
	}
	return rt, nil
}

func ensureNamespaceDirs(dataRoot, namespaceID string) error {
	nsDir := filepath.Join(dataRoot, "ns", namespaceID)
	for _, sub := range []string{"", "delta", "meta"} {
		p := nsDir
		if sub != "" {
			p = filepath.Join(nsDir, sub)
		}
		if err := os.MkdirAll(p, 0o755); err != nil {
			return err
		}
	}
	meta := filepath.Join(nsDir, "meta.json")
	if _, err := os.Stat(meta); os.IsNotExist(err) {
		if err := os.WriteFile(meta, []byte(`{"namespaceId":"`+namespaceID+`"}`), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// CatalogFromNamespace returns the catalog segment of a namespace id (before '/').
func CatalogFromNamespace(namespaceID string) string {
	if i := indexByte(namespaceID, '/'); i >= 0 {
		return namespaceID[:i]
	}
	return namespaceID
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
