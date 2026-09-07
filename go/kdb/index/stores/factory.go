// Package stores wires the concrete index implementations to index.Registry: a StoreFactory
// that dispatches on Descriptor.Type, and Open, which loads a namespace's registry from its
// index directory. It lives outside package index only to avoid an import cycle (fulltext and
// vector import index).
package stores

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/index/fulltext"
	"github.com/limidus/kdb/go/kdb/index/vector"
)

// Options tunes the stores the factory creates.
type Options struct {
	FullText fulltext.Options
	Vector   vector.Options
}

// NewFactory returns a StoreFactory bound to d: HASH and BTREE descriptors get a
// MemoryStore, FULLTEXT a fulltext.Store, VECTOR a vector.Store.
func NewFactory(d *dag.InMemoryCommitDag, opts Options) index.StoreFactory {
	return index.StoreFactoryFunc(func(desc index.Descriptor) (index.Store, error) {
		switch desc.Type {
		case index.IndexTypeHash, index.IndexTypeBTree:
			return index.NewMemoryStore(desc, d), nil
		case index.IndexTypeFullText:
			return fulltext.NewFullTextStore(desc, d, opts.FullText)
		case index.IndexTypeVector:
			return vector.NewVectorStore(desc, d, opts.Vector)
		default:
			return nil, fmt.Errorf("index %s: unsupported type %s", desc.IndexID, desc.Type)
		}
	})
}

// Open loads the registry persisted under dir (see index.LoadRegistry) with the default
// factory. Stores listed in the report as Stale must be rebuilt by the caller with
// Registry.RebuildFromScan before they are searched.
func Open(dir, namespaceID string, d *dag.InMemoryCommitDag, opts Options) (*index.Registry, index.LoadReport, error) {
	return index.LoadRegistry(dir, namespaceID, d, NewFactory(d, opts))
}
