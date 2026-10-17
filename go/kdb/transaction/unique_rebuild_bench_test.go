package transaction_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// BenchmarkRegistryRebuild is the shape of #316: one namespace of many logical collections,
// thirteen single-field unique constraints on synthetic _u_* fields of which each document
// carries at most one, and a share of the documents holding a ~6 KB packed embedding string.
// Rebuild runs at every open, so its cost is a restart's outage.
func BenchmarkRegistryRebuild(b *testing.B) {
	const docs, withEmbedding, constraints = 20_000, 4_000, 13
	fields := make([]schema.Field, 0, constraints)
	for c := 0; c < constraints; c++ {
		fields = append(fields, schema.MustField(fmt.Sprintf("_u_%d", c), schema.StringType{}, false, true, true))
	}
	sch, err := schema.Build(fields, 1, codec.Timestamp{}, "bench")
	if err != nil {
		b.Fatal(err)
	}
	store := mem.NewInMemoryStorageAdapter()
	embedding := strings.Repeat("Ab3+", 1500)
	for i := 0; i < docs; i++ {
		id, _ := codec.RandomUUID()
		body := fmt.Sprintf(`{"_c":"c%d","status":"pending","createdAt":%d,"title":"document number %d"`, i%constraints, i, i)
		if i%4 == 0 {
			body += fmt.Sprintf(`,"_u_%d":"key-%d"`, i%constraints, i)
		}
		if i < withEmbedding {
			body += `,"embedding":"` + embedding + `"`
		}
		body += `}`
		if err := store.PutDocument(uniqueNS, document.Document{ID: id, JSON: body}); err != nil {
			b.Fatal(err)
		}
	}
	tree, err := store.CommitTree(uniqueNS, codec.Hash{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := transaction.NewUniqueKeyRegistry()
		if err := r.Rebuild(uniqueNS, store, tree.TreeHash, sch); err != nil {
			b.Fatal(err)
		}
		if r.Len() != docs/4 {
			b.Fatalf("claimed %d keys, want %d", r.Len(), docs/4)
		}
	}
}
