package stores_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/index/stores"
)

// benchIndex is an index of one kind with docs documents and writes rewrites already applied,
// writesPerCommit per commit, read at head after every commit (see TestIndexMemoryProfile).
type benchIndex struct {
	d     *dag.InMemoryCommitDag
	s     index.DocumentStore
	ids   []codec.UUID
	head  codec.Hash
	heads []codec.Hash
	rng   *rand.Rand
	kind  index.IndexType
	docs  int
}

func newBenchIndex(b *testing.B, kind index.IndexType, docs, writes int) *benchIndex {
	b.Helper()
	d, _ := dag.NewInMemoryCommitDag("ns/bench")
	s, err := stores.NewFactory(d, memOpts).Create(memDesc(kind))
	if err != nil {
		b.Fatal(err)
	}
	bi := &benchIndex{d: d, s: s.(index.DocumentStore), rng: rand.New(rand.NewSource(1)), kind: kind, docs: docs}
	bi.head, _ = d.Head()
	bi.ids = make([]codec.UUID, docs)
	for i := range bi.ids {
		bi.ids[i] = codec.UUID{MSB: bi.rng.Int63(), LSB: bi.rng.Int63()}
	}
	total := docs + writes
	for i := 0; i < total; i += writesPerCommit {
		h := bi.commit(b)
		for j := i; j < i+writesPerCommit && j < total; j++ {
			doc := bi.ids[j%docs]
			if j >= docs {
				doc = bi.ids[bi.rng.Intn(docs)]
			}
			if err := bi.s.PutDocument(doc, h, memDoc(kind, bi.rng, docs)); err != nil {
				b.Fatal(err)
			}
		}
		if (i/writesPerCommit)%10 == 0 {
			bi.read(nil)
		}
	}
	bi.read(nil)
	return bi
}

func (bi *benchIndex) commit(b *testing.B) codec.Hash {
	tx := document.Transaction{ID: codec.UUID{MSB: bi.rng.Int63(), LSB: 5}, BaseVersion: bi.head, Timestamp: codec.TimestampNow()}
	c, err := bi.d.AppendCommit(tx, bi.head, document.EmptyDocumentTree(), nil, "")
	if err != nil {
		b.Fatal(err)
	}
	bi.head = c.Hash
	bi.heads = append(bi.heads, c.Hash)
	return c.Hash
}

func (bi *benchIndex) read(at *codec.Hash) {
	switch bi.kind {
	case index.IndexTypeHash:
		_, _ = bi.s.Lookup(index.StringKey{Value: fmt.Sprintf("v%d", bi.rng.Intn(bi.docs/10+1))}, at)
	case index.IndexTypeBTree:
		lo := int64(bi.rng.Intn(bi.docs))
		_, _ = bi.s.Range(index.Int64Key{Value: lo}, index.Int64Key{Value: lo + 100}, at, 0, true)
	case index.IndexTypeFullText:
		_, _ = bi.s.Search(memWords[bi.rng.Intn(len(memWords))]+" "+memWords[bi.rng.Intn(len(memWords))], at, 10)
	default:
		q := make([]float32, memVecDims)
		for i := range q {
			q[i] = float32(bi.rng.Float64()*2 - 1)
		}
		_, _ = bi.s.NearestNeighbours(q, 10, at)
	}
}

type benchShape struct {
	kind         index.IndexType
	docs, writes int
}

var benchShapes = []benchShape{
	{index.IndexTypeHash, 50_000, 100_000},
	{index.IndexTypeBTree, 50_000, 100_000},
	{index.IndexTypeFullText, 2_000, 5_000},
	{index.IndexTypeVector, 1_000, 3_000},
}

// BenchmarkIndexHeadRead: a read at head with nothing written since the last one.
func BenchmarkIndexHeadRead(b *testing.B) {
	for _, sh := range benchShapes {
		b.Run(sh.kind.String(), func(b *testing.B) {
			bi := newBenchIndex(b, sh.kind, sh.docs, sh.writes)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bi.read(nil)
			}
		})
	}
}

// BenchmarkIndexRecentRead: a read pinned at the commit before head (a session read head one
// commit behind).
func BenchmarkIndexRecentRead(b *testing.B) {
	for _, sh := range benchShapes {
		b.Run(sh.kind.String(), func(b *testing.B) {
			bi := newBenchIndex(b, sh.kind, sh.docs, sh.writes)
			at := bi.heads[len(bi.heads)-2]
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bi.read(&at)
			}
		})
	}
}

// BenchmarkIndexWriteThenRead: one commit of writesPerCommit rewrites followed by a head read -
// the write throughput of an indexed namespace that is also being queried.
func BenchmarkIndexWriteThenRead(b *testing.B) {
	for _, sh := range benchShapes {
		b.Run(sh.kind.String(), func(b *testing.B) {
			bi := newBenchIndex(b, sh.kind, sh.docs, sh.writes)
			texts := make([]string, 4096)
			for i := range texts {
				texts[i] = memDoc(sh.kind, bi.rng, sh.docs)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h := bi.commit(b)
				for j := 0; j < writesPerCommit; j++ {
					if err := bi.s.PutDocument(bi.ids[bi.rng.Intn(sh.docs)], h, texts[(i*writesPerCommit+j)%len(texts)]); err != nil {
						b.Fatal(err)
					}
				}
				bi.read(nil)
			}
		})
	}
}
