package vector_test

import (
	"encoding/json"
	"errors"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/index/vector"
)

const fixturePath = "../../../testdata/golden/search/vector_corpus.json"

type corpusFile struct {
	Dimensions int `json:"dimensions"`
	Documents  []struct {
		ID     string    `json:"id"`
		Vector []float32 `json:"vector"`
	} `json:"documents"`
	Queries []struct {
		Metric   string              `json:"metric"`
		Vector   []float32           `json:"vector"`
		K        int                 `json:"k"`
		Expected [][]json.RawMessage `json:"expected"`
	} `json:"queries"`
}

func mustUUID(t *testing.T, s string) codec.UUID {
	t.Helper()
	id, err := codec.UUIDFromString(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func newStore(t *testing.T, d *dag.InMemoryCommitDag, dims int, metric string, opts vector.Options) *vector.Store {
	t.Helper()
	desc := index.Descriptor{
		IndexID: mustUUID(t, "00000000-0000-4000-8000-000000009002"), FieldName: "embedding",
		Fields: []string{"embedding"}, Type: index.IndexTypeVector,
		Options: map[string]string{index.OptionDimensions: itoa(dims), index.OptionMetric: metric},
	}
	s, err := vector.NewVectorStore(desc, d, opts)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func itoa(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}

// TestVectorGoldenExactQueries pins exact search for all three metrics - order and scores -
// to the fixture the Kotlin tree asserts against (tolerance 1e-5, §7).
func TestVectorGoldenExactQueries(t *testing.T) {
	raw, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var c corpusFile
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	if len(c.Queries) < 3 {
		t.Fatalf("fixture has %d queries", len(c.Queries))
	}
	d, err := dag.NewInMemoryCommitDag("fixtures/vectors")
	if err != nil {
		t.Fatal(err)
	}
	head, _ := d.Head()
	stores := map[string]*vector.Store{}
	for _, q := range c.Queries {
		if stores[q.Metric] != nil {
			continue
		}
		s := newStore(t, d, c.Dimensions, q.Metric, vector.Options{})
		for _, doc := range c.Documents {
			if err := s.Put(index.Entry{DocID: mustUUID(t, doc.ID), Key: index.NewVectorKey(doc.Vector), CommitHash: head}); err != nil {
				t.Fatal(err)
			}
		}
		stores[q.Metric] = s
	}
	for qi, q := range c.Queries {
		t.Run(q.Metric+"/"+itoa(qi), func(t *testing.T) {
			got, err := stores[q.Metric].NearestNeighbours(q.Vector, q.K, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(q.Expected) {
				t.Fatalf("got %d hits, want %d", len(got), len(q.Expected))
			}
			for i, row := range q.Expected {
				var idStr string
				if err := json.Unmarshal(row[0], &idStr); err != nil {
					t.Fatal(err)
				}
				var score float64
				if err := json.Unmarshal(row[1], &score); err != nil {
					t.Fatal(err)
				}
				if got[i].DocID != mustUUID(t, idStr) {
					t.Fatalf("position %d: got %s, want %s", i, got[i].DocID, idStr)
				}
				if math.Abs(float64(got[i].Score)-score) > 1e-5 {
					t.Errorf("position %d: score %v, want %v", i, got[i].Score, score)
				}
			}
		})
	}
}

// TestMetricFormulas guards the three similarity definitions (§7), including the zero-norm
// cosine case that would otherwise be NaN and reorder every result.
func TestMetricFormulas(t *testing.T) {
	a := []float32{1, 0}
	b := []float32{0, 1}
	same := []float32{1, 0}
	zero := []float32{0, 0}
	if got := vector.Score(vector.Cosine, a, same); math.Abs(float64(got)-1) > 1e-6 {
		t.Errorf("cosine of a vector with itself = %v, want 1", got)
	}
	if got := vector.Score(vector.Cosine, a, b); math.Abs(float64(got)) > 1e-6 {
		t.Errorf("cosine of orthogonal vectors = %v, want 0", got)
	}
	if got := vector.Score(vector.Cosine, a, zero); got != 0 {
		t.Errorf("cosine against the zero vector = %v, want 0", got)
	}
	// l2: ‖a − b‖ = sqrt(2), so score = 1/(1+sqrt2) = 0.41421356
	if got := vector.Score(vector.L2, a, b); math.Abs(float64(got)-1/(1+math.Sqrt2)) > 1e-6 {
		t.Errorf("l2 = %v, want %v", got, 1/(1+math.Sqrt2))
	}
	if got := vector.Score(vector.L2, a, same); math.Abs(float64(got)-1) > 1e-6 {
		t.Errorf("l2 of identical vectors = %v, want 1", got)
	}
	if got := vector.Score(vector.InnerProduct, []float32{2, 3}, []float32{4, 5}); math.Abs(float64(got)-23) > 1e-6 {
		t.Errorf("inner product = %v, want 23", got)
	}
}

// TestDimensionMismatchIsATypedSchemaViolation: a wrong-length vector must be a typed error
// the commit path can recognise (§7, §10), not a generic failure or a silent skip.
func TestDimensionMismatchIsATypedSchemaViolation(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/dims")
	store := newStore(t, d, 4, "cosine", vector.Options{})
	_, err := store.PrepareDocument(mustUUID(t, "00000000-0000-4000-8000-000000000001"), `{"embedding":[1,2,3]}`)
	if err == nil {
		t.Fatal("expected an error for a 3-element vector in a 4-dimensional index")
	}
	var mismatch *index.DimensionMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("error %v (%T) is not a *index.DimensionMismatchError", err, err)
	}
	if mismatch.Expected != 4 || mismatch.Actual != 3 {
		t.Errorf("mismatch = expected %d actual %d, want 4 and 3", mismatch.Expected, mismatch.Actual)
	}
	var ex kdberr.Exception
	if !errors.As(err, &ex) || ex.Code() != kdberr.SchemaViolation {
		t.Errorf("the error must carry the schema-violation code so the commit is rejected: %v", err)
	}
	// A query vector of the wrong length is the same class of error.
	if _, err := store.NearestNeighbours([]float32{1, 2}, 5, nil); !errors.As(err, &mismatch) {
		t.Errorf("a wrong-length query vector must report a dimension mismatch: %v", err)
	}
}

// TestPrepareDocumentDoesNotMutateOnFailure is the §10 guarantee: validation happens before
// anything is applied, so a rejected document leaves the index exactly as it was.
func TestPrepareDocumentDoesNotMutateOnFailure(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/atomic")
	head, _ := d.Head()
	store := newStore(t, d, 3, "cosine", vector.Options{})
	good := mustUUID(t, "00000000-0000-4000-8000-000000000001")
	if err := store.PutDocument(good, head, `{"embedding":[1,0,0]}`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PrepareDocument(mustUUID(t, "00000000-0000-4000-8000-000000000002"), `{"embedding":[1,0]}`); err == nil {
		t.Fatal("expected a dimension mismatch")
	}
	n, err := store.LiveCount(nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("live vectors = %d, want 1: the rejected document must not have been applied", n)
	}
}

// TestMissingVectorFieldRemovesTheDocument: a document that no longer carries the indexed
// path applies as a delete rather than keeping its stale vector.
func TestMissingVectorFieldRemovesTheDocument(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/missing")
	genesis, _ := d.Head()
	store := newStore(t, d, 2, "cosine", vector.Options{})
	docID := mustUUID(t, "00000000-0000-4000-8000-000000000001")
	if err := store.PutDocument(docID, genesis, `{"embedding":[1,0]}`); err != nil {
		t.Fatal(err)
	}
	second := appendCommit(t, d, genesis)
	if err := store.PutDocument(docID, second, `{"title":"no vector any more"}`); err != nil {
		t.Fatal(err)
	}
	if n, _ := store.LiveCount(nil); n != 0 {
		t.Errorf("live vectors at head = %d, want 0", n)
	}
	if n, _ := store.LiveCount(&genesis); n != 1 {
		t.Errorf("live vectors as of genesis = %d, want 1", n)
	}
}

// TestTombstonesHonourAtCommit: a delete hides the vector at head while an as-of read before
// the tombstone still returns it (§7).
func TestTombstonesHonourAtCommit(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/tombstone")
	genesis, _ := d.Head()
	store := newStore(t, d, 2, "cosine", vector.Options{})
	docID := mustUUID(t, "00000000-0000-4000-8000-000000000001")
	if err := store.PutDocument(docID, genesis, `{"embedding":[1,0]}`); err != nil {
		t.Fatal(err)
	}
	second := appendCommit(t, d, genesis)
	if err := store.Delete(docID, second); err != nil {
		t.Fatal(err)
	}
	if hits, _ := store.NearestNeighbours([]float32{1, 0}, 5, nil); len(hits) != 0 {
		t.Errorf("at head the vector is gone: %v", hits)
	}
	if hits, _ := store.NearestNeighbours([]float32{1, 0}, 5, &genesis); len(hits) != 1 {
		t.Errorf("as of genesis the vector is still there: %v", hits)
	}
}

// TestExactSearchOrdersTiesByDocID: equal scores resolve by document id ascending, so two
// identical vectors always come back in the same order.
func TestExactSearchOrdersTiesByDocID(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/ties")
	head, _ := d.Head()
	store := newStore(t, d, 2, "cosine", vector.Options{})
	ids := []string{
		"00000000-0000-4000-8000-000000000003",
		"00000000-0000-4000-8000-000000000001",
		"00000000-0000-4000-8000-000000000002",
	}
	for _, s := range ids {
		if err := store.PutDocument(mustUUID(t, s), head, `{"embedding":[1,1]}`); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 10; i++ {
		hits, err := store.NearestNeighbours([]float32{1, 1}, 3, nil)
		if err != nil {
			t.Fatal(err)
		}
		for j, want := range []string{
			"00000000-0000-4000-8000-000000000001",
			"00000000-0000-4000-8000-000000000002",
			"00000000-0000-4000-8000-000000000003",
		} {
			if hits[j].DocID.String() != want {
				t.Fatalf("run %d position %d: got %s, want %s", i, j, hits[j].DocID, want)
			}
		}
	}
}

// TestHNSWRecallAgainstExactOracle is the §7 acceptance gate: with the graph forced on, the
// approximate search must find at least 95% of the true top 10 over 2 000 random 32-d
// vectors, measured against the brute-force oracle on the same store.
func TestHNSWRecallAgainstExactOracle(t *testing.T) {
	const (
		docs    = 2000
		dims    = 32
		k       = 10
		queries = 50
	)
	d, _ := dag.NewInMemoryCommitDag("ns/recall")
	head, _ := d.Head()
	// A negative threshold forces HNSW at every size, so this exercises the graph rather
	// than falling back to the exact path the oracle uses.
	store := newStore(t, d, dims, "cosine", vector.Options{ExactThreshold: -1})
	rng := rand.New(rand.NewSource(20260905))
	for i := 0; i < docs; i++ {
		vec := make([]float32, dims)
		for j := range vec {
			vec[j] = float32(rng.NormFloat64())
		}
		id, err := codec.UUIDFromBytes(idBytes(i))
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Put(index.Entry{DocID: id, Key: index.NewVectorKey(vec), CommitHash: head}); err != nil {
			t.Fatal(err)
		}
	}
	found, total := 0, 0
	for q := 0; q < queries; q++ {
		query := make([]float32, dims)
		for j := range query {
			query[j] = float32(rng.NormFloat64())
		}
		approx, err := store.NearestNeighbours(query, k, nil)
		if err != nil {
			t.Fatal(err)
		}
		exact, err := store.ExactNearestNeighbours(query, k, nil)
		if err != nil {
			t.Fatal(err)
		}
		truth := make(map[codec.UUID]struct{}, len(exact))
		for _, r := range exact {
			truth[r.DocID] = struct{}{}
		}
		for _, r := range approx {
			if _, ok := truth[r.DocID]; ok {
				found++
			}
		}
		total += len(exact)
	}
	recall := float64(found) / float64(total)
	if recall < 0.95 {
		t.Fatalf("HNSW recall@%d = %.4f over %d queries, want at least 0.95", k, recall, queries)
	}
	t.Logf("HNSW recall@%d = %.4f", k, recall)
}

// idBytes builds a deterministic 16-byte id for the recall corpus.
func idBytes(i int) []byte {
	b := make([]byte, 16)
	b[0] = byte(i >> 24)
	b[1] = byte(i >> 16)
	b[2] = byte(i >> 8)
	b[3] = byte(i)
	b[6] = 0x40
	b[8] = 0x80
	return b
}

// TestExactThresholdSwitchesToTheGraph: below the threshold the store answers exactly, above
// it it uses the graph - and on a small corpus the graph still returns the exact answer, so
// the switch itself is observable only through the configuration.
func TestExactThresholdSwitchesToTheGraph(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/threshold")
	head, _ := d.Head()
	store := newStore(t, d, 4, "l2", vector.Options{ExactThreshold: 2})
	rng := rand.New(rand.NewSource(7))
	for i := 1; i <= 12; i++ {
		vec := []float32{float32(rng.Float64()), float32(rng.Float64()), float32(rng.Float64()), float32(rng.Float64())}
		id, _ := codec.UUIDFromBytes(idBytes(i))
		if err := store.Put(index.Entry{DocID: id, Key: index.NewVectorKey(vec), CommitHash: head}); err != nil {
			t.Fatal(err)
		}
	}
	query := []float32{0.5, 0.5, 0.5, 0.5}
	approx, err := store.NearestNeighbours(query, 5, nil)
	if err != nil {
		t.Fatal(err)
	}
	exact, err := store.ExactNearestNeighbours(query, 5, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(approx) != len(exact) {
		t.Fatalf("graph returned %d hits, oracle %d", len(approx), len(exact))
	}
	for i := range exact {
		if approx[i].DocID != exact[i].DocID {
			t.Errorf("position %d: graph %s, oracle %s", i, approx[i].DocID, exact[i].DocID)
		}
	}
}

// TestSnapshotRoundTripRebuildsTheGraph: the graph is not persisted (§7), so a restored store
// must rebuild it and answer identically, history included.
func TestSnapshotRoundTripRebuildsTheGraph(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/vsnapshot")
	genesis, _ := d.Head()
	store := newStore(t, d, 3, "cosine", vector.Options{})
	docA := mustUUID(t, "00000000-0000-4000-8000-000000000001")
	docB := mustUUID(t, "00000000-0000-4000-8000-000000000002")
	if err := store.PutDocument(docA, genesis, `{"embedding":[1,0,0]}`); err != nil {
		t.Fatal(err)
	}
	second := appendCommit(t, d, genesis)
	if err := store.PutDocument(docB, second, `{"embedding":[0,1,0]}`); err != nil {
		t.Fatal(err)
	}
	third := appendCommit(t, d, second)
	if err := store.Delete(docA, third); err != nil {
		t.Fatal(err)
	}

	snap, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := newStore(t, d, 3, "cosine", vector.Options{})
	if err := restored.RestoreSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	for _, cutoff := range []*codec.Hash{nil, &genesis, &second} {
		want, _ := store.NearestNeighbours([]float32{1, 0, 0}, 5, cutoff)
		got, _ := restored.NearestNeighbours([]float32{1, 0, 0}, 5, cutoff)
		if len(got) != len(want) {
			t.Fatalf("cutoff %v: got %d hits, want %d", cutoff, len(got), len(want))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("cutoff %v position %d: got %+v, want %+v", cutoff, i, got[i], want[i])
			}
		}
	}
}

// TestSaveAndLoadFromDir round-trips through the filesystem with the manifest naming the head.
func TestSaveAndLoadFromDir(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/vpersist")
	head, _ := d.Head()
	store := newStore(t, d, 2, "inner_product", vector.Options{})
	if err := store.PutDocument(mustUUID(t, "00000000-0000-4000-8000-000000000001"), head, `{"embedding":[2,3]}`); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "vec")
	if err := store.SaveToDir(dir, head); err != nil {
		t.Fatal(err)
	}
	restored := newStore(t, d, 2, "inner_product", vector.Options{})
	m, err := restored.LoadFromDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if m.HeadCommitHex != head.Hex() {
		t.Errorf("manifest head = %s, want %s", m.HeadCommitHex, head.Hex())
	}
	hits, err := restored.NearestNeighbours([]float32{4, 5}, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || math.Abs(float64(hits[0].Score)-23) > 1e-6 {
		t.Fatalf("restored search gave %v, want one hit scoring 23", hits)
	}
}

// TestDescriptorOptionsAreValidated: dimensions is required and must be positive, and an
// unknown metric is rejected at construction rather than silently defaulting.
func TestDescriptorOptionsAreValidated(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/opts")
	base := index.Descriptor{FieldName: "embedding", Fields: []string{"embedding"}, Type: index.IndexTypeVector}
	for name, opts := range map[string]map[string]string{
		"no dimensions":     {},
		"zero dimensions":   {index.OptionDimensions: "0"},
		"negative":          {index.OptionDimensions: "-4"},
		"unparsable":        {index.OptionDimensions: "many"},
		"unknown metric":    {index.OptionDimensions: "4", index.OptionMetric: "manhattan"},
		"m below the floor": {index.OptionDimensions: "4", index.OptionM: "1"},
	} {
		desc := base
		desc.Options = opts
		if _, err := vector.NewVectorStore(desc, d, vector.Options{}); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func appendCommit(t *testing.T, d *dag.InMemoryCommitDag, parent codec.Hash) codec.Hash {
	t.Helper()
	txID, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	tx := document.Transaction{ID: txID, BaseVersion: parent, Timestamp: codec.TimestampNow(), AuthorNodeID: author}
	c, err := d.AppendCommit(tx, parent, document.EmptyDocumentTree(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	return c.Hash
}
