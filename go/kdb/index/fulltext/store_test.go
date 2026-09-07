package fulltext_test

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/index/fulltext"
)

const fixturePath = "../../../testdata/golden/search/bm25_corpus.json"

type corpusFile struct {
	Index struct {
		Fields []struct {
			Path   string  `json:"path"`
			Weight float64 `json:"weight"`
		} `json:"fields"`
	} `json:"index"`
	Documents []struct {
		ID   string `json:"id"`
		JSON string `json:"json"`
	} `json:"documents"`
	Queries []struct {
		Query    string              `json:"query"`
		Expected [][]json.RawMessage `json:"expected"`
	} `json:"queries"`
}

func loadCorpus(t *testing.T) corpusFile {
	t.Helper()
	raw, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var c corpusFile
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

// corpusStore builds the fixture's index and loads every fixture document at head.
func corpusStore(t *testing.T, c corpusFile) (*fulltext.Store, *dag.InMemoryCommitDag, codec.Hash) {
	t.Helper()
	d, err := dag.NewInMemoryCommitDag("fixtures/tasks")
	if err != nil {
		t.Fatal(err)
	}
	head, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	var fields []string
	var weights []string
	for _, f := range c.Index.Fields {
		fields = append(fields, f.Path)
		if f.Weight != 1 {
			weights = append(weights, f.Path+"="+trimFloat(f.Weight))
		}
	}
	desc := index.Descriptor{
		IndexID: mustUUID(t, "00000000-0000-4000-8000-000000009001"), NamespaceID: "fixtures/tasks",
		FieldName: fields[0], Fields: fields, Type: index.IndexTypeFullText,
		Options: map[string]string{index.OptionIndexName: "tasks_text", index.OptionWeights: join(weights)},
	}
	store, err := fulltext.NewFullTextStore(desc, d, fulltext.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, doc := range c.Documents {
		if err := store.PutDocument(mustUUID(t, doc.ID), head, doc.JSON); err != nil {
			t.Fatal(err)
		}
	}
	return store, d, head
}

func join(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ","
		}
		out += p
	}
	return out
}

func trimFloat(f float64) string {
	b, _ := json.Marshal(f)
	return string(b)
}

func mustUUID(t *testing.T, s string) codec.UUID {
	t.Helper()
	id, err := codec.UUIDFromString(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func expectRanked(t *testing.T, rows [][]json.RawMessage) []index.RankedResult {
	t.Helper()
	out := make([]index.RankedResult, 0, len(rows))
	for _, row := range rows {
		var idStr string
		if err := json.Unmarshal(row[0], &idStr); err != nil {
			t.Fatal(err)
		}
		var score float64
		if err := json.Unmarshal(row[1], &score); err != nil {
			t.Fatal(err)
		}
		out = append(out, index.RankedResult{DocID: mustUUID(t, idStr), Score: float32(score)})
	}
	return out
}

// TestBM25GoldenQueries pins rank order and scores for every fixture query - including the
// phrase query and the stopword-only query that must return nothing - to the file the Kotlin
// tree asserts against. Scores agree to a relative 1e-4 (§6.4).
func TestBM25GoldenQueries(t *testing.T) {
	c := loadCorpus(t)
	if len(c.Queries) < 8 {
		t.Fatalf("fixture has %d queries, spec requires at least 8", len(c.Queries))
	}
	store, _, _ := corpusStore(t, c)
	for _, q := range c.Queries {
		t.Run(q.Query, func(t *testing.T) {
			got, err := store.Search(q.Query, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			want := expectRanked(t, q.Expected)
			if len(got) != len(want) {
				t.Fatalf("got %d hits, want %d:\n got %v\n want %v", len(got), len(want), got, want)
			}
			for i := range got {
				if got[i].DocID != want[i].DocID {
					t.Fatalf("position %d: got %s, want %s", i, got[i].DocID, want[i].DocID)
				}
				if rel := math.Abs(float64(got[i].Score-want[i].Score)) / math.Max(1e-12, math.Abs(float64(want[i].Score))); rel > 1e-4 {
					t.Errorf("position %d (%s): score %v, want %v", i, got[i].DocID, got[i].Score, want[i].Score)
				}
			}
		})
	}
}

// TestBM25ScoreMatchesHandComputation derives a fixture query by hand from the spec formula
// rather than from the implementation, so a change that broke scoring and regenerated the
// fixture to match would still fail here.
//
// Query "database" over the fixture corpus. The analyzer stems it to "databas". Two documents
// contain it: doc 4 (title "Staging database migration" and tags ["database","ops","staging"])
// and doc 7 (description "The reporting dashboard times out; profile the database queries" and
// tags ["database","performance"]). So N = 12, n_t = 2, and
//
//	idf = ln(1 + (12 − 2 + 0.5)/(2 + 0.5)) = ln(1 + 4.2) = ln 5.2 = 1.64865863
//
// Field lengths (tokens after stopword removal) total 36 for title, 87 for description and 20
// for tags over the 12 documents, so avglen_title = 3, avglen_desc = 7.25, avglen_tags = 5/3.
// With k1 = 1.2, b = 0.75 and tfnorm = tf·2.2 / (tf + 1.2·(0.25 + 0.75·len/avglen)):
//
//	doc 4, title (w 3):  len 3, tf 1 → 2.2 / (1 + 1.2·(0.25 + 0.75·3/3))       = 2.2/2.2   = 1.0
//	                     contribution 3 · 1.64865863 · 1.0                                 = 4.94597588
//	doc 4, tags  (w 2):  len 3, tf 1 → 2.2 / (1 + 1.2·(0.25 + 0.75·3/(5/3)))   = 2.2/2.92  = 0.75342466
//	                     contribution 2 · 1.64865863 · 0.75342466                          = 2.48428012
//	doc 4 total                                                                            = 7.43025600
//
//	doc 7, desc  (w 1):  len 7, tf 1 → 2.2 / (1 + 1.2·(0.25 + 0.75·7/7.25))    = 2.2/2.16897 = 1.01430802
//	                     contribution 1 · 1.64865863 · 1.01430802                           = 1.67225109
//	doc 7, tags  (w 2):  len 2, tf 1 → 2.2 / (1 + 1.2·(0.25 + 0.75·2/(5/3)))   = 2.2/2.38  = 0.92436975
//	                     contribution 2 · 1.64865863 · 0.92436975                           = 3.04793756
//	doc 7 total                                                                            = 4.72018865
//
// doc 4 outranks doc 7 despite both matching once per field, because its match is in the
// weight-3 title at exactly the average title length.
func TestBM25ScoreMatchesHandComputation(t *testing.T) {
	c := loadCorpus(t)
	store, _, _ := corpusStore(t, c)
	got, err := store.Search("database", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		id    string
		score float64
	}{
		{"00000000-0000-4000-8000-000000000004", 7.43025600},
		{"00000000-0000-4000-8000-000000000007", 4.72018865},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d hits, want %d: %v", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i].DocID != mustUUID(t, w.id) {
			t.Fatalf("position %d: got %s, want %s", i, got[i].DocID, w.id)
		}
		if math.Abs(float64(got[i].Score)-w.score) > 1e-5 {
			t.Errorf("position %d: score %v, want the hand-derived %v", i, got[i].Score, w.score)
		}
	}
}

// TestStatsReportNAndAvgLen: N counts documents with at least one indexed token, and avglen is
// per field - the two corpus statistics every score depends on.
func TestBM25StatsReportNAndAvgLen(t *testing.T) {
	c := loadCorpus(t)
	store, _, _ := corpusStore(t, c)
	st, err := store.Stats(nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.N != 12 {
		t.Errorf("N = %d, want 12", st.N)
	}
	for field, want := range map[string]float64{"title": 3, "description": 87.0 / 12, "tags": 20.0 / 12} {
		if math.Abs(st.AvgLen[field]-want) > 1e-9 {
			t.Errorf("avglen[%s] = %v, want %v", field, st.AvgLen[field], want)
		}
	}
}

// TestPhraseQueryRequiresContiguousTerms: a quoted phrase restricts hits to documents where
// the analyzed terms are adjacent in one field, while the same words unquoted match under OR.
func TestPhraseQueryRequiresContiguousTerms(t *testing.T) {
	c := loadCorpus(t)
	store, _, _ := corpusStore(t, c)
	phrase, err := store.Search(`"deploy staging"`, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(phrase) != 1 || phrase[0].DocID != mustUUID(t, "00000000-0000-4000-8000-000000000001") {
		t.Fatalf(`"deploy staging" matched %v, want only document 1`, phrase)
	}
	loose, err := store.Search("deploy staging", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(loose) <= len(phrase) {
		t.Fatalf("the unquoted query matched %d documents, want more than the phrase's %d", len(loose), len(phrase))
	}
}

// TestPhraseIgnoresStopwordsButNotOrder: positions skip stopwords, so "the deploy" matches a
// document whose text is "the deploy of the staging"; reversing the phrase does not match.
func TestPhraseIgnoresStopwordsButNotOrder(t *testing.T) {
	d, err := dag.NewInMemoryCommitDag("ns/phrase")
	if err != nil {
		t.Fatal(err)
	}
	head, _ := d.Head()
	store := newStore(t, d, []string{"body"}, "")
	docID := mustUUID(t, "00000000-0000-4000-8000-000000000001")
	if err := store.PutDocument(docID, head, `{"body":"the deploy of the staging cluster"}`); err != nil {
		t.Fatal(err)
	}
	hits, err := store.Search(`"deploy staging"`, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 {
		t.Fatalf(`"deploy staging" over "the deploy of the staging cluster" gave %v, want one hit`, hits)
	}
	rev, err := store.Search(`"staging deploy"`, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rev) != 0 {
		t.Fatalf("the reversed phrase must not match: %v", rev)
	}
}

// TestArrayElementsDoNotFormPhrases: an array field contributes every element, but a position
// gap between elements stops a phrase spanning two of them.
func TestArrayElementsDoNotFormPhrases(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/array")
	head, _ := d.Head()
	store := newStore(t, d, []string{"tags"}, "")
	docID := mustUUID(t, "00000000-0000-4000-8000-000000000001")
	if err := store.PutDocument(docID, head, `{"tags":["deploy","staging"]}`); err != nil {
		t.Fatal(err)
	}
	if hits, err := store.Search("deploy", nil, 0); err != nil || len(hits) != 1 {
		t.Fatalf("each element is indexed: hits=%v err=%v", hits, err)
	}
	hits, err := store.Search(`"deploy staging"`, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 0 {
		t.Fatalf("a phrase must not span two array elements: %v", hits)
	}
}

// TestSearchAtCommitFiltersByAncestry: a put is visible only at cutoffs descended from its
// commit, and a delete is a tombstone, so an as-of read before it still sees the document.
func TestSearchAtCommitFiltersByAncestry(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/ancestry")
	genesis, _ := d.Head()
	store := newStore(t, d, []string{"body"}, "")
	docA := mustUUID(t, "00000000-0000-4000-8000-000000000001")
	docB := mustUUID(t, "00000000-0000-4000-8000-000000000002")
	if err := store.PutDocument(docA, genesis, `{"body":"alpha deploy"}`); err != nil {
		t.Fatal(err)
	}

	second := appendCommit(t, d, genesis)
	if err := store.PutDocument(docB, second, `{"body":"beta deploy"}`); err != nil {
		t.Fatal(err)
	}

	if hits, _ := store.Search("deploy", &genesis, 0); len(hits) != 1 || hits[0].DocID != docA {
		t.Fatalf("as of genesis: got %v, want only docA", hits)
	}
	if hits, _ := store.Search("deploy", nil, 0); len(hits) != 2 {
		t.Fatalf("at head: got %d hits, want 2", len(hits))
	}

	third := appendCommit(t, d, second)
	if err := store.Delete(docA, third); err != nil {
		t.Fatal(err)
	}
	if hits, _ := store.Search("deploy", nil, 0); len(hits) != 1 || hits[0].DocID != docB {
		t.Fatalf("after the tombstone: got %v, want only docB", hits)
	}
	if hits, _ := store.Search("deploy", &second, 0); len(hits) != 2 {
		t.Fatalf("a read before the tombstone still sees docA: got %v", hits)
	}
}

// TestReindexReplacesTheOldVersion: putting a document again supersedes its previous text at
// the new commit, and the corpus statistics follow.
func TestReindexReplacesTheOldVersion(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/reindex")
	genesis, _ := d.Head()
	store := newStore(t, d, []string{"body"}, "")
	docID := mustUUID(t, "00000000-0000-4000-8000-000000000001")
	if err := store.PutDocument(docID, genesis, `{"body":"alpha"}`); err != nil {
		t.Fatal(err)
	}
	second := appendCommit(t, d, genesis)
	if err := store.PutDocument(docID, second, `{"body":"beta"}`); err != nil {
		t.Fatal(err)
	}
	if hits, _ := store.Search("alpha", nil, 0); len(hits) != 0 {
		t.Errorf("the superseded text must not match at head: %v", hits)
	}
	if hits, _ := store.Search("beta", nil, 0); len(hits) != 1 {
		t.Errorf("the new text matches at head: %v", hits)
	}
	if hits, _ := store.Search("alpha", &genesis, 0); len(hits) != 1 {
		t.Errorf("an as-of read still sees the old text: %v", hits)
	}
	if st, _ := store.Stats(nil); st.N != 1 {
		t.Errorf("N = %d, want 1: a reindexed document is still one document", st.N)
	}
}

// TestSnapshotRoundTripPreservesHistory: a restored index answers head and as-of queries
// exactly as the original, because the snapshot carries the whole event history (§6.5).
func TestSnapshotRoundTripPreservesHistory(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/snapshot")
	genesis, _ := d.Head()
	store := newStore(t, d, []string{"title", "body"}, "title=3")
	docA := mustUUID(t, "00000000-0000-4000-8000-000000000001")
	docB := mustUUID(t, "00000000-0000-4000-8000-000000000002")
	if err := store.PutDocument(docA, genesis, `{"title":"deploy staging","body":"alpha"}`); err != nil {
		t.Fatal(err)
	}
	second := appendCommit(t, d, genesis)
	if err := store.PutDocument(docB, second, `{"title":"other","body":"deploy beta"}`); err != nil {
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
	restored := newStore(t, d, []string{"title", "body"}, "title=3")
	if err := restored.RestoreSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	for _, cutoff := range []*codec.Hash{nil, &genesis, &second} {
		want, err := store.Search("deploy", cutoff, 0)
		if err != nil {
			t.Fatal(err)
		}
		got, err := restored.Search("deploy", cutoff, 0)
		if err != nil {
			t.Fatal(err)
		}
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

// TestSaveAndLoadFromDirDetectStaleness: a snapshot's manifest names the head it was taken
// at, which is how open decides between restoring and rebuilding by scan (§6.5).
func TestSaveAndLoadFromDirDetectStaleness(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/persist")
	genesis, _ := d.Head()
	store := newStore(t, d, []string{"body"}, "")
	docID := mustUUID(t, "00000000-0000-4000-8000-000000000001")
	if err := store.PutDocument(docID, genesis, `{"body":"deploy"}`); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "idx")
	if err := store.SaveToDir(dir, genesis); err != nil {
		t.Fatal(err)
	}

	reopened := newStore(t, d, []string{"body"}, "")
	m, err := reopened.LoadFromDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if m.HeadCommitHex != genesis.Hex() {
		t.Errorf("manifest head = %s, want %s", m.HeadCommitHex, genesis.Hex())
	}
	if hits, _ := reopened.Search("deploy", &genesis, 0); len(hits) != 1 {
		t.Fatalf("the restored index answers the query: %v", hits)
	}
	if n := m.Stats["N"]; n != 1 {
		t.Errorf("manifest stats N = %v, want 1", n)
	}

	// A commit after the snapshot makes it stale: the head moved on, so the caller must
	// rebuild rather than trust the file.
	newHead := appendCommit(t, d, genesis)
	m2, err := reopened.LoadFromDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if m2.HeadCommitHex == newHead.Hex() {
		t.Error("the manifest must still name the commit the snapshot was taken at")
	}
}

// TestPutRequiresJSONKey: the Store interface's Put carries the document JSON in a StringKey;
// anything else is a caller error rather than a silently empty document.
func TestPutRequiresJSONKey(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/put")
	head, _ := d.Head()
	store := newStore(t, d, []string{"body"}, "")
	err := store.Put(index.Entry{DocID: mustUUID(t, "00000000-0000-4000-8000-000000000001"), Key: index.Int64Key{Value: 3}, CommitHash: head})
	if err == nil {
		t.Fatal("expected an error for a non-string key")
	}
}

// TestNonFullTextDescriptorIsRejected: constructing a full-text store over, say, a hash
// descriptor is a programming error, caught at construction rather than at query time.
func TestNonFullTextDescriptorIsRejected(t *testing.T) {
	d, _ := dag.NewInMemoryCommitDag("ns/type")
	_, err := fulltext.NewFullTextStore(index.Descriptor{FieldName: "body", Fields: []string{"body"}, Type: index.IndexTypeHash}, d, fulltext.Options{})
	if err == nil {
		t.Fatal("expected a type mismatch error")
	}
}

// newStore builds a full-text store over fields with the given weights option.
func newStore(t *testing.T, d *dag.InMemoryCommitDag, fields []string, weights string) *fulltext.Store {
	t.Helper()
	desc := index.Descriptor{
		IndexID:   mustUUID(t, "00000000-0000-4000-8000-000000009001"),
		FieldName: fields[0], Fields: fields, Type: index.IndexTypeFullText,
		Options: map[string]string{index.OptionWeights: weights},
	}
	store, err := fulltext.NewFullTextStore(desc, d, fulltext.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// appendCommit appends one empty commit on top of parent and returns its hash.
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
