package hybrid

import (
	"errors"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/policy"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/storage"
)

// stubSQL stands in for the planner: these tests are about which commit a
// statement resolves to, not about what the statement returns.
type stubSQL struct{ lastCommit *codec.Hash }

func (s *stubSQL) Execute(_ string, ctx sql.QueryContext) (sql.QueryResult, error) {
	s.lastCommit = ctx.AtCommit
	return sql.QueryResult{}, nil
}
func (s *stubSQL) ExecuteDML(_ string, _ sql.QueryContext) (sql.DMLResult, error) {
	return sql.DMLResult{}, nil
}

func buildDAG(t *testing.T, n int) (*dag.InMemoryCommitDag, []codec.Hash) {
	t.Helper()
	d, err := dag.NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	head, _ := d.Head()
	base := codec.TimestampNow()
	var out []codec.Hash
	for i := 0; i < n; i++ {
		txID, _ := codec.RandomUUID()
		author, _ := codec.RandomUUID()
		tx := document.Transaction{
			ID: txID, BaseVersion: head,
			Timestamp:    codec.Timestamp{EpochMillis: base.EpochMillis + int64(i) + 1},
			AuthorNodeID: author,
		}
		c, err := d.AppendCommit(tx, head, document.EmptyDocumentTree(), nil, "m")
		if err != nil {
			t.Fatal(err)
		}
		head = c.Hash
		out = append(out, c.Hash)
	}
	return d, out
}

func registryWith(t *testing.T, p policy.NamespacePolicy) policy.Registry {
	t.Helper()
	reg := policy.NewInMemoryRegistry()
	if err := reg.Put(p); err != nil {
		t.Fatal(err)
	}
	return reg
}

// The regression this locks down: AtTag and AtTime used to return head, so
// a query pinned to a version that did not exist silently read current
// data and reported success.
func TestUnresolvableClausesFailInsteadOfReadingHead(t *testing.T) {
	d, _ := buildDAG(t, 3)
	head, _ := d.Head()
	r := NewDefaultVersionResolver()

	for _, clause := range []VersionClause{
		AtTag{Tag: "never-created"},
		AtCommit{Hex: "0000000000000000000000000000000000000000000000000000000000000000"},
		AtCommit{Hex: "head~99"},
	} {
		got, err := r.Resolve(d, clause, nil)
		if err == nil {
			t.Fatalf("%#v resolved to %s instead of failing", clause, got.Hex())
		}
		if got == head {
			t.Fatalf("%#v fell back to head", clause)
		}
	}
}

// A time before the first real commit resolves to genesis, whose
// timestamp is the epoch - so it reports the empty database that existed
// before anything was written, rather than failing. That is the honest
// answer and, unlike a fallback to head, it is not the *current* data
// wearing a past timestamp.
func TestTimeBeforeTheFirstCommitResolvesToGenesis(t *testing.T) {
	d, hashes := buildDAG(t, 3)
	head, _ := d.Head()
	got, err := NewDefaultVersionResolver().Resolve(d, AtTime{ISO8601: "1999-01-01T00:00:00Z"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got == head {
		t.Fatal("a time before the namespace existed resolved to head")
	}
	for _, h := range hashes {
		if got == h {
			t.Fatal("a time before the first commit resolved to a real commit")
		}
	}
	c, ok := d.GetCommit(got)
	if !ok || len(c.ParentHashes) != 0 {
		t.Fatal("expected genesis")
	}
}

func TestResolveAcceptsRelativeRevisions(t *testing.T) {
	d, hashes := buildDAG(t, 6)
	r := NewDefaultVersionResolver()
	got, err := r.Resolve(d, AtCommit{Hex: "head~2"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != hashes[len(hashes)-3] {
		t.Fatal("AT COMMIT 'head~2' resolved to the wrong commit")
	}
}

func TestResolveAtTimeAndTag(t *testing.T) {
	d, hashes := buildDAG(t, 4)
	if _, err := d.CreateTag("v1", hashes[1], ""); err != nil {
		t.Fatal(err)
	}
	r := NewDefaultVersionResolver()
	got, err := r.Resolve(d, AtTag{Tag: "v1"}, nil)
	if err != nil || got != hashes[1] {
		t.Fatalf("tag resolved to %v (%v)", got, err)
	}

	c, _ := d.GetCommit(hashes[2])
	when := time.UnixMicro(c.Timestamp.EpochMicros()).UTC().Format(time.RFC3339Nano)
	got, err = r.Resolve(d, AtTime{ISO8601: when}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != hashes[2] {
		t.Fatal("AT TIME resolved to the wrong commit")
	}
}

func TestParseISO8601Forms(t *testing.T) {
	for _, s := range []string{
		"2026-09-08T12:00:00Z", "2026-09-08T12:00:00.123456Z",
		"2026-09-08T12:00:00", "2026-09-08 12:00:00", "2026-09-08",
	} {
		if _, err := parseISO8601(s); err != nil {
			t.Fatalf("%q should parse: %v", s, err)
		}
	}
	if _, err := parseISO8601("last tuesday"); err == nil {
		t.Fatal("a non-timestamp should not parse")
	}
}

func TestExecuteReadsAtTheResolvedCommit(t *testing.T) {
	d, hashes := buildDAG(t, 5)
	sqlStub := &stubSQL{}
	e := NewEngine(Config{
		SQL: sqlStub, DAG: d,
		PolicyRegistry: registryWith(t, policy.DefaultMutable("ns", nil)),
	})

	res, err := e.Execute("SELECT * FROM t AT COMMIT 'head~1'", Request{NamespaceID: "ns"})
	if err != nil {
		t.Fatal(err)
	}
	want := hashes[len(hashes)-2]
	if res.ResolvedCommit != want {
		t.Fatal("statement did not resolve to head~1")
	}
	if sqlStub.lastCommit == nil || *sqlStub.lastCommit != want {
		t.Fatal("the resolved commit was not passed down to the planner")
	}
	if !res.ReadOnly {
		t.Fatal("a version-pinned read should report itself read-only")
	}

	// And a plain read at head is not a checkout, which the hardcoded
	// ReadOnly:true used to claim it was.
	res, err = e.Execute("SELECT * FROM t", Request{NamespaceID: "ns"})
	if err != nil {
		t.Fatal(err)
	}
	if res.ReadOnly {
		t.Fatal("an ordinary read at head reported itself read-only")
	}
}

func TestCheckoutRefusesAnUnknownBranch(t *testing.T) {
	d, _ := buildDAG(t, 3)
	head, _ := d.Head()
	e := NewEngine(Config{
		SQL: &stubSQL{}, DAG: d,
		PolicyRegistry: registryWith(t, policy.DefaultMutable("ns", nil)),
	})
	got, err := e.Checkout("ns", dag.RefByBranch{Name: "does-not-exist"})
	if err == nil {
		t.Fatalf("checkout of an unknown branch succeeded at %s", got.CommitHash.Hex())
	}
	if got.CommitHash == head {
		t.Fatal("checkout of an unknown branch fell back to head")
	}
}

func TestCheckoutPinsSubsequentReads(t *testing.T) {
	d, hashes := buildDAG(t, 4)
	e := NewEngine(Config{
		SQL: &stubSQL{}, DAG: d,
		PolicyRegistry: registryWith(t, policy.DefaultMutable("ns", nil)),
	})
	if _, err := e.Checkout("ns", dag.RefByHash{Hex: hashes[1].Hex()}); err != nil {
		t.Fatal(err)
	}
	res, err := e.Execute("SELECT * FROM t", Request{NamespaceID: "ns"})
	if err != nil {
		t.Fatal(err)
	}
	if res.ResolvedCommit != hashes[1] {
		t.Fatal("a read with an active checkout did not use it")
	}
	if err := e.ResetCheckout("ns"); err != nil {
		t.Fatal(err)
	}
	res, err = e.Execute("SELECT * FROM t", Request{NamespaceID: "ns"})
	if err != nil {
		t.Fatal(err)
	}
	head, _ := d.Head()
	if res.ResolvedCommit != head {
		t.Fatal("reset did not return reads to head")
	}
}

func TestNoneModeExplainsWhyAVersionIsGone(t *testing.T) {
	d, _ := buildDAG(t, 3)
	p := policy.CacheNoHistory("ns")
	e := NewEngine(Config{
		SQL: &stubSQL{}, DAG: d, PolicyRegistry: registryWith(t, p),
	})
	_, err := e.Execute(
		"SELECT * FROM t AT COMMIT '0000000000000000000000000000000000000000000000000000000000000000'",
		Request{NamespaceID: "ns"})
	var notRetained *HistoryNotRetainedError
	if !errors.As(err, &notRetained) {
		t.Fatalf("want HistoryNotRetainedError under history=NONE, got %v", err)
	}
	if notRetained.Window.Duration != storage.DefaultRetentionDuration {
		t.Fatal("the error should name the window that governs the reclamation")
	}
}

func TestNoneModeWithNoWindowSaysHistoryIsDisabled(t *testing.T) {
	d, _ := buildDAG(t, 3)
	p := policy.CacheNoHistory("ns")
	p.Retain = storage.RetentionWindow{Duration: storage.RetainNothing}
	e := NewEngine(Config{
		SQL: &stubSQL{}, DAG: d, PolicyRegistry: registryWith(t, p),
	})
	_, err := e.Execute("SELECT * FROM t AT VERSION 'v-gone'", Request{NamespaceID: "ns"})
	var disabled *HistoryDisabledError
	if !errors.As(err, &disabled) {
		t.Fatalf("want HistoryDisabledError with a zero window, got %v", err)
	}
}

// A full-history namespace keeps the original error, which already says
// everything true about it; nothing should mention a retention window.
func TestFullModeDoesNotMentionRetention(t *testing.T) {
	d, _ := buildDAG(t, 3)
	e := NewEngine(Config{
		SQL: &stubSQL{}, DAG: d,
		PolicyRegistry: registryWith(t, policy.DefaultMutable("ns", nil)),
	})
	_, err := e.Execute("SELECT * FROM t AT VERSION 'nope'", Request{NamespaceID: "ns"})
	if err == nil {
		t.Fatal("an unknown tag should fail")
	}
	var notRetained *HistoryNotRetainedError
	if errors.As(err, &notRetained) {
		t.Fatal("history=FULL reclaims nothing and must not blame retention")
	}
}
