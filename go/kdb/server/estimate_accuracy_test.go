package server

// End-to-end test for kdb-spec-layer13 §12 test 10 (estimateAccuracy): scans and point reads
// take real grants over the real wire path, their measured actuals feed back, and the resulting
// estimate-vs-actual error stays within bound - with under-estimates rare and small.

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/wire"
)

// listenGoverned starts a real wire listener over a runtime with admission enabled and no
// synthetic pressure, seeds the namespace with docCount documents of ~docBytes JSON each, and
// returns the runtime plus a session-ready client.
func listenGoverned(t *testing.T, docCount, docBytes int) (*KdbServerRuntime, *rawWireClient, string) {
	t.Helper()
	srv := newTestRuntime(t)
	srv.SetMemoryLimit(testBudget, 0.85)
	t.Cleanup(func() { srv.memGuard.Stop() })

	filler := make([]byte, docBytes)
	for i := range filler {
		filler[i] = 'a' + byte(i%26)
	}
	for i := 0; i < docCount; i++ {
		docID, err := codec.RandomUUID()
		if err != nil {
			t.Fatal(err)
		}
		body := fmt.Sprintf(`{"n":%d,"v":%q}`, i, string(filler))
		if _, err := srv.Upsert("app/data", docID, body, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}

	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", srv)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	client := dialRawWireClient(t, "tcp://"+ln.Addr().String())
	client.handshake(t, wire.ClientSQL, "app/data")
	ack := client.sessionBegin(t, "app/data", "READ_COMMITTED")
	return srv, client, ack.SessionID
}

// Scans must take grants sized by the structural/learned estimator, report exact actuals, and
// keep the p95 of actual/estimate at or under 1: the estimator may over-reserve (that costs
// admission headroom) but must not systematically under-reserve (that costs the process its
// life). This is the signal that gates ever raising --memory-budget-mb closer to the container
// limit.
func TestScanEstimateAccuracyOverWire(t *testing.T) {
	srv, client, session := listenGoverned(t, 200, 1024)
	adm := srv.Admission()
	if adm == nil {
		t.Fatal("admission should be enabled")
	}

	for i := 0; i < 32; i++ {
		result := client.sqlExec(t, "app/data", session, "SELECT * FROM t")
		if result.Error != nil {
			t.Fatalf("scan %d failed: %s", i, *result.Error)
		}
	}

	if got := adm.Stats().Granted[ClassScan].Load(); got < 32 {
		t.Errorf("scans should each take a grant, got %d grants for 32 scans", got)
	}
	p95 := adm.Costs().AccuracyP95(ClassScan)
	if p95 == 0 {
		t.Fatal("no accuracy observations recorded - the feedback loop is not running")
	}
	if p95 > 1.0 {
		t.Errorf("scan estimate accuracy p95 = %v: actuals exceed estimates at the tail, meaning admission systematically under-reserves", p95)
	}
	if adm.Costs().LearnedCells() == 0 {
		t.Error("repeated identical scans should have produced a learned shape cell")
	}
}

// The learned tier must actually tighten repeated shapes: after enough observations, the
// reservation for this exact query drops below the structural worst case, freeing admission
// capacity without losing the conservative first-contact estimate for unknown shapes.
func TestRepeatedScanShapeLearnsTighterEstimate(t *testing.T) {
	srv, client, session := listenGoverned(t, 500, 512)
	adm := srv.Admission()

	// One scan to seed doc sizes, then read the structural estimate for the shape via a fresh
	// model-free measurement: compare granted bytes early vs late.
	result := client.sqlExec(t, "app/data", session, "SELECT kdb_id FROM t LIMIT 5")
	if result.Error != nil {
		t.Fatalf("seed scan failed: %s", *result.Error)
	}
	early := adm.OutstandingBytes() // nothing in flight now; sanity only
	_ = early

	var firstEstimate, laterEstimate int64
	for i := 0; i < minCellObservations+4; i++ {
		before := adm.Stats().Granted[ClassScan].Load()
		res := client.sqlExec(t, "app/data", session, "SELECT kdb_id FROM t LIMIT 5")
		if res.Error != nil {
			t.Fatalf("scan %d failed: %s", i, *res.Error)
		}
		if got := adm.Stats().Granted[ClassScan].Load(); got != before+1 {
			t.Fatalf("scan %d did not take exactly one grant", i)
		}
		// Estimates aren't directly observable per-request from out here, so re-derive from
		// the model the same way execRead does.
		est := adm.Costs().EstimateScan(scanInputForTest(t, srv, "app/data", "SELECT kdb_id FROM t LIMIT 5"))
		if i == 0 {
			firstEstimate = est
		}
		laterEstimate = est
	}
	if laterEstimate >= firstEstimate {
		t.Errorf("after %d identical scans the learned estimate (%d) should be tighter than the first structural one (%d)",
			minCellObservations+4, laterEstimate, firstEstimate)
	}
}

// Point reads learn document sizes and stay accurate.
func TestPointReadEstimateAccuracyOverWire(t *testing.T) {
	srv, client, _ := listenGoverned(t, 10, 8192)
	adm := srv.Admission()

	// Fetch each document twice so the second pass estimates from observed sizes.
	head, err := srv.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	commit, ok := srv.Runtime.DAG.GetCommit(head)
	if !ok {
		t.Fatal("head commit missing")
	}
	tree, ok := srv.Runtime.DAG.GetDocumentTree(commit.DocumentTreeHash)
	if !ok {
		t.Fatal("head tree missing")
	}
	var ids []codec.UUID
	tree.Walk(func(id codec.UUID, _ codec.Hash) bool {
		ids = append(ids, id)
		return true
	})
	for pass := 0; pass < 2; pass++ {
		for _, id := range ids {
			reply := client.request(t, wire.DocumentGetMessage{
				H:         wire.Header{MessageType: wire.MsgDocumentGet, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: client.nextCorrelation()},
				Namespace: "app/data",
				DocID:     id.String(),
			})
			res, ok := reply.(wire.DocumentGetResultMessage)
			if !ok {
				t.Fatalf("expected DocumentGetResultMessage, got %T", reply)
			}
			if res.Error != nil {
				t.Fatalf("get failed: %s", *res.Error)
			}
		}
	}

	if got := adm.Stats().Granted[ClassPointRead].Load(); got < int64(2*len(ids)) {
		t.Errorf("point reads should each take a grant: %d grants for %d gets", got, 2*len(ids))
	}
	if p95 := adm.Costs().AccuracyP95(ClassPointRead); p95 > 1.0 {
		t.Errorf("point-read accuracy p95 = %v: 8KiB documents should be fully covered once observed", p95)
	}
	// The estimator has now seen 8KiB documents; its estimate must cover them.
	if est := adm.Costs().EstimatePointRead("app/data"); est < 16<<10 {
		t.Errorf("point-read estimate %d after observing 8KiB documents, want >= 2x the document", est)
	}
}

// scanInputForTest mirrors execRead's estimate construction for a test-known query.
func scanInputForTest(t *testing.T, srv *KdbServerRuntime, namespace, sqlText string) ScanEstimateInput {
	t.Helper()
	stmt, err := sql.DefaultParser{}.Parse(sqlText)
	if err != nil {
		t.Fatal(err)
	}
	sel, ok := stmt.(sql.StmtSelect)
	if !ok {
		t.Fatalf("not a SELECT: %s", sqlText)
	}
	head, err := srv.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	return ScanEstimateInput{
		Namespace: namespace,
		Shape:     sql.ShapeOfSelect(sel.Query),
		TreeSize:  srv.treeSizeAt(head),
		MaxRows:   10_000,
		RowBudget: int(srv.admission.ScanRowBudget()),
	}
}
