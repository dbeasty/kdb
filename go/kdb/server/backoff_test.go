package server

import (
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/wire"
)

// A conflict is the refusal a contended workload produces the most of, and it was the one the
// server could not pace: the response carried a report and nothing else, so a client's only
// option was to retry instantly and collide again. This is the end-to-end check that a real
// conflict now arrives with both a code and a delay.
func TestConflictCarriesRetryAfter(t *testing.T) {
	srv := newTestRuntime(t)
	ns := "app/data"
	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Upsert(ns, docID, `{"v":1}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	// The base version this transaction will claim, captured before another writer moves it.
	stale, err := srv.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Upsert(ns, docID, `{"v":2}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}

	txID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	_, err = srv.Commit(ns, document.Transaction{
		ID:          txID,
		BaseVersion: stale,
		Operations:  []document.Op{document.WriteOp{DocID: docID, Patch: `{"v":3}`}},
		Timestamp:   codec.TimestampNow(),
	}, "", auth.Principal{})

	var conflictErr *ConflictError
	if !errors.As(err, &conflictErr) {
		t.Fatalf("expected *ConflictError writing against a stale base, got %v", err)
	}
	if conflictErr.RetryAfterMs < minConflictRetryMs {
		t.Fatalf("conflict carried no usable retry hint: %d ms", conflictErr.RetryAfterMs)
	}

	code, retryAfterMs := classifyError(conflictErr)
	if code != wire.ErrorCodeConflict {
		t.Fatalf("expected CONFLICT, got %s", code)
	}
	if retryAfterMs == nil || *retryAfterMs != conflictErr.RetryAfterMs {
		t.Fatalf("classifyError dropped the retry hint: %v", retryAfterMs)
	}

	// And the same numbers reach the wire, which is the only place a client can see them.
	msg, ok := conflictReport(7, ns, conflictErr).(wire.ConflictReportMessage)
	if !ok {
		t.Fatal("conflictReport did not produce a ConflictReportMessage")
	}
	if msg.ErrorCode == nil || *msg.ErrorCode != wire.ErrorCodeConflict {
		t.Fatalf("wire conflict report missing CONFLICT code: %v", msg.ErrorCode)
	}
	if msg.RetryAfterMs == nil || *msg.RetryAfterMs < minConflictRetryMs {
		t.Fatalf("wire conflict report missing retry hint: %v", msg.RetryAfterMs)
	}
	if len(msg.ReportBytes) == 0 {
		t.Fatal("wire conflict report lost the structured report")
	}
}

// The hint must stay inside its bounds whatever the gate reports, and it must not hand every
// caller the same number - identical delays reassemble the herd the delay exists to break up.
func TestConflictRetryAfterIsBoundedAndJittered(t *testing.T) {
	srv := newTestRuntime(t)
	seen := map[int]int{}
	for i := 0; i < 400; i++ {
		ms := srv.conflictRetryAfterMs()
		if ms < minConflictRetryMs || ms > maxConflictRetryMs {
			t.Fatalf("retry hint %d outside [%d, %d]", ms, minConflictRetryMs, maxConflictRetryMs)
		}
		seen[ms]++
	}
	// With no commits yet the ceiling is the floor, so a degenerate implementation and a correct
	// one agree here; assert the property that actually matters once the gate has data.
	srv.writeGate.observeService(20_000_000) // 20ms
	seen = map[int]int{}
	for i := 0; i < 400; i++ {
		seen[srv.conflictRetryAfterMs()]++
	}
	if len(seen) < 5 {
		t.Fatalf("expected jittered hints, got %d distinct values across 400 draws", len(seen))
	}
	for ms := range seen {
		if ms < minConflictRetryMs || ms > maxConflictRetryMs {
			t.Fatalf("retry hint %d outside [%d, %d]", ms, minConflictRetryMs, maxConflictRetryMs)
		}
	}
}

// Reads can be shed under load exactly like writes. Until DocumentGetResultMessage carried a
// code, a point read refused for pure load arrived as prose and a client had nothing to pace on.
func TestPointReadErrorIsClassified(t *testing.T) {
	msg := wire.DocumentGetMessage{
		H:         wire.Header{MessageType: wire.MsgDocumentGet, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 3},
		Namespace: "app/data",
		DocID:     "doc",
	}
	reply, ok := documentGetErrorClassified(msg, &BusyError{RetryAfterMs: 50, Reason: "write queue is full"}).(wire.DocumentGetResultMessage)
	if !ok {
		t.Fatal("expected a DocumentGetResultMessage")
	}
	if reply.ErrorCode == nil || *reply.ErrorCode != wire.ErrorCodeBusy {
		t.Fatalf("expected BUSY on a shed point read, got %v", reply.ErrorCode)
	}
	if reply.RetryAfterMs == nil || *reply.RetryAfterMs != 50 {
		t.Fatalf("expected the retry-after to survive, got %v", reply.RetryAfterMs)
	}
	if reply.Error == nil {
		t.Fatal("expected the prose Error to remain populated for an older client")
	}
}
