package server

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/policy"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Document expiry (kdb-spec-layer16 §9.5, Component 73).
//
// Two halves. The read side hides expired documents at head - GetDocument, and every read the
// SQL engine makes through the expiryHidingAdapter it is constructed with - so a document is
// gone the instant its timestamp passes, not at the next sweep. The write side is a sweeper
// goroutine owned by the runtime that periodically scans head and commits DeleteOps for what has
// expired, so the namespace does not accumulate dead documents forever. Historical reads
// (atCommit older than head) are never filtered: what was in a commit stays in it.

// ExpirySweepBatch caps DeleteOps per sweep commit (§9.5: "batches of at most 500 per commit").
const ExpirySweepBatch = 500

// ExpirySweepMessage is the commit message every sweep commit carries.
const ExpirySweepMessage = "expiry sweep"

// systemPrincipalID names the runtime's own principal, used for sweep commits. Commits made by
// the system principal bypass RBAC: they are the runtime acting on its own policy, not a client
// acting on a grant. It is never handed out by an authenticator, so a client cannot claim it -
// the bypass is keyed on the txOptions.system flag set inside this package, not on the id.
const systemPrincipalID = "kdb:system"

func systemPrincipal() auth.Principal { return auth.Principal{ID: systemPrincipalID} }

// documentExpiry is the runtime's resolved expiry setting.
type documentExpiry struct {
	fieldPath string
	grace     time.Duration
	interval  time.Duration
}

// SetDocumentExpiry configures document expiry from p, replacing any previous setting; nil
// disables expiry (and stops a running sweeper). The read-side predicate takes effect
// immediately. The sweeper starts only on a writable runtime with a positive sweep interval -
// a read-only runtime (a follower opened alongside a writer) hides expired documents but never
// deletes them, since it has no write path at all.
func (s *KdbServerRuntime) SetDocumentExpiry(p *policy.DocumentExpiryPolicy) {
	s.stopSweeper()
	s.expiryMu.Lock()
	if p == nil || p.FieldPath == "" {
		s.expiry = nil
		s.expiryMu.Unlock()
		return
	}
	interval := time.Duration(p.SweepIntervalMillis) * time.Millisecond
	if p.SweepIntervalMillis <= 0 {
		interval = time.Duration(policy.DefaultSweepIntervalMillis) * time.Millisecond
	}
	grace := time.Duration(p.GraceMillis) * time.Millisecond
	if grace < 0 {
		grace = 0
	}
	s.expiry = &documentExpiry{fieldPath: p.FieldPath, grace: grace, interval: interval}
	s.expiryMu.Unlock()
	if s.Runtime.AssertWritable() != nil {
		return
	}
	s.startSweeper(interval)
}

// DocumentExpiry returns the active expiry policy, or nil when documents never expire.
func (s *KdbServerRuntime) DocumentExpiry() *policy.DocumentExpiryPolicy {
	e := s.expirySetting()
	if e == nil {
		return nil
	}
	return &policy.DocumentExpiryPolicy{
		FieldPath:           e.fieldPath,
		GraceMillis:         e.grace.Milliseconds(),
		SweepIntervalMillis: e.interval.Milliseconds(),
	}
}

func (s *KdbServerRuntime) expirySetting() *documentExpiry {
	s.expiryMu.RLock()
	defer s.expiryMu.RUnlock()
	return s.expiry
}

// SetClockForTest replaces the clock expiry is evaluated against. Tests only.
func (s *KdbServerRuntime) SetClockForTest(now func() time.Time) {
	s.expiryMu.Lock()
	s.now = now
	s.expiryMu.Unlock()
}

func (s *KdbServerRuntime) clock() time.Time {
	s.expiryMu.RLock()
	now := s.now
	s.expiryMu.RUnlock()
	if now != nil {
		return now()
	}
	return time.Now()
}

// isExpiredAtHead reports whether a document body read at head is expired under the active
// policy. False when no policy is set, when the field is absent, or when its value is not a
// timestamp in one of the two accepted forms.
func (s *KdbServerRuntime) isExpiredAtHead(jsonBody string) bool {
	e := s.expirySetting()
	if e == nil {
		return false
	}
	at, ok := expiryTimestamp(jsonBody, e.fieldPath)
	if !ok {
		return false
	}
	return !at.After(s.clock().Add(-e.grace))
}

// expiryTimestamp extracts the timestamp at fieldPath: an RFC 3339 string or a number of epoch
// milliseconds. Anything else - absent, null, other strings, booleans, objects - is (zero, false):
// "never expires".
func expiryTimestamp(jsonBody, fieldPath string) (time.Time, bool) {
	raw, err := schema.FieldValue(jsonBody, fieldPath)
	if err != nil || raw == nil {
		return time.Time{}, false
	}
	switch v := raw.(type) {
	case string:
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			t, err = time.Parse(time.RFC3339, v)
		}
		if err != nil {
			return time.Time{}, false
		}
		return t, true
	case float64:
		return time.UnixMilli(int64(v)), true
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return time.Time{}, false
		}
		return time.UnixMilli(int64(f)), true
	default:
		return time.Time{}, false
	}
}

// headTreeHash returns the document tree hash at the live head, or false when there is none.
func (s *KdbServerRuntime) headTreeHash() (codec.Hash, bool) {
	_, commit, ok, err := s.Runtime.DAG.HeadCommit()
	if err != nil || !ok {
		return codec.Hash{}, false
	}
	return commit.DocumentTreeHash, true
}

// expiryHidingAdapter is the storage.Adapter the runtime's SQL engine reads through. Every read
// at the head tree hash drops expired documents; reads at any other tree (historical
// atCommit, a SNAPSHOT session's pin) pass through untouched, as do all writes. The transaction
// engines deliberately keep the raw adapter: a write must see the real tree it lands on.
type expiryHidingAdapter struct {
	storage.Adapter
	runtime *KdbServerRuntime
}

// hidesAt reports whether reads at treeHash are subject to expiry: only when a policy is set and
// treeHash is the live head's tree.
func (a *expiryHidingAdapter) hidesAt(treeHash codec.Hash) bool {
	if a.runtime.expirySetting() == nil {
		return false
	}
	head, ok := a.runtime.headTreeHash()
	return ok && head == treeHash
}

func (a *expiryHidingAdapter) GetDocument(namespaceID string, docID codec.UUID, atCommit codec.Hash) (*document.Document, error) {
	doc, err := a.Adapter.GetDocument(namespaceID, docID, atCommit)
	if err != nil || doc == nil {
		return doc, err
	}
	if a.hidesAt(atCommit) && a.runtime.isExpiredAtHead(doc.JSON) {
		return nil, nil
	}
	return doc, nil
}

func (a *expiryHidingAdapter) GetDocumentOrThrow(namespaceID string, docID codec.UUID, atCommit codec.Hash) (document.Document, error) {
	doc, err := a.GetDocument(namespaceID, docID, atCommit)
	if err != nil {
		return document.Document{}, err
	}
	if doc == nil {
		return document.Document{}, fmt.Errorf("kdb: document %s not found in %s", docID, namespaceID)
	}
	return *doc, nil
}

func (a *expiryHidingAdapter) GetDocuments(namespaceID string, docIDs []codec.UUID, atCommit codec.Hash) ([]*document.Document, error) {
	docs, err := a.Adapter.GetDocuments(namespaceID, docIDs, atCommit)
	if err != nil || !a.hidesAt(atCommit) {
		return docs, err
	}
	for i, doc := range docs {
		if doc != nil && a.runtime.isExpiredAtHead(doc.JSON) {
			docs[i] = nil
		}
	}
	return docs, nil
}

func (a *expiryHidingAdapter) ScanDocuments(namespaceID string, atCommit codec.Hash, batchSize int, onBatch func([]document.Document) error) error {
	if !a.hidesAt(atCommit) {
		return a.Adapter.ScanDocuments(namespaceID, atCommit, batchSize, onBatch)
	}
	return a.Adapter.ScanDocuments(namespaceID, atCommit, batchSize, func(batch []document.Document) error {
		live := batch[:0:0]
		for _, doc := range batch {
			if !a.runtime.isExpiredAtHead(doc.JSON) {
				live = append(live, doc)
			}
		}
		if len(live) == 0 {
			return nil
		}
		return onBatch(live)
	})
}

// startSweeper launches the sweep goroutine. Caller must have stopped any previous one.
func (s *KdbServerRuntime) startSweeper(interval time.Duration) {
	if interval <= 0 {
		return
	}
	s.sweeperMu.Lock()
	defer s.sweeperMu.Unlock()
	stop := make(chan struct{})
	done := make(chan struct{})
	s.sweeperStop = stop
	s.sweeperDone = done
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if _, err := s.SweepExpiredNow(); err != nil {
					slog.Warn("expiry sweep failed", "namespace", s.Runtime.DefaultNamespace, "error", err)
				}
			}
		}
	}()
}

// stopSweeper stops the sweep goroutine, if any, and waits for it to exit - so a sweep that was
// mid-commit at Release finishes before storage closes under it.
func (s *KdbServerRuntime) stopSweeper() {
	s.sweeperMu.Lock()
	stop, done := s.sweeperStop, s.sweeperDone
	s.sweeperStop, s.sweeperDone = nil, nil
	s.sweeperMu.Unlock()
	if stop == nil {
		return
	}
	close(stop)
	<-done
}

// SweepingForTest reports whether a sweep goroutine is currently running. Tests only.
func (s *KdbServerRuntime) SweepingForTest() bool {
	s.sweeperMu.Lock()
	defer s.sweeperMu.Unlock()
	return s.sweeperStop != nil
}

// SweepExpiredNow performs one sweep synchronously: scan head, collect every expired document,
// and commit DeleteOps for them in batches of at most ExpirySweepBatch under the LAST_WRITE
// engine as the system principal with message ExpirySweepMessage. Returns how many documents
// were deleted. A no-op (0, nil) when no policy is set, when nothing has expired, or on a
// read-only runtime. Exported so tests - and an operator tool - can sweep on demand rather than
// wait for the ticker.
func (s *KdbServerRuntime) SweepExpiredNow() (int, error) {
	if s.expirySetting() == nil || s.Runtime.AssertWritable() != nil || s.draining.Load() {
		return 0, nil
	}
	treeHash, ok := s.headTreeHash()
	if !ok {
		return 0, nil
	}
	ns := s.Runtime.DefaultNamespace
	var expired []codec.UUID
	err := s.Runtime.Storage.ScanDocuments(ns, treeHash, 256, func(batch []document.Document) error {
		for _, doc := range batch {
			if s.isExpiredAtHead(doc.JSON) {
				expired = append(expired, doc.ID)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	deleted := 0
	for len(expired) > 0 {
		n := len(expired)
		if n > ExpirySweepBatch {
			n = ExpirySweepBatch
		}
		batch := expired[:n]
		expired = expired[n:]
		if err := s.commitExpiryBatch(batch); err != nil {
			return deleted, err
		}
		deleted += n
	}
	return deleted, nil
}

func (s *KdbServerRuntime) commitExpiryBatch(docIDs []codec.UUID) error {
	head, err := s.Runtime.DAG.Head()
	if err != nil {
		return err
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		return err
	}
	ops := make([]document.Op, len(docIDs))
	for i, id := range docIDs {
		ops[i] = document.DeleteOp{DocID: id}
	}
	tx := document.Transaction{
		ID:          txID,
		BaseVersion: head,
		Operations:  ops,
		Timestamp:   codec.TimestampNow(),
	}
	_, err = s.runTransaction(tx, systemPrincipal(), txOptions{system: true}, func() (transactionResult, error) {
		return s.UpsertEngine.Commit(tx, s.dag, s.Runtime.Storage, s.Schema(), nil, ExpirySweepMessage)
	})
	return err
}

// ExpirySummary renders the active expiry setting for the service's startup log line.
func (s *KdbServerRuntime) ExpirySummary() string {
	p := s.DocumentExpiry()
	if p == nil {
		return "disabled"
	}
	return fmt.Sprintf("field %s, grace %dms, sweep every %dms", p.FieldPath, p.GraceMillis, p.SweepIntervalMillis)
}

// sweeperState is embedded in KdbServerRuntime (see server_runtime.go) - declared here so the
// expiry machinery's fields live next to the code that uses them.
type sweeperState struct {
	expiryMu    sync.RWMutex
	expiry      *documentExpiry
	now         func() time.Time
	sweeperMu   sync.Mutex
	sweeperStop chan struct{}
	sweeperDone chan struct{}
}
