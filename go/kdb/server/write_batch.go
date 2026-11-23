package server

import (
	"context"
	"errors"
	"sync"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// Several writes per commit (W8, docs/kdb-basic-rw-performance-plan.md).
//
// A commit does a fixed amount of serial work under the namespace's write gate whatever it
// changes - one Merkle path to the root, one DAG append, one log record, one tree publish - so a
// namespace that commits once per write commits at most ~25k times a second, however many CPUs
// it has. With CoalesceWrites on, the writer that reaches the gate commits the writes queued
// behind it together with its own, as one transaction: one trie update over all of their
// documents (whose upper levels they share), one DAG append, one log record, one fsync. Each
// writer still waits for, and is answered with, the commit holding its write.
//
// Cassandra: group commit - the commit log's group/batch sync already shares one fsync between
// concurrent writers (KDB copies that); this goes one step further, as applying a batch of
// mutations to a partition is one update.
//
// Only writes nobody can tell apart from a commit of their own are batched: last-write-wins
// writes at head (Upsert, Replace, Delete) whose transaction id the server minted, so no client
// can look a write up by it. A batch never holds two writes to one document - the second one
// closes the batch and leads the next - so every version of every document still has its own
// commit and tree, and per-document history is exactly what it would be without batching. If
// the transaction a batch makes is rejected (a schema or index violation by one of its writes),
// every write in it is retried on its own, so each gets the answer it would have got alone.

// Batch caps: at most this many writes, and this many bytes of bodies, in one commit.
//
// The byte cap is what a cold read pays for. A version is read back through its commit's frame
// (a location record), and a read decompresses the whole frame: batches capped at 1 MiB made the
// frames of 1 KB documents ~31 KB, and zstd-decoding them was 18% of kdb-service's CPU in point
// reads right after a batched load (read throughput -20%). 16 KiB is Cassandra's default
// compressed chunk (chunk_length_in_kb), the bound on what one of its reads decompresses, and the
// size the large-frame bodies code (engine/large_frame_bodies.go) already holds reads to. A
// write larger than the cap still commits, alone. The count cap matters only for small
// documents.
const (
	coalesceMaxWrites = 64
	coalesceMaxBytes  = 16 << 10
)

// batchedWrite is one single-document write waiting to be committed, alone or in a batch.
type batchedWrite struct {
	// tx is the write as its own transaction: one operation, a server-minted id, its timestamp.
	tx    document.Transaction
	docID codec.UUID
	bytes int
	// op is the operation to commit, worked out at the front of the gate against the head the
	// batch lands on (headOK false when the DAG has no head yet).
	op func(headOK bool, headTree codec.Hash) document.Op

	// served is closed by the leader that committed this write in its batch, once commit, err
	// and alone are set. Never closed for a write that leads its own batch.
	served chan struct{}
	commit document.Commit
	err    error
	// alone: the batch was rejected before it committed; retry this write by itself.
	alone bool
}

// writeBatcher holds the batchable writes queued at a runtime's write gate.
type writeBatcher struct {
	mu      sync.Mutex
	pending []*batchedWrite
}

func (b *writeBatcher) add(w *batchedWrite) {
	b.mu.Lock()
	b.pending = append(b.pending, w)
	b.mu.Unlock()
}

// remove takes w out of the queue, and reports whether it was still there (not yet taken into a
// batch).
func (b *writeBatcher) remove(w *batchedWrite) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, p := range b.pending {
		if p == w {
			b.pending = append(b.pending[:i], b.pending[i+1:]...)
			return true
		}
	}
	return false
}

// lead forms the batch w leads, for a caller holding the write gate: w, then the writes queued
// behind it in arrival order, until the caps or the first write to a document already in the
// batch. Writes queued at the same time are concurrent, so committing them in this order rather
// than arrival order is one of the orders a client could have observed anyway. ok is false when
// w is no longer queued - an earlier leader already took it.
func (b *writeBatcher) lead(w *batchedWrite) (batch []*batchedWrite, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	at := -1
	for i, p := range b.pending {
		if p == w {
			at = i
			break
		}
	}
	if at < 0 {
		return nil, false
	}
	batch = append(batch, w)
	docs := map[codec.UUID]bool{w.docID: true}
	size := w.bytes
	taken := map[int]bool{at: true}
	for i, p := range b.pending {
		if i == at {
			continue
		}
		if len(batch) == coalesceMaxWrites || size+p.bytes > coalesceMaxBytes || docs[p.docID] {
			break
		}
		batch = append(batch, p)
		docs[p.docID] = true
		size += p.bytes
		taken[i] = true
	}
	rest := b.pending[:0]
	for i, p := range b.pending {
		if !taken[i] {
			rest = append(rest, p)
		}
	}
	for i := len(rest); i < len(b.pending); i++ {
		b.pending[i] = nil
	}
	b.pending = rest
	return batch, true
}

// coalescing reports whether this runtime batches eligible writes (CoalesceWrites). Not under
// projection write-back, which records each local write in its commit's message.
func (s *KdbServerRuntime) coalescing() bool {
	return s.CoalesceWrites && !s.writeBackOn() && s.dag != nil
}

// commitCoalesced commits w, as the leader of a batch or inside another writer's. alone is true
// when the caller must commit w by itself instead (its batch was rejected), in which case commit
// and err are meaningless. The checks runTransaction makes before the gate are made here, for w
// alone, before it can join anything.
func (s *KdbServerRuntime) commitCoalesced(w *batchedWrite, principal auth.Principal) (commit document.Commit, err error, alone bool) {
	if err := s.admitWrite(w.tx, principal, false); err != nil {
		return document.Commit{}, err, false
	}
	timeout := s.WriteTimeout
	if timeout <= 0 {
		timeout = DefaultWriteTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	grant, err := s.admission.Acquire(ctx, ClassWrite, transactionPayloadBytes(w.tx))
	if err != nil {
		return document.Commit{}, err, false
	}
	defer grant.Release()

	w.served = make(chan struct{})
	s.batcher.add(w)
	release, err := s.writeGate.acquireUnlessServed(ctx, w.served)
	if errors.Is(err, errServedWhileQueued) {
		return w.commit, w.err, w.alone
	}
	if err != nil {
		if s.batcher.remove(w) {
			return document.Commit{}, err, false
		}
		// Taken into a batch already: it is being committed, so its outcome is the answer.
		<-w.served
		return w.commit, w.err, w.alone
	}
	var releaseOnce sync.Once
	releaseNow := func() { releaseOnce.Do(release) }
	defer releaseNow()
	batch, led := s.batcher.lead(w)
	if !led {
		releaseNow()
		<-w.served
		return w.commit, w.err, w.alone
	}
	commit, committed, err := s.commitBatchAtGateFront(batch, releaseNow)
	retry := err != nil && !committed && len(batch) > 1
	for _, m := range batch {
		m.commit, m.err, m.alone = commit, err, retry
		if m != w {
			close(m.served)
		}
	}
	return commit, err, retry
}

// commitBatchAtGateFront commits batch as one transaction, for a caller holding the gate, through
// the same path every single write takes (commitAtGateFront: indexes, the engine, durability,
// notification). committed reports whether the engine accepted the transaction - an error after
// that (a failed sync) is the batch's, and is not retried write by write.
func (s *KdbServerRuntime) commitBatchAtGateFront(batch []*batchedWrite, releaseNow func()) (document.Commit, bool, error) {
	lead := batch[0].tx
	tx := document.Transaction{ID: lead.ID, AuthorNodeID: lead.AuthorNodeID, Timestamp: lead.Timestamp}
	tx.Operations = make([]document.Op, 0, len(batch))
	for _, m := range batch {
		tx.Operations = append(tx.Operations, m.tx.Operations...)
		// The latest of the writes' own times, so no write is recorded as older than it was
		// made; the DAG raises it past the parent's anyway (causal timestamps).
		if m.tx.Timestamp.EpochMicros() > tx.Timestamp.EpochMicros() {
			tx.Timestamp = m.tx.Timestamp
		}
	}
	committed := false
	commit, err := s.commitAtGateFront(tx, func() (transactionResult, error) {
		live := tx
		head, headCommit, ok, err := s.Runtime.DAG.HeadCommit()
		if err != nil {
			return nil, err
		}
		live.BaseVersion = head
		live.Operations = make([]document.Op, len(batch))
		for i, m := range batch {
			live.Operations[i] = m.op(ok, headCommit.DocumentTreeHash)
		}
		r, err := s.UpsertEngine.Commit(live, s.dag, s.Runtime.Storage, s.Schema(), nil, s.clientMessage(live))
		if _, success := r.(transaction.ResultSuccess); success && err == nil {
			committed = true
		}
		return r, err
	}, releaseNow)
	return commit, committed, err
}

// singleOp is the op function of a write committed as given, whatever the head.
func singleOp(op document.Op) func(bool, codec.Hash) document.Op {
	return func(bool, codec.Hash) document.Op { return op }
}
