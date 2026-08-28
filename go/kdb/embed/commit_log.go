package embed

import (
	"errors"
	"fmt"
	"sync"

	"github.com/limidus/kdb/go/kdb/metrics"
	"github.com/limidus/kdb/go/kdb/storage"
)

// maxLogBatch bounds how many queued records one drain writes before flushing.
// Purely a latency guard: without it, a sustained writer could keep the drain
// loop appending indefinitely while the callers already queued behind it wait
// for a flush that keeps getting deferred.
const maxLogBatch = 256

// ErrCommitLogClosed is returned to callers that enqueue after Close.
var ErrCommitLogClosed = errors.New("kdb: commit log writer is closed")

// commitLogWriter serializes commit records onto the delta log from a single
// background goroutine, coalescing the fsync across everything queued at the
// time it runs - the group commit the blob WAL has had since Phase 1
// (storage/wal.GroupCommitter) and the commit log, which is what document
// durability actually rides on, never did.
//
// Why this exists: KdbServerRuntime admits one commit at a time (server's
// writeGate, capacity 1), and PersistingCommitDAG.Persist used to run inside
// that exclusive section - framing, compressing, appending and then a full
// physical fsync, per commit, unbatched. Server write throughput was therefore
// 1/(work + fsync) with no batching possible no matter how many clients were
// writing. Handing the record to this writer instead lets the gate release as
// soon as the commit's order is fixed, so the next commit's validate/apply
// overlaps the previous one's disk write and concurrent commits share one sync.
//
// Ordering is preserved because callers enqueue while holding the write gate,
// so enqueue order is commit order, and a single drain goroutine appends in
// that order - which delta replay depends on.
type commitLogWriter struct {
	writer     storage.DeltaSegmentWriter
	durability storage.Durability

	reqs chan *logRequest
	done chan struct{}

	// mu guards failure, the fail-stop latch described on Enqueue.
	mu      sync.Mutex
	failure error

	// sendMu makes closing reqs safe. Enqueue holds it for read across its
	// send; Close takes it for write before closing, so a close can never
	// interleave with an in-flight send (which would panic). A sender blocked
	// on a full buffer does not deadlock Close: the drain goroutine is
	// independent of sendMu and keeps consuming, so the send completes and
	// releases its read lock.
	sendMu    sync.RWMutex
	closeOnce sync.Once
	closed    chan struct{}
}

type logRequest struct {
	rec storage.DeltaRecord
	// ack is nil for records whose caller does not wait (DurabilityAsync).
	ack chan error
}

func newCommitLogWriter(w storage.DeltaSegmentWriter, durability storage.Durability) *commitLogWriter {
	c := &commitLogWriter{
		writer:     w,
		durability: durability,
		// Buffered so an async caller hands off without waiting for the drain
		// goroutine to be scheduled; sync callers block on their ack anyway.
		reqs:   make(chan *logRequest, maxLogBatch),
		done:   make(chan struct{}),
		closed: make(chan struct{}),
	}
	go c.run()
	return c
}

// Enqueue hands rec to the log writer.
//
// Under DurabilitySync it returns only once rec is on disk and fsynced, so a
// returned nil means the commit is durable - the same guarantee the previous
// inline Append+Flush gave, minus holding the write gate for it. Under
// DurabilityAsync it returns as soon as rec is queued: the data is in memory
// and in the queue, and a crash can lose up to one in-flight batch.
//
// Once any append or flush fails, the failure is latched and returned to every
// later caller: a commit log with a hole in it is not something to keep
// appending to, since replay would silently stop at the hole.
func (c *commitLogWriter) Enqueue(rec storage.DeltaRecord) error {
	wait, err := c.EnqueueAsync(rec)
	if err != nil {
		return err
	}
	return wait()
}

// EnqueueAsync splits Enqueue in two so a caller holding a lock that fixes
// commit order can release it before waiting for disk. The queueing half must
// happen under that lock - queue order is commit order, and delta replay
// depends on it - while the returned wait is safe, and meant, to be called
// after releasing it. wait is never nil on a nil error, and is a no-op under
// DurabilityAsync.
func (c *commitLogWriter) EnqueueAsync(rec storage.DeltaRecord) (wait func() error, err error) {
	if err := c.latched(); err != nil {
		return nil, err
	}
	req := &logRequest{rec: rec}
	if c.durability == storage.DurabilitySync {
		req.ack = make(chan error, 1)
	}
	c.sendMu.RLock()
	select {
	case <-c.closed:
		c.sendMu.RUnlock()
		return nil, ErrCommitLogClosed
	default:
	}
	c.reqs <- req
	c.sendMu.RUnlock()

	if req.ack == nil {
		return func() error { return nil }, nil
	}
	return func() error { return <-req.ack }, nil
}

func (c *commitLogWriter) latched() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failure
}

func (c *commitLogWriter) latch(err error) {
	c.mu.Lock()
	if c.failure == nil {
		c.failure = err
	}
	c.mu.Unlock()
}

func (c *commitLogWriter) run() {
	defer close(c.done)
	for {
		first, ok := <-c.reqs
		if !ok {
			return
		}
		batch := c.drain(first)
		err := c.writeBatch(batch)
		if err != nil {
			c.latch(err)
		}
		for _, r := range batch {
			if r.ack != nil {
				r.ack <- err
			}
		}
	}
}

// drain collects everything already queued behind first, up to maxLogBatch.
// This is where group commit actually happens: whatever accumulated while the
// previous batch was being fsynced shares this batch's single flush.
func (c *commitLogWriter) drain(first *logRequest) []*logRequest {
	batch := make([]*logRequest, 0, 8)
	batch = append(batch, first)
	for len(batch) < maxLogBatch {
		select {
		case r, ok := <-c.reqs:
			if !ok {
				return batch
			}
			batch = append(batch, r)
		default:
			return batch
		}
	}
	return batch
}

func (c *commitLogWriter) writeBatch(batch []*logRequest) error {
	for _, r := range batch {
		if _, err := c.writer.Append(r.rec); err != nil {
			return fmt.Errorf("appending commit %s to the delta log: %w", r.rec.CommitHash.Hex(), err)
		}
	}
	// One flush for the whole batch - the point of the exercise. Recorded under
	// the same stage name the blob path uses, so /metrics and the benchmarks
	// show commit-log fsyncs too; until this existed the commit path's fsync -
	// the one document durability actually rides on - was invisible there.
	defer metrics.Default.Track(metrics.StageFsyncWait)()
	return c.writer.Flush()
}

// Close stops accepting records, waits for everything already queued to be
// written and flushed, and reports the first error seen. Safe to call twice.
func (c *commitLogWriter) Close() error {
	c.closeOnce.Do(func() {
		c.sendMu.Lock()
		close(c.closed)
		close(c.reqs)
		c.sendMu.Unlock()
	})
	<-c.done
	return c.latched()
}
