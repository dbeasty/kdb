package embed

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/metrics"
	"github.com/limidus/kdb/go/kdb/storage"
)

// maxLogBatch bounds how many queued records one drain writes before flushing.
// Purely a latency guard: without it, a sustained writer could keep the drain
// loop appending indefinitely while the callers already queued behind it wait
// for a flush that keeps getting deferred.
const maxLogBatch = 256

// defaultAsyncFlushInterval is the physical flush period under DurabilityAsync
// when the caller didn't configure one - matching the engine's blob-WAL
// background sync default (storage/engine.startAsyncSync).
const defaultAsyncFlushInterval = 5 * time.Millisecond

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
	// flushInterval is how often appended-but-unflushed records are physically
	// flushed under DurabilityAsync. Sync mode ignores it (every batch flushes
	// before its callers are acked). See runAsync.
	flushInterval time.Duration

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

	// onSealed is told when a rotation has sealed a segment, which is the
	// moment something new becomes reclaimable - nothing below the open
	// segment changes until one is. Called from the writer's own goroutine,
	// so an implementation must not do the reclaiming itself; the
	// maintenance scheduler's NotifySegmentSealed only wakes its loop.
	// nil means nobody is listening, which is the case for every runtime
	// that is not running a seal-triggered reclaim mode.
	onSealed func()

	// onPersisted is told where each record landed, once the append knows.
	// Set before the writer is used and not changed after, so it needs no
	// lock of its own.
	onPersisted func(treeHash codec.Hash, segmentSeq, frameOffset int64)
}

type logRequest struct {
	rec storage.DeltaRecord
	// treeHash names the document tree the commit produced, so whatever is
	// waiting on this record's log position can be found once the append
	// reports one. Zero when the caller has nothing waiting.
	treeHash codec.Hash
	// ack is nil for records whose caller does not wait (DurabilityAsync).
	ack chan error
}

func newCommitLogWriter(w storage.DeltaSegmentWriter, durability storage.Durability, asyncFlushInterval time.Duration) *commitLogWriter {
	if asyncFlushInterval <= 0 {
		asyncFlushInterval = defaultAsyncFlushInterval
	}
	c := &commitLogWriter{
		writer:        w,
		durability:    durability,
		flushInterval: asyncFlushInterval,
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
	wait, err := c.EnqueueAsync(rec, codec.Hash{})
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
func (c *commitLogWriter) EnqueueAsync(rec storage.DeltaRecord, treeHash codec.Hash) (wait func() error, err error) {
	if err := c.latched(); err != nil {
		return nil, err
	}
	req := &logRequest{rec: rec, treeHash: treeHash}
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
	if c.durability == storage.DurabilityAsync {
		c.runAsync()
		return
	}
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

// runAsync is the DurabilityAsync drain loop: records are appended as soon as
// they arrive (into the OS page cache, so an acknowledged write survives a
// process crash), but the physical flush runs at most once per flushInterval -
// the same journaling model MongoDB's default write concern uses. Before this
// existed, async mode flushed after every drained batch just like sync mode
// (callers merely didn't wait for it), so a single sustained writer still
// drove one physical fsync per commit in the background. The crash-loss
// window is unchanged in kind - "whatever had not been flushed" - and now
// bounded by flushInterval instead of by batch timing.
func (c *commitLogWriter) runAsync() {
	timer := time.NewTimer(c.flushInterval)
	if !timer.Stop() {
		<-timer.C
	}
	// dirty tracks "appended since the last flush"; the timer is armed exactly
	// when dirty is set and its channel is drained whenever dirty is cleared,
	// so Reset is always safe.
	dirty := false
	flush := func() {
		stop := metrics.Default.Track(metrics.StageFsyncWait)
		if err := c.writer.Flush(); err != nil {
			c.latch(err)
		}
		stop()
		dirty = false
	}
	for {
		var first *logRequest
		var ok bool
		if dirty {
			select {
			case first, ok = <-c.reqs:
			case <-timer.C:
				flush()
				continue
			}
		} else {
			first, ok = <-c.reqs
		}
		if !ok {
			if dirty {
				if !timer.Stop() {
					<-timer.C
				}
				flush()
			}
			return
		}
		batch := c.drain(first)
		err := c.appendBatch(batch)
		if err != nil {
			c.latch(err)
		}
		for _, r := range batch {
			if r.ack != nil {
				r.ack <- err
			}
		}
		if !dirty {
			dirty = true
			timer.Reset(c.flushInterval)
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

// deltaSegmentRotator is the part of the delta writer the commit log needs to keep segments from
// growing without bound. An interface rather than the concrete type because the commit log is
// written against storage.DeltaSegmentWriter and several tests substitute their own, which simply
// do not rotate.
type deltaSegmentRotator interface {
	RotateIfNeeded() (bool, error)
}

// currentSequence reports the segment a writer is on, for logging. -1 when the writer does not
// report one, which is only the case for a test double.
func currentSequence(w storage.DeltaSegmentWriter) int64 {
	if s, ok := w.(storage.DeltaSegmentSequencer); ok {
		return s.SequenceNumber()
	}
	return -1
}

// appendBatch writes each record to the segment without flushing.
func (c *commitLogWriter) appendBatch(batch []*logRequest) error {
	// Rotate here, before the sequence is read, and never once the loop
	// below has started: every record in this batch is reported at the one
	// sequence read on the next line, so a rotation partway through would
	// file the later ones under the segment they are not in. Doing it at
	// the boundary is what keeps (sequence, offset) true for every record -
	// see DefaultWriter.RotateIfNeeded, which says the same thing from the
	// other side.
	//
	// A rotation failure is not fatal to the batch: the old segment is
	// either still writable (nothing happened) or already sealed, and in
	// the sealed case the append below fails with a clear error of its own.
	// Logging and continuing keeps a full disk from looking like a commit
	// bug.
	if rotator, ok := c.writer.(deltaSegmentRotator); ok {
		if rotated, err := rotator.RotateIfNeeded(); err != nil {
			log.Printf("kdb: could not rotate the delta segment (%v); continuing on the current one", err)
		} else if rotated {
			log.Printf("kdb: rotated to delta segment %d", currentSequence(c.writer))
			// A segment just became eligible. Tell whoever is listening
			// rather than reclaiming here: this is the writer's own
			// goroutine, and putting a checkpoint plus a possible SSTable
			// rewrite behind the commit that happened to fill the segment
			// is exactly the latency spike maintenance is arranged to
			// avoid.
			if c.onSealed != nil {
				c.onSealed()
			}
		}
	}
	seq, hasSeq := int64(0), false
	if s, ok := c.writer.(storage.DeltaSegmentSequencer); ok {
		seq, hasSeq = s.SequenceNumber(), true
	}
	for _, r := range batch {
		offset, err := c.writer.Append(r.rec)
		if err != nil {
			return fmt.Errorf("appending commit %s to the delta log: %w", r.rec.CommitHash.Hex(), err)
		}
		// Reported after the append rather than before it, because the
		// offset is not knowable until then: a batch appends several
		// records and only Append says where each one went.
		if c.onPersisted != nil && hasSeq && r.treeHash != (codec.Hash{}) {
			c.onPersisted(r.treeHash, seq, offset)
		}
	}
	return nil
}

func (c *commitLogWriter) writeBatch(batch []*logRequest) error {
	if err := c.appendBatch(batch); err != nil {
		return err
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
