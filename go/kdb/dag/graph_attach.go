package dag

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
)

// The DAG's half of the on-disk commit graph: when to write segments, which kind, and attaching
// them. The embedding runtime decides where they live (ConfigureGraphFile) and when size-triggered
// writes may start (StartGraphAutoFlush, once the graph is open); what goes in them is decided
// here, under the same lock that guards the commits themselves.
//
// The scheme is an LSM's, the way Cassandra runs one (docs/kdb-basic-rw-performance-plan.md, M1):
//
//   - flush: the resident tier, as a new segment, when it reaches graph.rebuildCommits commits -
//     a memtable flushed by size, on a goroutine, streamed from slots frozen in place;
//   - merge: segments of similar size, four or more at a time, into one - SizeTieredCompaction-
//     Strategy, so a record is rewritten O(log n) times, not once per full rewrite; a segment
//     more than GraphTombstoneRatio hidden is rewritten alone - single-SSTable tombstone
//     compaction;
//   - major compaction (CompactGraphFile): everything, as one segment marked full.
//
// A flush and a merge can run at once: the flush takes only commits no segment has a record for,
// the merge only segment records.

// GraphTombstoneRatio is the share of a segment's records that may be hidden (deleted from the DAG
// since they were written) before it is rewritten alone to drop them - Cassandra's
// tombstone_threshold.
const GraphTombstoneRatio = 0.2

// Size-tiered merge parameters - Cassandra's SizeTieredCompactionStrategy defaults. Segments whose
// record counts are within [low, high] x a bucket's mean share the bucket; segments smaller than
// one flush (graph.rebuildCommits) all share one; a bucket of at least min merges, at most max at
// a time, smallest first.
const (
	graphMergeMin      = 4
	graphMergeMax      = 32
	graphBucketLow     = 0.5
	graphBucketHigh    = 1.5
	graphRetryDelay    = 5 * time.Second
	graphPendingSuffix = ".pending"
	graphTempMarker    = ".tmp"
)

// ConfigureGraphFile names the directory this DAG's graph segments live in. Without one no
// segment is ever written or read, whatever GraphSettings.FileEnabled says. A read-only DAG maps
// segments but never writes or deletes one: it shares the directory with a writer.
func (d *InMemoryCommitDag) ConfigureGraphFile(dir string, readOnly bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.graphDir = dir
	d.graphReadOnly = readOnly
}

// OpenGraphFile attaches the segments in the configured directory. Missing segments are not an
// error. If any segment is unusable, none is used: the DAG keeps every commit resident, exactly
// where it was before, the segments are removed (unless read-only), and the next write is a full
// rewrite. All-or-nothing because segments are only disjoint as a set; keeping some of a set that
// a damaged full segment had superseded could show a commit twice.
//
// Files a crash left half-made - a temporary being written, a merge output still waiting for its
// inputs to go (.pending) - are never segments here: this path holds every commit already, so they
// could only take commits out of the heap, and a pending one shares records with files that are
// still on disk. They are removed (unless read-only).
func (d *InMemoryCommitDag) OpenGraphFile() error {
	d.mu.RLock()
	dir, ro, enabled := d.graphDir, d.graphReadOnly, d.graph.FileEnabled
	d.mu.RUnlock()
	if dir == "" || !enabled {
		return nil
	}
	seqs, err := listGraphSegments(dir)
	if err != nil {
		return err
	}
	maxSeq := maxGraphFileSeq(dir, seqs)
	if !ro {
		removeGraphLeftovers(dir, nil)
	}
	var opened []*graphBase // ascending seq
	var firstErr error
	for _, seq := range seqs {
		b, err := openGraphBase(filepath.Join(dir, graphSegmentName(seq)), d.NamespaceID)
		if err == nil && b.seq != seq {
			b.close()
			err = fmt.Errorf("%w: segment %d names itself %d", errGraphFile, seq, b.seq)
		}
		if err != nil {
			firstErr = err
			break
		}
		opened = append(opened, b)
	}
	if firstErr != nil {
		for _, b := range opened {
			b.close()
		}
		if !ro {
			for _, seq := range seqs {
				os.Remove(filepath.Join(dir, graphSegmentName(seq)))
			}
		}
		d.mu.Lock()
		d.graphNextSeq = max(d.graphNextSeq, maxSeq+1)
		d.mu.Unlock()
		return firstErr
	}
	// A full segment supersedes everything below it: what a crash between a full rewrite's rename
	// and its deletes leaves behind.
	start := 0
	for i, b := range opened {
		if b.full {
			start = i
		}
	}
	for _, b := range opened[:start] {
		b.close()
		if !ro {
			os.Remove(b.path)
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, b := range opened[start:] {
		d.attachLocked(b, nil)
	}
	if maxSeq > 0 || len(seqs) > 0 {
		d.graphNextSeq = max(d.graphNextSeq, maxSeq+1)
	}
	return nil
}

// maxGraphFileSeq is the highest sequence any file in dir uses - segments, pending merge outputs
// and temporaries alike - so a new segment never reuses a number a crash left behind.
func maxGraphFileSeq(dir string, segs []uint64) uint64 {
	var m uint64
	if n := len(segs); n > 0 {
		m = segs[n-1]
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if seq, ok := graphLeftoverSeq(e.Name()); ok && seq > m {
			m = seq
		}
	}
	return m
}

// graphLeftoverSeq parses the name of a pending merge output or a temporary segment.
func graphLeftoverSeq(name string) (uint64, bool) {
	i := strings.Index(name, graphSegmentSuffix)
	if i < 0 || i+len(graphSegmentSuffix) == len(name) {
		return 0, false
	}
	rest := name[i+len(graphSegmentSuffix):]
	if rest != graphPendingSuffix && !strings.HasPrefix(rest, graphTempMarker) {
		return 0, false
	}
	return graphSegmentSeq(name[:i+len(graphSegmentSuffix)])
}

// removeGraphLeftovers removes every temporary and pending file in dir except the pending files
// of the sequences in keep - the ones attached from there.
func removeGraphLeftovers(dir string, keep map[uint64]bool) {
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		seq, ok := graphLeftoverSeq(e.Name())
		if !ok || (keep[seq] && strings.HasSuffix(e.Name(), graphPendingSuffix)) {
			continue
		}
		os.Remove(filepath.Join(dir, e.Name()))
	}
}

// graphWritableLocked reports whether this DAG writes segments at all. Must hold mu.
func (d *InMemoryCommitDag) graphWritableLocked() bool {
	return d.graphDir != "" && d.graph.FileEnabled && !d.graphReadOnly && !d.nodes.goMode && !d.graphStop.Load()
}

// StartGraphAutoFlush turns on size-triggered writes: from now on a commit that takes the resident
// tier to graph.rebuildCommits starts a flush on its own goroutine, and every flush starts the
// merges it makes due. For the embedding runtime to call once the graph is open - before, a flush
// would write commits the segments on disk may already hold. A no-op where the DAG writes no
// segments.
func (d *InMemoryCommitDag) StartGraphAutoFlush() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.graphWritableLocked() {
		return
	}
	d.graphAuto = true
	d.graphFlushFloor = 0
	d.kickGraphFlushLocked()
	d.kickGraphMergeLocked()
}

// StopGraphWork ends size-triggered writes and waits for any in flight to finish or give up; a
// write that gives up leaves everything where it was. For a DAG about to close: its last
// checkpoint should not race a merge it would only have to wait for.
func (d *InMemoryCommitDag) StopGraphWork() {
	d.graphStop.Store(true)
	d.mu.Lock()
	d.graphAuto = false
	d.mu.Unlock()
	d.graphWork.Wait()
	// A synchronous write (maintenance, CompactGraphFile) sees graphStop too; wait it out.
	d.graphFlushMu.Lock()
	d.graphMergeMu.Lock()
	d.graphMergeMu.Unlock()
	d.graphFlushMu.Unlock()
}

// kickGraphFlushLocked starts the flusher when the resident tier is past its bound. Must hold mu.
// The bound counts from what the last flush left behind - commits that cannot be flushed yet
// (provisional, unlogged) would otherwise start a flush that writes nothing on every commit.
func (d *InMemoryCommitDag) kickGraphFlushLocked() {
	if !d.graphAuto || d.graphFlushing || d.nodes.residentLen() < d.graphFlushFloor+d.graph.RebuildCommits {
		return
	}
	if !d.graphRetryAt.IsZero() && time.Now().Before(d.graphRetryAt) {
		return
	}
	d.graphFlushing = true
	d.graphWork.Add(1)
	go d.graphFlusher()
}

func (d *InMemoryCommitDag) kickGraphMergeLocked() {
	if !d.graphAuto || d.graphMerging {
		return
	}
	d.graphMerging = true
	d.graphWork.Add(1)
	go d.graphCompactor()
}

func (d *InMemoryCommitDag) graphFlusher() {
	defer d.graphWork.Done()
	for {
		did, err := d.flushResident(false)
		d.mu.Lock()
		d.noteBackgroundErrLocked(err)
		again := err == nil && did && d.graphAuto && d.nodes.residentLen() >= d.graphFlushFloor+d.graph.RebuildCommits
		if did {
			d.kickGraphMergeLocked()
		}
		if !again {
			d.graphFlushing = false
		}
		d.mu.Unlock()
		if !again {
			return
		}
	}
}

func (d *InMemoryCommitDag) graphCompactor() {
	defer d.graphWork.Done()
	for {
		did, err := d.mergeOnce()
		d.mu.Lock()
		d.noteBackgroundErrLocked(err)
		again := err == nil && did && d.graphAuto
		if !again {
			d.graphMerging = false
		}
		d.mu.Unlock()
		if !again {
			return
		}
	}
}

// noteBackgroundErrLocked records a background write's outcome: a failure is logged and the next
// attempt waits graphRetryDelay (the commits stay resident meanwhile, as they did before any
// segment existed). Must hold mu.
func (d *InMemoryCommitDag) noteBackgroundErrLocked(err error) {
	switch {
	case err == nil:
		d.graphRetryAt = time.Time{}
	case errors.Is(err, errGraphWriteStopped):
	default:
		d.graphRetryAt = time.Now().Add(graphRetryDelay)
		log.Printf("kdb: namespace %s: could not write the graph file in the background (%v) - commits stay in memory; retrying in %s",
			d.NamespaceID, err, graphRetryDelay)
	}
}

// MaybeRebuildGraphFile writes whatever the policy calls for now - a flush of the resident tier if
// it has reached graph.rebuildCommits, then every merge that is due - and reports whether it wrote.
// Safe to call concurrently with reads, writes and the background writers: segments are written
// without the DAG's lock. The maintenance pass and open call it; with StartGraphAutoFlush it is
// only a backstop.
func (d *InMemoryCommitDag) MaybeRebuildGraphFile() (bool, error) {
	did, err := d.flushResident(false)
	if err != nil {
		return did, err
	}
	for {
		merged, err := d.mergeOnce()
		if err != nil {
			return did, err
		}
		if !merged {
			return did, nil
		}
		did = true
	}
}

// RebuildGraphFile is CompactGraphFile.
func (d *InMemoryCommitDag) RebuildGraphFile() error { return d.CompactGraphFile() }

// graphInclude is which resident commits may go to a segment, and graphFlags what a segment
// record says about them that the resident record does not. Must hold mu.
func (d *InMemoryCommitDag) graphIncludeLocked() (func(codec.Hash, []byte) bool, func(codec.Hash, []byte) uint32) {
	// Commits a checkpoint must leave out - unlogged, and everything descending from one - must
	// not reach a segment either, or a checkpoint that references the segment would bring them
	// back. See unloggedClosureLocked.
	excluded := d.unloggedClosureLocked()
	include := func(h codec.Hash, r []byte) bool {
		if d.nodes.namespaces[recNsIdx(r)] != d.NamespaceID {
			return false
		}
		if _, ok := excluded[h]; ok {
			return false
		}
		// Commits that may yet be taken back stay resident: a group part not yet decided, and one
		// received from a peer that is not in the log.
		if _, ok := d.provisional[h]; ok {
			return false
		}
		if _, ok := d.unlogged[h]; ok {
			return false
		}
		return true
	}
	flags := func(h codec.Hash, r []byte) uint32 {
		var f uint32
		// Whether the commit wrote anything is not in the resident record: resident, it is
		// answered by the ops side map and the eviction set. On disk the record must say.
		if len(d.nodes.ops[h]) > 0 || d.opsEvictedFor(h) {
			f |= entryHasOps
		}
		if got, ok := d.txIndex[recTx(r)]; ok && got == h {
			f |= entryOwnsTx
		}
		return f
	}
	return include, flags
}

// graphSnapshotBatch is how many resident slots a flush notes per hold of the DAG's lock: a
// flush of 50,000 commits taken in one hold stalled every writer for ~8 ms.
const graphSnapshotBatch = 1024

// yieldGraphLock pauses a background writer between two holds of the DAG's lock for as long as
// the last one lasted (at least graphYieldMin), so writers parked on the lock take it rather than
// the background goroutine - still running, it would win the lock straight back (sync.Mutex
// favours the running goroutine until a waiter has waited a millisecond). Caps a flush at half
// the lock while it notes what it takes; the flush is not in a hurry, the commits are.
func yieldGraphLock(held time.Duration) {
	time.Sleep(max(held, graphYieldMin))
}

const graphYieldMin = 50 * time.Microsecond

// flushResident writes the resident tier as a new segment when it holds at least
// graph.rebuildCommits commits (or force), streaming the records from where they are. The first
// segment a namespace writes is marked full: it is the whole graph.
//
// The DAG's write lock is held only in short pieces: the freeze, one batch of slots at a time, and
// an attach proportional to what stays resident. Writers run in between.
func (d *InMemoryCommitDag) flushResident(force bool) (bool, error) {
	d.graphFlushMu.Lock()
	defer d.graphFlushMu.Unlock()

	d.mu.Lock()
	if !d.graphWritableLocked() || (!force && d.nodes.residentLen() < d.graph.RebuildCommits) {
		d.mu.Unlock()
		return false, nil
	}
	full := len(d.nodes.segs) == 0
	// Generations go into the segment, so they must be sound before the records are read. One
	// recomputed after this rewrites its record into a fresh slot, which the next flush takes.
	d.ensureGenerationsLocked()
	include, flags := d.graphIncludeLocked()
	in := &graphWriteInput{stop: &d.graphStop}
	limit := d.nodes.beginResidentWrite(in)
	dir, seq := d.graphDir, d.graphNextSeq
	d.graphNextSeq++
	d.mu.Unlock()

	abort := func(err error) (bool, error) {
		d.mu.Lock()
		d.nodes.unfreeze()
		d.mu.Unlock()
		return false, err
	}
	for lo := uint32(0); lo < limit; lo += graphSnapshotBatch {
		d.mu.Lock()
		held := time.Now()
		d.nodes.snapshotSlots(in, lo, min(lo+graphSnapshotBatch, limit), include, flags, false)
		d.mu.Unlock()
		yieldGraphLock(time.Since(held))
		if d.graphStop.Load() {
			return abort(errGraphWriteStopped)
		}
	}
	if len(in.resident) == 0 {
		d.mu.Lock()
		d.nodes.unfreeze()
		d.graphFlushFloor = len(in.heldBack)
		d.mu.Unlock()
		return false, nil // everything resident is held back; nothing to flush
	}
	in.sortResident()
	tmp, written, err := writeGraphSegment(dir, d.NamespaceID, seq, full, in)
	d.graphWritten.Add(int64(written))
	if err != nil {
		return abort(err)
	}
	// Named before it is attached: a crash in between leaves a segment no checkpoint names (the
	// checkpoint open removes it) or one whose commits the replay holds anyway.
	path := filepath.Join(dir, graphSegmentName(seq))
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return abort(err)
	}
	syncGraphDir(dir)
	nb, err := openGraphBase(path, d.NamespaceID)
	if err != nil {
		os.Remove(path)
		return abort(err)
	}
	d.mu.Lock()
	fast := d.nodes.attachFlushed(nb, in)
	if !fast {
		d.nodes.unfreeze()
		d.attachLocked(nb, nil)
	}
	// What stays resident however often this runs (provisional, unlogged, shadowed): the next
	// size-triggered flush counts its threshold from there.
	d.graphFlushFloor = len(in.heldBack)
	d.mu.Unlock()
	if fast {
		d.dropFlushedIndexEntries(nb, in.resident)
	}
	return true, nil
}

// dropFlushedIndexEntries removes the flushed commits' transaction-index and eviction-set entries,
// which the segment now answers, a batch per hold of the lock. Until then they give the same
// answers the segment would: the commit each names is in the DAG, in nb.
func (d *InMemoryCommitDag) dropFlushedIndexEntries(nb *graphBase, entries []residentEntry) {
	for lo := 0; lo < len(entries); lo += graphSnapshotBatch {
		if lo > 0 {
			yieldGraphLock(graphYieldMin)
		}
		d.mu.Lock()
		for pos := lo; pos < min(lo+graphSnapshotBatch, len(entries)); pos++ {
			h := entries[pos].hash
			if d.nodes.isResident(h) {
				continue
			}
			if entries[pos].flags&entryOwnsTx != 0 {
				if tx := nb.tx(pos); d.txIndex[tx] == h {
					delete(d.txIndex, tx)
				}
			}
			delete(d.opsEvicted, h)
		}
		d.mu.Unlock()
	}
	d.mu.Lock()
	d.txIndex = shrunk(d.txIndex) // see nodeStore.attach
	d.opsEvicted = shrunk(d.opsEvicted)
	d.mu.Unlock()
}

// pickMergeLocked is the next merge the policy calls for, oldest first, or nil. Must hold mu.
func (d *InMemoryCommitDag) pickMergeLocked() []*graphBase {
	segs := d.nodes.segs
	if len(segs) == 0 {
		return nil
	}
	// A segment mostly of dead records is rewritten on its own, worst first.
	var worst *graphBase
	for _, b := range segs {
		if b.count > 0 && float64(b.hiddenRecs) > GraphTombstoneRatio*float64(b.count) {
			if worst == nil || b.hiddenRecs*worst.count > worst.hiddenRecs*b.count {
				worst = b
			}
		}
	}
	if worst != nil {
		return []*graphBase{worst}
	}
	bySize := append([]*graphBase(nil), segs...)
	sort.Slice(bySize, func(a, b int) bool { return bySize[a].count < bySize[b].count })
	pick := sizeTieredBucket(bySize, d.graph.RebuildCommits)
	if pick == nil && len(segs) > d.graph.MaxSegments {
		// Too many segments for every lookup to pass, and no tier full: merge the smallest.
		pick = bySize[:min(len(bySize), graphMergeMin)]
		if len(pick) < 2 {
			pick = nil
		}
	}
	sort.Slice(pick, func(a, b int) bool { return pick[a].seq < pick[b].seq })
	return pick
}

// sizeTieredBucket is Cassandra's SizeTieredCompactionStrategy bucketing over record counts: sizes
// within [low, high] of a bucket's mean join it, sizes below minSize all share one, and the
// smallest bucket of at least graphMergeMin is merged, at most graphMergeMax of it, smallest first.
// segs must be sorted by count.
func sizeTieredBucket(segs []*graphBase, minSize int) []*graphBase {
	type bucket struct {
		members []*graphBase
		mean    float64
	}
	var buckets []*bucket
	for _, b := range segs {
		size := float64(b.count)
		var into *bucket
		for _, bk := range buckets {
			if (size >= graphBucketLow*bk.mean && size <= graphBucketHigh*bk.mean) ||
				(b.count < minSize && bk.mean < float64(minSize)) {
				into = bk
				break
			}
		}
		if into == nil {
			into = &bucket{}
			buckets = append(buckets, into)
		}
		into.mean = (into.mean*float64(len(into.members)) + size) / float64(len(into.members)+1)
		into.members = append(into.members, b)
	}
	for _, bk := range buckets { // ascending mean, since segs are sorted
		if len(bk.members) >= graphMergeMin {
			return append([]*graphBase(nil), bk.members[:min(len(bk.members), graphMergeMax)]...)
		}
	}
	return nil
}

// mergeOnce runs the next merge the policy calls for, if any, and reports whether it ran.
//
// File order (crash safety without a new file kind - see the plan doc): the output is written under
// a temporary name and attached in memory; every input no checkpoint names is deleted; then the
// output takes its final name if none of its inputs is left on disk, or waits as .pending until a
// checkpoint has named the output and the inputs have gone. So the segments on disk never share a
// commit, for this reader or an older one.
func (d *InMemoryCommitDag) mergeOnce() (bool, error) {
	d.graphMergeMu.Lock()
	defer d.graphMergeMu.Unlock()

	d.mu.Lock()
	if !d.graphWritableLocked() {
		d.mu.Unlock()
		return false, nil
	}
	inputs := d.pickMergeLocked()
	if len(inputs) == 0 {
		d.mu.Unlock()
		return false, nil
	}
	in := &graphWriteInput{stop: &d.graphStop, throttle: graphMergeThroughput}
	d.nodes.segmentsForWrite(in, inputs)
	epoch := d.nodes.genEpoch
	dir, seq := d.graphDir, d.graphNextSeq
	d.graphNextSeq++
	d.mu.Unlock()
	defer in.release()
	if len(in.inputs) != len(inputs) {
		return false, fmt.Errorf("%w: a merge input could not be mapped", errGraphFile)
	}

	tmp, written, err := writeGraphSegment(dir, d.NamespaceID, seq, false, in)
	d.graphWritten.Add(int64(written))
	if err != nil {
		return false, err
	}
	nb, err := openGraphBase(tmp, d.NamespaceID)
	if err != nil {
		os.Remove(tmp)
		return false, err
	}
	nb.path = filepath.Join(dir, graphSegmentName(seq))

	d.mu.Lock()
	if !d.nodes.attachMerged(nb, inputs, epoch) {
		d.attachLocked(nb, inputs)
	}
	d.graphFiles.Lock()
	d.retireInputsLocked(nb, inputs)
	d.graphFiles.Unlock()
	d.mu.Unlock()
	syncGraphDir(dir)
	return true, nil
}

// CompactGraphFile rewrites every segment and the resident tier as one segment marked full,
// dropping the records of deleted commits - the operator's major compaction. It holds the flush
// and merge locks, so the full mark is true: nothing below its sequence is left that it does not
// supersede. A no-op where no graph directory is configured or the DAG is read-only.
func (d *InMemoryCommitDag) CompactGraphFile() error {
	d.graphFlushMu.Lock()
	defer d.graphFlushMu.Unlock()
	d.graphMergeMu.Lock()
	defer d.graphMergeMu.Unlock()

	d.mu.Lock()
	if !d.graphWritableLocked() {
		d.mu.Unlock()
		return nil
	}
	d.ensureGenerationsLocked()
	include, flags := d.graphIncludeLocked()
	in := &graphWriteInput{stop: &d.graphStop}
	// Resident commits a segment still has a hidden record for go too: every segment is an input,
	// and the merge drops hidden records, so nothing is written twice.
	d.nodes.residentForWrite(in, include, flags, true)
	inputs := append([]*graphBase(nil), d.nodes.segs...)
	d.nodes.segmentsForWrite(in, inputs)
	dir, seq := d.graphDir, d.graphNextSeq
	d.graphNextSeq++
	d.mu.Unlock()
	defer in.release()
	abort := func(err error) error {
		d.mu.Lock()
		d.nodes.unfreeze()
		d.mu.Unlock()
		return err
	}
	if len(in.inputs) != len(inputs) {
		return abort(fmt.Errorf("%w: a segment could not be mapped", errGraphFile))
	}
	in.sortResident()
	tmp, written, err := writeGraphSegment(dir, d.NamespaceID, seq, true, in)
	d.graphWritten.Add(int64(written))
	if err != nil {
		return abort(err)
	}
	path := filepath.Join(dir, graphSegmentName(seq))
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return abort(err)
	}
	nb, err := openGraphBase(path, d.NamespaceID)
	if err != nil {
		os.Remove(path)
		return abort(err)
	}
	d.mu.Lock()
	d.nodes.unfreeze()
	// Everything attached now is what this one supersedes: segment writes are serialized, so
	// nothing was added since the snapshot.
	d.attachLocked(nb, inputs)
	// After the attach, so a crash before these deletes leaves a full segment that the next open
	// recognises as superseding them. Unlinking a mapped file is safe: the mapping keeps the
	// inode until the snapshot's reference is released.
	d.graphFiles.Lock()
	d.retireInputsLocked(nb, inputs)
	d.graphFiles.Unlock()
	d.mu.Unlock()
	syncGraphDir(dir)
	return nil
}

// retireInputsLocked settles the files of a write's inputs: one the checkpoint on disk (or one
// being written) names waits in graphDeferred, the rest are deleted, and nb - unless it is full,
// whose mark already supersedes them - waits as .pending while any of them (or anything they
// waited on) is still on disk. Must hold mu and graphFiles.
func (d *InMemoryCommitDag) retireInputsLocked(nb *graphBase, inputs []*graphBase) {
	var waits []uint64
	for _, b := range inputs {
		waits = append(waits, b.waitsOn...)
		if d.graphPinned[b.seq] {
			d.graphDeferred = append(d.graphDeferred, deferredSegment{seq: b.seq, path: b.path, diskPath: b.diskPath, waitsOn: b.waitsOn})
			waits = append(waits, b.seq)
			continue
		}
		os.Remove(b.diskPath)
	}
	nb.waitsOn = waits
	d.publishPendingLocked(nb)
}

// publishPendingLocked gives every file waiting on others (nb included, if it is not attached yet
// - pass nil otherwise) its final name once none of the files it waits on is on disk: from then
// it shares no record with any other segment there. A full segment keeps its list only for whoever
// merges it next. Must hold mu and graphFiles.
func (d *InMemoryCommitDag) publishPendingLocked(nb *graphBase) {
	onDisk := make(map[uint64]bool, len(d.nodes.segs)+len(d.graphDeferred))
	for _, b := range d.nodes.segs {
		onDisk[b.seq] = true
	}
	for _, ds := range d.graphDeferred {
		onDisk[ds.seq] = true
	}
	settle := func(waitsOn *[]uint64, diskPath *string, path string, full bool) {
		kept := (*waitsOn)[:0]
		for _, s := range *waitsOn {
			if onDisk[s] {
				kept = append(kept, s)
			}
		}
		*waitsOn = kept
		if len(kept) > 0 && !full {
			if *diskPath != path+graphPendingSuffix {
				if err := os.Rename(*diskPath, path+graphPendingSuffix); err == nil {
					*diskPath = path + graphPendingSuffix
				}
			}
			return
		}
		if *diskPath != path {
			if err := os.Rename(*diskPath, path); err == nil {
				*diskPath = path
			}
		}
	}
	seen := false
	for _, b := range d.nodes.segs {
		settle(&b.waitsOn, &b.diskPath, b.path, b.full)
		seen = seen || b == nb
	}
	if nb != nil && !seen {
		settle(&nb.waitsOn, &nb.diskPath, nb.path, nb.full)
	}
	for i := range d.graphDeferred {
		ds := &d.graphDeferred[i]
		settle(&ds.waitsOn, &ds.diskPath, ds.path, false)
	}
}

type deferredSegment struct {
	seq      uint64
	path     string // its final name
	diskPath string // where it is: path, or path+graphPendingSuffix
	waitsOn  []uint64
}

// attachLocked makes nb the newest segment and drops the transaction-index entries the segments
// now answer, checking every record (nodeStore.attach). For an open, a major compaction, and any
// write whose fast attach could not be sure of itself. Must hold mu exclusively.
func (d *InMemoryCommitDag) attachLocked(nb *graphBase, replaced []*graphBase) {
	d.nodes.attach(nb, replaced)
	for tx, h := range d.txIndex {
		if d.nodes.isResident(h) {
			continue
		}
		if got, ok := d.segmentTxLocked(tx); ok && got == h {
			delete(d.txIndex, tx)
		}
	}
	d.txIndex = shrunk(d.txIndex) // see nodeStore.attach
	// Segment records carry whether a commit's operations must be loaded, so the eviction set
	// keeps entries for the resident tier only.
	for h := range d.opsEvicted {
		if !d.nodes.isResident(h) && d.nodes.has(h) {
			delete(d.opsEvicted, h)
		}
	}
	d.opsEvicted = shrunk(d.opsEvicted)
	// The segments answer for the same graph, so ancestry memos stay valid; the head snapshot
	// holds a copy and is unaffected. Nothing to republish.
}

// segmentTxLocked resolves a transaction id through the segments, newest first. The bloom filters
// answer the usual case - a new transaction id no segment has - without a search.
func (d *InMemoryCommitDag) segmentTxLocked(tx codec.UUID) (codec.Hash, bool) {
	key := bloomKeyTx(tx)
	for _, b := range d.nodes.segs {
		if !b.bloom.has(key) {
			continue
		}
		if h, ok := b.findTx(tx); ok {
			return h, true
		}
	}
	return codec.Hash{}, false
}

// CloseGraphFile stops any background write and unmaps every segment. The DAG must not be used
// afterwards: commits that lived only in segments are no longer reachable.
func (d *InMemoryCommitDag) CloseGraphFile() {
	d.StopGraphWork()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nodes.detach()
}

// GraphFileStatus describes the on-disk graph as this DAG sees it.
type GraphFileStatus struct {
	Segments        int   // mapped segment files
	FileCommits     int   // commits the segments answer for
	HiddenRecords   int   // segment records for commits since deleted; dropped by a rewrite
	ResidentCommits int   // commits held in memory
	FileBytes       int64 // total size of the segment files
	Pending         int   // segments (attached or superseded) waiting for a checkpoint to take their final name
	RecordsWritten  int64 // segment records written since the DAG was made: flushes, merges, rewrites
}

// GraphFileStats reports the on-disk graph's shape.
func (d *InMemoryCommitDag) GraphFileStats() GraphFileStatus {
	d.mu.RLock()
	defer d.mu.RUnlock()
	st := GraphFileStatus{
		Segments:        len(d.nodes.segs),
		FileCommits:     d.nodes.segVisible(),
		HiddenRecords:   len(d.nodes.hidden),
		ResidentCommits: d.nodes.residentLen(),
		RecordsWritten:  d.graphWritten.Load(),
	}
	for _, b := range d.nodes.segs {
		st.FileBytes += b.fileBytes
		if b.diskPath != b.path {
			st.Pending++
		}
	}
	d.graphFiles.Lock()
	for _, ds := range d.graphDeferred {
		if ds.diskPath != ds.path {
			st.Pending++
		}
	}
	d.graphFiles.Unlock()
	return st
}
