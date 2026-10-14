package peersync

import (
	"container/heap"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Serving a large snapshot from a sorted copy of its bodies.
//
// A snapshot page is documents in id order, and the host reads them in id order. Its storage
// does not keep them that way: the engine finds a document's body through a location record in
// the blob store and then a frame of the delta log, both keyed by things unrelated to the id, so
// every document of a page is two random disk reads once the namespace outgrows the caches. At a
// million documents that was three quarters of a bootstrap's wall time, all of it on the host.
//
// So for a large snapshot the host reads the bodies the cheap way once - the delta log front to
// back (storage.TreeBodyStreamer) - and sorts them by id on disk: runs of snapshotRunBytes, each
// sorted in memory and spooled (pageSpool), merged as pages are served. It is Cassandra's
// streaming again, from the other end: the sender streams files it can read sequentially rather
// than looking each row up.
//
// The copy is built in the background, and pages are served by point reads until it is ready, so
// no request waits for it - the first page of a 10M-document snapshot is as fast as it ever was.
// Pages after that come from the copy, through a cursor that merges the runs forward from the
// page's position. A document the copy lacks (the log does not hold its version: a truncated
// segment, a body that lives only in the blob store) is read the ordinary way, so the copy changes
// how fast a page is served, never what is in it.
//
// Memory: one run while building, and one small batch per run per cursor while serving. Disk: the
// live bodies, once, in the staging directory, until the copy has been idle for
// snapshotSourceIdle.

var (
	// snapshotSortMinDocs is the smallest snapshot served from a sorted copy. Below it point
	// reads are cheap enough (the namespace fits the caches) and a copy is not worth its disk.
	snapshotSortMinDocs = 100_000
	// snapshotRunBytes is how many bytes of bodies one sorted run holds: what building the copy
	// holds in memory at once.
	snapshotRunBytes = 32 << 20
)

const (
	// snapshotRunBatchBytes is how much of each run a cursor holds while merging: a batch of
	// documents and a read buffer of this size each.
	snapshotRunBatchBytes = 32 << 10
	// snapshotSourceIdle is how long a copy no page has used is kept.
	snapshotSourceIdle = 2 * time.Minute
	// maxSourceCursors bounds the cursors a copy keeps for reuse: one per peer paging it.
	maxSourceCursors = 4
)

type sourceKey struct {
	store  storage.Adapter
	commit codec.Hash
}

var snapshotSources = struct {
	sync.Mutex
	m map[sourceKey]*sortedBodies
}{m: map[sourceKey]*sortedBodies{}}

// sortedBodies is one snapshot's bodies, sorted by id on disk.
type sortedBodies struct {
	key  sourceKey
	done chan struct{} // closed when building ends, either way

	mu       sync.Mutex
	ready    bool
	failed   bool
	closed   bool
	runs     []*pageSpool
	idle     []*bodyCursor
	inUse    int
	lastUsed time.Time
	// cursors counts the cursors made, for tests: paging forward reuses one.
	cursors int
}

// sortedBodiesFor returns the sorted copy of the snapshot of commit, whose tree is tree, starting
// to build it if there is none. nil when the snapshot is too small to want one or storage cannot
// stream its bodies.
func sortedBodiesFor(env IngestEnv, commit codec.Hash, tree document.DocumentTree) *sortedBodies {
	streamer, ok := env.Storage.(storage.TreeBodyStreamer)
	if !ok || tree.Size() < snapshotSortMinDocs {
		return nil
	}
	key := sourceKey{store: env.Storage, commit: commit}
	snapshotSources.Lock()
	defer snapshotSources.Unlock()
	if s := snapshotSources.m[key]; s != nil {
		return s
	}
	s := &sortedBodies{key: key, done: make(chan struct{}), lastUsed: time.Now()}
	snapshotSources.m[key] = s
	go s.build(env, streamer, tree)
	time.AfterFunc(snapshotSourceIdle, s.expire)
	return s
}

// build streams the tree's bodies out of storage into sorted runs.
func (s *sortedBodies) build(env IngestEnv, streamer storage.TreeBodyStreamer, tree document.DocumentTree) {
	defer close(s.done)
	var runs []*pageSpool
	var recs []wire.SnapshotDoc
	size := 0
	spill := func() error {
		if len(recs) == 0 {
			return nil
		}
		sort.Slice(recs, func(i, j int) bool { return recs[i].DocID < recs[j].DocID })
		run, err := newPageSpool(env.StagingDir)
		if err != nil {
			return err
		}
		runs = append(runs, run)
		for _, r := range recs {
			if err := run.add(r.DocID, r.Body); err != nil {
				return err
			}
		}
		if err := run.w.Flush(); err != nil {
			return run.writeErr(err)
		}
		clear(recs)
		recs, size = recs[:0], 0
		return nil
	}
	err := streamer.StreamTreeBodies(env.NamespaceID, tree, func(id codec.UUID, body string) error {
		if s.isClosed() {
			return errSourceClosed
		}
		recs = append(recs, wire.SnapshotDoc{DocID: id.String(), Body: body})
		if size += len(body) + 64; size >= snapshotRunBytes {
			return spill()
		}
		return nil
	})
	if err == nil {
		err = spill()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil || s.closed {
		s.failed = true
		for _, r := range runs {
			r.close()
		}
		return
	}
	s.runs, s.ready = runs, true
}

type sourceClosedError struct{}

func (sourceClosedError) Error() string { return "peer sync: snapshot copy closed" }

var errSourceClosed error = sourceClosedError{}

func (s *sortedBodies) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// expire drops the copy once no page has used it for snapshotSourceIdle, and checks again later
// otherwise.
func (s *sortedBodies) expire() {
	s.mu.Lock()
	if wait := snapshotSourceIdle - time.Since(s.lastUsed); s.inUse > 0 || wait > 0 {
		s.mu.Unlock()
		time.AfterFunc(max(wait, time.Second), s.expire)
		return
	}
	s.mu.Unlock()
	s.close()
}

// close forgets the copy and releases its files. A build still running notices and stops.
func (s *sortedBodies) close() {
	snapshotSources.Lock()
	if snapshotSources.m[s.key] == s {
		delete(snapshotSources.m, s.key)
	}
	snapshotSources.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for _, r := range s.runs {
		r.close()
	}
	s.runs, s.idle = nil, nil
}

// cursorAt lends a cursor that can serve ids after after: a kept one that has not moved past it,
// or a new one. nil while the copy is not ready. Every cursor lent is given back with release.
func (s *sortedBodies) cursorAt(after string) *bodyCursor {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastUsed = time.Now()
	if !s.ready || s.closed {
		return nil
	}
	best := -1
	for i, c := range s.idle {
		if c.dropped <= after && (best < 0 || c.dropped > s.idle[best].dropped) {
			best = i
		}
	}
	var c *bodyCursor
	if best >= 0 {
		c = s.idle[best]
		s.idle = append(s.idle[:best], s.idle[best+1:]...)
	} else {
		var err error
		if c, err = newBodyCursor(s.runs); err != nil {
			return nil
		}
		s.cursors++
	}
	s.inUse++
	return c
}

// release gives a cursor back. A broken one is dropped.
func (s *sortedBodies) release(c *bodyCursor) {
	if c == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inUse--
	s.lastUsed = time.Now()
	// One that has run out of runs is kept: past the copy's last document there can still be
	// documents the copy lacks, and it answers for them as cheaply as ever.
	if s.closed || c.err != nil {
		return
	}
	s.idle = append(s.idle, c)
	if len(s.idle) > maxSourceCursors {
		s.idle = s.idle[1:]
	}
}

// bodyCursor merges a copy's runs forward in id order.
type bodyCursor struct {
	h runHeap
	// dropped is the greatest id the cursor has moved past: it can serve any id after it.
	dropped string
	err     error
}

type runReader struct {
	next  func() ([]wire.SnapshotDoc, error)
	batch []wire.SnapshotDoc
	i     int
}

func (r *runReader) cur() *wire.SnapshotDoc { return &r.batch[r.i] }

// advance moves to the next document; false at the run's end.
func (r *runReader) advance() (bool, error) {
	if r.i++; r.i < len(r.batch) {
		return true, nil
	}
	b, err := r.next()
	if err != nil || len(b) == 0 {
		return false, err
	}
	r.batch, r.i = b, 0
	return true, nil
}

type runHeap []*runReader

func (h runHeap) Len() int           { return len(h) }
func (h runHeap) Less(i, j int) bool { return h[i].cur().DocID < h[j].cur().DocID }
func (h runHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *runHeap) Push(x any)        { *h = append(*h, x.(*runReader)) }
func (h *runHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

func newBodyCursor(runs []*pageSpool) (*bodyCursor, error) {
	c := &bodyCursor{}
	for _, run := range runs {
		next, err := run.readerSized(snapshotRunBatchBytes, -1, snapshotRunBatchBytes)
		if err != nil {
			return nil, err
		}
		r := &runReader{next: next, i: -1}
		ok, err := r.advance()
		if err != nil {
			return nil, err
		}
		if ok {
			c.h = append(c.h, r)
		}
	}
	heap.Init(&c.h)
	return c, nil
}

// body returns the body of id, if the copy has it. Ids must be asked for in ascending order. A
// cursor that failed to read its runs has nothing more (err says why), and the caller reads from
// storage: the copy is an optimisation, never the only way to a document.
func (c *bodyCursor) body(id string) (string, bool) {
	if c.err != nil {
		return "", false
	}
	for len(c.h) > 0 && c.h[0].cur().DocID < id {
		c.dropped = c.h[0].cur().DocID
		ok, err := c.h[0].advance()
		if err != nil {
			c.err = err
			return "", false
		}
		if ok {
			heap.Fix(&c.h, 0)
		} else {
			heap.Pop(&c.h)
		}
	}
	if len(c.h) > 0 && c.h[0].cur().DocID == id {
		return c.h[0].cur().Body, true
	}
	return "", false
}

// pageFromCursor adds ids' documents to page, bodies from cur where it has them, until the page
// is full (true). A document the copy lacks is read from storage.
func pageFromCursor(page *wire.SnapshotPageMessage, size *int, state snapshotState, cur *bodyCursor, ids []codec.UUID, commit document.Commit, maxBytes int) (bool, error) {
	for _, id := range ids {
		sid := id.String()
		body, ok := cur.body(sid)
		if !ok {
			docs, err := state.read([]codec.UUID{id})
			if err != nil {
				return false, err
			}
			if docs[0] == nil {
				return false, fmt.Errorf("peer sync: document %s of tree %s is not readable", id, commit.DocumentTreeHash.Hex())
			}
			body = docs[0].JSON
		}
		if len(page.Docs) > 0 && *size+len(body) > maxBytes {
			page.Done = false
			page.Next = page.Docs[len(page.Docs)-1].DocID
			return true, nil
		}
		page.Docs = append(page.Docs, wire.SnapshotDoc{DocID: sid, Body: body})
		*size += len(body)
	}
	return false, nil
}
