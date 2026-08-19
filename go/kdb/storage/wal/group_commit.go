package wal

import "sync"

// GroupCommitter coalesces concurrent fsync requests into as few physical
// sync calls as possible while preserving the guarantee that SyncTo(seq)
// only returns once every write up to and including seq is durable.
//
// Correctness relies on the caller only requesting SyncTo(seq) with a seq
// obtained from a WAL Append call that has already returned - i.e. the
// write itself is already visible to the platform IO layer before we ask
// to have it synced. Waiters that join while a sync is already physically
// in flight are deferred to the *next* round rather than folded into the
// in-flight one, since their append may not have happened-before that
// fsync call started.
type GroupCommitter struct {
	mu        sync.Mutex
	syncedSeq int64
	inFlight  bool
	waiters   []gcWaiter
}

type gcWaiter struct {
	seq int64
	ch  chan error
}

// NewGroupCommitter returns an idle committer.
func NewGroupCommitter() *GroupCommitter {
	return &GroupCommitter{}
}

// SyncTo blocks until the WAL is durably synced through at least seq,
// calling doSync at most once per outstanding batch of waiters.
func (g *GroupCommitter) SyncTo(seq int64, doSync func() error) error {
	g.mu.Lock()
	if g.syncedSeq >= seq {
		g.mu.Unlock()
		return nil
	}
	ch := make(chan error, 1)
	g.waiters = append(g.waiters, gcWaiter{seq: seq, ch: ch})
	if !g.inFlight {
		g.inFlight = true
		go g.runRounds(doSync)
	}
	g.mu.Unlock()
	return <-ch
}

// runRounds drains g.waiters in rounds until empty, doing exactly one
// doSync call per round. Waiters registered before a round's snapshot is
// taken are guaranteed covered by that round's fsync; anyone who joins
// while a round is in flight lands in the next round instead.
func (g *GroupCommitter) runRounds(doSync func() error) {
	for {
		g.mu.Lock()
		batch := g.waiters
		g.waiters = nil
		g.mu.Unlock()

		if len(batch) == 0 {
			g.mu.Lock()
			g.inFlight = false
			g.mu.Unlock()
			return
		}

		err := doSync()

		var maxSeq int64
		for _, w := range batch {
			if w.seq > maxSeq {
				maxSeq = w.seq
			}
		}

		g.mu.Lock()
		if err == nil && maxSeq > g.syncedSeq {
			g.syncedSeq = maxSeq
		}
		g.mu.Unlock()

		for _, w := range batch {
			w.ch <- err
		}
	}
}

// SyncedSeq reports the highest WAL sequence known durable so far.
func (g *GroupCommitter) SyncedSeq() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.syncedSeq
}
