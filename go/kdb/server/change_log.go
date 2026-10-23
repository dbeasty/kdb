package server

import (
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// changeLog remembers, for the most recent commits made through this runtime's commit path,
// which documents each one wrote: what an immediate UPDATE needs to know under the write gate
// about the commits that landed between finding its candidates and its turn (immediateAmong).
// Reading it is a walk along the commits' parents; diffing the two document trees, which it
// replaces, cost a queue claim 0.27 ms of gate time at 32 concurrent workers, re-reading every
// changed subtree for some 30 commits. As Cassandra answers "what was written" from its commit
// log rather than by comparing SSTables.
//
// It only ever names a superset of what changed: a commit is recorded only when every one of its
// operations names the document it changes, and a walk that meets a commit not recorded - a merge,
// a peer's commits, a schema change, one older than the log holds - reports that it cannot answer,
// and the caller diffs the trees as before.
type changeLog struct {
	mu       sync.Mutex
	byCommit map[codec.Hash]changeEntry
	order    []codec.Hash
}

type changeEntry struct {
	parent codec.Hash
	docs   []codec.UUID
}

// changeLogCommits is how many recent commits the log holds.
const changeLogCommits = 4096

// record notes which documents c wrote, when it can say exactly.
func (l *changeLog) record(c document.Commit) {
	if len(c.ParentHashes) != 1 {
		return
	}
	docs := make([]codec.UUID, 0, len(c.Operations))
	for _, op := range c.Operations {
		switch o := op.(type) {
		case document.WriteOp:
			docs = append(docs, o.DocID)
		case document.DeleteOp:
			docs = append(docs, o.DocID)
		case document.AttachmentPutOp:
			docs = append(docs, o.DocID)
		case document.AttachmentDeleteOp:
			docs = append(docs, o.DocID)
		default:
			return // an operation that changes what no document id names
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.byCommit == nil {
		l.byCommit = make(map[codec.Hash]changeEntry, changeLogCommits)
	}
	if _, seen := l.byCommit[c.Hash]; seen {
		return
	}
	l.byCommit[c.Hash] = changeEntry{parent: c.ParentHashes[0], docs: docs}
	l.order = append(l.order, c.Hash)
	if len(l.order) > changeLogCommits*5/4 {
		drop := len(l.order) - changeLogCommits
		for _, h := range l.order[:drop] {
			delete(l.byCommit, h)
		}
		l.order = append(l.order[:0], l.order[drop:]...)
	}
}

// changedSince lists the documents written by the commits after from up to and including to,
// walking to's parents back to from, and false when a commit on the way is not recorded, the walk
// passes maxCommits, or more than maxDocs documents changed.
func (l *changeLog) changedSince(from, to codec.Hash, maxCommits, maxDocs int) ([]codec.UUID, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []codec.UUID
	seen := make(map[codec.UUID]struct{})
	for cur, steps := to, 0; cur != from; steps++ {
		e, ok := l.byCommit[cur]
		if !ok || steps >= maxCommits {
			return nil, false
		}
		for _, id := range e.docs {
			if _, dup := seen[id]; !dup {
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
		if len(out) > maxDocs {
			return nil, false
		}
		cur = e.parent
	}
	return out, true
}
