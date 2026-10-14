package server

import (
	"bytes"
	"container/list"
	"context"
	"sync"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Fan-out sharing (memory/Cassandra plan, B8).
//
// Each subscriber still has its own sendLoop, position and pull-based catch-up: nothing here
// makes one subscriber wait for another's socket. What is shared is the work that depends only
// on the commit and the filter, not on who is reading:
//
//   - a commit's operations, loaded once into a small LRU (fanoutCache) instead of once per
//     subscriber;
//   - per distinct filter text, the filtered view of those operations (each document's filter
//     verdict) and its frames, encoded once with correlation id 0.
//
// Per-subscriber work is what must stay per subscriber: the read check of every write in the
// view against the subscriber's own principal, and the correlation id. When the check denies
// nothing - every subscriber of an anonymous or unrestricted hub, and most subscribers under
// RBAC - the shared frames are sent with only the id patched (wire.SetCorrelationID). When it
// denies something, that subscriber's frames are encoded for it alone, exactly as before.

const (
	// fanoutCacheMaxBytes bounds what the cache holds: operations plus encoded frames.
	fanoutCacheMaxBytes = 16 << 20
	// fanoutCacheMaxEntries bounds the number of commits, so a run of tiny commits cannot grow the
	// map without limit. Twice the per-commit catch-up reach: a subscriber walking its whole chain
	// finds the commits a faster one has just loaded.
	fanoutCacheMaxEntries = 2 * maxPerCommitCatchUp
	// fanoutEntryOverhead is a commit's bookkeeping cost on top of its operations' bytes.
	fanoutEntryOverhead = 256
)

// fanoutCache is a byte-bounded LRU of recent commits, keyed by hash. A commit's hash names its
// content, so an entry never goes stale; eviction only costs a reload.
type fanoutCache struct {
	mu     sync.Mutex
	bytes  int
	order  *list.List // of *fanoutEntry, most recently used first
	byHash map[codec.Hash]*list.Element
}

func newFanoutCache() *fanoutCache {
	return &fanoutCache{order: list.New(), byHash: map[codec.Hash]*list.Element{}}
}

// fanoutEntry is one commit with its operations, and its views by filter text.
type fanoutEntry struct {
	commit document.Commit // Operations loaded
	bytes  int             // charged to the cache while resident (under fanoutCache.mu)
	// resident is false once evicted (under fanoutCache.mu): a view computed after that is not
	// charged, since the cache no longer holds it.
	resident bool

	mu    sync.Mutex
	views map[string]*fanoutView
}

// fanoutView is a commit's operations as seen through one filter, and those operations encoded
// as the frames of a delta from the commit's first parent - with correlation id 0, to be patched.
type fanoutView struct {
	once   sync.Once
	ops    []document.Op
	frames [][]byte
	err    error
}

// get returns the entry for hash, loading it with load on a miss. Concurrent misses on one hash
// may both load; the first to insert wins, which costs a duplicate load, never a wrong result.
func (c *fanoutCache) get(hash codec.Hash, load func() (document.Commit, error)) (*fanoutEntry, error) {
	c.mu.Lock()
	if el, ok := c.byHash[hash]; ok {
		c.order.MoveToFront(el)
		e := el.Value.(*fanoutEntry)
		c.mu.Unlock()
		return e, nil
	}
	c.mu.Unlock()
	commit, err := load()
	if err != nil {
		return nil, err
	}
	e := &fanoutEntry{commit: commit, bytes: fanoutEntryOverhead + opsBytes(commit.Operations)}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.byHash[hash]; ok {
		c.order.MoveToFront(el)
		return el.Value.(*fanoutEntry), nil
	}
	if e.bytes > fanoutCacheMaxBytes {
		return e, nil // too big to keep: served to this caller, not cached
	}
	e.resident = true
	c.byHash[hash] = c.order.PushFront(e)
	c.bytes += e.bytes
	c.evictLocked()
	return e, nil
}

// charge adds n bytes (a view's frames) to e's share of the cache, if it is still held.
func (c *fanoutCache) charge(e *fanoutEntry, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !e.resident {
		return
	}
	e.bytes += n
	c.bytes += n
	c.evictLocked()
}

func (c *fanoutCache) evictLocked() {
	for c.order.Len() > 0 && (c.bytes > fanoutCacheMaxBytes || c.order.Len() > fanoutCacheMaxEntries) {
		el := c.order.Back()
		e := el.Value.(*fanoutEntry)
		c.order.Remove(el)
		delete(c.byHash, e.commit.Hash)
		c.bytes -= e.bytes
		e.resident = false
	}
}

// opsBytes approximates what ops hold in memory: their strings plus a fixed share each.
func opsBytes(ops []document.Op) int {
	n := 0
	for _, op := range ops {
		n += 64
		switch o := op.(type) {
		case document.WriteOp:
			n += len(o.Patch)
		case document.FileWriteOp:
			n += len(o.Path)
		case document.SchemaMigrationOp:
			n += len(o.MigrationPayload)
		}
	}
	return n
}

// view returns e's operations through filter, with their frames in c's encoding, computing them
// on first use. key names the filter and the encoding together; h supplies the namespace.
func (h *StreamHub) view(e *fanoutEntry, key string, filter sql.Expr, c wire.Codec) *fanoutView {
	e.mu.Lock()
	if e.views == nil {
		e.views = map[string]*fanoutView{}
	}
	v, ok := e.views[key]
	if !ok {
		v = &fanoutView{}
		e.views[key] = v
	}
	e.mu.Unlock()
	v.once.Do(func() {
		v.ops = filterView(filter, e.commit.Operations)
		v.frames, v.err = h.encodeDelta(c, e.commit.Hash, e.commit.ParentHashes[0], e.commit.Timestamp.EpochMicros(), v.ops)
		if v.err == nil {
			n := 0
			for _, f := range v.frames {
				n += len(f)
			}
			h.fanout.charge(e, n)
		}
	})
	return v
}

// filterView is ops as a subscription with filter sees them, before any read check: a write of a
// document outside the filter becomes a delete (the subscriber may have held it), and operations
// that name no document pass only when there is no filter. Each document is parsed at most once
// here (EvalPredicate parses lazily), and once per commit per distinct filter across all the
// subscribers that share it.
func filterView(filter sql.Expr, ops []document.Op) []document.Op {
	if filter == nil {
		return ops
	}
	out := make([]document.Op, 0, len(ops))
	for _, op := range ops {
		switch o := op.(type) {
		case document.WriteOp:
			if !sql.EvalPredicate(filter, document.Document{ID: o.DocID, JSON: o.Patch}, schema.None(), nil) {
				out = append(out, document.DeleteOp{DocID: o.DocID})
				continue
			}
			out = append(out, o)
		case document.DeleteOp:
			out = append(out, o)
		}
	}
	return out
}

// encodeDelta encodes the operations taking a subscriber from parent to commit as deltaCommit
// frames, split by bytes (see sendDelta), each under correlation id 0.
func (h *StreamHub) encodeDelta(c wire.Codec, commit, parent codec.Hash, ts int64, ops []document.Op) ([][]byte, error) {
	parts := splitOpsByBytes(ops, maxDeltaFrameOpBytes)
	frames := make([][]byte, len(parts))
	for i, part := range parts {
		named := commit
		if i < len(parts)-1 {
			named = parent
		}
		frame, err := c.Encode(wire.DeltaCommitMessage{
			H: wire.Header{MessageType: wire.MsgDeltaCommit, ProtocolVersion: wire.KdbWireProtocolVersion},
			Payload: wire.DeltaCommitPayload{
				Namespace: h.namespaceID, CommitHash: named, ParentHash: parent,
				TimestampMicros: ts, Operations: part,
			},
		})
		if err != nil {
			return nil, err
		}
		frames[i] = frame
	}
	return frames, nil
}

// sendCommit sends sub the commit in e, which follows sub's position on the first-parent line.
// The filtered view and its frames are shared with every subscriber with the same filter; the
// read check is sub's own. If it denies anything, sub's frames are encoded for it alone.
func (h *StreamHub) sendCommit(sub *registeredSubscriber, e *fanoutEntry) error {
	v := h.view(e, sub.viewKey, sub.filter, sub.codec)
	if v.err != nil {
		return v.err
	}
	if sub.checkReads {
		if own, denied := h.readChecked(sub, v.ops); denied {
			frames, err := h.encodeDelta(sub.codec, e.commit.Hash, e.commit.ParentHashes[0], e.commit.Timestamp.EpochMicros(), own)
			if err != nil {
				return err
			}
			return h.sendFrames(sub, frames)
		}
	}
	return h.sendFrames(sub, v.frames)
}

// readChecked is ops with every write sub may not read turned into a delete - it may have held
// the document before, and must not keep it. denied reports whether any was; when none was, ops
// itself is returned, unchanged and uncopied.
func (h *StreamHub) readChecked(sub *registeredSubscriber, ops []document.Op) (own []document.Op, denied bool) {
	authz := h.runtime.AuthEngine.Authorizer()
	for i, op := range ops {
		w, ok := op.(document.WriteOp)
		if !ok {
			continue
		}
		if authz.Authorize(context.Background(), sub.principal, auth.DocumentReadAction{Namespace: h.namespaceID, DocID: w.DocID.String()}) == nil {
			continue
		}
		if !denied {
			own = append(make([]document.Op, 0, len(ops)), ops...)
			denied = true
		}
		own[i] = document.DeleteOp{DocID: w.DocID}
	}
	if !denied {
		return ops, false
	}
	return own, true
}

// sendFrames sends each of frames to sub under a correlation id of its own. frames may be shared
// and are never modified: each is copied before its id is patched.
func (h *StreamHub) sendFrames(sub *registeredSubscriber, frames [][]byte) error {
	synchronous := false
	if s, ok := sub.conn.(core.SynchronousSender); ok {
		synchronous = s.SendCompletesSynchronously()
	}
	for _, shared := range frames {
		cid := h.nextCorrelation()
		if !synchronous {
			// The connection may keep the slice after Send returns: give it one of its own.
			frame := bytes.Clone(shared)
			wire.SetCorrelationID(frame, cid)
			if err := sub.conn.Send(frame); err != nil {
				return err
			}
			continue
		}
		bp := fanoutSendBufs.Get().(*[]byte)
		frame := append((*bp)[:0], shared...)
		wire.SetCorrelationID(frame, cid)
		err := sub.conn.Send(frame)
		if cap(frame) <= maxPooledSendBuf {
			*bp = frame[:0]
			fanoutSendBufs.Put(bp)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// maxPooledSendBuf bounds the send buffers kept for reuse, as wire's frame pool does: a pool
// that kept every buffer would pin the largest catch-up frame ever sent.
const maxPooledSendBuf = 1 << 20

var fanoutSendBufs = sync.Pool{New: func() any { b := make([]byte, 0, 4096); return &b }}

func (h *StreamHub) nextCorrelation() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	cid := h.correlation
	h.correlation++
	return cid
}
