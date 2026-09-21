package embed

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// TxnCoordinator decides cross-namespace groups for one host, and answers - during replay - whether
// a group a namespace's log mentions committed.
//
// # Epochs
//
// Decisions are kept one file per *epoch*. A writer starts an epoch lazily, on its first group,
// and ends it one of two ways:
//
//   - Sealed: nothing in flight and nothing failed, at a clean Host.Close or once the file passes
//     decisionRollThreshold. Sealing deletes the file, and the absence of a file for an epoch is
//     itself the statement "every group of this epoch committed" - so a clean shutdown leaves no
//     decision state behind at all, and a long-running writer's file stays bounded.
//   - Dead: the writer went away any other way. The next writer to open the host renames the file
//     to .dead, which is the one case where the file must be kept - it is the only record of which
//     of that epoch's groups committed and which were cut off mid-flight.
//
// A process that never runs a cross-namespace group never creates the txn directory.
type TxnCoordinator struct {
	dataRoot string
	readOnly bool
	syncMode storio.SyncMode
	// hostID names this data root in every group marker it writes, so a part that travels to
	// another data root (peer sync) is never judged by that root's unrelated decision log. Empty
	// until the first epoch assigns one; see localHost.
	hostID string

	mu sync.Mutex
	// epoch and log are zero/nil until the first group starts an epoch, and again after a seal.
	epoch uint64
	log   *decisionLog
	// committed are this epoch's durably decided groups, for resolving parts this same process
	// replays (a namespace closed and reopened while the host stays up).
	committed map[codec.UUID]struct{}
	// inflight are groups that have begun and not finished. A roll or a seal needs none.
	inflight map[codec.UUID]*TxnGroup
	// failed is set when any group of this epoch failed after publishing: its parts are on disk
	// with no decision, so the epoch must not be sealed - sealing would declare them committed.
	failed bool
	closed bool
	// dead caches the decision sets of dead epochs, which never change.
	dead map[uint64]map[codec.UUID]struct{}

	// beforeDecision, when set by a test, runs after a group's parts are durable and before its
	// decision is queued - the one instant a crash leaves parts on disk with no decision.
	beforeDecision func(group codec.UUID)
	// failDecision, when set by a test, is returned in place of queuing a decision: a decision
	// log write failure, the one post-publication failure a test can provoke on demand.
	failDecision error
	// rollThreshold is the decision file size past which an idle epoch is sealed and a new one
	// started. decisionRollThreshold unless a test lowers it.
	rollThreshold int64
}

// SetDecisionFailureForTest makes every later decision fail with err, as a decision log that could
// not be written would. Never set in production.
func (c *TxnCoordinator) SetDecisionFailureForTest(err error) {
	c.mu.Lock()
	c.failDecision = err
	c.mu.Unlock()
}

// SetRollThresholdForTest lowers the size at which an epoch rolls, so a test can reach it with a
// handful of groups. Never set in production.
func (c *TxnCoordinator) SetRollThresholdForTest(bytes int64) {
	c.mu.Lock()
	c.rollThreshold = bytes
	c.mu.Unlock()
}

// NewMemoryTxnCoordinator returns a coordinator with no durable state, for runtimes that have
// none either: decisions are immediate and nothing is written.
func NewMemoryTxnCoordinator() *TxnCoordinator {
	return &TxnCoordinator{
		committed: make(map[codec.UUID]struct{}),
		inflight:  make(map[codec.UUID]*TxnGroup),
		dead:      make(map[uint64]map[codec.UUID]struct{}),

		rollThreshold: decisionRollThreshold,
	}
}

// openTxnCoordinator binds a coordinator to dataRoot. A writer - which holds the data root's
// exclusive lock, so no other writer is alive - marks every epoch left open by a previous writer
// dead before anything replays.
func openTxnCoordinator(dataRoot string, readOnly bool, syncMode storio.SyncMode) (*TxnCoordinator, error) {
	c := NewMemoryTxnCoordinator()
	c.dataRoot = dataRoot
	c.readOnly = readOnly
	c.syncMode = syncMode
	if dataRoot == "" {
		return c, nil
	}
	host, err := readHostID(dataRoot)
	if err != nil {
		return nil, err
	}
	c.hostID = host
	if readOnly {
		return c, nil
	}
	entries, err := os.ReadDir(txnDir(dataRoot))
	if errors.Is(err, os.ErrNotExist) {
		return c, nil
	}
	if err != nil {
		return nil, err
	}
	renamed := false
	for _, e := range entries {
		epoch, suffix, ok := parseDecisionFileName(e.Name())
		if !ok || suffix != decisionLiveSuffix {
			continue
		}
		if err := os.Rename(decisionPath(dataRoot, epoch, decisionLiveSuffix), decisionPath(dataRoot, epoch, decisionDeadSuffix)); err != nil {
			return nil, err
		}
		renamed = true
		log.Printf("kdb: cross-namespace epoch %d was left open by a writer that did not shut down cleanly; "+
			"groups it never decided will be rolled back as their namespaces open", epoch)
	}
	if renamed {
		if err := syncDir(txnDir(dataRoot)); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// durable reports whether this coordinator writes decisions to disk.
func (c *TxnCoordinator) durable() bool { return c.dataRoot != "" }

// SetBeforeDecisionHookForTest installs a hook run after a group's parts are durable and before
// its decision is queued. Tests use it to capture the data directory at exactly the moment a
// crash would leave a group undecided. Never set in production.
func (c *TxnCoordinator) SetBeforeDecisionHookForTest(fn func(group codec.UUID)) {
	c.mu.Lock()
	c.beforeDecision = fn
	c.mu.Unlock()
}

// Epoch reports the current epoch, 0 when none is open.
func (c *TxnCoordinator) Epoch() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epoch
}

// Begin starts a group over the given namespaces. Nothing is written until a part is added; a
// group that turns out to have nothing to publish must be Abandoned.
func (c *TxnCoordinator) Begin(namespaces []string) (*TxnGroup, error) {
	if c.readOnly {
		return nil, ErrReadOnly
	}
	id, err := codec.RandomUUID()
	if err != nil {
		return nil, err
	}
	parts := append([]string(nil), namespaces...)
	sort.Strings(parts)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrDecisionLogClosed
	}
	if c.durable() && c.log == nil {
		if err := c.startEpochLocked(); err != nil {
			return nil, fmt.Errorf("kdb: starting a cross-namespace epoch: %w", err)
		}
	}
	g := &TxnGroup{
		c:        c,
		ID:       id,
		Epoch:    c.epoch,
		host:     c.hostID,
		parts:    parts,
		enqueued: make(chan struct{}),
		done:     make(chan struct{}),
	}
	c.inflight[id] = g
	return g, nil
}

// startEpochLocked opens a new epoch: one past both the recorded epoch and any epoch that has a
// file, so a lost EPOCH file can never cause an epoch number to be reused.
func (c *TxnCoordinator) startEpochLocked() error {
	dir := txnDir(c.dataRoot)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	next, err := readEpochFile(c.dataRoot)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if epoch, _, ok := parseDecisionFileName(e.Name()); ok && epoch > next {
			next = epoch
		}
	}
	if c.epoch > next {
		next = c.epoch
	}
	next++
	if c.hostID == "" {
		id, err := codec.RandomUUID()
		if err != nil {
			return err
		}
		if err := writeHostID(c.dataRoot, id.String()); err != nil {
			return err
		}
		c.hostID = id.String()
	}
	// EPOCH first: were the decision file created first and the process to die before EPOCH
	// was written, the scan above would still find the file, so either order is safe - but
	// recording the number before using it is the order that needs no such argument.
	if err := writeEpochFile(c.dataRoot, next); err != nil {
		return err
	}
	l, err := createDecisionLog(decisionPath(c.dataRoot, next, decisionLiveSuffix), next, c.syncMode)
	if err != nil {
		return err
	}
	c.epoch = next
	c.log = l
	c.committed = make(map[codec.UUID]struct{})
	c.failed = false
	return nil
}

// decide queues g's decision record. The returned wait reports when it is durable.
func (c *TxnCoordinator) decide(g *TxnGroup) (wait func() error, err error) {
	c.mu.Lock()
	hook := c.beforeDecision
	c.mu.Unlock()
	if hook != nil {
		hook(g.ID)
	}
	c.mu.Lock()
	l := c.log
	epoch := c.epoch
	closed := c.closed
	injected := c.failDecision
	c.mu.Unlock()
	if injected != nil {
		return nil, injected
	}
	if !c.durable() {
		return func() error { return nil }, nil
	}
	if closed || l == nil || epoch != g.Epoch {
		// Epochs only roll or seal with nothing in flight, so a live group's epoch is always the
		// open one; anything else means the host closed underneath it.
		return nil, ErrDecisionLogClosed
	}
	return l.enqueue(g.ID)
}

// finished records a group's outcome and, when it leaves nothing in flight, rolls an epoch whose
// file has grown past the threshold.
func (c *TxnCoordinator) finished(g *TxnGroup, committed, published bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.inflight, g.ID)
	if committed {
		if g.Epoch == c.epoch {
			c.committed[g.ID] = struct{}{}
		}
	} else if published {
		c.failed = true
	}
	if len(c.inflight) == 0 && !c.failed && c.log != nil && c.log.size.Load() >= c.rollThreshold {
		if err := c.sealLocked(); err != nil {
			log.Printf("kdb: could not seal cross-namespace epoch %d (%v); it stays open", c.epoch, err)
		}
	}
}

// sealLocked ends the current epoch as "every group committed" by deleting its file. Only valid
// with nothing in flight and nothing failed.
func (c *TxnCoordinator) sealLocked() error {
	if c.log == nil {
		return nil
	}
	if err := c.log.close(); err != nil {
		c.log = nil
		c.failed = true
		return err
	}
	path := c.log.path
	c.log = nil
	if err := os.Remove(path); err != nil {
		return err
	}
	return syncDir(txnDir(c.dataRoot))
}

// close ends this coordinator's epoch: sealed when that is safe, otherwise left for the next
// writer to mark dead.
func (c *TxnCoordinator) close() error {
	// Let groups already past their gates finish first. Host.Close runs this after every
	// namespace has closed, and a namespace's close drains its commit log - so their parts are on
	// disk and all they still need is this log. Closing it under them would fail groups whose
	// every part is durable, on nothing more than a clean shutdown. Bounded, because a group
	// stuck on something else must not hold the process open.
	deadline := time.Now().Add(closeDrainTimeout)
	c.mu.Lock()
	for len(c.inflight) > 0 && time.Now().Before(deadline) && !c.closed {
		c.mu.Unlock()
		time.Sleep(time.Millisecond)
		c.mu.Lock()
	}
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.log == nil {
		return nil
	}
	if len(c.inflight) == 0 && !c.failed {
		return c.sealLocked()
	}
	err := c.log.close()
	c.log = nil
	return err
}

// closeDrainTimeout bounds how long close waits for in-flight groups.
const closeDrainTimeout = 5 * time.Second

// partDecision is what replay should do with one participant commit.
type partDecision int

const (
	partCommitted partDecision = iota
	partAborted
	// partHeld: undecided, and a live writer may still decide it. Skip it and everything that
	// descends from it for now; a later replay reconsiders.
	partHeld
)

// resolvePart classifies a commit read back from a namespace log. isPart is false for every
// ordinary commit.
func (c *TxnCoordinator) resolvePart(commit document.Commit) (decision partDecision, isPart bool) {
	m, ok := ParseGroupMarker(commit.Message)
	if !ok {
		return partCommitted, false
	}
	if c == nil || !c.durable() {
		// Nothing to consult: a memory runtime has no log to replay, and a caller that replays
		// without a coordinator (auth namespaces, tools) gets the pre-group behaviour.
		return partCommitted, true
	}
	if m.Host != c.localHost() {
		// Decided by another data root's log, not this one's - a part that arrived by peer sync.
		// Whatever epoch number it carries means nothing here.
		return partCommitted, true
	}
	c.mu.Lock()
	if !c.readOnly && c.log != nil && m.Epoch == c.epoch {
		_, done := c.committed[m.Group]
		_, running := c.inflight[m.Group]
		c.mu.Unlock()
		switch {
		case done:
			return partCommitted, true
		case running:
			return partHeld, true
		default:
			return partAborted, true
		}
	}
	cached, isCached := c.dead[m.Epoch]
	c.mu.Unlock()
	if isCached {
		return presence(cached, m.Group, partAborted), true
	}

	// The live name first, then the dead one: a writer opening concurrently renames .log to
	// .dead atomically, so checking in this order can see the file under one name or the other
	// but never miss it between the two - whereas the reverse order could look for .dead just
	// before the rename and for .log just after it, and read a live epoch as sealed.
	if set, err := readDecisions(decisionPath(c.dataRoot, m.Epoch, decisionLiveSuffix)); err == nil {
		// A live epoch this coordinator does not own: for a reader, the writer's current epoch,
		// whose undecided groups may still commit. A writer never finds one - it renamed every
		// .log it did not own at open - so for a writer this is a dead epoch like any other.
		absent := partHeld
		if !c.readOnly {
			absent = partAborted
		}
		return presence(set, m.Group, absent), true
	} else if !errors.Is(err, os.ErrNotExist) {
		log.Printf("kdb: could not read cross-namespace decisions for epoch %d (%v); holding group %s", m.Epoch, err, m.Group)
		return partHeld, true
	}
	if set, err := readDecisions(decisionPath(c.dataRoot, m.Epoch, decisionDeadSuffix)); err == nil {
		c.mu.Lock()
		c.dead[m.Epoch] = set
		c.mu.Unlock()
		return presence(set, m.Group, partAborted), true
	} else if !errors.Is(err, os.ErrNotExist) {
		log.Printf("kdb: could not read cross-namespace decisions for epoch %d (%v); holding group %s", m.Epoch, err, m.Group)
		return partHeld, true
	}
	// No file at all: the epoch was sealed.
	return partCommitted, true
}

// localHost is this data root's host id. A reader re-checks for one it has not seen yet: the
// writer assigns it lazily, at its first cross-namespace transaction, which may be after the
// reader attached.
func (c *TxnCoordinator) localHost() string {
	c.mu.Lock()
	host := c.hostID
	c.mu.Unlock()
	if host != "" || !c.readOnly {
		return host
	}
	if id, err := readHostID(c.dataRoot); err == nil && id != "" {
		c.mu.Lock()
		c.hostID = id
		c.mu.Unlock()
		return id
	}
	return ""
}

func presence(set map[codec.UUID]struct{}, id codec.UUID, absent partDecision) partDecision {
	if _, ok := set[id]; ok {
		return partCommitted
	}
	return absent
}
