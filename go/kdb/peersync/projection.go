package peersync

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Filtered projections: a replica that holds only the documents of a source namespace matching a
// filter, and only those its principal may read. It is not a peer of the source - it cannot
// verify the source's commits without the documents it does not hold - so it keeps the
// projection in a namespace of its own, with commits it authors, each naming the source commit it
// brings the projection up to. The source is trusted for content; the connection is what
// authenticates it.

// ProjectionNamespace is where a replica keeps the projection of ns through filter: a sibling of
// the source's name, distinct per filter.
func ProjectionNamespace(ns, filter string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(filter)))
	return ns + ".projection-" + hex.EncodeToString(sum[:4])
}

// ProjectionSource reports the source namespace a projection namespace was named for.
func ProjectionSource(ns string) (string, bool) {
	i := strings.LastIndex(ns, ".projection-")
	if i <= 0 || len(ns)-i != len(".projection-")+8 {
		return "", false
	}
	return ns[:i], true
}

const projectionPrefix = "kdb:projection/1 "

// ProjectionMessage is the commit message a projection commit carries.
func ProjectionMessage(source string, complete bool) string {
	return fmt.Sprintf("%ssource=%s complete=%t", projectionPrefix, source, complete)
}

// ParseProjectionMessage reads a projection commit message.
func ParseProjectionMessage(msg string) (source string, complete bool, ok bool) {
	if !strings.HasPrefix(msg, projectionPrefix) {
		return "", false, false
	}
	for _, f := range strings.Fields(strings.TrimPrefix(msg, projectionPrefix)) {
		k, v, _ := strings.Cut(f, "=")
		switch k {
		case "source":
			source = v
		case "complete":
			complete = v == "true"
		}
	}
	return source, complete, source != ""
}

// projectPage computes one page of the projection of env's namespace through filter, from the
// source commit fromHex to atHex. canRead drops documents the requesting principal may not read -
// as deletes, so a document it loses access to leaves its projection.
func projectPage(env IngestEnv, filter sql.Expr, canRead func(codec.UUID) bool, fromHex, atHex, after string, maxBytes int) (wire.ProjectPageMessage, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultPageBytes
	}
	at, err := env.DAG.Head()
	if err != nil {
		return wire.ProjectPageMessage{}, err
	}
	if atHex != "" {
		if at, err = codec.HashFromHex(atHex); err != nil {
			return wire.ProjectPageMessage{}, err
		}
	}
	atCommit, err := env.DAG.GetCommitOrThrow(at)
	if err != nil {
		return wire.ProjectPageMessage{}, err
	}
	page := wire.ProjectPageMessage{Namespace: env.NamespaceID, AtHex: at.Hex(), Done: true}
	matches := func(d document.Document) bool {
		return sql.EvalPredicate(filter, d, schema.None(), nil) && (canRead == nil || canRead(d.ID))
	}
	reset := true
	var from codec.Hash
	if fromHex != "" {
		if from, err = codec.HashFromHex(fromHex); err == nil && env.DAG.HasCommit(from) &&
			(from == at || env.DAG.IsAncestor(from, at)) {
			reset = false
		}
	}
	page.Reset = reset
	pp := projectionPager{env: env, filter: filter, matches: matches, page: page, after: after, maxBytes: maxBytes}
	walker, ok := env.Storage.(storage.TreeWalker)
	if !ok {
		return wire.ProjectPageMessage{}, fmt.Errorf("peer sync: storage for %s cannot serve projections", env.NamespaceID)
	}
	// The seeking and diffing pagers need the commits' trees; when a tree cannot be resolved they
	// report !ok and the original walk-and-sort or replay pager serves the page.
	if reset {
		if out, ok, err := pp.resetSeek(walker, atCommit); ok || err != nil {
			return out, err
		}
		return pp.resetScan(walker, atCommit)
	}
	fromCommit, err := env.DAG.GetCommitOrThrow(from)
	if err != nil {
		return wire.ProjectPageMessage{}, err
	}
	if out, ok, err := pp.deltaDiff(walker, fromCommit, atCommit); ok || err != nil {
		return out, err
	}
	return pp.deltaReplay(at, fromCommit)
}

// projectionPager builds one PROJECT_PAGE.
type projectionPager struct {
	env      IngestEnv
	filter   sql.Expr
	matches  func(document.Document) bool
	page     wire.ProjectPageMessage
	after    string
	maxBytes int
	size     int
}

func (pp *projectionPager) full() bool { return pp.size >= pp.maxBytes }

func (pp *projectionPager) write(id codec.UUID, body string) {
	pp.page.Writes = append(pp.page.Writes, wire.SnapshotDoc{DocID: id.String(), Body: body})
	pp.size += len(body)
}

func (pp *projectionPager) delete(id codec.UUID) {
	pp.page.Deletes = append(pp.page.Deletes, id.String())
	pp.size += 40
}

// resetSeek serves a reset page by seeking the target's trie to the cursor and reading forward,
// as snapshotPageSeek does: the work is the page plus the trie's depth, not the namespace. The
// original pager walked and sorted every id after the cursor for every page - O(D) per page and
// O(D^2/page) for a filtered bootstrap.
func (pp *projectionPager) resetSeek(walker storage.TreeWalker, atCommit document.Commit) (wire.ProjectPageMessage, bool, error) {
	cursor, ok := snapshotCursor(pp.after)
	if !ok {
		return wire.ProjectPageMessage{}, false, nil
	}
	// A filter that confines the projection to an id range (kdb_id >= ... AND kdb_id < ...) is a
	// seek to the range's start and a stop at its end: a replica bootstrapping one slice of a
	// namespace pays for that slice, not for the namespace.
	bounds := filterIDRange(pp.filter)
	if start, ok := bounds.seekFrom(); ok && (cursor == nil || uuidLess(*cursor, start)) {
		cursor = &start
	}
	state, err := stateAt(pp.env, walker, atCommit)
	if err != nil || state.tree == nil {
		return wire.ProjectPageMessage{}, false, err
	}
	tree, ok, err := state.tree()
	if err != nil || !ok {
		return wire.ProjectPageMessage{}, false, err
	}
	ids := make([]codec.UUID, 0, snapshotChunk)
	collect := func(id codec.UUID, _ codec.Hash) bool {
		ids = append(ids, id)
		return len(ids) < snapshotChunk
	}
	last := pp.after // the last id this page has passed over
	for {
		ids = ids[:0]
		if cursor == nil {
			tree.WalkOrdered(collect)
		} else {
			tree.WalkAfter(*cursor, collect)
		}
		if len(ids) == 0 {
			return pp.page, true, nil
		}
		docs, err := state.read(ids)
		if err != nil {
			return wire.ProjectPageMessage{}, true, err
		}
		for i, d := range docs {
			if pp.full() {
				pp.page.Done, pp.page.Next = false, last
				return pp.page, true, nil
			}
			if bounds.beyond(ids[i].String()) {
				return pp.page, true, nil
			}
			if d != nil && pp.matches(*d) {
				pp.write(d.ID, d.JSON)
			}
			last = ids[i].String()
		}
		next := ids[len(ids)-1]
		cursor = &next
	}
}

// idRange is the range of document ids a filter confines a projection to: lo inclusive, hi
// inclusive or not; "" is unbounded. Only bounds that are prefixes of canonical ids are used, since
// only for those does string order agree with the trie's byte order.
type idRange struct {
	lo, hi      string
	hiInclusive bool
}

var canonicalIDPrefix = regexp.MustCompile(`^[0-9a-f]{0,8}(-[0-9a-f]{0,4}(-[0-9a-f]{0,4}(-[0-9a-f]{0,4}(-[0-9a-f]{0,12})?)?)?)?$`)

func idLiteral(e sql.Expr) (string, bool) {
	lit, ok := e.(sql.ExprLiteral)
	if !ok {
		return "", false
	}
	s, ok := lit.Cell.(sql.CellString)
	if !ok || !canonicalIDPrefix.MatchString(s.Value) {
		return "", false
	}
	return s.Value, true
}

func isIDColumn(e sql.Expr) bool {
	c, ok := e.(sql.ExprColumnRef)
	return ok && c.Name == "kdb_id"
}

// filterIDRange reads the id bounds a filter's top-level conjunction states. Anything it does not
// recognise leaves the range unbounded on that side, which is always safe: the filter itself is
// still evaluated for every document.
func filterIDRange(e sql.Expr) idRange {
	var r idRange
	raise := func(lo string) {
		if lo > r.lo {
			r.lo = lo
		}
	}
	lower := func(hi string, inclusive bool) {
		if r.hi == "" || hi < r.hi || (hi == r.hi && !inclusive) {
			r.hi, r.hiInclusive = hi, inclusive
		}
	}
	var walk func(sql.Expr)
	walk = func(e sql.Expr) {
		switch x := e.(type) {
		case sql.ExprBinary:
			if x.Op == sql.BinaryOpAnd {
				walk(x.Left)
				walk(x.Right)
				return
			}
			op, col, val := x.Op, x.Left, x.Right
			if !isIDColumn(col) {
				// literal on the left: flip the comparison
				col, val = x.Right, x.Left
				switch op {
				case sql.BinaryOpLT:
					op = sql.BinaryOpGT
				case sql.BinaryOpLE:
					op = sql.BinaryOpGE
				case sql.BinaryOpGT:
					op = sql.BinaryOpLT
				case sql.BinaryOpGE:
					op = sql.BinaryOpLE
				}
			}
			v, ok := idLiteral(val)
			if !isIDColumn(col) || !ok {
				return
			}
			switch op {
			case sql.BinaryOpGE, sql.BinaryOpGT:
				// > v still admits ids that have v as a proper prefix, so both start at v.
				raise(v)
			case sql.BinaryOpLT:
				lower(v, false)
			case sql.BinaryOpLE:
				lower(v, true)
			case sql.BinaryOpEQ:
				raise(v)
				lower(v, true)
			}
		case sql.ExprBetween:
			lo, okLo := idLiteral(x.Low)
			hi, okHi := idLiteral(x.High)
			if isIDColumn(x.Expr) {
				if okLo {
					raise(lo)
				}
				if okHi {
					lower(hi, true)
				}
			}
		}
	}
	if e != nil {
		walk(e)
	}
	return r
}

// seekFrom is the cursor to walk after so that the walk starts at the first id >= lo: the id one
// below lo padded with zeros. ok is false when there is no lower bound or it is the smallest id.
func (r idRange) seekFrom() (codec.UUID, bool) {
	if r.lo == "" {
		return codec.UUID{}, false
	}
	digits := strings.ReplaceAll(r.lo, "-", "")
	digits += strings.Repeat("0", 32-len(digits))
	b, err := hex.DecodeString(digits)
	if err != nil {
		return codec.UUID{}, false
	}
	// one below, as a 128-bit big-endian number
	for i := 15; i >= 0; i-- {
		if b[i] > 0 {
			b[i]--
			break
		}
		if i == 0 {
			return codec.UUID{}, false // lo is the smallest id: start at the beginning
		}
		b[i] = 0xff
	}
	return codec.UUID{MSB: int64(binary.BigEndian.Uint64(b[:8])), LSB: int64(binary.BigEndian.Uint64(b[8:]))}, true
}

// beyond reports whether id is past the range's upper bound, so no later id can match.
func (r idRange) beyond(id string) bool {
	if r.hi == "" {
		return false
	}
	if r.hiInclusive {
		return id > r.hi // string order, as the filter compares: an id with hi as a proper prefix is > hi
	}
	return id >= r.hi
}

// uuidLess orders ids as the trie does.
func uuidLess(a, b codec.UUID) bool {
	if a.MSB != b.MSB {
		return uint64(a.MSB) < uint64(b.MSB)
	}
	return uint64(a.LSB) < uint64(b.LSB)
}

// resetScan is the original reset pager: every id after the cursor, sorted, read from the front.
func (pp *projectionPager) resetScan(walker storage.TreeWalker, atCommit document.Commit) (wire.ProjectPageMessage, error) {
	var ids []codec.UUID
	if err := walker.WalkTree(pp.env.NamespaceID, atCommit.DocumentTreeHash, func(id codec.UUID, _ codec.Hash) bool {
		if id.String() > pp.after {
			ids = append(ids, id)
		}
		return true
	}); err != nil {
		return wire.ProjectPageMessage{}, err
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	const chunk = 128
	for start := 0; start < len(ids); start += chunk {
		end := min(start+chunk, len(ids))
		docs, err := pp.env.Storage.GetDocuments(pp.env.NamespaceID, ids[start:end], atCommit.DocumentTreeHash)
		if err != nil {
			return wire.ProjectPageMessage{}, err
		}
		for i, d := range docs {
			if pp.full() {
				pp.page.Done, pp.page.Next = false, ids[start+i-1].String()
				return pp.page, nil
			}
			if d != nil && pp.matches(*d) {
				pp.write(d.ID, d.JSON)
			}
		}
	}
	return pp.page, nil
}

// deltaDiff serves an incremental page from the difference between the two commits' trees
// (document.DiffTrees), the way squash catches a node up: the work is the documents that differ,
// however many commits lie between. The original pager re-collected and replayed every commit in
// the range for every page - O(backlog) per page, which a long-offline replica pays in full.
func (pp *projectionPager) deltaDiff(walker storage.TreeWalker, fromCommit, atCommit document.Commit) (wire.ProjectPageMessage, bool, error) {
	fromState, err := stateAt(pp.env, walker, fromCommit)
	if err != nil || fromState.tree == nil {
		return wire.ProjectPageMessage{}, false, err
	}
	atState, err := stateAt(pp.env, walker, atCommit)
	if err != nil || atState.tree == nil {
		return wire.ProjectPageMessage{}, false, err
	}
	fromTree, ok, err := fromState.tree()
	if err != nil || !ok {
		return wire.ProjectPageMessage{}, false, err
	}
	atTree, ok, err := atState.tree()
	if err != nil || !ok {
		return wire.ProjectPageMessage{}, false, err
	}
	diff, err := document.DiffTrees(document.LocalNodes(fromTree, DefaultTreeEntryLimit), document.LocalNodes(atTree, DefaultTreeEntryLimit))
	if err != nil {
		return wire.ProjectPageMessage{}, true, err
	}
	// A document leaves the projection only if the replica can hold it: it matched at the
	// position the replica is at (see deltaReplay's heldAtFrom).
	heldAtFrom := func(d document.TreeDifference) (bool, error) {
		if d.Local == (codec.Hash{}) {
			return false, nil
		}
		prev, err := fromState.read([]codec.UUID{d.DocID})
		if err != nil {
			return false, err
		}
		return len(prev) == 1 && prev[0] != nil && sql.EvalPredicate(pp.filter, *prev[0], schema.None(), nil), nil
	}
	for _, d := range diff {
		if d.DocID.String() <= pp.after {
			continue
		}
		if pp.full() {
			pp.page.Done = false
			return pp.page, true, nil
		}
		pp.page.Next = d.DocID.String()
		if d.Remote != (codec.Hash{}) {
			docs, err := atState.read([]codec.UUID{d.DocID})
			if err != nil {
				return wire.ProjectPageMessage{}, true, err
			}
			if len(docs) == 1 && docs[0] != nil && pp.matches(*docs[0]) {
				pp.write(d.DocID, docs[0].JSON)
				continue
			}
		}
		held, err := heldAtFrom(d)
		if err != nil {
			return wire.ProjectPageMessage{}, true, err
		}
		if held {
			pp.delete(d.DocID)
		}
	}
	pp.page.Next = ""
	return pp.page, true, nil
}

// deltaReplay is the original incremental pager: the net effect of every commit in the range.
func (pp *projectionPager) deltaReplay(at codec.Hash, fromCommit document.Commit) (wire.ProjectPageMessage, error) {
	env := pp.env
	commits, err := commitsBetween(env.DAG, at, fromCommit.Hash)
	if err != nil {
		return wire.ProjectPageMessage{}, err
	}
	// heldAtFrom reports whether the replica can hold id: it matched the filter at the position
	// the replica is at. Only such a document needs a delete when it stops matching (Cimbiosys's
	// move-out); sending one for every changed document outside the filter made a selective
	// projection's deltas almost all deletes of documents it never held - 99% of entries at a 1%
	// filter (docs/kdb-distributed-self-healing-research.md, Phase 15, M2). Read rights are left out
	// on purpose: a document the replica held and may no longer read must still be deleted, and an
	// extra delete of one it never held is harmless.
	heldAtFrom := func(id codec.UUID) bool {
		prev, err := env.Storage.GetDocument(env.NamespaceID, id, fromCommit.DocumentTreeHash)
		if err != nil {
			return true // cannot tell: delete, as before
		}
		return prev != nil && sql.EvalPredicate(pp.filter, *prev, schema.None(), nil)
	}
	for _, op := range netEffect(commits) {
		id := opDocID(op)
		if id.String() <= pp.after {
			continue
		}
		if pp.full() {
			pp.page.Done = false
			return pp.page, nil
		}
		pp.page.Next = id.String()
		w, isWrite := op.(document.WriteOp)
		if isWrite && pp.matches(document.Document{ID: id, JSON: w.Patch}) {
			pp.write(id, w.Patch)
		} else if heldAtFrom(id) {
			// Deleted at the source, no longer matching, or no longer readable: gone from the
			// projection.
			pp.delete(id)
		}
	}
	pp.page.Next = ""
	return pp.page, nil
}

// ProjectionTarget is the local namespace a projection is kept in.
type ProjectionTarget interface {
	// LastSource is the source commit the last complete transfer brought the projection to,
	// or "" if none has completed.
	LastSource() (string, error)
	// DocIDs lists every document the projection holds.
	DocIDs() ([]codec.UUID, error)
	// Apply commits ops to the projection, recording source and whether this completes a
	// transfer. Called once per page; a transfer interrupted part-way is resumed from the last
	// complete source, which is safe because every page carries final states.
	Apply(ops []document.Op, source string, complete bool) error
}

// PendingWrite is one local transaction on a write-back projection that the source has not yet
// decided on.
type PendingWrite struct {
	// Commit is the local commit that made the write.
	Commit codec.Hash
	// TxID is the id the source commits it under: derived from Commit, so a resend after a lost
	// reply is recognised rather than applied twice.
	TxID codec.UUID
	Docs []wire.WriteBackDoc
}

// WriteBackTxID is the source transaction id for the local write-back commit c.
func WriteBackTxID(c codec.Hash) codec.UUID {
	return codec.DerivedUUID("kdb:writeback/1:" + c.Hex())
}

// WriteBackTarget is a projection that takes local writes and sends them to its source.
type WriteBackTarget interface {
	ProjectionTarget
	// PendingWrites lists the local writes not yet decided on, oldest first. They are sent in
	// this order, so a write that depends on an earlier one is never applied without it.
	PendingWrites() ([]PendingWrite, error)
	// ResolveWrite records the source's decision on w. Not applied: the documents go back to the
	// source's state, and the attempt is kept where an operator or the application can find it.
	ResolveWrite(w PendingWrite, res wire.ProjectWriteResultMessage) error
}

// ProjectionConfig configures one projection sync.
type ProjectionConfig struct {
	NodeID            string
	PeerURI           string
	ConnectionContext auth.ConnectionContext
	TLS               *core.TransportTlsSettings
	Namespace         string
	Filter            string
	PageBytes         int
	Timeout           time.Duration
	Target            ProjectionTarget
	// WriteBack sends the target's local writes to the source before each pull. The target must
	// be a WriteBackTarget, and the source must accept write-back.
	WriteBack bool
}

// ProjectionResult reports one projection sync.
type ProjectionResult struct {
	Source  string
	Reset   bool
	Writes  int
	Deletes int
	// Written counts local writes the source applied this sync; Rejected those it did not
	// (conflicts and refusals), now in the conflict queue.
	Written, Rejected int
}

// SyncProjection brings a projection up to its source's current head.
func SyncProjection(w wire.Codec, transport stream.Transport, cfg ProjectionConfig) (ProjectionResult, error) {
	if _, err := sql.ParseFilter(cfg.Filter); err != nil {
		return ProjectionResult{}, fmt.Errorf("peer sync: projection filter: %w", err)
	}
	conn, err := dial(transport, cfg.PeerURI, cfg.TLS, cfg.ConnectionContext)
	if err != nil {
		return ProjectionResult{}, err
	}
	defer conn.Close()
	var wb WriteBackTarget
	caps := []string{wire.SyncCapFilter}
	if cfg.WriteBack {
		var ok bool
		if wb, ok = cfg.Target.(WriteBackTarget); !ok {
			return ProjectionResult{}, fmt.Errorf("peer sync: projection target %T cannot send writes back", cfg.Target)
		}
		caps = append(caps, wire.SyncCapWriteBack)
	}
	c := &v2Conn{wire: w, conn: conn, correlation: 20000, timeout: cfg.Timeout}
	reply, err := c.request(wire.SyncHelloMessage{
		H: header(wire.MsgSyncHello, c.next()), NodeID: cfg.NodeID, Protocol: wire.SyncProtocolVersion,
		Capabilities: caps,
		User:         cfg.ConnectionContext.User, Password: cfg.ConnectionContext.Password, Token: cfg.ConnectionContext.Token,
	})
	if err != nil {
		return ProjectionResult{}, err
	}
	if ack, ok := reply.(wire.SyncHelloAckMessage); !ok || !ack.Accepted {
		reason := ""
		if ok {
			reason = ack.Reason
		}
		return ProjectionResult{}, NewError("peer refused the projection session: "+reason, nil)
	}
	var res ProjectionResult
	if wb != nil {
		if !slices.Contains(reply.(wire.SyncHelloAckMessage).Capabilities, wire.SyncCapWriteBack) {
			return res, NewError("peer does not accept write-back to "+cfg.Namespace, nil)
		}
		// Writes go first, so the pull that follows already carries what the source made of them.
		if err := pushWrites(c, wb, cfg.Namespace, &res); err != nil {
			return res, err
		}
	}
	from, err := cfg.Target.LastSource()
	if err != nil {
		return ProjectionResult{}, err
	}
	var at, after string
	received := map[codec.UUID]struct{}{}
	for {
		reply, err := c.request(wire.ProjectFetchMessage{
			H: header(wire.MsgProjectFetch, c.next()), Namespace: cfg.Namespace, Filter: cfg.Filter,
			FromHex: from, AtHex: at, After: after, MaxBytes: cfg.PageBytes,
		})
		if err != nil {
			return res, err
		}
		page, ok := reply.(wire.ProjectPageMessage)
		if !ok {
			return res, NewError(fmt.Sprintf("expected PROJECT_PAGE, got %T", reply), nil)
		}
		at, res.Source, res.Reset = page.AtHex, page.AtHex, page.Reset
		var ops []document.Op
		for _, d := range page.Writes {
			id, err := codec.ParseUUID(d.DocID)
			if err != nil {
				return res, err
			}
			received[id] = struct{}{}
			ops = append(ops, document.WriteOp{DocID: id, Patch: d.Body})
		}
		for _, s := range page.Deletes {
			id, err := codec.ParseUUID(s)
			if err != nil {
				return res, err
			}
			ops = append(ops, document.DeleteOp{DocID: id})
		}
		res.Writes += len(page.Writes)
		res.Deletes += len(page.Deletes)
		if page.Done && page.Reset {
			// A full snapshot replaces the projection: whatever it did not deliver goes.
			held, err := cfg.Target.DocIDs()
			if err != nil {
				return res, err
			}
			for _, id := range held {
				if _, ok := received[id]; !ok {
					ops = append(ops, document.DeleteOp{DocID: id})
					res.Deletes++
				}
			}
		}
		if len(ops) > 0 || (page.Done && at != from) {
			if err := cfg.Target.Apply(ops, at, page.Done); err != nil {
				return res, err
			}
		}
		if page.Done {
			return res, nil
		}
		after = page.Next
	}
}

// pushWrites sends every pending write in order. A write the source decides on - applied or not -
// is resolved and the next one goes; anything else (the source unavailable, not the home, the
// connection gone) stops here with the rest still pending, to go again next sync.
func pushWrites(c *v2Conn, wb WriteBackTarget, ns string, res *ProjectionResult) error {
	pending, err := wb.PendingWrites()
	if err != nil {
		return err
	}
	for _, w := range pending {
		reply, err := c.request(wire.ProjectWriteMessage{
			H: header(wire.MsgProjectWrite, c.next()), Namespace: ns, TxID: w.TxID.String(), Docs: w.Docs,
		})
		if err != nil {
			return fmt.Errorf("peer sync: write-back of %s: %w", w.Commit.Hex(), err)
		}
		r, ok := reply.(wire.ProjectWriteResultMessage)
		if !ok {
			return NewError(fmt.Sprintf("expected PROJECT_WRITE_RESULT, got %T", reply), nil)
		}
		switch r.Outcome {
		case wire.WriteBackApplied:
			res.Written++
		case wire.WriteBackConflict, wire.WriteBackRefused:
			res.Rejected++
		default:
			return NewError("unknown write-back outcome "+r.Outcome, nil)
		}
		if err := wb.ResolveWrite(w, r); err != nil {
			return err
		}
	}
	return nil
}
