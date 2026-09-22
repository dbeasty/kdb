package peersync

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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
	size := 0
	full := func() bool { return size >= maxBytes }

	if reset {
		walker, ok := env.Storage.(storage.TreeWalker)
		if !ok {
			return wire.ProjectPageMessage{}, fmt.Errorf("peer sync: storage for %s cannot serve projections", env.NamespaceID)
		}
		var ids []codec.UUID
		if err := walker.WalkTree(env.NamespaceID, atCommit.DocumentTreeHash, func(id codec.UUID, _ codec.Hash) bool {
			if id.String() > after {
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
			docs, err := env.Storage.GetDocuments(env.NamespaceID, ids[start:end], atCommit.DocumentTreeHash)
			if err != nil {
				return wire.ProjectPageMessage{}, err
			}
			for i, d := range docs {
				if full() {
					page.Done, page.Next = false, ids[start+i-1].String()
					return page, nil
				}
				if d != nil && matches(*d) {
					page.Writes = append(page.Writes, wire.SnapshotDoc{DocID: d.ID.String(), Body: d.JSON})
					size += len(d.JSON)
				}
			}
		}
		return page, nil
	}

	commits, err := commitsBetween(env.DAG, at, from)
	if err != nil {
		return wire.ProjectPageMessage{}, err
	}
	for _, op := range netEffect(commits) {
		id := opDocID(op)
		if id.String() <= after {
			continue
		}
		if full() {
			page.Done = false
			return page, nil
		}
		page.Next = id.String()
		w, isWrite := op.(document.WriteOp)
		if isWrite && matches(document.Document{ID: id, JSON: w.Patch}) {
			page.Writes = append(page.Writes, wire.SnapshotDoc{DocID: id.String(), Body: w.Patch})
			size += len(w.Patch)
		} else {
			// Deleted at the source, no longer matching, or no longer readable: gone from the
			// projection. A delete of a document the replica never held is harmless.
			page.Deletes = append(page.Deletes, id.String())
			size += 40
		}
	}
	page.Next = ""
	return page, nil
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
