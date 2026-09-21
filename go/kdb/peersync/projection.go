package peersync

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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
}

// ProjectionResult reports one projection sync.
type ProjectionResult struct {
	Source  string
	Reset   bool
	Writes  int
	Deletes int
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
	c := &v2Conn{wire: w, conn: conn, correlation: 20000, timeout: cfg.Timeout}
	reply, err := c.request(wire.SyncHelloMessage{
		H: header(wire.MsgSyncHello, c.next()), NodeID: cfg.NodeID, Protocol: wire.SyncProtocolVersion,
		Capabilities: []string{wire.SyncCapFilter},
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
	from, err := cfg.Target.LastSource()
	if err != nil {
		return ProjectionResult{}, err
	}
	var res ProjectionResult
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
