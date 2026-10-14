package peersync

import (
	"sort"

	"context"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"

	"errors"
	"fmt"
	"github.com/limidus/kdb/go/kdb/auth"
	"io"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage/attachstore"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Attachment blobs in peer sync (docs/kdb-attachments-plan.md, PR 5).
//
// Commits carry attachment refs - content hashes - and the blobs travel separately:
//   - pulling, a node fetches the blobs a page of commits attaches, and that it lacks, with
//     BLOB_FETCH before it stores the page, so no commit it adopts names bytes it does not hold. Each
//     chunk arrives with its RFC 6962 audit path and is checked before it is written;
//   - pushing, a node uploads them over the sync session (ATTACHMENT_UPLOAD with Expect, so a blob
//     the host already holds costs one round trip) before the REF_UPDATE that names them;
//   - a host serves BLOB_FETCH for an attachment the requested commit's table references with that
//     hash, which is how it checks the asker may read it: the session's pull grant covers the
//     namespace, and the reference proves the bytes belong to it.
//
// A version the host no longer retains (its attachment history is "latest") answers NOT_FOUND for
// that entry; the puller goes on without it, and a read of that version on the puller then fails
// the same way it would on the host.

const (
	blobFetchPageBytes = 8 << 20
	blobFetchBatch     = 64
)

// --- host --------------------------------------------------------------------------------

func (h *V2Host) blobFetch(m wire.BlobFetchMessage) (wire.Message, error) {
	env, err := h.env(m.Namespace, false, dirPull)
	var canRead func(codec.UUID) bool
	if err != nil {
		// A projection session (a replica) reads what its subscription and document reads allow.
		var perr error
		if env, canRead, perr = h.projectionEnv(m.Namespace, false); perr != nil {
			return nil, err
		}
	}
	if env.Attachments == nil {
		return nil, &UnsupportedFrameError{Type: m.H.MessageType}
	}
	budget := m.MaxBytes
	if budget <= 0 || budget > blobFetchPageBytes {
		budget = blobFetchPageBytes
	}
	out := wire.BlobFetchResultMessage{H: header(wire.MsgBlobFetchResult, m.H.CorrelationID), Namespace: m.Namespace}
	var head *document.AttachmentTable
	if hh, err := env.DAG.Head(); err == nil {
		if t, err := env.DAG.AttachmentTableAt(hh); err == nil {
			head = &t
		}
	}
	for i, e := range m.Entries {
		if budget <= 0 {
			out.More = true
			break
		}
		if canRead != nil && !canRead(e.DocID) {
			out.Parts = append(out.Parts, wire.BlobFetchPart{Index: i, Hash: e.Hash, Offset: e.Offset,
				Error: "document " + e.DocID.String() + " is not readable by this session", ErrorCode: wire.ErrorCodeUnauthorized})
			continue
		}
		part, used := serveBlobEntry(env, e, budget, head)
		part.Index = i
		budget -= used
		out.Parts = append(out.Parts, part)
	}
	return out, nil
}

func serveBlobEntry(env IngestEnv, e wire.BlobFetchEntry, budget int64, head *document.AttachmentTable) (wire.BlobFetchPart, int64) {
	part := wire.BlobFetchPart{Hash: e.Hash, Offset: e.Offset}
	fail := func(code wire.ErrorCode, format string, args ...any) (wire.BlobFetchPart, int64) {
		part.Error, part.ErrorCode = fmt.Sprintf(format, args...), code
		return part, 0
	}
	// failRead is a failed read of the blob's stored bytes: INTEGRITY when this node's copy is
	// damaged (logged - the puller tries another peer, but only an operator repairs this one),
	// otherwise the code given.
	failRead := func(code wire.ErrorCode, err error) (wire.BlobFetchPart, int64) {
		if attachstore.ReportCorrupt(err, "namespace", env.NamespaceID, "doc", e.DocID, "attachment", e.AttachmentID, "hash", e.Hash.Hex(), "via", "blob_fetch") {
			code = wire.ErrorCodeIntegrity
		}
		return fail(code, "blob %s: %v", e.Hash.Hex(), err)
	}
	commit, err := codec.HashFromHex(e.CommitHex)
	if err != nil {
		return fail(wire.ErrorCodeSchemaViolation, "invalid commit: %v", err)
	}
	table, err := env.DAG.AttachmentTableAt(commit)
	if err != nil {
		return fail(wire.ErrorCodeNotFound, "commit %s: %v", e.CommitHex, err)
	}
	a, ok := table.Get(e.DocID, e.AttachmentID)
	if !ok || a.ContentHash != e.Hash {
		return fail(wire.ErrorCodeNotFound, "attachment %s of %s at %s does not reference %s", e.AttachmentID, e.DocID, e.CommitHex, e.Hash.Hex())
	}
	const chunk = document.AttachmentChunkBytes
	if e.Offset < 0 || e.Offset%chunk != 0 || e.Offset > a.Size {
		return fail(wire.ErrorCodeSchemaViolation, "offset %d is not a chunk boundary within %d bytes", e.Offset, a.Size)
	}
	r, err := env.Attachments.Open(e.Hash)
	if errors.Is(err, attachstore.ErrCorrupt) {
		return failRead(wire.ErrorCodeIntegrity, err)
	}
	if err != nil {
		return fail(wire.ErrorCodeNotFound, "blob %s is not retained here: %v", e.Hash.Hex(), err)
	}
	defer r.Close()
	n := (a.Size + chunk - 1) / chunk
	if n == 0 {
		n = 1
	}
	leaves := make([]codec.Hash, n)
	for i := range leaves {
		if leaves[i], err = r.ChunkLeaf(int64(i)); err != nil {
			return failRead(wire.ErrorCodeIntegrity, err)
		}
	}
	part.Size, part.Tree = a.Size, document.MerkleTreeHash(leaves)
	want := e.MaxBytes
	if want <= 0 || want > budget {
		want = budget
	}
	want = (want + chunk - 1) / chunk * chunk // whole chunks, at least one
	end := min(a.Size, e.Offset+want)
	data := make([]byte, end-e.Offset)
	if len(data) > 0 {
		if _, err := r.ReadAt(data, e.Offset); err != nil && !errors.Is(err, io.EOF) {
			return failRead(wire.ErrorCodeInternal, err)
		}
	}
	part.Data = data
	for off := e.Offset; off < end || (a.Size == 0 && off == 0 && len(part.Chunks) == 0); off += chunk {
		idx := int(off / chunk)
		part.Chunks = append(part.Chunks, wire.BlobChunkProof{Leaf: int64(idx), Path: document.MerkleInclusionProof(leaves, idx)})
		if a.Size == 0 {
			break
		}
	}
	part.EOF = end == a.Size
	if part.EOF && head != nil {
		now, ok := head.Get(e.DocID, e.AttachmentID)
		part.Superseded = !ok || now.ContentHash != e.Hash
	}
	return part, int64(len(data))
}

// hostUploads are a sync session's blob uploads (a pushing peer's).
type hostUploads struct {
	mu sync.Mutex
	m  map[string]*hostUpload
	n  int
}

type hostUpload struct {
	ns       string
	up       *attachstore.Upload
	received int64
}

func (h *V2Host) attachmentUpload(m wire.AttachmentUploadMessage) (wire.Message, error) {
	env, err := h.env(m.Namespace, false, dirPush)
	if err != nil {
		// A write-back projection uploads the blobs its writes attach before PROJECT_WRITE.
		var perr error
		if env, _, perr = h.projectionEnv(m.Namespace, true); perr != nil {
			return nil, err
		}
	}
	if env.Attachments == nil {
		return nil, &UnsupportedFrameError{Type: m.H.MessageType}
	}
	out := wire.AttachmentUploadResultMessage{H: header(wire.MsgAttachmentUploadResult, m.H.CorrelationID), Namespace: m.Namespace, UploadID: m.UploadID}
	if m.UploadID == "" && m.Expect != (codec.Hash{}) {
		if r, err := env.Attachments.Open(m.Expect); err == nil {
			out.Hash, out.Size, out.Received = m.Expect, r.Size(), r.Size()
			_ = r.Close()
			// The REF_UPDATE referencing it comes next: hold the blob until it does.
			if _, err := env.Attachments.Hold(m.Expect); err != nil {
				return nil, err
			}
			return out, nil
		}
	}
	if len(m.Data) > wire.MaxAttachmentChunkBytes {
		return nil, fmt.Errorf("peer sync: upload chunk of %d bytes", len(m.Data))
	}
	h.uploads.mu.Lock()
	if h.uploads.m == nil {
		h.uploads.m = map[string]*hostUpload{}
	}
	up := h.uploads.m[m.UploadID]
	if m.UploadID == "" {
		h.uploads.n++
		out.UploadID = fmt.Sprintf("s%d", h.uploads.n)
		up = &hostUpload{ns: m.Namespace, up: env.Attachments.Begin()}
		h.uploads.m[out.UploadID] = up
	}
	h.uploads.mu.Unlock()
	if up == nil || up.ns != m.Namespace {
		return nil, fmt.Errorf("peer sync: unknown upload %q", m.UploadID)
	}
	if m.Offset != up.received {
		out.Received = up.received
		out.Error, out.ErrorCode = fmt.Sprintf("chunk at %d does not follow the %d bytes received", m.Offset, up.received), wire.ErrorCodeConflict
		return out, nil
	}
	if _, err := up.up.Write(m.Data); err != nil {
		h.dropHostUpload(out.UploadID, up)
		return nil, err
	}
	up.received += int64(len(m.Data))
	out.Received = up.received
	if m.Final {
		info, err := up.up.Finish()
		h.dropHostUpload(out.UploadID, nil)
		if err != nil {
			return nil, err
		}
		out.Hash, out.Size = info.Hash, info.Size
	}
	return out, nil
}

func (h *V2Host) dropHostUpload(id string, up *hostUpload) {
	if up != nil {
		up.up.Abort()
	}
	h.uploads.mu.Lock()
	delete(h.uploads.m, id)
	h.uploads.mu.Unlock()
}

// closeUploads aborts the session's unfinished uploads.
func (h *V2Host) closeUploads() {
	h.uploads.mu.Lock()
	defer h.uploads.mu.Unlock()
	for id, up := range h.uploads.m {
		up.up.Abort()
		delete(h.uploads.m, id)
	}
}

// --- client ------------------------------------------------------------------------------

// blobNeed is a blob a page of commits attaches.
type blobNeed struct {
	entry wire.BlobFetchEntry
	up    *attachstore.Upload
	done  bool
}

// attachedBlobs lists the blobs commits attach that store lacks, once each.
func attachedBlobs(store *attachstore.Store, commits []document.Commit) ([]*blobNeed, error) {
	seen := map[codec.Hash]bool{}
	var out []*blobNeed
	for _, c := range commits {
		for _, op := range c.Operations {
			put, ok := op.(document.AttachmentPutOp)
			if !ok || seen[put.ContentHash] {
				continue
			}
			seen[put.ContentHash] = true
			has, err := store.Has(put.ContentHash)
			if err != nil {
				return nil, err
			}
			if !has {
				out = append(out, &blobNeed{entry: wire.BlobFetchEntry{DocID: put.DocID, AttachmentID: put.AttachmentID,
					CommitHex: c.Hash.Hex(), Hash: put.ContentHash}})
			}
		}
	}
	return out, nil
}

// fetchBlobs takes in the blobs commits attach that this node lacks, from the host.
func (c *v2Conn) fetchBlobs(env IngestEnv, ns string, commits []document.Commit) error {
	if env.Attachments == nil || !containsString(c.caps, wire.SyncCapAttachments) {
		return nil
	}
	needs, err := attachedBlobs(env.Attachments, commits)
	if err != nil || len(needs) == 0 {
		return err
	}
	defer func() {
		for _, n := range needs {
			if n.up != nil && !n.done {
				n.up.Abort()
			}
		}
	}()
	for {
		var batch []*blobNeed
		for _, n := range needs {
			if !n.done && len(batch) < blobFetchBatch {
				batch = append(batch, n)
			}
		}
		if len(batch) == 0 {
			return nil
		}
		msg := wire.BlobFetchMessage{H: header(wire.MsgBlobFetch, c.next()), Namespace: ns, MaxBytes: blobFetchPageBytes}
		for _, n := range batch {
			msg.Entries = append(msg.Entries, n.entry)
		}
		reply, err := c.request(msg)
		if err != nil {
			return err
		}
		res, ok := reply.(wire.BlobFetchResultMessage)
		if !ok {
			return NewError(fmt.Sprintf("expected BLOB_FETCH_RESULT, got %T", reply), nil)
		}
		if res.Error != "" {
			return &RemoteError{Code: res.ErrorCode, Message: res.Error}
		}
		progressed := false
		for _, p := range res.Parts {
			if p.Index < 0 || p.Index >= len(batch) {
				return NewError("BLOB_FETCH_RESULT names an entry that was not asked for", nil)
			}
			n := batch[p.Index]
			if p.Error != "" {
				if p.ErrorCode == wire.ErrorCodeNotFound {
					n.done = true // not retained there: go on without it
					progressed = true
					continue
				}
				return &RemoteError{Code: p.ErrorCode, Message: p.Error}
			}
			if err := n.take(env.Attachments, p); err != nil {
				return err
			}
			progressed = true
		}
		if !progressed {
			return NewError("BLOB_FETCH made no progress", nil)
		}
	}
}

// take checks one part's chunks against the blob's tree and writes them.
func (n *blobNeed) take(store *attachstore.Store, p wire.BlobFetchPart) error {
	const chunk = document.AttachmentChunkBytes
	if p.Offset != n.entry.Offset || p.Hash != n.entry.Hash {
		return NewError(fmt.Sprintf("BLOB_FETCH part for %s at %d, asked at %d", p.Hash.Hex(), p.Offset, n.entry.Offset), nil)
	}
	if document.AttachmentIdentity(p.Tree, p.Size) != n.entry.Hash {
		return NewError("BLOB_FETCH tree and size do not hash to the blob asked for", nil)
	}
	leaves := int((p.Size + chunk - 1) / chunk)
	if leaves == 0 {
		leaves = 1
	}
	data := p.Data
	first := int(n.entry.Offset / chunk)
	for i, proof := range p.Chunks {
		idx := first + i
		if int(proof.Leaf) != idx {
			return NewError("BLOB_FETCH proofs out of order", nil)
		}
		size := min(int64(chunk), p.Size-int64(idx)*chunk)
		if size < 0 || int64(len(data)) < size {
			return NewError("BLOB_FETCH part is shorter than its chunks", nil)
		}
		if !document.VerifyMerkleInclusion(document.ChunkLeafHash(data[:size]), idx, leaves, proof.Path, p.Tree) {
			return NewError(fmt.Sprintf("BLOB_FETCH chunk %d of %s fails its proof", idx, p.Hash.Hex()), nil)
		}
		if n.up == nil {
			n.up = store.Begin()
		}
		if _, err := n.up.Write(data[:size]); err != nil {
			return err
		}
		data = data[size:]
		n.entry.Offset += size
	}
	if len(data) != 0 {
		return NewError("BLOB_FETCH part carries bytes no proof covers", nil)
	}
	if n.entry.Offset < p.Size {
		return nil
	}
	if n.up == nil {
		n.up = store.Begin()
	}
	info, err := n.up.Finish()
	n.done = true
	if err != nil {
		return err
	}
	if info.Hash != n.entry.Hash {
		return NewError("fetched blob does not hash to what was asked for", nil)
	}
	return nil
}

// uploadBlobs sends the host the blobs commits attach that it lacks, before the commits.
func (c *v2Conn) uploadBlobs(env IngestEnv, ns string, commits []document.Commit) error {
	if env.Attachments == nil || !containsString(c.caps, wire.SyncCapAttachments) {
		return nil
	}
	seen := map[codec.Hash]bool{}
	for _, cm := range commits {
		for _, op := range cm.Operations {
			put, ok := op.(document.AttachmentPutOp)
			if !ok || seen[put.ContentHash] {
				continue
			}
			seen[put.ContentHash] = true
			if err := c.uploadBlob(env.Attachments, ns, put.ContentHash); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *v2Conn) uploadBlob(store *attachstore.Store, ns string, h codec.Hash) error {
	r, err := store.Open(h)
	if err != nil {
		return nil // not held here either (latest-only history): the host must do without it
	}
	defer r.Close()
	send := func(m wire.AttachmentUploadMessage) (wire.AttachmentUploadResultMessage, error) {
		m.H, m.Namespace = header(wire.MsgAttachmentUpload, c.next()), ns
		reply, err := c.request(m)
		if err != nil {
			return wire.AttachmentUploadResultMessage{}, err
		}
		res, ok := reply.(wire.AttachmentUploadResultMessage)
		if !ok {
			return res, NewError(fmt.Sprintf("expected ATTACHMENT_UPLOAD_RESULT, got %T", reply), nil)
		}
		if res.Error != "" {
			return res, &RemoteError{Code: res.ErrorCode, Message: res.Error}
		}
		return res, nil
	}
	buf := make([]byte, document.AttachmentChunkBytes)
	var id string
	first := true
	for off := int64(0); ; {
		n, err := r.ReadAt(buf, off)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		final := off+int64(n) >= r.Size()
		m := wire.AttachmentUploadMessage{UploadID: id, Offset: off, Data: buf[:n], Final: final}
		if first {
			m.Expect = h
		}
		res, err := send(m)
		if err != nil {
			return err
		}
		if first && res.Hash == h && res.UploadID == "" {
			return nil // the host holds it already
		}
		first, id = false, res.UploadID
		off += int64(n)
		if final {
			if res.Hash != h {
				return NewError("the host hashed an uploaded blob differently", nil)
			}
			return nil
		}
	}
}

// attachmentTreeNodesPage is TREE_NODES of the attachment table at a commit: the same trie nodes
// as the document tree's, entries mapping a document to its attachment-set hash, and the listed
// documents' attachments themselves - so a replica diffs attachments the way it diffs documents.
func attachmentTreeNodesPage(env IngestEnv, m wire.TreeNodesMessage) (wire.TreeNodesResultMessage, error) {
	commit, err := env.DAG.Head()
	if m.CommitHex != "" {
		commit, err = codec.HashFromHex(m.CommitHex)
	}
	if err != nil {
		return wire.TreeNodesResultMessage{}, err
	}
	table, err := env.DAG.AttachmentTableAt(commit)
	if err != nil {
		return wire.TreeNodesResultMessage{}, err
	}
	limit := m.EntryLimit
	if limit <= 0 {
		limit = DefaultTreeEntryLimit
	}
	out := wire.TreeNodesResultMessage{Namespace: env.NamespaceID, Nodes: make([]wire.TreeNodeInfo, len(m.Prefixes))}
	if root := table.Root(); root != nil {
		out.TreeHex = root.Hex()
	}
	tree := table.Tree()
	for i, p := range m.Prefixes {
		n, err := tree.Node(p, limit)
		if err != nil {
			return wire.TreeNodesResultMessage{}, err
		}
		info := nodeToWire(n)
		if info.HasEntries && len(info.Entries) > 0 {
			info.Attachments = map[string][]wire.TreeAttachment{}
			for docHex := range info.Entries {
				id, err := codec.ParseUUID(docHex)
				if err != nil {
					continue
				}
				for _, a := range table.List(id) {
					info.Attachments[docHex] = append(info.Attachments[docHex], wire.TreeAttachment{
						ID: a.ID.String(), Name: a.Name, Hash: a.ContentHash.Hex(), Size: a.Size, Mime: a.Mime})
				}
			}
		}
		out.Nodes[i] = info
	}
	return out, nil
}

// seedingSnapshotPages wraps a snapshot page source so the attachment table a last page carries is
// seeded into env's DAG as the page arrives - before the snapshot's root, which has no history here
// to derive it from, is admitted.
func seedingSnapshotPages(env IngestEnv, fetch func(after string) (wire.SnapshotPageMessage, error)) func(after string) (wire.SnapshotPageMessage, error) {
	return func(after string) (wire.SnapshotPageMessage, error) {
		page, err := fetch(after)
		if err != nil || len(page.AttachmentTables) == 0 {
			return page, err
		}
		if err := env.DAG.SeedPackedAttachmentTables(page.AttachmentTables); err != nil {
			return page, fmt.Errorf("peer sync: snapshot attachment table: %w", err)
		}
		if env.AttachmentTableSeeded != nil {
			if err := env.AttachmentTableSeeded(page.AttachmentTables); err != nil {
				return page, err
			}
		}
		return page, nil
	}
}

// tableBlobs is commit's attachment table as puts of one synthetic commit, named commit - so
// fetchBlobs asks for them at it, and uploadBlobs sends them. ok=false when commit has none.
func tableBlobs(env IngestEnv, commit codec.Hash) (document.Commit, bool, error) {
	cm, err := env.DAG.GetCommitOrThrow(commit)
	if err != nil || cm.AttachmentTreeHash == nil {
		return document.Commit{}, false, err
	}
	table, err := env.DAG.AttachmentTableAt(commit)
	if err != nil {
		return document.Commit{}, false, err
	}
	synthetic := document.Commit{Hash: commit}
	table.Walk(func(doc codec.UUID, a document.Attachment) bool {
		synthetic.Operations = append(synthetic.Operations, document.AttachmentPutOp{DocID: doc, AttachmentID: a.ID,
			Name: a.Name, Scheme: a.Scheme, ContentHash: a.ContentHash, Size: a.Size, Mime: a.Mime})
		return true
	})
	return synthetic, true, nil
}

// fetchTableBlobs takes in every blob commit's attachment table names that this node lacks - after
// a snapshot, graft or boundary extension admitted commit without the commits that attached them.
func (c *v2Conn) fetchTableBlobs(env IngestEnv, ns string, commit codec.Hash) error {
	if env.Attachments == nil || !containsString(c.caps, wire.SyncCapAttachments) {
		return nil
	}
	synthetic, ok, err := tableBlobs(env, commit)
	if err != nil || !ok {
		return err
	}
	return c.fetchBlobs(env, ns, []document.Commit{synthetic})
}

// uploadTableBlobs sends the host every blob commit's attachment table names, before a pushed
// graft or boundary state of commit - the host cannot fetch from a node that dialled it.
func (c *v2Conn) uploadTableBlobs(env IngestEnv, ns string, commit codec.Hash) error {
	if env.Attachments == nil || !containsString(c.caps, wire.SyncCapAttachments) {
		return nil
	}
	synthetic, ok, err := tableBlobs(env, commit)
	if err != nil || !ok {
		return err
	}
	return c.uploadBlobs(env, ns, []document.Commit{synthetic})
}

// admitGraftAttachments seeds a pushed graft's attachment table before its root is admitted, and
// refuses a graft whose root has attachments this node would then hold neither the table nor the
// bytes of.
func admitGraftAttachments(env IngestEnv, stage *graftStage) error {
	root := stage.commit.AttachmentTreeHash
	if root == nil {
		return nil
	}
	if len(stage.attachTables) == 0 {
		return fmt.Errorf("peer sync: pushed graft of %s has attachments but not their table - the pusher must be upgraded", stage.commit.Hash.Hex())
	}
	tables, err := document.ParsePackedAttachmentTables(stage.attachTables)
	if err != nil {
		return fmt.Errorf("peer sync: pushed graft attachment table: %w", err)
	}
	var table *document.AttachmentTable
	for i := range tables {
		if r := tables[i].Root(); r != nil && *r == *root {
			table = &tables[i]
		}
	}
	if table == nil {
		return fmt.Errorf("peer sync: pushed graft of %s carries no attachment table with its root %s", stage.commit.Hash.Hex(), root.Hex())
	}
	if env.Attachments != nil {
		var missing error
		table.Walk(func(_ codec.UUID, a document.Attachment) bool {
			if has, err := env.Attachments.Has(a.ContentHash); err != nil || !has {
				missing = fmt.Errorf("peer sync: pushed graft of %s attaches %s, which was not uploaded", stage.commit.Hash.Hex(), a.ContentHash.Hex())
				return false
			}
			return true
		})
		if missing != nil {
			return missing
		}
	}
	env.DAG.SeedAttachmentTable(*table)
	if env.AttachmentTableSeeded != nil {
		return env.AttachmentTableSeeded(document.AppendPackedAttachmentTables(nil, []document.AttachmentTable{*table}))
	}
	return nil
}

// withProjectedAttachments adds each written document's attachments at the page's commit.
func withProjectedAttachments(env IngestEnv, page *wire.ProjectPageMessage) error {
	if len(page.Writes) == 0 || !env.DAG.HasAttachments() {
		return nil
	}
	at, err := codec.HashFromHex(page.AtHex)
	if err != nil {
		return err
	}
	table, err := env.DAG.AttachmentTableAt(at)
	if err != nil {
		return err
	}
	for _, w := range page.Writes {
		id, err := codec.ParseUUID(w.DocID)
		if err != nil {
			continue
		}
		for _, a := range table.List(id) {
			if page.Attachments == nil {
				page.Attachments = map[string][]wire.TreeAttachment{}
			}
			page.Attachments[w.DocID] = append(page.Attachments[w.DocID], wire.TreeAttachment{
				ID: a.ID.String(), Name: a.Name, Hash: a.ContentHash.Hex(), Size: a.Size, Mime: a.Mime})
		}
	}
	return nil
}

// AttachmentsUnsupportedError refuses to send a peer of an older build commits that carry
// attachments: it cannot decode them. Upgrade the peer; until then the namespace does not sync
// with it (rolling upgrades work as long as no attachment is written before every node runs a build
// that has them).
type AttachmentsUnsupportedError struct {
	Namespace string
	Commit    string
}

func (e *AttachmentsUnsupportedError) Error() string {
	return fmt.Sprintf("peer sync: namespace %s has attachments (commit %s) and the peer's build does not support them - upgrade it", e.Namespace, e.Commit)
}

// carriesAttachments reports the first commit an attachment-unaware peer could not decode.
func carriesAttachments(commits []document.Commit) (codec.Hash, bool) {
	for _, c := range commits {
		if c.AttachmentTreeHash != nil || document.TouchesAttachments(c.Operations, false) {
			return c.Hash, true
		}
	}
	return codec.Hash{}, false
}

func (h *V2Host) checkPeerAttachments(ns string, commits []document.Commit) error {
	h.mu.Lock()
	ok := h.peerAttachments
	h.mu.Unlock()
	if ok {
		return nil
	}
	if c, bad := carriesAttachments(commits); bad {
		return &AttachmentsUnsupportedError{Namespace: ns, Commit: c.Hex()}
	}
	return nil
}

// projectionEnv is the namespace for a projection session's attachment frames: a subscriber may
// read the blobs of documents it may read, and - on a host taking write-back - one that may commit
// may upload. canRead answers per document.
func (h *V2Host) projectionEnv(ns string, write bool) (IngestEnv, func(codec.UUID) bool, error) {
	h.mu.Lock()
	principal := h.principal
	h.mu.Unlock()
	authz := h.auth.Authorizer()
	if err := authz.Authorize(context.Background(), principal, auth.StreamSubscribeAction{Namespace: ns}); err != nil {
		return IngestEnv{}, nil, err
	}
	if write {
		if h.cfg.WriteBack == nil {
			return IngestEnv{}, nil, &UnsupportedFrameError{Type: wire.MsgAttachmentUpload}
		}
		if err := authz.Authorize(context.Background(), principal, auth.TxCommitAction{Namespace: ns}); err != nil {
			return IngestEnv{}, nil, err
		}
	}
	env, err := h.cfg.Namespaces.Env(ns, false)
	if err != nil {
		return IngestEnv{}, nil, err
	}
	return env, func(id codec.UUID) bool {
		return authz.Authorize(context.Background(), principal, auth.DocumentReadAction{Namespace: ns, DocID: id.String()}) == nil
	}, nil
}

// withAttachmentOnlyChanges adds, to the last page of a delta transfer, every document in the
// projection whose attachments changed between the transfer's commits while its body did not: the
// pagers diff document trees, which an attachment change leaves alone. Each goes as a write of its
// (unchanged) body, so withProjectedAttachments then carries its new set.
func withAttachmentOnlyChanges(env IngestEnv, page *wire.ProjectPageMessage, filter sql.Expr, canRead func(codec.UUID) bool, fromHex string) error {
	if page.Reset || !page.Done || fromHex == "" || !env.DAG.HasAttachments() {
		return nil
	}
	from, err := codec.HashFromHex(fromHex)
	if err != nil {
		return nil
	}
	at, err := codec.HashFromHex(page.AtHex)
	if err != nil {
		return err
	}
	before, err := env.DAG.AttachmentTableAt(from)
	if err != nil {
		return nil // the start is not resolvable here: the pagers already sent a reset
	}
	after, err := env.DAG.AttachmentTableAt(at)
	if err != nil {
		return err
	}
	if sameRootPtr(before.Root(), after.Root()) {
		return nil
	}
	changed := map[codec.UUID]bool{}
	sets := map[codec.UUID]codec.Hash{}
	after.WalkDocuments(func(id codec.UUID, h codec.Hash) bool {
		sets[id] = h
		return true
	})
	before.WalkDocuments(func(id codec.UUID, h codec.Hash) bool {
		if now, ok := sets[id]; !ok || now != h {
			changed[id] = true
		}
		delete(sets, id)
		return true
	})
	for id := range sets {
		changed[id] = true
	}
	if len(changed) == 0 {
		return nil
	}
	// Documents the transfer already wrote or deleted carry their own state.
	seen := map[string]bool{}
	for _, w := range page.Writes {
		seen[w.DocID] = true
	}
	for _, d := range page.Deletes {
		seen[d] = true
	}
	atCommit, err := env.DAG.GetCommitOrThrow(at)
	if err != nil {
		return err
	}
	ids := make([]codec.UUID, 0, len(changed))
	for id := range changed {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	for _, id := range ids {
		if seen[id.String()] {
			continue
		}
		doc, err := env.Storage.GetDocument(env.NamespaceID, id, atCommit.DocumentTreeHash)
		if err != nil {
			return err
		}
		if doc == nil || !sql.EvalPredicate(filter, *doc, schema.None(), nil) || (canRead != nil && !canRead(id)) {
			continue
		}
		page.Writes = append(page.Writes, wire.SnapshotDoc{DocID: id.String(), Body: doc.JSON})
	}
	return nil
}

func sameRootPtr(a, b *codec.Hash) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// AttachmentProjectionTarget is a projection target that keeps the attachments of the documents it
// holds: page writes carry each document's set (applied as attachment operations, after the blobs
// are fetched), and a write-back sends its writes' sets (after uploading the blobs).
type AttachmentProjectionTarget interface {
	AttachmentsOf(doc codec.UUID) (document.AttachmentSet, error)
	AttachmentStore() *attachstore.Store
}

func parseTreeAttachments(items []wire.TreeAttachment) (document.AttachmentSet, error) {
	out := make(document.AttachmentSet, 0, len(items))
	for _, it := range items {
		id, err := codec.ParseUUID(it.ID)
		if err != nil {
			return nil, err
		}
		h, err := codec.HashFromHex(it.Hash)
		if err != nil {
			return nil, err
		}
		out = append(out, document.Attachment{ID: id, Name: it.Name, AttachmentRef: document.AttachmentRef{
			Scheme: document.AttachmentHashSchemeChunked1, ContentHash: h, Size: it.Size, Mime: it.Mime}})
	}
	return out, nil
}

// projectedAttachmentOps are the attachment operations that give every document a page writes the
// set the page carries for it, its blobs fetched first.
func (c *v2Conn) projectedAttachmentOps(t AttachmentProjectionTarget, env IngestEnv, ns string, page wire.ProjectPageMessage) ([]document.Op, error) {
	at, err := codec.HashFromHex(page.AtHex)
	if err != nil {
		return nil, err
	}
	fetch := document.Commit{Hash: at}
	var ops []document.Op
	for _, w := range page.Writes {
		id, err := codec.ParseUUID(w.DocID)
		if err != nil {
			return nil, err
		}
		want, err := parseTreeAttachments(page.Attachments[w.DocID])
		if err != nil {
			return nil, err
		}
		have, err := t.AttachmentsOf(id)
		if err != nil {
			return nil, err
		}
		docOps := document.AttachmentSetOps(id, have, want)
		for _, op := range docOps {
			if put, ok := op.(document.AttachmentPutOp); ok {
				fetch.Operations = append(fetch.Operations, put)
			}
		}
		ops = append(ops, docOps...)
	}
	if err := c.fetchBlobs(env, ns, []document.Commit{fetch}); err != nil {
		return nil, err
	}
	return ops, nil
}

// writeBlobs is a pending write's attachments as puts, for uploadBlobs.
func writeBlobs(w PendingWrite) document.Commit {
	var c document.Commit
	for _, d := range w.Docs {
		id, err := codec.ParseUUID(d.DocID)
		if err != nil || !d.AttachmentsChanged {
			continue
		}
		set, err := parseTreeAttachments(d.Attachments)
		if err != nil {
			continue
		}
		for _, a := range set {
			c.Operations = append(c.Operations, document.AttachmentPutOp{DocID: id, AttachmentID: a.ID, Name: a.Name,
				Scheme: a.Scheme, ContentHash: a.ContentHash, Size: a.Size, Mime: a.Mime})
		}
	}
	return c
}

// fetchCurrentBlobs fetches the blobs of the source's current state a refused write reports.
func (c *v2Conn) fetchCurrentBlobs(env IngestEnv, ns string, r wire.ProjectWriteResultMessage) error {
	at, err := codec.HashFromHex(r.CommitHex)
	if err != nil {
		return nil // no commit to fetch at: nothing with attachments was reported
	}
	fetch := document.Commit{Hash: at}
	for _, cur := range r.Current {
		id, err := codec.ParseUUID(cur.DocID)
		if err != nil {
			continue
		}
		set, err := parseTreeAttachments(cur.Attachments)
		if err != nil {
			return err
		}
		for _, a := range set {
			fetch.Operations = append(fetch.Operations, document.AttachmentPutOp{DocID: id, AttachmentID: a.ID, Name: a.Name,
				Scheme: a.Scheme, ContentHash: a.ContentHash, Size: a.Size, Mime: a.Mime})
		}
	}
	return c.fetchBlobs(env, ns, []document.Commit{fetch})
}
