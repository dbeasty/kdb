package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/storage/attachstore"
	"github.com/limidus/kdb/go/kdb/wire"
)

// The attachment client messages (wire/attachment_ops.go): uploads, paged reads pinned to the
// version they started on, and listings.
//
// Uploads belong to the server, not to a connection, so a client that loses its connection resumes
// from the last result's Received on a new one. An upload idle for attachmentUploadIdle is dropped
// (its partial bytes are collected with the store's unfinished segments). Reads belong to the
// connection: each pins its attachment's version until its last page, a Close, or the connection
// ending - and the store's read lease bounds even that.

const (
	attachmentUploadIdle    = time.Hour
	attachmentPageDefault   = 1 << 20
	attachmentReadsPerConn  = 64
	attachmentUploadsPerRun = 1024
)

type attachmentUpload struct {
	mu        sync.Mutex
	ns        string
	principal string
	up        *attachstore.Upload
	received  int64
	last      time.Time
	done      *attachstore.Info
}

// attachmentUploads is a runtime's in-flight uploads.
type attachmentUploads struct {
	mu sync.Mutex
	m  map[string]*attachmentUpload
}

func (u *attachmentUploads) sweepLocked(now time.Time) {
	for id, up := range u.m {
		if now.Sub(up.last) > attachmentUploadIdle {
			if up.up != nil {
				up.up.Abort()
			}
			delete(u.m, id)
		}
	}
}

type attachmentRead struct {
	reader *attachstore.Reader
	ns     string
	doc    codec.UUID
	info   wire.AttachmentInfo
	commit codec.Hash
}

// attachmentReads is one connection's open reads.
type attachmentReads struct {
	mu sync.Mutex
	m  map[string]*attachmentRead
}

func (r *attachmentReads) closeAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, rd := range r.m {
		_ = rd.reader.Close()
		delete(r.m, id)
	}
}

func newToken() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func errCodeFor(err error) wire.ErrorCode {
	switch {
	case errors.Is(err, embed.ErrNoAttachment), errors.Is(err, embed.ErrNoSuchDocument),
		errors.Is(err, embed.ErrAttachmentNotRetained):
		return wire.ErrorCodeNotFound
	case errors.Is(err, attachstore.ErrTooLarge):
		return wire.ErrorCodeResourceExhausted
	}
	var authErr *AuthorizationError
	if errors.As(err, &authErr) {
		return wire.ErrorCodeUnauthorized
	}
	code, _ := classifyError(err)
	return code
}

func (h *sqlWireConnHandler) authorizeOn(rt *KdbServerRuntime, action auth.Action) error {
	principal, ok := h.principalSnapshot()
	if !ok {
		return &AuthorizationError{Cause: errors.New("not authenticated")}
	}
	if err := rt.AuthEngine.Authorizer().Authorize(context.Background(), principal, action); err != nil {
		return &AuthorizationError{Cause: err}
	}
	return nil
}

func (h *sqlWireConnHandler) principalID() string {
	p, _ := h.principalSnapshot()
	return p.ID
}

func (h *sqlWireConnHandler) handleAttachmentUpload(msg wire.AttachmentUploadMessage) wire.Message {
	reply := func(up *attachmentUpload, id string, err error) wire.Message {
		out := wire.AttachmentUploadResultMessage{H: header(msg.H.CorrelationID, wire.MsgAttachmentUploadResult), Namespace: msg.Namespace, UploadID: id}
		if up != nil {
			out.Received = up.received
			if up.done != nil {
				out.Hash, out.Size = up.done.Hash, up.done.Size
			}
		}
		if err != nil {
			out.Error, out.ErrorCode = err.Error(), errCodeFor(err)
		}
		return out
	}
	rt, err := h.runtimeForWrite(msg.Namespace)
	if err != nil {
		return reply(nil, msg.UploadID, err)
	}
	ns := msg.Namespace
	if ns == "" {
		ns = rt.Runtime.DefaultNamespace
	}
	// Uploading is the first half of committing: it takes the commit permission.
	if err := h.authorizeOn(rt, auth.TxCommitAction{Namespace: ns}); err != nil {
		return reply(nil, msg.UploadID, err)
	}
	if len(msg.Data) > wire.MaxAttachmentChunkBytes {
		return reply(nil, msg.UploadID, fmt.Errorf("chunk of %d bytes is over the %d limit", len(msg.Data), wire.MaxAttachmentChunkBytes))
	}
	if err := rt.Runtime.AssertWritable(); err != nil {
		return reply(nil, msg.UploadID, err)
	}
	ups := &rt.attachUploads
	now := time.Now()
	ups.mu.Lock()
	ups.sweepLocked(now)
	id := msg.UploadID
	var up *attachmentUpload
	if id == "" && msg.Expect != (codec.Hash{}) {
		ups.mu.Unlock()
		if info, ok := heldBlob(rt.Runtime.AttachmentStore(ns), msg.Expect); ok {
			return reply(&attachmentUpload{received: info.Size, done: &info}, "", nil)
		}
		ups.mu.Lock()
	}
	if id == "" {
		if len(ups.m) >= attachmentUploadsPerRun {
			ups.mu.Unlock()
			return reply(nil, "", &BusyError{RetryAfterMs: 1000, Reason: "too many uploads in progress"})
		}
		id = newToken()
		up = &attachmentUpload{ns: ns, principal: h.principalID(), up: rt.Runtime.AttachmentStore(ns).Begin(), last: now}
		if ups.m == nil {
			ups.m = map[string]*attachmentUpload{}
		}
		ups.m[id] = up
	} else if up = ups.m[id]; up == nil {
		ups.mu.Unlock()
		return reply(nil, id, fmt.Errorf("%w: upload %s (expired or never started)", embed.ErrNoAttachment, id))
	}
	ups.mu.Unlock()

	up.mu.Lock()
	defer up.mu.Unlock()
	if up.ns != ns || up.principal != h.principalID() {
		return reply(nil, id, &AuthorizationError{Cause: errors.New("upload belongs to another namespace or principal")})
	}
	up.last = now
	if up.done != nil {
		return reply(up, id, nil) // a retried final chunk: already finished
	}
	if msg.Offset != up.received {
		return reply(up, id, fmt.Errorf("chunk at offset %d does not follow the %d bytes received: resume from there", msg.Offset, up.received))
	}
	if _, err := up.up.Write(msg.Data); err != nil {
		h.dropUpload(rt, id, up)
		return reply(up, id, err)
	}
	up.received += int64(len(msg.Data))
	if msg.Final {
		info, err := up.up.Finish()
		if err != nil {
			h.dropUpload(rt, id, up)
			return reply(up, id, err)
		}
		up.done, up.up = &info, nil
	}
	return reply(up, id, nil)
}

func (h *sqlWireConnHandler) dropUpload(rt *KdbServerRuntime, id string, up *attachmentUpload) {
	if up.up != nil {
		up.up.Abort()
		up.up = nil
	}
	rt.attachUploads.mu.Lock()
	delete(rt.attachUploads.m, id)
	rt.attachUploads.mu.Unlock()
}

// readHeadFor is the commit a read without AsOfHex sees: the session's read head, or the
// namespace's head.
func (h *sqlWireConnHandler) readHeadFor(rt *KdbServerRuntime, sessionID, asOfHex string) (codec.Hash, error) {
	if asOfHex != "" {
		return codec.HashFromHex(asOfHex)
	}
	if sessionID != "" {
		if sess, ok := h.sessions.Get(sessionID); ok {
			return sess.ReadHead(rt.Runtime.DAG.Head)
		}
	}
	return rt.Runtime.DAG.Head()
}

func toWireInfo(in embed.AttachmentInfo) wire.AttachmentInfo {
	return wire.AttachmentInfo{ID: in.ID, Name: in.Name, Hash: in.ContentHash, Size: in.Size, Mime: in.Mime}
}

func (h *sqlWireConnHandler) handleAttachmentList(msg wire.AttachmentListMessage) wire.Message {
	out := wire.AttachmentListResultMessage{H: header(msg.H.CorrelationID, wire.MsgAttachmentListResult), Namespace: msg.Namespace}
	fail := func(err error) wire.Message {
		out.Error, out.ErrorCode = err.Error(), errCodeFor(err)
		return out
	}
	rt, err := h.runtimeFor(msg.Namespace)
	if err != nil {
		return fail(err)
	}
	ns := msg.Namespace
	if ns == "" {
		ns = rt.Runtime.DefaultNamespace
	}
	if err := h.authorizeOn(rt, auth.DocumentReadAction{Namespace: ns, DocID: msg.DocID.String()}); err != nil {
		return fail(err)
	}
	at, err := h.readHeadFor(rt, msg.SessionID, msg.AsOfHex)
	if err != nil {
		return fail(err)
	}
	items, err := rt.Runtime.ListAttachments(ns, msg.DocID, at)
	if err != nil {
		return fail(err)
	}
	out.CommitHex = at.Hex()
	for _, in := range items {
		out.Items = append(out.Items, toWireInfo(in))
	}
	return out
}

func (h *sqlWireConnHandler) handleAttachmentGet(msg wire.AttachmentGetMessage) wire.Message {
	out := wire.AttachmentDataMessage{H: header(msg.H.CorrelationID, wire.MsgAttachmentData), Namespace: msg.Namespace, ReadID: msg.ReadID, DocID: msg.DocID}
	reads := &h.attachReads
	var rd *attachmentRead
	fail := func(err error) wire.Message {
		// A damaged stored copy goes out as INTEGRITY (errCodeFor), and to the log: the client
		// can read another replica, but only an operator can repair this one.
		if rd != nil {
			attachstore.ReportCorrupt(err, "namespace", rd.ns, "doc", rd.doc, "attachment", rd.info.ID, "name", rd.info.Name, "hash", rd.info.Hash.Hex(), "offset", msg.Offset)
		} else {
			attachstore.ReportCorrupt(err, "namespace", msg.Namespace, "doc", msg.DocID, "attachment", msg.AttachmentID, "name", msg.Name)
		}
		out.Error, out.ErrorCode = err.Error(), errCodeFor(err)
		return out
	}
	if msg.ReadID != "" {
		reads.mu.Lock()
		rd = reads.m[msg.ReadID]
		if rd != nil && msg.Close {
			delete(reads.m, msg.ReadID)
		}
		reads.mu.Unlock()
		if rd == nil {
			return fail(fmt.Errorf("%w: read %s (finished, closed or expired)", embed.ErrNoAttachment, msg.ReadID))
		}
		if msg.Close {
			_ = rd.reader.Close()
			out.EOF = true
			return out
		}
	} else {
		rt, err := h.runtimeFor(msg.Namespace)
		if err != nil {
			return fail(err)
		}
		ns := msg.Namespace
		if ns == "" {
			ns = rt.Runtime.DefaultNamespace
		}
		if err := h.authorizeOn(rt, auth.DocumentReadAction{Namespace: ns, DocID: msg.DocID.String()}); err != nil {
			return fail(err)
		}
		at, err := h.readHeadFor(rt, msg.SessionID, msg.AsOfHex)
		if err != nil {
			return fail(err)
		}
		sel := embed.AttachmentByName(msg.Name)
		if msg.AttachmentID != (codec.UUID{}) {
			sel = embed.AttachmentByID(msg.AttachmentID)
		}
		reader, info, err := rt.Runtime.GetAttachment(ns, msg.DocID, sel, at)
		if err != nil {
			return fail(err)
		}
		rd = &attachmentRead{reader: reader, ns: ns, doc: msg.DocID, info: toWireInfo(info), commit: at}
		reads.mu.Lock()
		if reads.m == nil {
			reads.m = map[string]*attachmentRead{}
		}
		if len(reads.m) >= attachmentReadsPerConn {
			reads.mu.Unlock()
			_ = reader.Close()
			return fail(&BusyError{RetryAfterMs: 1000, Reason: "too many open attachment reads on this connection"})
		}
		out.ReadID = newToken()
		reads.m[out.ReadID] = rd
		reads.mu.Unlock()
	}
	out.Info, out.CommitHex = rd.info, rd.commit.Hex()
	max := msg.MaxBytes
	if max <= 0 {
		max = attachmentPageDefault
	}
	if max > wire.MaxAttachmentChunkBytes {
		max = wire.MaxAttachmentChunkBytes
	}
	off := msg.Offset
	if off < 0 || off > rd.info.Size {
		return fail(fmt.Errorf("offset %d outside the attachment's %d bytes", off, rd.info.Size))
	}
	n := min(max, rd.info.Size-off)
	buf := make([]byte, n)
	got, err := rd.reader.ReadAt(buf, off)
	if err != nil && !(errors.Is(err, io.EOF) && int64(got) == n) {
		h.closeRead(out.ReadID)
		return fail(err)
	}
	out.Offset, out.Data = off, buf[:got]
	if off+int64(got) >= rd.info.Size {
		out.EOF = true
		h.closeRead(out.ReadID)
		out.Superseded = h.attachmentSuperseded(rd)
	}
	return out
}

func (h *sqlWireConnHandler) closeRead(id string) {
	h.attachReads.mu.Lock()
	rd := h.attachReads.m[id]
	delete(h.attachReads.m, id)
	h.attachReads.mu.Unlock()
	if rd != nil {
		_ = rd.reader.Close()
	}
}

// attachmentSuperseded reports whether the attachment a finished read served is no longer what the
// namespace's head has.
func (h *sqlWireConnHandler) attachmentSuperseded(rd *attachmentRead) bool {
	rt, err := h.runtimeFor(rd.ns)
	if err != nil {
		return false
	}
	head, err := rt.Runtime.DAG.Head()
	if err != nil || head == rd.commit {
		return false
	}
	now, err := rt.Runtime.AttachmentInfoAt(rd.ns, rd.doc, embed.AttachmentByID(rd.info.ID), head)
	if err != nil {
		return true // removed, or the document is gone
	}
	return now.ContentHash != rd.info.Hash
}

// heldBlob reports whether store holds h, and its size.
func heldBlob(store *attachstore.Store, h codec.Hash) (attachstore.Info, bool) {
	if has, err := store.Has(h); err != nil || !has {
		return attachstore.Info{}, false
	}
	r, err := store.Open(h)
	if err != nil {
		return attachstore.Info{}, false
	}
	defer r.Close()
	return attachstore.Info{Hash: h, Size: r.Size()}, true
}
