package client

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Attachments over the wire (docs/kdb-attachments-plan.md; wire/attachment_ops.go).
//
// A document has any number of attachments, each with a permanent ID and a name unique within
// the document. Bytes go up with UploadAttachment - which only stores them and returns their
// content hash - and an attachment is created, replaced, renamed or removed by a transaction:
// PutAttachment, RenameAttachment and DeleteAttachment commit one on their own, and
// Transaction.Attachments carries such changes in a commit beside document writes. Reads stream,
// pinned to the version they started on and checked against the content hash.
//
// Every call needs a server that agreed the "attachments" wire feature; against one that did not,
// they fail with ErrUnsupported.

// AttachmentChunkBytes is the upload chunk and default read page size.
const AttachmentChunkBytes = 1 << 20

// ErrAttachmentCorrupt reports attachment bytes that do not match their content hash: either the
// download failed this client's verification, or the server refused to serve its stored copy
// because that copy no longer matches (wire code INTEGRITY - from an attachment read, a later page
// of one, or a SQL ATTACHMENT()). Either way that server's copy is unusable: read from another
// replica, or have it repaired; retrying the same server cannot help.
var ErrAttachmentCorrupt = errors.New("kdb: attachment bytes do not match their content hash")

// ErrAttachmentRange is what an AttachmentRangeError matches with errors.Is.
var ErrAttachmentRange = errors.New("kdb: offset outside the attachment")

// AttachmentRangeError reports a ranged read starting past the attachment's end. Size is the
// attachment's length, or -1 when the offset was refused before the server was asked.
type AttachmentRangeError struct {
	Offset int64
	Size   int64
}

func (e *AttachmentRangeError) Error() string {
	if e.Size < 0 {
		return fmt.Sprintf("kdb: attachment offset %d is negative", e.Offset)
	}
	return fmt.Sprintf("kdb: attachment offset %d is past its %d bytes", e.Offset, e.Size)
}

func (e *AttachmentRangeError) Is(target error) bool { return target == ErrAttachmentRange }

// AttachmentInfo describes one attachment.
type AttachmentInfo struct {
	ID   string
	Name string
	Hash string // content hash, hex
	Size int64
	Mime string
}

// Blob is uploaded bytes: their content hash (hex) and size.
type Blob struct {
	Hash string
	Size int64
}

// MarshalJSON makes a Blob a SQL parameter: bound to `?` or `STREAM ?` where an attachment's bytes
// go, it attaches the uploaded blob.
func (b Blob) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{"$attachment": map[string]any{"hash": b.Hash, "size": b.Size}})
}

// Bytes is a SQL parameter carrying bytes (an attachment's content, say). A plain []byte binds as
// text, as it always has.
type Bytes []byte

// MarshalJSON encodes the $bytes parameter form.
func (b Bytes) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{"$bytes": base64.StdEncoding.EncodeToString(b)})
}

// AttachmentChange is one attachment change in a Transaction: a put (Hash set) creates or replaces
// attachment ID of DocID with uploaded bytes, under Name; a delete (Delete set) removes it.
type AttachmentChange struct {
	DocID  string
	ID     string
	Name   string
	Hash   string
	Size   int64
	Mime   string
	Delete bool
}

func (a AttachmentChange) op() (document.Op, error) {
	doc, err := codec.UUIDFromString(a.DocID)
	if err != nil {
		return nil, fmt.Errorf("kdb: invalid attachment docID %q: %w", a.DocID, err)
	}
	id, err := codec.UUIDFromString(a.ID)
	if err != nil {
		return nil, fmt.Errorf("kdb: invalid attachment ID %q: %w", a.ID, err)
	}
	if a.Delete {
		return document.AttachmentDeleteOp{DocID: doc, AttachmentID: id}, nil
	}
	h, err := codec.HashFromHex(a.Hash)
	if err != nil {
		return nil, fmt.Errorf("kdb: invalid attachment hash: %w", err)
	}
	return document.AttachmentPutOp{DocID: doc, AttachmentID: id, Name: a.Name,
		Scheme: document.AttachmentHashSchemeChunked1, ContentHash: h, Size: a.Size, Mime: a.Mime}, nil
}

// AttachmentRef selects an attachment of a document: by ID, or by name.
type AttachmentRef struct {
	ID   string
	Name string
}

// ByAttachmentID and ByAttachmentName build an AttachmentRef.
func ByAttachmentID(id string) AttachmentRef     { return AttachmentRef{ID: id} }
func ByAttachmentName(name string) AttachmentRef { return AttachmentRef{Name: name} }

func (c *Client) requireAttachments() error {
	if !slices.Contains(c.features, wire.WireFeatureAttachments) {
		return &UnsupportedError{Message: "the server did not agree the attachments wire feature"}
	}
	return nil
}

func hdr(c *Client, t wire.MessageType) wire.Header {
	return wire.Header{MessageType: t, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()}
}

func attachmentError(msg string, code wire.ErrorCode) error {
	if code == wire.ErrorCodeNotFound {
		return fmt.Errorf("%w: %s", ErrNotFound, msg)
	}
	if code == "" {
		return fmt.Errorf("kdb: %s", msg)
	}
	return classifiedError(msg, &code, nil)
}

// UploadError reports an upload that failed part way; ResumeUpload continues it.
type UploadError struct {
	UploadID string
	Received int64
	Err      error
}

func (e *UploadError) Error() string {
	return fmt.Sprintf("kdb: upload %s failed after %d bytes: %v", e.UploadID, e.Received, e.Err)
}
func (e *UploadError) Unwrap() error { return e.Err }

// UploadAttachment stores r's bytes in ns's attachment store and returns their hash and size. The
// bytes are durable when it returns, and referenced by nothing until a transaction attaches them.
func (c *Client) UploadAttachment(ctx context.Context, ns string, r io.Reader) (Blob, error) {
	return c.ResumeUpload(ctx, ns, "", 0, r)
}

// ResumeUpload continues an upload from an UploadError: r yields the bytes from received on.
func (c *Client) ResumeUpload(ctx context.Context, ns, uploadID string, received int64, r io.Reader) (Blob, error) {
	if err := c.requireAttachments(); err != nil {
		return Blob{}, err
	}
	buf := make([]byte, AttachmentChunkBytes)
	next := make([]byte, AttachmentChunkBytes)
	n, err := io.ReadFull(r, buf)
	eof := errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
	if err != nil && !eof {
		return Blob{}, err
	}
	offset := received
	for {
		var m int
		final := eof
		if !eof {
			// Read one chunk ahead, so the last chunk is the one marked final.
			m, err = io.ReadFull(r, next)
			final = m == 0 && (errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF))
			if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return Blob{}, &UploadError{UploadID: uploadID, Received: offset, Err: err}
			}
			eof = err != nil
		}
		reply, rerr := c.request(ctx, wire.AttachmentUploadMessage{H: hdr(c, wire.MsgAttachmentUpload), Namespace: ns,
			UploadID: uploadID, Offset: offset, Data: buf[:n], Final: final})
		if rerr != nil {
			return Blob{}, &UploadError{UploadID: uploadID, Received: offset, Err: rerr}
		}
		res, ok := reply.(wire.AttachmentUploadResultMessage)
		if !ok {
			return Blob{}, fmt.Errorf("kdb: expected ATTACHMENT_UPLOAD_RESULT, got %T", reply)
		}
		if res.UploadID != "" {
			uploadID = res.UploadID
		}
		if res.Error != "" {
			return Blob{}, &UploadError{UploadID: uploadID, Received: res.Received, Err: attachmentError(res.Error, res.ErrorCode)}
		}
		offset = res.Received
		if final {
			return Blob{Hash: res.Hash.Hex(), Size: res.Size}, nil
		}
		buf, next, n = next, buf, m
	}
}

// ListAttachments lists docID's attachments, sorted by name, as of asOf (empty: the head), and the
// commit they were read at.
func (c *Client) ListAttachments(ctx context.Context, ns, docID, asOf string) ([]AttachmentInfo, string, error) {
	if err := c.requireAttachments(); err != nil {
		return nil, "", err
	}
	doc, err := codec.UUIDFromString(docID)
	if err != nil {
		return nil, "", fmt.Errorf("kdb: invalid docID: %w", err)
	}
	reply, err := c.request(ctx, wire.AttachmentListMessage{H: hdr(c, wire.MsgAttachmentList), Namespace: ns, DocID: doc, AsOfHex: asOf})
	if err != nil {
		return nil, "", err
	}
	res, ok := reply.(wire.AttachmentListResultMessage)
	if !ok {
		return nil, "", fmt.Errorf("kdb: expected ATTACHMENT_LIST_RESULT, got %T", reply)
	}
	if res.Error != "" {
		return nil, "", attachmentError(res.Error, res.ErrorCode)
	}
	out := make([]AttachmentInfo, len(res.Items))
	for i, in := range res.Items {
		out[i] = fromWireInfo(in)
	}
	return out, res.CommitHex, nil
}

func fromWireInfo(in wire.AttachmentInfo) AttachmentInfo {
	return AttachmentInfo{ID: in.ID.String(), Name: in.Name, Hash: in.Hash.Hex(), Size: in.Size, Mime: in.Mime}
}

// AttachmentReader streams one attachment, verifying its bytes against the content hash. It reads
// the version that was current when it was opened, to the end (or the end of its range), whatever
// commits meanwhile.
type AttachmentReader struct {
	c          *Client
	ctx        context.Context
	ns         string
	readID     string
	info       AttachmentInfo
	commit     string
	page       []byte
	offset     int64
	eof        bool
	closed     bool
	superseded bool
	merkle     document.MerkleBuilder
	chunk      []byte

	// A ranged read (OpenAttachmentRange): pages are whole chunks from pageOff, each proved against
	// tree, and only [offset, end) of them is handed out.
	ranged  bool
	length  int64 // as asked; -1 to the end
	end     int64 // -1 until the first page gives the size
	pageOff int64
	tree    codec.Hash
}

// Info describes the attachment being read.
func (r *AttachmentReader) Info() AttachmentInfo { return r.info }

// Commit is the commit the attachment was read at.
func (r *AttachmentReader) Commit() string { return r.commit }

// Superseded, after the last byte has been read (of the range, for a ranged read), reports that the
// attachment has changed since the read began: a newer version exists, or it was removed.
func (r *AttachmentReader) Superseded() bool { return r.superseded }

// OpenAttachment opens one of docID's attachments as of asOf (empty: the head).
func (c *Client) OpenAttachment(ctx context.Context, ns, docID string, ref AttachmentRef, asOf string) (*AttachmentReader, error) {
	if err := c.requireAttachments(); err != nil {
		return nil, err
	}
	msg, err := attachmentGet(c, ns, docID, ref, asOf)
	if err != nil {
		return nil, err
	}
	msg.MaxBytes = AttachmentChunkBytes
	r := &AttachmentReader{c: c, ctx: ctx, ns: ns}
	if err := r.fetch(msg); err != nil {
		return nil, err
	}
	if err := r.verify(r.page); err != nil {
		return nil, err
	}
	return r, nil
}

func attachmentGet(c *Client, ns, docID string, ref AttachmentRef, asOf string) (wire.AttachmentGetMessage, error) {
	doc, err := codec.UUIDFromString(docID)
	if err != nil {
		return wire.AttachmentGetMessage{}, fmt.Errorf("kdb: invalid docID: %w", err)
	}
	msg := wire.AttachmentGetMessage{H: hdr(c, wire.MsgAttachmentGet), Namespace: ns, DocID: doc, Name: ref.Name, AsOfHex: asOf}
	if ref.ID != "" {
		if msg.AttachmentID, err = codec.UUIDFromString(ref.ID); err != nil {
			return wire.AttachmentGetMessage{}, fmt.Errorf("kdb: invalid attachment ID: %w", err)
		}
	}
	return msg, nil
}

// OpenAttachmentRange opens length bytes of one of docID's attachments from offset (length -1: to
// the end; a range running past the end stops there), as of asOf (empty: the head). Every chunk is
// checked against the content hash with its inclusion proof before any of its bytes is returned,
// so a mismatch is ErrAttachmentCorrupt with no bad byte handed out. An offset past the end is an
// AttachmentRangeError. The server must have agreed the "attachment-proofs" wire feature.
func (c *Client) OpenAttachmentRange(ctx context.Context, ns, docID string, ref AttachmentRef, asOf string, offset, length int64) (*AttachmentReader, error) {
	if err := c.requireAttachments(); err != nil {
		return nil, err
	}
	if !slices.Contains(c.features, wire.WireFeatureAttachmentProofs) {
		return nil, &UnsupportedError{Message: "the server did not agree the attachment-proofs wire feature"}
	}
	if offset < 0 {
		return nil, &AttachmentRangeError{Offset: offset, Size: -1}
	}
	if length < -1 {
		return nil, fmt.Errorf("kdb: invalid attachment range length %d", length)
	}
	msg, err := attachmentGet(c, ns, docID, ref, asOf)
	if err != nil {
		return nil, err
	}
	msg.Offset, msg.Proofs = offset, true
	msg.MaxBytes = wire.MaxAttachmentChunkBytes
	if length >= 0 {
		msg.MaxBytes = min(max(length, 1), wire.MaxAttachmentChunkBytes)
	}
	r := &AttachmentReader{c: c, ctx: ctx, ns: ns, ranged: true, offset: offset, length: length, end: -1, pageOff: offset / AttachmentChunkBytes * AttachmentChunkBytes}
	reply, err := c.request(ctx, msg)
	if err != nil {
		return nil, err
	}
	res, ok := reply.(wire.AttachmentDataMessage)
	if !ok {
		return nil, fmt.Errorf("kdb: expected ATTACHMENT_DATA, got %T", reply)
	}
	if res.Error != "" && res.Info.ID != (codec.UUID{}) && offset > res.Info.Size {
		return nil, &AttachmentRangeError{Offset: offset, Size: res.Info.Size}
	}
	if err := r.takeVerified(res); err != nil {
		if r.readID != "" {
			_ = r.Close()
		}
		return nil, err
	}
	return r, nil
}

// takeVerified checks a ranged read's page - every chunk against the content hash by its proof -
// and keeps the part of it inside the range.
func (r *AttachmentReader) takeVerified(res wire.AttachmentDataMessage) error {
	if res.Error != "" {
		return attachmentError(res.Error, res.ErrorCode)
	}
	if r.end < 0 {
		r.readID, r.info, r.commit = res.ReadID, fromWireInfo(res.Info), res.CommitHex
		r.tree, r.end = res.Tree, res.Info.Size
		if length := r.end - r.offset; r.length >= 0 && r.length < length {
			r.end = r.offset + r.length
		}
		if res.Scheme != int(document.AttachmentHashSchemeChunked1) {
			return fmt.Errorf("kdb: attachment content hash scheme %d is not supported", res.Scheme)
		}
		if document.AttachmentIdentity(res.Tree, res.Info.Size).Hex() != r.info.Hash {
			return ErrAttachmentCorrupt
		}
	}
	if res.Offset != r.pageOff || res.Tree != r.tree {
		return fmt.Errorf("kdb: attachment page at %d, expected %d", res.Offset, r.pageOff)
	}
	if err := verifyChunks(res.Data, res.Offset, r.info.Size, res.Chunks, r.tree); err != nil {
		return err
	}
	r.pageOff += int64(len(res.Data))
	if len(res.Data) == 0 && !res.EOF {
		return fmt.Errorf("kdb: empty attachment page at %d before the end", res.Offset)
	}
	from, to := r.offset-res.Offset, min(r.end, r.pageOff)-res.Offset
	r.page = nil
	if from < to {
		r.page = res.Data[from:to]
	}
	r.eof, r.superseded = res.EOF, res.Superseded
	return nil
}

// verifyChunks checks that data, read from offset (a chunk boundary) of an attachment of size
// bytes, is whole chunks each proved by chunks against tree, and nothing more.
func verifyChunks(data []byte, offset, size int64, chunks []wire.BlobChunkProof, tree codec.Hash) error {
	leaves := document.AttachmentChunkCount(size)
	first := offset / AttachmentChunkBytes
	if offset%AttachmentChunkBytes != 0 {
		return fmt.Errorf("kdb: attachment page at %d is not chunk-aligned", offset)
	}
	for i, proof := range chunks {
		idx := first + int64(i)
		if proof.Leaf != idx {
			return fmt.Errorf("kdb: attachment proof for chunk %d where %d was expected", proof.Leaf, idx)
		}
		n := min(AttachmentChunkBytes, size-idx*AttachmentChunkBytes)
		if n < 0 || int64(len(data)) < n {
			return ErrAttachmentCorrupt
		}
		if !document.VerifyMerkleInclusion(document.ChunkLeafHash(data[:n]), int(idx), int(leaves), proof.Path, tree) {
			return ErrAttachmentCorrupt
		}
		data = data[n:]
	}
	if len(data) != 0 {
		return ErrAttachmentCorrupt // bytes no proof covers
	}
	return nil
}

func (r *AttachmentReader) fetch(msg wire.AttachmentGetMessage) error {
	reply, err := r.c.request(r.ctx, msg)
	if err != nil {
		return err
	}
	res, ok := reply.(wire.AttachmentDataMessage)
	if !ok {
		return fmt.Errorf("kdb: expected ATTACHMENT_DATA, got %T", reply)
	}
	if res.Error != "" {
		return attachmentError(res.Error, res.ErrorCode)
	}
	if r.readID == "" {
		r.readID, r.info, r.commit = res.ReadID, fromWireInfo(res.Info), res.CommitHex
	}
	if res.Offset != r.offset {
		return fmt.Errorf("kdb: attachment page at %d, expected %d", res.Offset, r.offset)
	}
	r.page, r.eof, r.superseded = res.Data, res.EOF, res.Superseded
	return nil
}

// Read implements io.Reader. The final bytes are returned only once the whole attachment has
// matched its content hash; a mismatch is ErrAttachmentCorrupt.
func (r *AttachmentReader) Read(p []byte) (int, error) {
	for len(r.page) == 0 {
		if r.eof {
			return 0, io.EOF
		}
		if r.ranged {
			if r.pageOff >= r.end {
				// The range is read, short of the attachment's end: release the pinned read.
				if err := r.Close(); err != nil {
					return 0, err
				}
				return 0, io.EOF
			}
			if err := r.nextVerified(); err != nil {
				return 0, err
			}
			continue
		}
		if err := r.fetch(wire.AttachmentGetMessage{H: hdr(r.c, wire.MsgAttachmentGet), Namespace: r.ns,
			ReadID: r.readID, Offset: r.offset, MaxBytes: AttachmentChunkBytes}); err != nil {
			return 0, err
		}
		if err := r.verify(r.page); err != nil {
			return 0, err
		}
	}
	n := copy(p, r.page)
	r.page = r.page[n:]
	r.offset += int64(n)
	return n, nil
}

func (r *AttachmentReader) nextVerified() error {
	reply, err := r.c.request(r.ctx, wire.AttachmentGetMessage{H: hdr(r.c, wire.MsgAttachmentGet), Namespace: r.ns,
		ReadID: r.readID, Offset: r.pageOff, MaxBytes: min(r.end-r.pageOff, wire.MaxAttachmentChunkBytes), Proofs: true})
	if err != nil {
		return err
	}
	res, ok := reply.(wire.AttachmentDataMessage)
	if !ok {
		return fmt.Errorf("kdb: expected ATTACHMENT_DATA, got %T", reply)
	}
	return r.takeVerified(res)
}

// verify feeds a fetched page to the content hash, checking it when the page is the last.
func (r *AttachmentReader) verify(page []byte) error {
	for len(page) > 0 {
		take := min(len(page), AttachmentChunkBytes-len(r.chunk))
		r.chunk = append(r.chunk, page[:take]...)
		page = page[take:]
		if len(r.chunk) == AttachmentChunkBytes {
			r.merkle.Add(document.ChunkLeafHash(r.chunk))
			r.chunk = r.chunk[:0]
		}
	}
	if !r.eof {
		return nil
	}
	if len(r.chunk) > 0 || r.merkle.Leaves() == 0 {
		r.merkle.Add(document.ChunkLeafHash(r.chunk))
	}
	if got := document.AttachmentIdentity(r.merkle.Root(), r.info.Size); got.Hex() != r.info.Hash {
		r.page = nil
		return ErrAttachmentCorrupt
	}
	return nil
}

// Close ends the read; the server releases the version it pinned.
func (r *AttachmentReader) Close() error {
	if r.closed || r.eof {
		r.closed = true
		return nil
	}
	r.closed = true
	reply, err := r.c.request(r.ctx, wire.AttachmentGetMessage{H: hdr(r.c, wire.MsgAttachmentGet), Namespace: r.ns, ReadID: r.readID, Close: true})
	if done, ok := reply.(wire.AttachmentDataMessage); ok && err == nil && done.Error == "" {
		// A server that reports it says whether the attachment changed while it was read.
		r.superseded = done.Superseded
	}
	return err
}

// ReadAttachmentAt reads len(p) bytes of one of docID's attachments from off, as of asOf (empty:
// the head), like io.ReaderAt: fewer than len(p) bytes come with io.EOF. Each call is its own read,
// so to read one version across calls pass a commit as asOf (an AttachmentReader's Commit, say).
// Every byte is verified by its chunk's inclusion proof; off past the end is an
// AttachmentRangeError.
func (c *Client) ReadAttachmentAt(ctx context.Context, ns, docID string, ref AttachmentRef, asOf string, p []byte, off int64) (int, error) {
	r, err := c.OpenAttachmentRange(ctx, ns, docID, ref, asOf, off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	defer r.Close()
	n, err := io.ReadFull(r, p)
	if errors.Is(err, io.ErrUnexpectedEOF) || (err == io.EOF && len(p) > 0) {
		err = io.EOF
	}
	return n, err
}

// ReadAttachment reads a whole attachment into memory.
func (c *Client) ReadAttachment(ctx context.Context, ns, docID string, ref AttachmentRef, asOf string) ([]byte, AttachmentInfo, error) {
	r, err := c.OpenAttachment(ctx, ns, docID, ref, asOf)
	if err != nil {
		return nil, AttachmentInfo{}, err
	}
	defer r.Close()
	var out bytes.Buffer
	out.Grow(int(min(r.info.Size, 1<<30)))
	if _, err := io.Copy(&out, r); err != nil {
		return nil, AttachmentInfo{}, err
	}
	return out.Bytes(), r.info, nil
}

// PutAttachmentOptions chooses which attachment PutAttachment writes:
//   - ID set: that attachment, created or replaced (Name, when set, renames it);
//   - only Name: the attachment of that name, replaced keeping its ID, or else a new one;
//   - neither: a new attachment, with a fresh ID as its name.
//
// Mime left empty keeps a replaced attachment's type.
type PutAttachmentOptions struct {
	ID   string
	Name string
	Mime string
}

// PutAttachment uploads r's bytes and commits them as one of docID's attachments. It returns the
// commit and the attachment.
func (c *Client) PutAttachment(ctx context.Context, ns, docID string, r io.Reader, opts PutAttachmentOptions) (string, AttachmentInfo, error) {
	blob, err := c.UploadAttachment(ctx, ns, r)
	if err != nil {
		return "", AttachmentInfo{}, err
	}
	return c.attach(ctx, ns, docID, opts, blob)
}

func (c *Client) attach(ctx context.Context, ns, docID string, opts PutAttachmentOptions, blob Blob) (string, AttachmentInfo, error) {
	a := AttachmentInfo{ID: opts.ID, Name: opts.Name, Hash: blob.Hash, Size: blob.Size, Mime: opts.Mime}
	if opts.ID != "" || opts.Name != "" {
		list, _, err := c.ListAttachments(ctx, ns, docID, "")
		if err != nil {
			return "", AttachmentInfo{}, err
		}
		for _, have := range list {
			if (opts.ID != "" && strings.EqualFold(have.ID, opts.ID)) || (opts.ID == "" && have.Name == opts.Name) {
				a.ID = have.ID
				if a.Name == "" {
					a.Name = have.Name
				}
				if a.Mime == "" {
					a.Mime = have.Mime
				}
			}
		}
	}
	if a.ID == "" {
		id, err := codec.RandomUUID()
		if err != nil {
			return "", AttachmentInfo{}, err
		}
		a.ID = id.String()
	}
	if a.Name == "" {
		a.Name = a.ID
	}
	commit, err := c.commitAttachmentChanges(ctx, ns, AttachmentChange{DocID: docID, ID: a.ID, Name: a.Name, Hash: a.Hash, Size: a.Size, Mime: a.Mime})
	if err != nil {
		return "", AttachmentInfo{}, err
	}
	return commit, a, nil
}

// RenameAttachment gives one of docID's attachments a new name, keeping its ID and bytes.
func (c *Client) RenameAttachment(ctx context.Context, ns, docID string, ref AttachmentRef, newName string) (string, error) {
	a, err := c.findAttachment(ctx, ns, docID, ref)
	if err != nil {
		return "", err
	}
	return c.commitAttachmentChanges(ctx, ns, AttachmentChange{DocID: docID, ID: a.ID, Name: newName, Hash: a.Hash, Size: a.Size, Mime: a.Mime})
}

// DeleteAttachment removes one of docID's attachments.
func (c *Client) DeleteAttachment(ctx context.Context, ns, docID string, ref AttachmentRef) (string, error) {
	id := ref.ID
	if id == "" {
		a, err := c.findAttachment(ctx, ns, docID, ref)
		if err != nil {
			return "", err
		}
		id = a.ID
	}
	return c.commitAttachmentChanges(ctx, ns, AttachmentChange{DocID: docID, ID: id, Delete: true})
}

func (c *Client) findAttachment(ctx context.Context, ns, docID string, ref AttachmentRef) (AttachmentInfo, error) {
	list, _, err := c.ListAttachments(ctx, ns, docID, "")
	if err != nil {
		return AttachmentInfo{}, err
	}
	for _, a := range list {
		if (ref.ID != "" && strings.EqualFold(a.ID, ref.ID)) || (ref.ID == "" && a.Name == ref.Name) {
			return a, nil
		}
	}
	return AttachmentInfo{}, fmt.Errorf("%w: attachment %s%s of %s", ErrNotFound, ref.ID, ref.Name, docID)
}

func (c *Client) commitAttachmentChanges(ctx context.Context, ns string, changes ...AttachmentChange) (string, error) {
	st, err := c.ensureNamespace(ctx, ns)
	if err != nil {
		return "", err
	}
	ops := make([]document.Op, 0, len(changes))
	for _, ch := range changes {
		op, err := ch.op()
		if err != nil {
			return "", err
		}
		ops = append(ops, op)
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		return "", err
	}
	st.mu.Lock()
	base := st.head
	st.mu.Unlock()
	tx := document.Transaction{ID: txID, BaseVersion: base, Operations: ops, Timestamp: codec.TimestampNow(), AuthorNodeID: c.authorNodeID}
	return c.commitTransaction(ctx, ns, st, tx)
}
