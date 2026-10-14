package wire

import (
	"github.com/limidus/kdb/go/kdb/codec"
)

// Attachment messages (docs/kdb-attachments-plan.md, PR 3 and PR 5).
//
// Client messages, behind wire feature WireFeatureAttachments:
//
//	ATTACHMENT_UPLOAD 0x44 -> ATTACHMENT_UPLOAD_RESULT 0x45   store a blob, chunk by chunk
//	ATTACHMENT_GET    0x46 -> ATTACHMENT_DATA          0x47   read one attachment, page by page
//	ATTACHMENT_LIST   0x48 -> ATTACHMENT_LIST_RESULT   0x49   a document's attachments
//
// An upload only stores bytes: it returns their content hash and size, and an ordinary
// transaction then references them with an AttachmentPutOp (or SQL binds them as an attachment
// reference parameter). So creating, replacing, renaming and deleting attachments need no messages
// of their own, and commit, conflict and lease rules are the transaction's.
//
// Peer message, behind peer-sync capability SyncCapAttachments:
//
//	BLOB_FETCH 0x42 -> BLOB_FETCH_RESULT 0x43   ranged, batched blob reads with per-chunk proofs
//
// Every one of these is always a binary body (tag 2, binary.go): bytes stay bytes. They are only
// ever sent to a peer that negotiated the feature, and every decoder accepts tag 2.

const (
	// WireFeatureAttachments: the peer speaks the attachment client messages and understands
	// CellKindBytes and the $bytes / $attachment SQL parameters. Only meaningful with
	// WireFeatureBinary.
	WireFeatureAttachments = "attachments"
	// SyncCapAttachments: the host serves BLOB_FETCH, accepts ATTACHMENT_UPLOAD on a sync session,
	// and TREE_NODES with Tree "attachments".
	SyncCapAttachments = "attachments"

	// MaxAttachmentChunkBytes bounds one upload chunk or one data page (frames are capped at 16
	// MiB). A client sends 1 MiB chunks.
	MaxAttachmentChunkBytes = 4 << 20
)

// AttachmentUploadMessage carries one chunk of a blob upload. The first chunk has no UploadID; the
// result names the upload, and every later chunk carries it with the Offset it starts at (the
// bytes received so far - so an interrupted upload resumes from the result's Received). Final
// finishes the upload: the blob is durable when the result of a Final chunk returns its hash.
//
// Expect, on a first chunk, names the blob about to be sent: a server that already holds it
// answers with its hash and size at once (an upload that is already finished), and the sender
// sends nothing more. Peer sync pushes use it to send only the blobs the host lacks.
type AttachmentUploadMessage struct {
	H         Header
	Namespace string
	SessionID string
	UploadID  string
	Offset    int64
	Data      []byte
	Final     bool
	Expect    codec.Hash
}

func (m AttachmentUploadMessage) Header() Header { return m.H }

// AttachmentUploadResultMessage answers an upload chunk: the bytes received so far and, after the
// final chunk, the blob's content hash and size.
type AttachmentUploadResultMessage struct {
	H         Header
	Namespace string
	UploadID  string
	Received  int64
	Hash      codec.Hash // set after Final
	Size      int64
	Error     string
	ErrorCode ErrorCode
}

func (m AttachmentUploadResultMessage) Header() Header { return m.H }

// AttachmentGetMessage reads one attachment of a document - by AttachmentID, or by Name - as of
// AsOfHex (empty: the session's read head), from Offset, at most MaxBytes (0: a server default).
// The first page opens a read that pins that version: its result's ReadID, sent with each later
// page, keeps reading the same bytes even if a newer version commits meanwhile. Close (with a
// ReadID) ends a read early.
type AttachmentGetMessage struct {
	H            Header
	Namespace    string
	SessionID    string
	DocID        codec.UUID
	AttachmentID codec.UUID
	Name         string
	AsOfHex      string
	Offset       int64
	MaxBytes     int64
	ReadID       string
	Close        bool
}

func (m AttachmentGetMessage) Header() Header { return m.H }

// AttachmentInfo describes one attachment.
type AttachmentInfo struct {
	ID   codec.UUID
	Name string
	Hash codec.Hash
	Size int64
	Mime string
}

// AttachmentDataMessage is one page of an attachment. EOF marks the last; the read is then closed.
// Superseded, on the last page, says the document's attachment has changed since the read began -
// a newer version exists (or it was removed).
type AttachmentDataMessage struct {
	H          Header
	Namespace  string
	ReadID     string
	DocID      codec.UUID
	Info       AttachmentInfo
	CommitHex  string
	Offset     int64
	Data       []byte
	EOF        bool
	Superseded bool
	Error      string
	ErrorCode  ErrorCode
}

func (m AttachmentDataMessage) Header() Header { return m.H }

// AttachmentListMessage lists a document's attachments as of AsOfHex (empty: the read head).
type AttachmentListMessage struct {
	H         Header
	Namespace string
	SessionID string
	DocID     codec.UUID
	AsOfHex   string
}

func (m AttachmentListMessage) Header() Header { return m.H }

// AttachmentListResultMessage is a document's attachments, sorted by name.
type AttachmentListResultMessage struct {
	H         Header
	Namespace string
	CommitHex string
	Items     []AttachmentInfo
	Error     string
	ErrorCode ErrorCode
}

func (m AttachmentListResultMessage) Header() Header { return m.H }

// BlobFetchEntry asks for part of one attachment blob: the attachment (DocID, AttachmentID) as of
// Commit must reference Hash, which is how the host checks the asker may read it. Offset must be a
// multiple of the 1 MiB chunk size; MaxBytes (0: no limit of its own) is rounded up to whole chunks.
type BlobFetchEntry struct {
	DocID        codec.UUID
	AttachmentID codec.UUID
	CommitHex    string
	Hash         codec.Hash
	Offset       int64
	MaxBytes     int64
}

// BlobFetchMessage is a batch of blob reads (BLOB_FETCH 0x42). MaxBytes bounds the whole result
// page (0: a host default).
type BlobFetchMessage struct {
	H         Header
	Namespace string
	Entries   []BlobFetchEntry
	MaxBytes  int64
}

func (m BlobFetchMessage) Header() Header { return m.H }

// BlobChunkProof proves one chunk of a result belongs to the blob: Leaf is its index, Path its RFC
// 6962 audit path to Tree.
type BlobChunkProof struct {
	Leaf int64
	Path []codec.Hash
}

// BlobFetchPart answers one entry, by its Index in the request. Data starts at Offset and covers
// whole chunks (the last may be short); Chunks proves each. Tree is the blob's Merkle tree hash:
// SHA-256(0x02 || be64 Size || Tree) is Hash. Error (with ErrorCode) replaces the data for an
// entry that failed - not retained, not referenced, not readable - without failing the others.
type BlobFetchPart struct {
	Index      int
	Hash       codec.Hash
	Size       int64
	Tree       codec.Hash
	Offset     int64
	Data       []byte
	Chunks     []BlobChunkProof
	EOF        bool
	Superseded bool
	Error      string
	ErrorCode  ErrorCode
}

// BlobFetchResultMessage answers BLOB_FETCH (BLOB_FETCH_RESULT 0x43), entries in request order.
// An entry the page had no room for is left out and More is set: ask again for what is missing,
// from the Offset each part ended at.
type BlobFetchResultMessage struct {
	H         Header
	Namespace string
	Parts     []BlobFetchPart
	More      bool
	Error     string
	ErrorCode ErrorCode
}

func (m BlobFetchResultMessage) Header() Header { return m.H }

// attachmentBinaryForm reports whether t is an always-binary attachment message.
func attachmentBinaryForm(t MessageType) bool {
	switch t {
	case MsgAttachmentUpload, MsgAttachmentUploadResult, MsgAttachmentGet, MsgAttachmentData,
		MsgAttachmentList, MsgAttachmentListResult, MsgBlobFetch, MsgBlobFetchResult:
		return true
	}
	return false
}

func appendInfo(b []byte, field int, in AttachmentInfo) []byte {
	return appendNested(b, field, func(b []byte) []byte {
		b = appendUUID(b, 1, in.ID)
		b = appendString(b, 2, in.Name)
		b = appendHash(b, 3, in.Hash)
		b = appendInt(b, 4, in.Size)
		return appendString(b, 5, in.Mime)
	})
}

func (r *binReader) info(wt int) AttachmentInfo {
	sub := r.sub(wt)
	var in AttachmentInfo
	for f, wt, ok := sub.next(); ok; f, wt, ok = sub.next() {
		switch f {
		case 1:
			in.ID = sub.uuid(wt)
		case 2:
			in.Name = sub.str(wt)
		case 3:
			in.Hash = sub.hash(wt)
		case 4:
			in.Size = sub.int(wt)
		case 5:
			in.Mime = sub.str(wt)
		default:
			sub.skip(wt)
		}
	}
	r.adopt(&sub)
	return in
}

func appendErr(b []byte, field int, msg string, code ErrorCode) []byte {
	b = appendString(b, field, msg)
	return appendString(b, field+1, string(code))
}

func appendAttachmentBody(b []byte, message Message) ([]byte, bool) {
	switch m := message.(type) {
	case AttachmentUploadMessage:
		b = appendString(b, 1, m.Namespace)
		b = appendString(b, 2, m.SessionID)
		b = appendString(b, 3, m.UploadID)
		b = appendInt(b, 4, m.Offset)
		b = appendBytes(b, 5, m.Data)
		b = appendBool(b, 6, m.Final)
		b = appendHash(b, 7, m.Expect)
	case AttachmentUploadResultMessage:
		b = appendString(b, 1, m.Namespace)
		b = appendString(b, 2, m.UploadID)
		b = appendInt(b, 3, m.Received)
		b = appendHash(b, 4, m.Hash)
		b = appendInt(b, 5, m.Size)
		b = appendErr(b, 6, m.Error, m.ErrorCode)
	case AttachmentGetMessage:
		b = appendString(b, 1, m.Namespace)
		b = appendString(b, 2, m.SessionID)
		b = appendUUID(b, 3, m.DocID)
		b = appendUUID(b, 4, m.AttachmentID)
		b = appendString(b, 5, m.Name)
		b = appendString(b, 6, m.AsOfHex)
		b = appendInt(b, 7, m.Offset)
		b = appendInt(b, 8, m.MaxBytes)
		b = appendString(b, 9, m.ReadID)
		b = appendBool(b, 10, m.Close)
	case AttachmentDataMessage:
		b = appendString(b, 1, m.Namespace)
		b = appendString(b, 2, m.ReadID)
		b = appendUUID(b, 3, m.DocID)
		b = appendInfo(b, 4, m.Info)
		b = appendString(b, 5, m.CommitHex)
		b = appendInt(b, 6, m.Offset)
		b = appendBytes(b, 7, m.Data)
		b = appendBool(b, 8, m.EOF)
		b = appendBool(b, 9, m.Superseded)
		b = appendErr(b, 10, m.Error, m.ErrorCode)
	case AttachmentListMessage:
		b = appendString(b, 1, m.Namespace)
		b = appendString(b, 2, m.SessionID)
		b = appendUUID(b, 3, m.DocID)
		b = appendString(b, 4, m.AsOfHex)
	case AttachmentListResultMessage:
		b = appendString(b, 1, m.Namespace)
		b = appendString(b, 2, m.CommitHex)
		for _, in := range m.Items {
			b = appendInfo(b, 3, in)
		}
		b = appendErr(b, 4, m.Error, m.ErrorCode)
	case BlobFetchMessage:
		b = appendString(b, 1, m.Namespace)
		for _, e := range m.Entries {
			b = appendNested(b, 2, func(b []byte) []byte {
				b = appendUUID(b, 1, e.DocID)
				b = appendUUID(b, 2, e.AttachmentID)
				b = appendString(b, 3, e.CommitHex)
				b = appendHash(b, 4, e.Hash)
				b = appendInt(b, 5, e.Offset)
				return appendInt(b, 6, e.MaxBytes)
			})
		}
		b = appendInt(b, 3, m.MaxBytes)
	case BlobFetchResultMessage:
		b = appendString(b, 1, m.Namespace)
		for _, p := range m.Parts {
			b = appendNested(b, 2, func(b []byte) []byte {
				b = appendInt(b, 1, int64(p.Index))
				b = appendHash(b, 2, p.Hash)
				b = appendInt(b, 3, p.Size)
				b = appendHash(b, 4, p.Tree)
				b = appendInt(b, 5, p.Offset)
				b = appendBytes(b, 6, p.Data)
				for _, c := range p.Chunks {
					b = appendNested(b, 7, func(b []byte) []byte {
						if c.Leaf != 0 {
							b = appendInt(b, 1, c.Leaf)
						}
						for _, h := range c.Path {
							b = appendBytes(b, 2, h.Bytes[:])
						}
						return b
					})
				}
				b = appendBool(b, 8, p.EOF)
				b = appendBool(b, 9, p.Superseded)
				return appendErr(b, 10, p.Error, p.ErrorCode)
			})
		}
		b = appendBool(b, 3, m.More)
		b = appendErr(b, 4, m.Error, m.ErrorCode)
	default:
		return b, false
	}
	return b, true
}

func decodeAttachmentBody(header Header, r *binReader) (Message, bool) {
	switch header.MessageType {
	case MsgAttachmentUpload:
		m := AttachmentUploadMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				m.SessionID = r.str(wt)
			case 3:
				m.UploadID = r.str(wt)
			case 4:
				m.Offset = r.int(wt)
			case 5:
				m.Data = cloneBytes(r.bytes(wt))
			case 6:
				m.Final = r.bool(wt)
			case 7:
				m.Expect = r.hash(wt)
			default:
				r.skip(wt)
			}
		}
		return m, true
	case MsgAttachmentUploadResult:
		m := AttachmentUploadResultMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				m.UploadID = r.str(wt)
			case 3:
				m.Received = r.int(wt)
			case 4:
				m.Hash = r.hash(wt)
			case 5:
				m.Size = r.int(wt)
			case 6:
				m.Error = r.str(wt)
			case 7:
				m.ErrorCode = ErrorCode(r.str(wt))
			default:
				r.skip(wt)
			}
		}
		return m, true
	case MsgAttachmentGet:
		m := AttachmentGetMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				m.SessionID = r.str(wt)
			case 3:
				m.DocID = r.uuid(wt)
			case 4:
				m.AttachmentID = r.uuid(wt)
			case 5:
				m.Name = r.str(wt)
			case 6:
				m.AsOfHex = r.str(wt)
			case 7:
				m.Offset = r.int(wt)
			case 8:
				m.MaxBytes = r.int(wt)
			case 9:
				m.ReadID = r.str(wt)
			case 10:
				m.Close = r.bool(wt)
			default:
				r.skip(wt)
			}
		}
		return m, true
	case MsgAttachmentData:
		m := AttachmentDataMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				m.ReadID = r.str(wt)
			case 3:
				m.DocID = r.uuid(wt)
			case 4:
				m.Info = r.info(wt)
			case 5:
				m.CommitHex = r.str(wt)
			case 6:
				m.Offset = r.int(wt)
			case 7:
				m.Data = cloneBytes(r.bytes(wt))
			case 8:
				m.EOF = r.bool(wt)
			case 9:
				m.Superseded = r.bool(wt)
			case 10:
				m.Error = r.str(wt)
			case 11:
				m.ErrorCode = ErrorCode(r.str(wt))
			default:
				r.skip(wt)
			}
		}
		return m, true
	case MsgAttachmentList:
		m := AttachmentListMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				m.SessionID = r.str(wt)
			case 3:
				m.DocID = r.uuid(wt)
			case 4:
				m.AsOfHex = r.str(wt)
			default:
				r.skip(wt)
			}
		}
		return m, true
	case MsgAttachmentListResult:
		m := AttachmentListResultMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				m.CommitHex = r.str(wt)
			case 3:
				m.Items = append(m.Items, r.info(wt))
			case 4:
				m.Error = r.str(wt)
			case 5:
				m.ErrorCode = ErrorCode(r.str(wt))
			default:
				r.skip(wt)
			}
		}
		return m, true
	case MsgBlobFetch:
		m := BlobFetchMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				sub := r.sub(wt)
				var e BlobFetchEntry
				for f, wt, ok := sub.next(); ok; f, wt, ok = sub.next() {
					switch f {
					case 1:
						e.DocID = sub.uuid(wt)
					case 2:
						e.AttachmentID = sub.uuid(wt)
					case 3:
						e.CommitHex = sub.str(wt)
					case 4:
						e.Hash = sub.hash(wt)
					case 5:
						e.Offset = sub.int(wt)
					case 6:
						e.MaxBytes = sub.int(wt)
					default:
						sub.skip(wt)
					}
				}
				r.adopt(&sub)
				m.Entries = append(m.Entries, e)
			case 3:
				m.MaxBytes = r.int(wt)
			default:
				r.skip(wt)
			}
		}
		return m, true
	case MsgBlobFetchResult:
		m := BlobFetchResultMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				sub := r.sub(wt)
				m.Parts = append(m.Parts, sub.blobPart())
				r.adopt(&sub)
			case 3:
				m.More = r.bool(wt)
			case 4:
				m.Error = r.str(wt)
			case 5:
				m.ErrorCode = ErrorCode(r.str(wt))
			default:
				r.skip(wt)
			}
		}
		return m, true
	}
	return nil, false
}

func (r *binReader) blobPart() BlobFetchPart {
	var p BlobFetchPart
	for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
		switch f {
		case 1:
			p.Index = int(r.int(wt))
		case 2:
			p.Hash = r.hash(wt)
		case 3:
			p.Size = r.int(wt)
		case 4:
			p.Tree = r.hash(wt)
		case 5:
			p.Offset = r.int(wt)
		case 6:
			p.Data = cloneBytes(r.bytes(wt))
		case 7:
			sub := r.sub(wt)
			var c BlobChunkProof
			for f, wt, ok := sub.next(); ok; f, wt, ok = sub.next() {
				switch f {
				case 1:
					c.Leaf = sub.int(wt)
				case 2:
					var h codec.Hash
					raw := sub.bytes(wt)
					if len(raw) != len(h.Bytes) {
						sub.fail("proof hash is not 32 bytes")
					}
					copy(h.Bytes[:], raw)
					c.Path = append(c.Path, h)
				default:
					sub.skip(wt)
				}
			}
			r.adopt(&sub)
			p.Chunks = append(p.Chunks, c)
		case 8:
			p.EOF = r.bool(wt)
		case 9:
			p.Superseded = r.bool(wt)
		case 10:
			p.Error = r.str(wt)
		case 11:
			p.ErrorCode = ErrorCode(r.str(wt))
		default:
			r.skip(wt)
		}
	}
	return p
}
