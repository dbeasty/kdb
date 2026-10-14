package wire

import (
	"github.com/limidus/kdb/go/kdb/document"
)

// Binary bodies of the rest of the client message set: search, document leases, history and
// revert, cross-namespace commit, and the subscription stream's DeltaCommit. Same rules as
// binary_messages.go: field numbers are protocol, never renumber or reuse one.
//
// Nested values (a search arm, a hit, a history commit, an operation) are length-delimited
// fields holding their own fields. Doubles are fixed64 (wire type 1). Ids and hashes inside a
// DeltaCommit are their raw 16 and 32 bytes, not hex.

// Operation kinds in a binary DeltaCommit.
const (
	binOpWrite           = 1
	binOpDelete          = 2
	binOpFileWrite       = 3
	binOpSchemaMigration = 4
	binOpAttachmentPut   = 5
	binOpAttachmentDel   = 6
)

func appendMoreBinaryBody(b []byte, message Message) ([]byte, bool) {
	switch m := message.(type) {
	case LockAcquireMessage:
		b = appendLockRequest(b, m.Namespace, m.SessionID, m.DocID, m.TTLMillis)
	case LockRenewMessage:
		b = appendLockRequest(b, m.Namespace, m.SessionID, m.DocID, m.TTLMillis)
	case LockReleaseMessage:
		b = appendLockRequest(b, m.Namespace, m.SessionID, m.DocID, 0)
	case LockResultMessage:
		b = appendString(b, 1, m.Namespace)
		b = appendString(b, 2, m.SessionID)
		b = appendString(b, 3, m.DocID)
		b = appendBool(b, 4, m.Granted)
		if m.Fence != 0 {
			b = appendKey(b, 5, wtVarint)
			b = appendUvarintValue(b, m.Fence)
		}
		b = appendInt(b, 6, m.ExpiresAtMillis)
		b = appendOptString(b, 7, m.HolderSessionID)
		b = appendOptString(b, 8, m.Error)
		b = appendOptErrorCode(b, 9, m.ErrorCode)
	case SearchMessage:
		b = appendString(b, 1, m.Namespace)
		b = appendString(b, 2, m.SessionID)
		if t := m.Text; t != nil {
			b = appendNested(b, 3, func(b []byte) []byte {
				b = appendString(b, 1, t.Index)
				b = appendString(b, 2, t.Query)
				b = appendInt(b, 3, int64(t.Depth))
				b = appendOptDouble(b, 4, t.MinScore)
				return appendOptDouble(b, 5, t.Weight)
			})
		}
		if v := m.Vector; v != nil {
			b = appendNested(b, 4, func(b []byte) []byte {
				b = appendString(b, 1, v.Index)
				for _, x := range v.Vector {
					b = appendRawDouble(b, 2, x)
				}
				b = appendInt(b, 3, int64(v.Depth))
				b = appendOptDouble(b, 4, v.MinScore)
				return appendOptDouble(b, 5, v.Weight)
			})
		}
		b = appendString(b, 5, m.Fusion)
		b = appendInt(b, 6, int64(m.Limit))
		b = appendBool(b, 7, m.IncludeJSON)
		b = appendString(b, 8, m.AtCommitHex)
	case SearchResultMessage:
		b = appendString(b, 1, m.Namespace)
		for _, h := range m.Hits {
			b = appendNested(b, 2, func(b []byte) []byte {
				b = appendString(b, 1, h.DocID)
				b = appendDouble(b, 2, h.Score)
				return appendOptString(b, 3, h.JSON)
			})
		}
		b = appendString(b, 3, m.ResolvedCommitHex)
		b = appendOptString(b, 4, m.Error)
		b = appendOptErrorCode(b, 5, m.ErrorCode)
		b = appendOptInt(b, 6, m.RetryAfterMs)
	case HistoryListMessage:
		b = appendString(b, 1, m.Namespace)
		b = appendString(b, 2, m.From)
		b = appendInt(b, 3, int64(m.Skip))
		b = appendInt(b, 4, int64(m.Limit))
	case HistoryResultMessage:
		b = appendString(b, 1, m.Namespace)
		for _, c := range m.Commits {
			b = appendNested(b, 2, func(b []byte) []byte {
				b = appendString(b, 1, c.CommitHex)
				for _, p := range c.ParentHexes {
					b = appendRawString(b, 2, p)
				}
				b = appendString(b, 3, c.TransactionID)
				b = appendInt(b, 4, c.TimestampMicros)
				b = appendString(b, 5, c.AuthorNodeID)
				b = appendString(b, 6, c.TreeHex)
				b = appendString(b, 7, c.Message)
				b = appendInt(b, 8, int64(c.OperationCount))
				return appendBool(b, 9, c.Stubbed)
			})
		}
		b = appendString(b, 3, m.ResolvedHex)
		b = appendOptString(b, 4, m.Error)
		b = appendOptErrorCode(b, 5, m.ErrorCode)
		b = appendOptInt(b, 6, m.RetryAfterMs)
	case RevertMessage:
		b = appendString(b, 1, m.Namespace)
		b = appendString(b, 2, m.To)
	case RevertResultMessage:
		b = appendString(b, 1, m.Namespace)
		b = appendString(b, 2, m.CommitHex)
		b = appendString(b, 3, m.TargetHex)
		b = appendInt(b, 4, int64(m.Restored))
		b = appendInt(b, 5, int64(m.Removed))
		b = appendOptString(b, 6, m.Error)
		b = appendOptErrorCode(b, 7, m.ErrorCode)
		b = appendOptInt(b, 8, m.RetryAfterMs)
	case TxCommitMultiMessage:
		b = appendString(b, 1, m.SessionID)
		for _, p := range m.Parts {
			b = appendNested(b, 2, func(b []byte) []byte {
				b = appendString(b, 1, p.Namespace)
				return appendBytes(b, 2, p.TransactionBytes)
			})
		}
	case TxCommitMultiResultMessage:
		b = appendString(b, 1, m.GroupID)
		for _, p := range m.Parts {
			b = appendNested(b, 2, func(b []byte) []byte {
				b = appendString(b, 1, p.Namespace)
				return appendString(b, 2, p.CommitHex)
			})
		}
		b = appendString(b, 3, m.FailedNamespace)
		b = appendBytes(b, 4, m.ConflictReport)
		b = appendOptString(b, 5, m.Error)
		b = appendOptErrorCode(b, 6, m.ErrorCode)
		b = appendOptInt(b, 7, m.RetryAfterMs)
	case DeltaCommitMessage:
		b = appendDeltaCommit(b, m.Payload)
	default:
		return b, false
	}
	return b, true
}

func appendLockRequest(b []byte, ns, sessionID, docID string, ttl int) []byte {
	b = appendString(b, 1, ns)
	b = appendString(b, 2, sessionID)
	b = appendString(b, 3, docID)
	return appendInt(b, 4, int64(ttl))
}

func appendUvarintValue(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

func appendDeltaCommit(b []byte, p DeltaCommitPayload) []byte {
	b = appendString(b, 1, p.Namespace)
	b = appendHash(b, 2, p.CommitHash)
	b = appendHash(b, 3, p.ParentHash)
	b = appendInt(b, 4, p.TimestampMicros)
	for _, op := range p.Operations {
		b = appendNested(b, 5, func(b []byte) []byte { return appendOp(b, op) })
	}
	for _, h := range p.IndexHints {
		b = appendNested(b, 6, func(b []byte) []byte {
			b = appendUUID(b, 1, h.IndexID)
			b = appendString(b, 2, h.FieldName)
			b = appendString(b, 3, h.IndexType)
			b = appendString(b, 4, h.Action)
			b = appendUUID(b, 5, h.DocID)
			b = appendOptString(b, 6, h.Key)
			return appendHash(b, 7, h.CommitHash)
		})
	}
	return appendBytes(b, 7, p.SchemaDeltaBytes)
}

// appendOp writes one operation. A kind this build does not know is written as kind 0, which a
// decoder refuses - exactly as the JSON envelope's "unknown" kind is refused.
func appendOp(b []byte, op document.Op) []byte {
	switch o := op.(type) {
	case document.WriteOp:
		b = appendInt(b, 1, binOpWrite)
		b = appendUUID(b, 2, o.DocID)
		b = appendRawString(b, 3, o.Patch)
		return appendBool(b, 4, o.Replace)
	case document.DeleteOp:
		b = appendInt(b, 1, binOpDelete)
		return appendUUID(b, 2, o.DocID)
	case document.FileWriteOp:
		b = appendInt(b, 1, binOpFileWrite)
		b = appendRawString(b, 5, o.Path)
		return appendHash(b, 6, o.BlobHash)
	case document.SchemaMigrationOp:
		b = appendInt(b, 1, binOpSchemaMigration)
		b = appendUUID(b, 7, o.MigrationID)
		return appendRawString(b, 8, o.MigrationPayload)
	case document.AttachmentPutOp:
		b = appendInt(b, 1, binOpAttachmentPut)
		b = appendUUID(b, 2, o.DocID)
		b = appendBytes(b, 6, o.ContentHash.Bytes[:])
		b = appendInt(b, 9, o.Size)
		return appendRawString(b, 10, o.Mime)
	case document.AttachmentDeleteOp:
		b = appendInt(b, 1, binOpAttachmentDel)
		return appendUUID(b, 2, o.DocID)
	default:
		return b
	}
}

func decodeMoreBinaryBody(header Header, r *binReader) (Message, bool) {
	switch header.MessageType {
	case MsgLockAcquire, MsgLockRenew, MsgLockRelease:
		var ns, sid, doc string
		var ttl int
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				ns = r.str(wt)
			case 2:
				sid = r.str(wt)
			case 3:
				doc = r.str(wt)
			case 4:
				ttl = int(r.int(wt))
			default:
				r.skip(wt)
			}
		}
		switch header.MessageType {
		case MsgLockAcquire:
			return LockAcquireMessage{H: header, Namespace: ns, SessionID: sid, DocID: doc, TTLMillis: ttl}, true
		case MsgLockRenew:
			return LockRenewMessage{H: header, Namespace: ns, SessionID: sid, DocID: doc, TTLMillis: ttl}, true
		default:
			return LockReleaseMessage{H: header, Namespace: ns, SessionID: sid, DocID: doc}, true
		}
	case MsgLockResult:
		m := LockResultMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				m.SessionID = r.str(wt)
			case 3:
				m.DocID = r.str(wt)
			case 4:
				m.Granted = r.bool(wt)
			case 5:
				m.Fence = uint64(r.int(wt))
			case 6:
				m.ExpiresAtMillis = r.int(wt)
			case 7:
				m.HolderSessionID = r.optStr(wt)
			case 8:
				m.Error = r.optStr(wt)
			case 9:
				m.ErrorCode = r.optErrorCode(wt)
			default:
				r.skip(wt)
			}
		}
		return m, true
	case MsgSearch:
		m := SearchMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				m.SessionID = r.str(wt)
			case 3:
				sub := r.sub(wt)
				t := &SearchTextArm{}
				for f, wt, ok := sub.next(); ok; f, wt, ok = sub.next() {
					switch f {
					case 1:
						t.Index = sub.str(wt)
					case 2:
						t.Query = sub.str(wt)
					case 3:
						t.Depth = int(sub.int(wt))
					case 4:
						t.MinScore = sub.optDouble(wt)
					case 5:
						t.Weight = sub.optDouble(wt)
					default:
						sub.skip(wt)
					}
				}
				r.adopt(&sub)
				m.Text = t
			case 4:
				sub := r.sub(wt)
				v := &SearchVectorArm{}
				for f, wt, ok := sub.next(); ok; f, wt, ok = sub.next() {
					switch f {
					case 1:
						v.Index = sub.str(wt)
					case 2:
						v.Vector = append(v.Vector, sub.double(wt))
					case 3:
						v.Depth = int(sub.int(wt))
					case 4:
						v.MinScore = sub.optDouble(wt)
					case 5:
						v.Weight = sub.optDouble(wt)
					default:
						sub.skip(wt)
					}
				}
				r.adopt(&sub)
				m.Vector = v
			case 5:
				m.Fusion = r.str(wt)
			case 6:
				m.Limit = int(r.int(wt))
			case 7:
				m.IncludeJSON = r.bool(wt)
			case 8:
				m.AtCommitHex = r.str(wt)
			default:
				r.skip(wt)
			}
		}
		return m, true
	case MsgSearchResult:
		m := SearchResultMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				sub := r.sub(wt)
				var h SearchHit
				for f, wt, ok := sub.next(); ok; f, wt, ok = sub.next() {
					switch f {
					case 1:
						h.DocID = sub.str(wt)
					case 2:
						h.Score = sub.double(wt)
					case 3:
						h.JSON = sub.optStr(wt)
					default:
						sub.skip(wt)
					}
				}
				r.adopt(&sub)
				m.Hits = append(m.Hits, h)
			case 3:
				m.ResolvedCommitHex = r.str(wt)
			case 4:
				m.Error = r.optStr(wt)
			case 5:
				m.ErrorCode = r.optErrorCode(wt)
			case 6:
				m.RetryAfterMs = r.optInt(wt)
			default:
				r.skip(wt)
			}
		}
		return m, true
	case MsgHistoryList:
		m := HistoryListMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				m.From = r.str(wt)
			case 3:
				m.Skip = int(r.int(wt))
			case 4:
				m.Limit = int(r.int(wt))
			default:
				r.skip(wt)
			}
		}
		return m, true
	case MsgHistoryResult:
		m := HistoryResultMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				sub := r.sub(wt)
				var c HistoryCommit
				for f, wt, ok := sub.next(); ok; f, wt, ok = sub.next() {
					switch f {
					case 1:
						c.CommitHex = sub.str(wt)
					case 2:
						c.ParentHexes = append(c.ParentHexes, sub.str(wt))
					case 3:
						c.TransactionID = sub.str(wt)
					case 4:
						c.TimestampMicros = sub.int(wt)
					case 5:
						c.AuthorNodeID = sub.str(wt)
					case 6:
						c.TreeHex = sub.str(wt)
					case 7:
						c.Message = sub.str(wt)
					case 8:
						c.OperationCount = int(sub.int(wt))
					case 9:
						c.Stubbed = sub.bool(wt)
					default:
						sub.skip(wt)
					}
				}
				r.adopt(&sub)
				m.Commits = append(m.Commits, c)
			case 3:
				m.ResolvedHex = r.str(wt)
			case 4:
				m.Error = r.optStr(wt)
			case 5:
				m.ErrorCode = r.optErrorCode(wt)
			case 6:
				m.RetryAfterMs = r.optInt(wt)
			default:
				r.skip(wt)
			}
		}
		return m, true
	case MsgRevert:
		m := RevertMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				m.To = r.str(wt)
			default:
				r.skip(wt)
			}
		}
		return m, true
	case MsgRevertResult:
		m := RevertResultMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				m.CommitHex = r.str(wt)
			case 3:
				m.TargetHex = r.str(wt)
			case 4:
				m.Restored = int(r.int(wt))
			case 5:
				m.Removed = int(r.int(wt))
			case 6:
				m.Error = r.optStr(wt)
			case 7:
				m.ErrorCode = r.optErrorCode(wt)
			case 8:
				m.RetryAfterMs = r.optInt(wt)
			default:
				r.skip(wt)
			}
		}
		return m, true
	case MsgTxCommitMulti:
		m := TxCommitMultiMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.SessionID = r.str(wt)
			case 2:
				sub := r.sub(wt)
				var p TxCommitMultiPart
				for f, wt, ok := sub.next(); ok; f, wt, ok = sub.next() {
					switch f {
					case 1:
						p.Namespace = sub.str(wt)
					case 2:
						p.TransactionBytes = cloneBytes(sub.bytes(wt))
					default:
						sub.skip(wt)
					}
				}
				r.adopt(&sub)
				m.Parts = append(m.Parts, p)
			default:
				r.skip(wt)
			}
		}
		return m, true
	case MsgTxCommitMultiResult:
		m := TxCommitMultiResultMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.GroupID = r.str(wt)
			case 2:
				sub := r.sub(wt)
				var p TxCommitMultiResultPart
				for f, wt, ok := sub.next(); ok; f, wt, ok = sub.next() {
					switch f {
					case 1:
						p.Namespace = sub.str(wt)
					case 2:
						p.CommitHex = sub.str(wt)
					default:
						sub.skip(wt)
					}
				}
				r.adopt(&sub)
				m.Parts = append(m.Parts, p)
			case 3:
				m.FailedNamespace = r.str(wt)
			case 4:
				m.ConflictReport = cloneBytes(r.bytes(wt))
			case 5:
				m.Error = r.optStr(wt)
			case 6:
				m.ErrorCode = r.optErrorCode(wt)
			case 7:
				m.RetryAfterMs = r.optInt(wt)
			default:
				r.skip(wt)
			}
		}
		return m, true
	case MsgDeltaCommit:
		return DeltaCommitMessage{H: header, Payload: decodeDeltaCommit(r)}, true
	default:
		return nil, false
	}
}

func decodeDeltaCommit(r *binReader) DeltaCommitPayload {
	var p DeltaCommitPayload
	for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
		switch f {
		case 1:
			p.Namespace = r.str(wt)
		case 2:
			p.CommitHash = r.hash(wt)
		case 3:
			p.ParentHash = r.hash(wt)
		case 4:
			p.TimestampMicros = r.int(wt)
		case 5:
			sub := r.sub(wt)
			op := decodeOp(&sub)
			r.adopt(&sub)
			if op != nil {
				p.Operations = append(p.Operations, op)
			}
		case 6:
			sub := r.sub(wt)
			var h IndexHint
			for f, wt, ok := sub.next(); ok; f, wt, ok = sub.next() {
				switch f {
				case 1:
					h.IndexID = sub.uuid(wt)
				case 2:
					h.FieldName = sub.str(wt)
				case 3:
					h.IndexType = sub.str(wt)
				case 4:
					h.Action = sub.str(wt)
				case 5:
					h.DocID = sub.uuid(wt)
				case 6:
					h.Key = sub.optStr(wt)
				case 7:
					h.CommitHash = sub.hash(wt)
				default:
					sub.skip(wt)
				}
			}
			r.adopt(&sub)
			p.IndexHints = append(p.IndexHints, h)
		case 7:
			p.SchemaDeltaBytes = cloneBytes(r.bytes(wt))
		default:
			r.skip(wt)
		}
	}
	return p
}

func decodeOp(r *binReader) document.Op {
	var (
		kind              int64
		write             document.WriteOp
		fileWrite         document.FileWriteOp
		migration         document.SchemaMigrationOp
		hasPatch, hasPath bool
		attach            document.AttachmentPutOp
		hasAttachHash     bool
	)
	for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
		switch f {
		case 1:
			kind = r.int(wt)
		case 2:
			write.DocID = r.uuid(wt)
		case 3:
			write.Patch, hasPatch = r.str(wt), true
		case 4:
			write.Replace = r.bool(wt)
		case 5:
			fileWrite.Path, hasPath = r.str(wt), true
		case 6:
			fileWrite.BlobHash = r.hash(wt)
		case 7:
			migration.MigrationID = r.uuid(wt)
		case 8:
			migration.MigrationPayload = r.str(wt)
		case 9:
			attach.Size = r.int(wt)
		case 10:
			attach.Mime = r.str(wt)
		default:
			r.skip(wt)
		}
		if f == 6 {
			hasAttachHash = true
		}
	}
	if r.err != nil {
		return nil
	}
	switch kind {
	case binOpWrite:
		if !hasPatch {
			r.fail("binary write operation has no patch")
			return nil
		}
		return write
	case binOpDelete:
		return document.DeleteOp{DocID: write.DocID}
	case binOpFileWrite:
		if !hasPath {
			r.fail("binary fileWrite operation has no path")
			return nil
		}
		return fileWrite
	case binOpSchemaMigration:
		return migration
	case binOpAttachmentPut:
		if !hasAttachHash || attach.Size < 0 {
			r.fail("binary attachmentPut operation has no content hash or a negative size")
			return nil
		}
		attach.DocID, attach.ContentHash = write.DocID, fileWrite.BlobHash
		return attach
	case binOpAttachmentDel:
		return document.AttachmentDeleteOp{DocID: write.DocID}
	default:
		r.fail("unknown operation kind in binary body")
		return nil
	}
}
