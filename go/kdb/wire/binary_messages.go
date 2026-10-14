package wire

import (
	"encoding/binary"

	"github.com/limidus/kdb/go/kdb/document"
)

// Binary bodies of the client load-path messages (see binary.go for the encoding). Field numbers
// are part of the protocol: never renumber or reuse one, only add.

// coreBinaryForm reports whether t is one of the load-path messages WireFeatureBinary covers.
func coreBinaryForm(t MessageType) bool {
	switch t {
	case MsgSessionBegin, MsgSessionBeginAck, MsgSqlExec, MsgSqlResult, MsgTxCommit, MsgTxRollback,
		MsgConflictReport, MsgDocumentGet, MsgDocumentGetResult, MsgUpsert, MsgUpsertResult:
		return true
	default:
		return false
	}
}

// moreBinaryForm reports whether t is one of the messages WireFeatureBinary2 adds
// (binary_messages_more.go).
func moreBinaryForm(t MessageType) bool {
	switch t {
	case MsgLockAcquire, MsgLockRenew, MsgLockRelease, MsgLockResult, MsgSearch, MsgSearchResult,
		MsgHistoryList, MsgHistoryResult, MsgRevert, MsgRevertResult, MsgTxCommitMulti,
		MsgTxCommitMultiResult, MsgDeltaCommit:
		return true
	default:
		return false
	}
}

// appendBinaryBody appends message's binary body to b. ok=false means the message has no binary
// form and b is returned unchanged.
func appendBinaryBody(b []byte, message Message) ([]byte, bool) {
	switch m := message.(type) {
	case SessionBeginMessage:
		b = appendString(b, 1, m.Namespace)
		b = appendOptString(b, 2, m.SessionID)
		b = appendString(b, 3, m.ReadConsistency)
		b = appendOptString(b, 4, m.BaseVersionHex)
	case SessionBeginAckMessage:
		b = appendString(b, 1, m.Namespace)
		b = appendString(b, 2, m.SessionID)
		b = appendString(b, 3, m.HeadHex)
		b = appendString(b, 4, m.ReadConsistency)
		b = appendOptString(b, 5, m.Error)
	case SqlExecMessage:
		b = appendString(b, 1, m.Namespace)
		b = appendString(b, 2, m.SessionID)
		b = appendString(b, 3, m.SQL)
		b = appendOptString(b, 4, m.ParametersJSON)
		b = appendInt(b, 5, int64(m.PageSize))
		b = appendString(b, 6, m.Continuation)
	case SqlResultMessage:
		b = appendSqlResult(b, m)
	case TxCommitMessage:
		b = appendString(b, 1, m.Namespace)
		b = appendString(b, 2, m.SessionID)
		b = appendBytes(b, 3, m.TransactionBytes)
	case TxRollbackMessage:
		b = appendString(b, 1, m.Namespace)
		b = appendString(b, 2, m.SessionID)
	case ConflictReportMessage:
		b = appendString(b, 1, m.Namespace)
		b = appendBytes(b, 2, m.ReportBytes)
		b = appendOptErrorCode(b, 3, m.ErrorCode)
		b = appendOptInt(b, 4, m.RetryAfterMs)
	case DocumentGetMessage:
		b = appendString(b, 1, m.Namespace)
		b = appendString(b, 2, m.DocID)
		b = appendString(b, 3, m.MinCommit)
	case DocumentGetResultMessage:
		b = appendString(b, 1, m.Namespace)
		b = appendString(b, 2, m.DocID)
		b = appendOptString(b, 3, m.JSON)
		b = appendString(b, 4, m.CommitHex)
		b = appendOptString(b, 5, m.Error)
		b = appendOptErrorCode(b, 6, m.ErrorCode)
		b = appendOptInt(b, 7, m.RetryAfterMs)
	case UpsertMessage:
		b = appendString(b, 1, m.Namespace)
		b = appendString(b, 2, m.DocID)
		b = appendString(b, 3, m.JSON)
		b = appendString(b, 4, m.SessionID)
	case UpsertResultMessage:
		b = appendString(b, 1, m.Namespace)
		b = appendString(b, 2, m.CommitHex)
		b = appendOptString(b, 3, m.Error)
		b = appendOptErrorCode(b, 4, m.ErrorCode)
		b = appendOptInt(b, 5, m.RetryAfterMs)
	default:
		if out, ok := appendMoreBinaryBody(b, message); ok {
			return out, true
		}
		return appendAttachmentBody(b, message)
	}
	return b, true
}

// binarySizeHint is a cheap upper-bound-ish estimate of a message's binary body, so an unpooled
// encode allocates its frame once instead of growing it from 16 bytes by doubling. 0 means small.
func binarySizeHint(message Message) int {
	const fixed = 256 // keys, lengths and the short fields
	switch m := message.(type) {
	case SqlResultMessage:
		n := fixed
		for _, c := range m.Columns {
			n += len(c) + 6
		}
		for _, row := range m.Rows {
			n += 6
			for _, cell := range row {
				n += len(cell) + 6
			}
		}
		return n
	case DocumentGetResultMessage:
		if m.JSON != nil {
			return fixed + len(*m.JSON)
		}
	case UpsertMessage:
		return fixed + len(m.JSON)
	case TxCommitMessage:
		return fixed + len(m.TransactionBytes)
	case DeltaCommitMessage:
		n := fixed + len(m.Payload.SchemaDeltaBytes)
		for _, op := range m.Payload.Operations {
			n += 64
			if w, ok := op.(document.WriteOp); ok {
				n += len(w.Patch)
			}
		}
		return n
	case SearchResultMessage:
		n := fixed
		for _, h := range m.Hits {
			n += 64
			if h.JSON != nil {
				n += len(*h.JSON)
			}
		}
		return n
	case SqlExecMessage:
		n := fixed + len(m.SQL) + len(m.Continuation)
		if m.ParametersJSON != nil {
			n += len(*m.ParametersJSON)
		}
		return n
	}
	return 0
}

// SqlResult field 4 is one row: its cells back to back, each a uvarint length and the cell's
// bytes. A row is one field rather than a field per cell, so a 10k-row result costs 10k keys, not
// 10k times the column count.
func appendSqlResult(b []byte, m SqlResultMessage) []byte {
	b = appendString(b, 1, m.Namespace)
	b = appendString(b, 2, m.SessionID)
	for _, c := range m.Columns {
		b = appendRawString(b, 3, c)
	}
	for _, row := range m.Rows {
		size := 0
		for _, cell := range row {
			size += uvarintLen(uint64(len(cell))) + len(cell)
		}
		b = appendKey(b, 4, wtBytes)
		b = binary.AppendUvarint(b, uint64(size))
		for _, cell := range row {
			b = binary.AppendUvarint(b, uint64(len(cell)))
			b = append(b, cell...)
		}
	}
	b = appendInt(b, 5, int64(m.RowsAffected))
	b = appendString(b, 6, m.ResolvedCommitHex)
	b = appendBool(b, 7, m.ReadOnly)
	b = appendOptString(b, 8, m.Error)
	for _, id := range m.GeneratedIDs {
		b = appendRawString(b, 9, id)
	}
	b = appendOptErrorCode(b, 10, m.ErrorCode)
	b = appendOptInt(b, 11, m.RetryAfterMs)
	b = appendString(b, 12, m.ContinuationToken)
	// Field 13: every cell's kind, one byte each, row by row (SqlResultMessage.CellKinds).
	if len(m.CellKinds) > 0 {
		b = appendKey(b, 13, wtBytes)
		b = binary.AppendUvarint(b, uint64(len(m.CellKinds)))
		for _, k := range m.CellKinds {
			b = append(b, byte(k))
		}
	}
	return b
}

// decodeBinaryBody decodes a tag-2 body of the message type named by header.
func decodeBinaryBody(header Header, body []byte) (Message, error) {
	r := &binReader{b: body}
	var msg Message
	switch header.MessageType {
	case MsgSessionBegin:
		m := SessionBeginMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				m.SessionID = r.optStr(wt)
			case 3:
				m.ReadConsistency = r.str(wt)
			case 4:
				m.BaseVersionHex = r.optStr(wt)
			default:
				r.skip(wt)
			}
		}
		msg = m
	case MsgSessionBeginAck:
		m := SessionBeginAckMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				m.SessionID = r.str(wt)
			case 3:
				m.HeadHex = r.str(wt)
			case 4:
				m.ReadConsistency = r.str(wt)
			case 5:
				m.Error = r.optStr(wt)
			default:
				r.skip(wt)
			}
		}
		msg = m
	case MsgSqlExec:
		m := SqlExecMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				m.SessionID = r.str(wt)
			case 3:
				m.SQL = r.str(wt)
			case 4:
				m.ParametersJSON = r.optStr(wt)
			case 5:
				m.PageSize = int(r.int(wt))
			case 6:
				m.Continuation = r.str(wt)
			default:
				r.skip(wt)
			}
		}
		msg = m
	case MsgSqlResult:
		m, err := decodeSqlResult(header, body)
		if err != nil {
			return nil, err
		}
		msg = m
	case MsgTxCommit:
		m := TxCommitMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				m.SessionID = r.str(wt)
			case 3:
				m.TransactionBytes = cloneBytes(r.bytes(wt))
			default:
				r.skip(wt)
			}
		}
		msg = m
	case MsgTxRollback:
		m := TxRollbackMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				m.SessionID = r.str(wt)
			default:
				r.skip(wt)
			}
		}
		msg = m
	case MsgConflictReport:
		m := ConflictReportMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				m.ReportBytes = cloneBytes(r.bytes(wt))
			case 3:
				m.ErrorCode = r.optErrorCode(wt)
			case 4:
				m.RetryAfterMs = r.optInt(wt)
			default:
				r.skip(wt)
			}
		}
		msg = m
	case MsgDocumentGet:
		m := DocumentGetMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				m.DocID = r.str(wt)
			case 3:
				m.MinCommit = r.str(wt)
			default:
				r.skip(wt)
			}
		}
		msg = m
	case MsgDocumentGetResult:
		m := DocumentGetResultMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				m.DocID = r.str(wt)
			case 3:
				m.JSON = r.optStr(wt)
			case 4:
				m.CommitHex = r.str(wt)
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
		msg = m
	case MsgUpsert:
		m := UpsertMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				m.DocID = r.str(wt)
			case 3:
				m.JSON = r.str(wt)
			case 4:
				m.SessionID = r.str(wt)
			default:
				r.skip(wt)
			}
		}
		msg = m
	case MsgUpsertResult:
		m := UpsertResultMessage{H: header}
		for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
			switch f {
			case 1:
				m.Namespace = r.str(wt)
			case 2:
				m.CommitHex = r.str(wt)
			case 3:
				m.Error = r.optStr(wt)
			case 4:
				m.ErrorCode = r.optErrorCode(wt)
			case 5:
				m.RetryAfterMs = r.optInt(wt)
			default:
				r.skip(wt)
			}
		}
		msg = m
	default:
		m, ok := decodeMoreBinaryBody(header, r)
		if !ok {
			m, ok = decodeAttachmentBody(header, r)
		}
		if !ok {
			return nil, newDecodeError("message type has no binary form")
		}
		msg = m
	}
	if r.err != nil {
		return nil, r.err
	}
	return msg, nil
}

// decodeSqlResult decodes a SqlResult body in two passes: the first counts rows and cells, so the
// second fills one [][]string and one shared []string of cells, each allocated once at its final
// size. Cell and column strings are substrings of a single copy of the body, so a result costs a
// handful of allocations however many cells it has, instead of one per cell.
func decodeSqlResult(header Header, body []byte) (SqlResultMessage, error) {
	m := SqlResultMessage{H: header}
	rows, cells, columns, ids := 0, 0, 0, 0
	r := &binReader{b: body}
	for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
		switch f {
		case 3:
			r.skip(wt)
			columns++
		case 4:
			row := r.sub(wt)
			for row.off < len(row.b) && row.err == nil {
				row.bytes(wtBytes)
				cells++
			}
			if row.err != nil {
				return m, row.err
			}
			rows++
		case 9:
			r.skip(wt)
			ids++
		default:
			r.skip(wt)
		}
	}
	if r.err != nil {
		return m, r.err
	}
	// text holds every string field's bytes at the same offsets as body.
	text := string(body)
	sub := func(start, n int) string { return text[start : start+n] }
	if columns > 0 {
		m.Columns = make([]string, 0, columns)
	}
	if rows > 0 {
		m.Rows = make([][]string, 0, rows)
	}
	if ids > 0 {
		m.GeneratedIDs = make([]string, 0, ids)
	}
	cellBuf := make([]string, cells)
	r = &binReader{b: body}
	for f, wt, ok := r.next(); ok; f, wt, ok = r.next() {
		switch f {
		case 1:
			m.Namespace = r.str(wt)
		case 2:
			m.SessionID = r.str(wt)
		case 3:
			m.Columns = append(m.Columns, sub(r.span(wt)))
		case 4:
			row := r.sub(wt)
			n := 0
			for row.off < len(row.b) {
				cellBuf[n] = sub(row.span(wtBytes))
				n++
			}
			m.Rows = append(m.Rows, cellBuf[:n:n])
			cellBuf = cellBuf[n:]
		case 5:
			m.RowsAffected = int(r.int(wt))
		case 6:
			m.ResolvedCommitHex = r.str(wt)
		case 7:
			m.ReadOnly = r.bool(wt)
		case 8:
			m.Error = r.optStr(wt)
		case 9:
			m.GeneratedIDs = append(m.GeneratedIDs, sub(r.span(wt)))
		case 10:
			m.ErrorCode = r.optErrorCode(wt)
		case 11:
			m.RetryAfterMs = r.optInt(wt)
		case 12:
			m.ContinuationToken = r.str(wt)
		case 13:
			p := r.bytes(wt)
			m.CellKinds = make([]CellKind, len(p))
			for i, k := range p {
				m.CellKinds[i] = CellKind(k)
			}
		default:
			r.skip(wt)
		}
	}
	return m, r.err
}

func cloneBytes(p []byte) []byte {
	if len(p) == 0 {
		return nil
	}
	return append([]byte(nil), p...)
}
