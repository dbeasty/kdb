package wire

// Cross-namespace commit messages (TX_COMMIT_MULTI 0x23 / TX_COMMIT_MULTI_RESULT 0x24).
//
// Go-only for now, like 0x14-0x22: there is no Kotlin counterpart yet. TX_COMMIT carries one
// namespace's transaction and nothing in the existing set can say "these commit together or not
// at all" - see docs/kdb-cross-namespace-transactions-plan.md.
//
// Sessionless, like DOCUMENT_GET: every participant carries its own already-encoded transaction,
// BaseVersion included, so the server needs no per-namespace session state to run it. A SessionID
// may still be supplied, and is what lets a caller holding a document lease write that document.

// TxCommitMultiPart is one namespace's transaction within a TX_COMMIT_MULTI.
type TxCommitMultiPart struct {
	Namespace string
	// TransactionBytes is wire.EncodeTransaction's encoding. A zero BaseVersion anchors the part
	// on its namespace's head at commit time.
	TransactionBytes []byte
}

// TxCommitMultiMessage commits several namespaces' transactions atomically.
type TxCommitMultiMessage struct {
	H         Header
	SessionID string
	Parts     []TxCommitMultiPart
}

func (m TxCommitMultiMessage) Header() Header { return m.H }

// TxCommitMultiResultPart is one namespace's commit in a successful TX_COMMIT_MULTI.
type TxCommitMultiResultPart struct {
	Namespace string
	CommitHex string
}

// TxCommitMultiResultMessage is TX_COMMIT_MULTI's reply.
//
// On success GroupID is the transaction id every participant commit carries and Parts lists each
// namespace's commit. On a refusal nothing was written anywhere: FailedNamespace names the
// participant that refused, Error says why, and ConflictReport carries the refusing namespace's
// conflict report when that is the reason (the same JSON CONFLICT_REPORT's ReportBytes carries).
type TxCommitMultiResultMessage struct {
	H       Header
	GroupID string
	Parts   []TxCommitMultiResultPart

	FailedNamespace string
	ConflictReport  []byte
	Error           *string
	ErrorCode       *ErrorCode
	RetryAfterMs    *int
}

func (m TxCommitMultiResultMessage) Header() Header { return m.H }

type txCommitMultiPartDto struct {
	Namespace        string        `json:"namespace"`
	TransactionBytes jsonByteArray `json:"transactionBytes"`
}

type txCommitMultiDto struct {
	SessionID string                 `json:"sessionId,omitempty"`
	Parts     []txCommitMultiPartDto `json:"parts"`
}

type txCommitMultiResultPartDto struct {
	Namespace string `json:"namespace"`
	CommitHex string `json:"commitHex"`
}

type txCommitMultiResultDto struct {
	GroupID         string                       `json:"groupId,omitempty"`
	Parts           []txCommitMultiResultPartDto `json:"parts,omitempty"`
	FailedNamespace string                       `json:"failedNamespace,omitempty"`
	ConflictReport  jsonByteArray                `json:"conflictReport,omitempty"`
	Error           *string                      `json:"error,omitempty"`
	ErrorCode       *ErrorCode                   `json:"errorCode,omitempty"`
	RetryAfterMs    *int                         `json:"retryAfterMs,omitempty"`
}

func encodeMultiCommitMessage(msg Message) (payloadEnvelope, bool, error) {
	switch m := msg.(type) {
	case TxCommitMultiMessage:
		parts := make([]txCommitMultiPartDto, len(m.Parts))
		for i, p := range m.Parts {
			parts[i] = txCommitMultiPartDto{Namespace: p.Namespace, TransactionBytes: p.TransactionBytes}
		}
		return payloadEnvelope{Kind: "txCommitMulti", TxCommitMulti: &txCommitMultiDto{
			SessionID: m.SessionID, Parts: parts,
		}}, true, nil
	case TxCommitMultiResultMessage:
		parts := make([]txCommitMultiResultPartDto, len(m.Parts))
		for i, p := range m.Parts {
			parts[i] = txCommitMultiResultPartDto{Namespace: p.Namespace, CommitHex: p.CommitHex}
		}
		return payloadEnvelope{Kind: "txCommitMultiResult", TxCommitMultiResult: &txCommitMultiResultDto{
			GroupID: m.GroupID, Parts: parts, FailedNamespace: m.FailedNamespace,
			ConflictReport: m.ConflictReport, Error: m.Error, ErrorCode: m.ErrorCode, RetryAfterMs: m.RetryAfterMs,
		}}, true, nil
	default:
		return payloadEnvelope{}, false, nil
	}
}

func decodeMultiCommitMessage(header Header, env payloadEnvelope) (Message, bool, error) {
	switch env.Kind {
	case "txCommitMulti":
		d := env.TxCommitMulti
		if d == nil {
			return nil, true, newDecodeError("missing txCommitMulti body")
		}
		parts := make([]TxCommitMultiPart, len(d.Parts))
		for i, p := range d.Parts {
			parts[i] = TxCommitMultiPart{Namespace: p.Namespace, TransactionBytes: p.TransactionBytes}
		}
		return TxCommitMultiMessage{H: header, SessionID: d.SessionID, Parts: parts}, true, nil
	case "txCommitMultiResult":
		d := env.TxCommitMultiResult
		if d == nil {
			return nil, true, newDecodeError("missing txCommitMultiResult body")
		}
		parts := make([]TxCommitMultiResultPart, len(d.Parts))
		for i, p := range d.Parts {
			parts[i] = TxCommitMultiResultPart{Namespace: p.Namespace, CommitHex: p.CommitHex}
		}
		return TxCommitMultiResultMessage{
			H: header, GroupID: d.GroupID, Parts: parts, FailedNamespace: d.FailedNamespace,
			ConflictReport: d.ConflictReport, Error: d.Error, ErrorCode: d.ErrorCode, RetryAfterMs: d.RetryAfterMs,
		}, true, nil
	default:
		return nil, false, nil
	}
}
