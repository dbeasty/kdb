package wire

// Component 40 (Go Client SDK) wire messages: direct document get/upsert. See types.go's
// MsgDocumentGet/MsgUpsert doc comment for why these exist alongside SqlExec rather than being
// expressed as SQL.

type DocumentGetMessage struct {
	H         Header
	Namespace string
	DocID     string
}

func (m DocumentGetMessage) Header() Header { return m.H }

type DocumentGetResultMessage struct {
	H         Header
	Namespace string
	DocID     string
	// JSON is nil if the document doesn't exist at CommitHex.
	JSON      *string
	CommitHex string
	Error     *string
}

func (m DocumentGetResultMessage) Header() Header { return m.H }

// UpsertMessage writes JSON at DocID unconditionally - create if absent, replace if present, no
// BaseVersion, no conflict possible (component 40 spec §5: "Upsert never conflicts and never
// needs a BaseVersion").
type UpsertMessage struct {
	H         Header
	Namespace string
	DocID     string
	JSON      string
	// SessionID is optional and additive: Upsert itself needs no session (it has no BaseVersion
	// and cannot conflict), but a document under a client-held lease must not be written by
	// anyone else, and without a session id on the message there is no way to tell the lease
	// holder's own upsert from a stranger's. An empty value is treated as "not the holder", so
	// an older client's upsert is refused while a lease is out rather than quietly allowed
	// through.
	SessionID string
}

func (m UpsertMessage) Header() Header { return m.H }

type UpsertResultMessage struct {
	H         Header
	Namespace string
	CommitHex string
	Error     *string
	// ErrorCode and RetryAfterMs are additive to Error - see SqlResultMessage's identical fields'
	// doc comment (kdb-spec-layer13 Component 51 §8.1).
	ErrorCode    *ErrorCode
	RetryAfterMs *int
}

func (m UpsertResultMessage) Header() Header { return m.H }

type documentGetDto struct {
	Namespace string `json:"namespace"`
	DocID     string `json:"docId"`
}

type documentGetResultDto struct {
	Namespace string  `json:"namespace"`
	DocID     string  `json:"docId"`
	JSON      *string `json:"json,omitempty"`
	CommitHex string  `json:"commitHex"`
	Error     *string `json:"error,omitempty"`
}

type upsertDto struct {
	Namespace string `json:"namespace"`
	DocID     string `json:"docId"`
	JSON      string `json:"json"`
	SessionID string `json:"sessionId,omitempty"`
}

type upsertResultDto struct {
	Namespace    string     `json:"namespace"`
	CommitHex    string     `json:"commitHex"`
	Error        *string    `json:"error,omitempty"`
	ErrorCode    *ErrorCode `json:"errorCode,omitempty"`
	RetryAfterMs *int       `json:"retryAfterMs,omitempty"`
}

func encodeDocumentOpMessage(msg Message) (payloadEnvelope, bool, error) {
	switch m := msg.(type) {
	case DocumentGetMessage:
		return payloadEnvelope{Kind: "documentGet", DocumentGet: &documentGetDto{
			Namespace: m.Namespace, DocID: m.DocID,
		}}, true, nil
	case DocumentGetResultMessage:
		return payloadEnvelope{Kind: "documentGetResult", DocumentGetResult: &documentGetResultDto{
			Namespace: m.Namespace, DocID: m.DocID, JSON: m.JSON, CommitHex: m.CommitHex, Error: m.Error,
		}}, true, nil
	case UpsertMessage:
		return payloadEnvelope{Kind: "upsert", Upsert: &upsertDto{
			Namespace: m.Namespace, DocID: m.DocID, JSON: m.JSON, SessionID: m.SessionID,
		}}, true, nil
	case UpsertResultMessage:
		return payloadEnvelope{Kind: "upsertResult", UpsertResult: &upsertResultDto{
			Namespace: m.Namespace, CommitHex: m.CommitHex, Error: m.Error,
			ErrorCode: m.ErrorCode, RetryAfterMs: m.RetryAfterMs,
		}}, true, nil
	default:
		return payloadEnvelope{}, false, nil
	}
}

func decodeDocumentOpMessage(header Header, env payloadEnvelope) (Message, bool, error) {
	switch env.Kind {
	case "documentGet":
		d := env.DocumentGet
		if d == nil {
			return nil, true, newDecodeError("missing documentGet body")
		}
		return DocumentGetMessage{H: header, Namespace: d.Namespace, DocID: d.DocID}, true, nil
	case "documentGetResult":
		d := env.DocumentGetResult
		if d == nil {
			return nil, true, newDecodeError("missing documentGetResult body")
		}
		return DocumentGetResultMessage{
			H: header, Namespace: d.Namespace, DocID: d.DocID, JSON: d.JSON, CommitHex: d.CommitHex, Error: d.Error,
		}, true, nil
	case "upsert":
		d := env.Upsert
		if d == nil {
			return nil, true, newDecodeError("missing upsert body")
		}
		return UpsertMessage{
			H: header, Namespace: d.Namespace, DocID: d.DocID, JSON: d.JSON, SessionID: d.SessionID,
		}, true, nil
	case "upsertResult":
		d := env.UpsertResult
		if d == nil {
			return nil, true, newDecodeError("missing upsertResult body")
		}
		return UpsertResultMessage{
			H: header, Namespace: d.Namespace, CommitHex: d.CommitHex, Error: d.Error,
			ErrorCode: d.ErrorCode, RetryAfterMs: d.RetryAfterMs,
		}, true, nil
	default:
		return nil, false, nil
	}
}
