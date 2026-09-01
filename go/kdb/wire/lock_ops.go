package wire

// Document-lease wire messages (LOCK_ACQUIRE / LOCK_RENEW / LOCK_RELEASE / LOCK_RESULT).
//
// These exist because pessimistic locking that spans client round trips cannot be expressed
// through the existing message set: the server's implicit, commit-scoped locks are taken and
// dropped inside one TxCommit, which is the right shape for a transaction but useless for a
// client that wants to hold a document while a human edits it. A client-visible lease needs its
// own acquire/renew/release verbs, and - critically - has to hand back a fence token, so a
// holder that stalls past its deadline is refused at commit rather than silently overwriting
// whoever took the document after it expired.
//
// Go-only for now, like 0x14-0x18: no Kotlin counterpart exists yet.

// LockAcquireMessage requests an exclusive lease on DocID for the session's remaining work.
type LockAcquireMessage struct {
	H         Header
	Namespace string
	SessionID string
	DocID     string
	// TTLMillis is how long the lease survives without a renewal. Zero asks the server for its
	// configured default rather than for a lease that never expires - a client-held lock with no
	// expiry is exactly the failure mode leases exist to remove.
	TTLMillis int
}

func (m LockAcquireMessage) Header() Header { return m.H }

// LockRenewMessage extends an existing lease, keeping its fence token.
type LockRenewMessage struct {
	H         Header
	Namespace string
	SessionID string
	DocID     string
	TTLMillis int
}

func (m LockRenewMessage) Header() Header { return m.H }

// LockReleaseMessage drops a lease early.
type LockReleaseMessage struct {
	H         Header
	Namespace string
	SessionID string
	DocID     string
}

func (m LockReleaseMessage) Header() Header { return m.H }

// LockResultMessage is the reply to all three lock verbs.
type LockResultMessage struct {
	H         Header
	Namespace string
	SessionID string
	DocID     string
	// Granted is false when Error explains why - a lock held by someone else, or a lease that
	// lapsed before the renew arrived.
	Granted bool
	// Fence is the lease's monotonic token. A client does not need to interpret it, but it is
	// what a commit is validated against, and seeing it change across a renew is the only way to
	// know a lease was lost and re-taken rather than extended.
	Fence uint64
	// ExpiresAtMillis is the lease deadline in epoch milliseconds; zero when not granted.
	ExpiresAtMillis int64
	// HolderSessionID names the current holder when Granted is false, so a client can report
	// who it is waiting on rather than only that it is waiting.
	HolderSessionID *string
	Error           *string
	ErrorCode       *ErrorCode
}

func (m LockResultMessage) Header() Header { return m.H }

type lockAcquireDto struct {
	Namespace string `json:"namespace"`
	SessionID string `json:"sessionId"`
	DocID     string `json:"docId"`
	TTLMillis int    `json:"ttlMillis"`
}

type lockRenewDto struct {
	Namespace string `json:"namespace"`
	SessionID string `json:"sessionId"`
	DocID     string `json:"docId"`
	TTLMillis int    `json:"ttlMillis"`
}

type lockReleaseDto struct {
	Namespace string `json:"namespace"`
	SessionID string `json:"sessionId"`
	DocID     string `json:"docId"`
}

type lockResultDto struct {
	Namespace       string     `json:"namespace"`
	SessionID       string     `json:"sessionId"`
	DocID           string     `json:"docId"`
	Granted         bool       `json:"granted"`
	Fence           uint64     `json:"fence"`
	ExpiresAtMillis int64      `json:"expiresAtMillis"`
	HolderSessionID *string    `json:"holderSessionId,omitempty"`
	Error           *string    `json:"error,omitempty"`
	ErrorCode       *ErrorCode `json:"errorCode,omitempty"`
}

func encodeLockOpMessage(msg Message) (payloadEnvelope, bool, error) {
	switch m := msg.(type) {
	case LockAcquireMessage:
		return payloadEnvelope{Kind: "lockAcquire", LockAcquire: &lockAcquireDto{
			Namespace: m.Namespace, SessionID: m.SessionID, DocID: m.DocID, TTLMillis: m.TTLMillis,
		}}, true, nil
	case LockRenewMessage:
		return payloadEnvelope{Kind: "lockRenew", LockRenew: &lockRenewDto{
			Namespace: m.Namespace, SessionID: m.SessionID, DocID: m.DocID, TTLMillis: m.TTLMillis,
		}}, true, nil
	case LockReleaseMessage:
		return payloadEnvelope{Kind: "lockRelease", LockRelease: &lockReleaseDto{
			Namespace: m.Namespace, SessionID: m.SessionID, DocID: m.DocID,
		}}, true, nil
	case LockResultMessage:
		return payloadEnvelope{Kind: "lockResult", LockResult: &lockResultDto{
			Namespace: m.Namespace, SessionID: m.SessionID, DocID: m.DocID,
			Granted: m.Granted, Fence: m.Fence, ExpiresAtMillis: m.ExpiresAtMillis,
			HolderSessionID: m.HolderSessionID, Error: m.Error, ErrorCode: m.ErrorCode,
		}}, true, nil
	default:
		return payloadEnvelope{}, false, nil
	}
}

func decodeLockOpMessage(header Header, env payloadEnvelope) (Message, bool, error) {
	switch env.Kind {
	case "lockAcquire":
		d := env.LockAcquire
		if d == nil {
			return nil, true, newDecodeError("missing lockAcquire body")
		}
		return LockAcquireMessage{
			H: header, Namespace: d.Namespace, SessionID: d.SessionID, DocID: d.DocID, TTLMillis: d.TTLMillis,
		}, true, nil
	case "lockRenew":
		d := env.LockRenew
		if d == nil {
			return nil, true, newDecodeError("missing lockRenew body")
		}
		return LockRenewMessage{
			H: header, Namespace: d.Namespace, SessionID: d.SessionID, DocID: d.DocID, TTLMillis: d.TTLMillis,
		}, true, nil
	case "lockRelease":
		d := env.LockRelease
		if d == nil {
			return nil, true, newDecodeError("missing lockRelease body")
		}
		return LockReleaseMessage{
			H: header, Namespace: d.Namespace, SessionID: d.SessionID, DocID: d.DocID,
		}, true, nil
	case "lockResult":
		d := env.LockResult
		if d == nil {
			return nil, true, newDecodeError("missing lockResult body")
		}
		return LockResultMessage{
			H: header, Namespace: d.Namespace, SessionID: d.SessionID, DocID: d.DocID,
			Granted: d.Granted, Fence: d.Fence, ExpiresAtMillis: d.ExpiresAtMillis,
			HolderSessionID: d.HolderSessionID, Error: d.Error, ErrorCode: d.ErrorCode,
		}, true, nil
	default:
		return nil, false, nil
	}
}
