package wire

// History wire messages (HISTORY_LIST 0x1F / HISTORY_RESULT 0x20,
// REVERT 0x21 / REVERT_RESULT 0x22).
//
// Go-only for now, like 0x14-0x1C: there is no Kotlin counterpart yet. The
// component 38 spec's note applies - extending go/kdb/wire itself is the
// right move when the existing message set cannot express something - and
// nothing in the existing set can. SQL_EXEC carries AT COMMIT / AT VERSION
// / AT TIME for *reading* at a past commit, but there is no way at all to
// ask what the past commits are, or to undo one.
//
// Sessionless, like DOCUMENT_GET and SEARCH. A history listing is a read
// of metadata the DAG already holds; a revert is an ordinary write that
// happens to compute its own operations.

// HistoryListMessage asks for commits, newest first, starting from a
// revision.
//
// From is a revision specification rather than a bare hash - "head",
// "head~10", "tag:v1", a hash, or any of those with ~N - because that is
// what a caller paging through history actually has, and resolving it
// server-side keeps one grammar (dag.ParseRevision) rather than two.
type HistoryListMessage struct {
	H         Header
	Namespace string
	// From is the revision to start at. Empty means head.
	From string
	// Skip and Limit page the listing. A zero Limit takes the server
	// default rather than returning nothing, because a client that omits
	// it wants commits, not an empty page.
	Skip  int
	Limit int
}

func (m HistoryListMessage) Header() Header { return m.H }

// HistoryCommit is one commit as a listing describes it.
//
// Operations are deliberately absent: they are the full text of every
// document the commit wrote, so a listing carrying them would cost the
// size of the history it exists to summarize. OperationCount is -1 when
// the count is not known without loading them.
type HistoryCommit struct {
	CommitHex     string
	ParentHexes   []string
	TransactionID string
	// TimestampMicros is epoch microseconds, matching codec.Timestamp's
	// own resolution rather than rounding to milliseconds on the wire.
	TimestampMicros int64
	AuthorNodeID    string
	TreeHex         string
	Message         string
	OperationCount  int
	Stubbed         bool
}

// HistoryResultMessage is HISTORY_LIST's reply. Error/ErrorCode follow the
// additive convention every other result frame uses.
type HistoryResultMessage struct {
	H            Header
	Namespace    string
	Commits      []HistoryCommit
	ResolvedHex  string
	Error        *string
	ErrorCode    *ErrorCode
	RetryAfterMs *int
}

func (m HistoryResultMessage) Header() Header { return m.H }

// RevertMessage restores the state at a revision by writing a new commit.
//
// Deliberately not "move head backwards". A commit's operations carry no
// pre-image, so history cannot be run backwards, and the storage engine
// keeps one live tree that every write continues from - so a backwards
// head would leave reads and writes disagreeing about what the database
// is. A revert goes forward: a new commit whose tree is the old one's.
type RevertMessage struct {
	H         Header
	Namespace string
	// To is a revision specification, same grammar as HistoryList.From.
	To string
}

func (m RevertMessage) Header() Header { return m.H }

// RevertResultMessage reports what a revert put back.
type RevertResultMessage struct {
	H         Header
	Namespace string
	// CommitHex is the new commit the revert wrote, TargetHex the commit
	// whose state it restored. They are never the same.
	CommitHex string
	TargetHex string
	Restored  int
	Removed   int

	Error        *string
	ErrorCode    *ErrorCode
	RetryAfterMs *int
}

func (m RevertResultMessage) Header() Header { return m.H }

type historyListDto struct {
	Namespace string `json:"namespace"`
	From      string `json:"from,omitempty"`
	Skip      int    `json:"skip,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

type historyCommitDto struct {
	CommitHex       string   `json:"commitHex"`
	ParentHexes     []string `json:"parentHexes,omitempty"`
	TransactionID   string   `json:"transactionId,omitempty"`
	TimestampMicros int64    `json:"timestampMicros"`
	AuthorNodeID    string   `json:"authorNodeId,omitempty"`
	TreeHex         string   `json:"treeHex,omitempty"`
	Message         string   `json:"message,omitempty"`
	OperationCount  int      `json:"operationCount"`
	Stubbed         bool     `json:"stubbed,omitempty"`
}

type historyResultDto struct {
	Namespace    string             `json:"namespace"`
	Commits      []historyCommitDto `json:"commits"`
	ResolvedHex  string             `json:"resolvedHex,omitempty"`
	Error        *string            `json:"error,omitempty"`
	ErrorCode    *ErrorCode         `json:"errorCode,omitempty"`
	RetryAfterMs *int               `json:"retryAfterMs,omitempty"`
}

type revertDto struct {
	Namespace string `json:"namespace"`
	To        string `json:"to"`
}

type revertResultDto struct {
	Namespace    string     `json:"namespace"`
	CommitHex    string     `json:"commitHex,omitempty"`
	TargetHex    string     `json:"targetHex,omitempty"`
	Restored     int        `json:"restored"`
	Removed      int        `json:"removed"`
	Error        *string    `json:"error,omitempty"`
	ErrorCode    *ErrorCode `json:"errorCode,omitempty"`
	RetryAfterMs *int       `json:"retryAfterMs,omitempty"`
}

func encodeHistoryMessage(msg Message) (payloadEnvelope, bool, error) {
	switch m := msg.(type) {
	case HistoryListMessage:
		return payloadEnvelope{Kind: "historyList", HistoryList: &historyListDto{
			Namespace: m.Namespace, From: m.From, Skip: m.Skip, Limit: m.Limit,
		}}, true, nil
	case HistoryResultMessage:
		commits := make([]historyCommitDto, len(m.Commits))
		for i, c := range m.Commits {
			commits[i] = historyCommitDto{
				CommitHex: c.CommitHex, ParentHexes: c.ParentHexes,
				TransactionID: c.TransactionID, TimestampMicros: c.TimestampMicros,
				AuthorNodeID: c.AuthorNodeID, TreeHex: c.TreeHex, Message: c.Message,
				OperationCount: c.OperationCount, Stubbed: c.Stubbed,
			}
		}
		return payloadEnvelope{Kind: "historyResult", HistoryResult: &historyResultDto{
			Namespace: m.Namespace, Commits: commits, ResolvedHex: m.ResolvedHex,
			Error: m.Error, ErrorCode: m.ErrorCode, RetryAfterMs: m.RetryAfterMs,
		}}, true, nil
	case RevertMessage:
		return payloadEnvelope{Kind: "revert", Revert: &revertDto{
			Namespace: m.Namespace, To: m.To,
		}}, true, nil
	case RevertResultMessage:
		return payloadEnvelope{Kind: "revertResult", RevertResult: &revertResultDto{
			Namespace: m.Namespace, CommitHex: m.CommitHex, TargetHex: m.TargetHex,
			Restored: m.Restored, Removed: m.Removed,
			Error: m.Error, ErrorCode: m.ErrorCode, RetryAfterMs: m.RetryAfterMs,
		}}, true, nil
	default:
		return payloadEnvelope{}, false, nil
	}
}

func decodeHistoryMessage(header Header, env payloadEnvelope) (Message, bool, error) {
	switch env.Kind {
	case "historyList":
		d := env.HistoryList
		if d == nil {
			return nil, true, newDecodeError("missing historyList body")
		}
		return HistoryListMessage{
			H: header, Namespace: d.Namespace, From: d.From, Skip: d.Skip, Limit: d.Limit,
		}, true, nil
	case "historyResult":
		d := env.HistoryResult
		if d == nil {
			return nil, true, newDecodeError("missing historyResult body")
		}
		commits := make([]HistoryCommit, len(d.Commits))
		for i, c := range d.Commits {
			commits[i] = HistoryCommit{
				CommitHex: c.CommitHex, ParentHexes: c.ParentHexes,
				TransactionID: c.TransactionID, TimestampMicros: c.TimestampMicros,
				AuthorNodeID: c.AuthorNodeID, TreeHex: c.TreeHex, Message: c.Message,
				OperationCount: c.OperationCount, Stubbed: c.Stubbed,
			}
		}
		return HistoryResultMessage{
			H: header, Namespace: d.Namespace, Commits: commits, ResolvedHex: d.ResolvedHex,
			Error: d.Error, ErrorCode: d.ErrorCode, RetryAfterMs: d.RetryAfterMs,
		}, true, nil
	case "revert":
		d := env.Revert
		if d == nil {
			return nil, true, newDecodeError("missing revert body")
		}
		return RevertMessage{H: header, Namespace: d.Namespace, To: d.To}, true, nil
	case "revertResult":
		d := env.RevertResult
		if d == nil {
			return nil, true, newDecodeError("missing revertResult body")
		}
		return RevertResultMessage{
			H: header, Namespace: d.Namespace, CommitHex: d.CommitHex, TargetHex: d.TargetHex,
			Restored: d.Restored, Removed: d.Removed,
			Error: d.Error, ErrorCode: d.ErrorCode, RetryAfterMs: d.RetryAfterMs,
		}, true, nil
	default:
		return nil, false, nil
	}
}
