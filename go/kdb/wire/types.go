package wire

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

const (
	KdbWireProtocolVersion          = 1
	MinSupportedWireProtocolVersion = 1
	DefaultMaxFrameBytes            = 16 * 1024 * 1024
	FrameHeaderSize                 = 12
)

// ProtocolVersion is an alias for KdbWireProtocolVersion.
const ProtocolVersion = KdbWireProtocolVersion

// Header is the fixed wire frame header (excluding payload).
type Header struct {
	MessageType     MessageType
	ProtocolVersion int
	CorrelationID   int
	PayloadLength   int
}

// MessageType identifies a wire message kind.
type MessageType uint16

const (
	MsgHandshake         MessageType = 0x01
	MsgDeltaCommit       MessageType = 0x02
	MsgCommitFetch       MessageType = 0x03
	MsgCommitPush        MessageType = 0x04
	MsgDagDiff           MessageType = 0x05
	MsgTransactionReplay MessageType = 0x06
	MsgConflictReport    MessageType = 0x07
	MsgCompactionNotice  MessageType = 0x08
	MsgIceArchiveNotice  MessageType = 0x09
	MsgSnapshotRequest   MessageType = 0x0A
	MsgSnapshotResponse  MessageType = 0x0B
	MsgPositionAck       MessageType = 0x0C
	MsgSchemaPush        MessageType = 0x0D
	MsgSessionBegin      MessageType = 0x0E
	MsgSqlExec           MessageType = 0x0F
	MsgSqlResult         MessageType = 0x10
	MsgTxCommit          MessageType = 0x11
	MsgTxRollback        MessageType = 0x12
	MsgSessionBeginAck   MessageType = 0x13

	// Component 40 (Go Client SDK) additions: direct document get/upsert, bypassing SQL - the
	// SQL_EXEC/INSERT path has no way to read or write one document by its own id (INSERT
	// always mints a fresh UUID; there's no point-lookup-by-id predicate), so PutJSON/GetJSON/
	// Upsert need their own wire messages rather than being expressed as SQL. Go-only for now;
	// no Kotlin counterpart exists yet, per component 38 spec's note that extending go/kdb/wire
	// itself is the right move when the existing message set can't express something.
	MsgDocumentGet       MessageType = 0x14
	MsgDocumentGetResult MessageType = 0x15
	MsgUpsert            MessageType = 0x16
	MsgUpsertResult      MessageType = 0x17

	// The CommitPush response required by kdb-spec-layer8-component23 §5 ("Idempotent putCommit;
	// returns ack frame with applied count") - specified from the start but never actually
	// built, so a non-conflicting push had no frame to reply with at all and left its client
	// blocked on a correlated response that never came. CONFLICT_REPORT is still the reply for
	// the conflicting case; this covers every other outcome. Go-only for now, like 0x14-0x17.
	MsgCommitPushAck MessageType = 0x18

	// Document-lease messages (see lock_ops.go): a client-held, expiring, fenced lock on one
	// document, for holds that span round trips. Go-only for now, like 0x14-0x18.
	MsgLockAcquire MessageType = 0x19
	MsgLockRenew   MessageType = 0x1A
	MsgLockRelease MessageType = 0x1B
	MsgLockResult  MessageType = 0x1C

	// Search over the wire (see search_ops.go), kdb-spec-layer16 §11 / Component 69.
	// Layer 16 — implemented in both trees.
	MsgSearch       MessageType = 0x1D
	MsgSearchResult MessageType = 0x1E

	// History over the wire (see history_ops.go): listing commits and
	// reverting to one. Go-only for now, like 0x14-0x1C.
	MsgHistoryList   MessageType = 0x1F
	MsgHistoryResult MessageType = 0x20
	MsgRevert        MessageType = 0x21
	MsgRevertResult  MessageType = 0x22

	// Cross-namespace commit (see multi_commit_ops.go): several namespaces' transactions,
	// committed together or not at all. Go-only for now, like 0x14-0x22.
	MsgTxCommitMulti       MessageType = 0x23
	MsgTxCommitMultiResult MessageType = 0x24

	// Peer-sync error reply (see PeerErrorMessage). Go-only, like 0x14-0x24.
	MsgPeerError MessageType = 0x25

	// Peer sync protocol v2 (see sync_v2_ops.go). Go-only, like 0x14-0x25.
	MsgSyncHello          MessageType = 0x26
	MsgSyncHelloAck       MessageType = 0x27
	MsgRefsRequest        MessageType = 0x28
	MsgRefsResult         MessageType = 0x29
	MsgFetchRequest       MessageType = 0x2A
	MsgPackPage           MessageType = 0x2B
	MsgRefUpdate          MessageType = 0x2C
	MsgRefUpdateAck       MessageType = 0x2D
	MsgSnapshotFetch      MessageType = 0x2E
	MsgSnapshotPage       MessageType = 0x2F
	MsgProjectFetch       MessageType = 0x30
	MsgProjectPage        MessageType = 0x31
	MsgProjectWrite       MessageType = 0x32
	MsgProjectWriteResult MessageType = 0x33
	MsgTreeNodes          MessageType = 0x34
	MsgTreeNodesResult    MessageType = 0x35
	MsgObjectFetch        MessageType = 0x36
	MsgObjectFetchResult  MessageType = 0x37
	MsgDocFetch           MessageType = 0x38
	MsgDocFetchResult     MessageType = 0x39
	MsgGraftPush          MessageType = 0x3A
	MsgGraftPushResult    MessageType = 0x3B
	MsgMetaView           MessageType = 0x3C
	MsgMetaViewResult     MessageType = 0x3D

	// Aliases for callers using SQL-prefixed names.
	MsgSQLExec   = MsgSqlExec
	MsgSQLResult = MsgSqlResult
)

func (t MessageType) String() string {
	switch t {
	case MsgHandshake:
		return "HANDSHAKE"
	case MsgDeltaCommit:
		return "DELTA_COMMIT"
	case MsgCommitFetch:
		return "COMMIT_FETCH"
	case MsgCommitPush:
		return "COMMIT_PUSH"
	case MsgDagDiff:
		return "DAG_DIFF"
	case MsgTransactionReplay:
		return "TRANSACTION_REPLAY"
	case MsgConflictReport:
		return "CONFLICT_REPORT"
	case MsgCompactionNotice:
		return "COMPACTION_NOTICE"
	case MsgIceArchiveNotice:
		return "ICE_ARCHIVE_NOTICE"
	case MsgSnapshotRequest:
		return "SNAPSHOT_REQUEST"
	case MsgSnapshotResponse:
		return "SNAPSHOT_RESPONSE"
	case MsgPositionAck:
		return "POSITION_ACK"
	case MsgSchemaPush:
		return "SCHEMA_PUSH"
	case MsgSessionBegin:
		return "SESSION_BEGIN"
	case MsgSqlExec:
		return "SQL_EXEC"
	case MsgSqlResult:
		return "SQL_RESULT"
	case MsgTxCommit:
		return "TX_COMMIT"
	case MsgTxRollback:
		return "TX_ROLLBACK"
	case MsgSessionBeginAck:
		return "SESSION_BEGIN_ACK"
	case MsgDocumentGet:
		return "DOCUMENT_GET"
	case MsgDocumentGetResult:
		return "DOCUMENT_GET_RESULT"
	case MsgUpsert:
		return "UPSERT"
	case MsgUpsertResult:
		return "UPSERT_RESULT"
	case MsgCommitPushAck:
		return "COMMIT_PUSH_ACK"
	case MsgLockAcquire:
		return "LOCK_ACQUIRE"
	case MsgLockRenew:
		return "LOCK_RENEW"
	case MsgLockRelease:
		return "LOCK_RELEASE"
	case MsgLockResult:
		return "LOCK_RESULT"
	case MsgSearch:
		return "SEARCH"
	case MsgSearchResult:
		return "SEARCH_RESULT"
	case MsgHistoryList:
		return "HISTORY_LIST"
	case MsgHistoryResult:
		return "HISTORY_RESULT"
	case MsgRevert:
		return "REVERT"
	case MsgRevertResult:
		return "REVERT_RESULT"
	case MsgTxCommitMulti:
		return "TX_COMMIT_MULTI"
	case MsgTxCommitMultiResult:
		return "TX_COMMIT_MULTI_RESULT"
	case MsgPeerError:
		return "PEER_ERROR"
	case MsgSyncHello:
		return "SYNC_HELLO"
	case MsgSyncHelloAck:
		return "SYNC_HELLO_ACK"
	case MsgRefsRequest:
		return "REFS_REQUEST"
	case MsgRefsResult:
		return "REFS_RESULT"
	case MsgFetchRequest:
		return "FETCH_REQUEST"
	case MsgPackPage:
		return "PACK_PAGE"
	case MsgRefUpdate:
		return "REF_UPDATE"
	case MsgRefUpdateAck:
		return "REF_UPDATE_ACK"
	case MsgSnapshotFetch:
		return "SNAPSHOT_FETCH"
	case MsgSnapshotPage:
		return "SNAPSHOT_PAGE"
	case MsgProjectFetch:
		return "PROJECT_FETCH"
	case MsgProjectPage:
		return "PROJECT_PAGE"
	case MsgProjectWrite:
		return "PROJECT_WRITE"
	case MsgProjectWriteResult:
		return "PROJECT_WRITE_RESULT"
	case MsgTreeNodes:
		return "TREE_NODES"
	case MsgTreeNodesResult:
		return "TREE_NODES_RESULT"
	case MsgObjectFetch:
		return "OBJECT_FETCH"
	case MsgObjectFetchResult:
		return "OBJECT_FETCH_RESULT"
	case MsgDocFetch:
		return "DOC_FETCH"
	case MsgDocFetchResult:
		return "DOC_FETCH_RESULT"
	case MsgGraftPush:
		return "GRAFT_PUSH"
	case MsgGraftPushResult:
		return "GRAFT_PUSH_RESULT"
	case MsgMetaView:
		return "META_VIEW"
	case MsgMetaViewResult:
		return "META_VIEW_RESULT"
	default:
		return "UNKNOWN"
	}
}

func MessageTypeFromCode(code uint16) (MessageType, bool) {
	switch code {
	case 0x01:
		return MsgHandshake, true
	case 0x02:
		return MsgDeltaCommit, true
	case 0x03:
		return MsgCommitFetch, true
	case 0x04:
		return MsgCommitPush, true
	case 0x05:
		return MsgDagDiff, true
	case 0x06:
		return MsgTransactionReplay, true
	case 0x07:
		return MsgConflictReport, true
	case 0x08:
		return MsgCompactionNotice, true
	case 0x09:
		return MsgIceArchiveNotice, true
	case 0x0A:
		return MsgSnapshotRequest, true
	case 0x0B:
		return MsgSnapshotResponse, true
	case 0x0C:
		return MsgPositionAck, true
	case 0x0D:
		return MsgSchemaPush, true
	case 0x0E:
		return MsgSessionBegin, true
	case 0x0F:
		return MsgSqlExec, true
	case 0x10:
		return MsgSqlResult, true
	case 0x11:
		return MsgTxCommit, true
	case 0x12:
		return MsgTxRollback, true
	case 0x13:
		return MsgSessionBeginAck, true
	case 0x14:
		return MsgDocumentGet, true
	case 0x15:
		return MsgDocumentGetResult, true
	case 0x16:
		return MsgUpsert, true
	case 0x17:
		return MsgUpsertResult, true
	case 0x18:
		return MsgCommitPushAck, true
	case 0x19:
		return MsgLockAcquire, true
	case 0x1A:
		return MsgLockRenew, true
	case 0x1B:
		return MsgLockRelease, true
	case 0x1C:
		return MsgLockResult, true
	case 0x1D:
		return MsgSearch, true
	case 0x1E:
		return MsgSearchResult, true
	case 0x1F:
		return MsgHistoryList, true
	case 0x20:
		return MsgHistoryResult, true
	case 0x21:
		return MsgRevert, true
	case 0x22:
		return MsgRevertResult, true
	case 0x23:
		return MsgTxCommitMulti, true
	case 0x24:
		return MsgTxCommitMultiResult, true
	case 0x25:
		return MsgPeerError, true
	case 0x26:
		return MsgSyncHello, true
	case 0x27:
		return MsgSyncHelloAck, true
	case 0x28:
		return MsgRefsRequest, true
	case 0x29:
		return MsgRefsResult, true
	case 0x2A:
		return MsgFetchRequest, true
	case 0x2B:
		return MsgPackPage, true
	case 0x2C:
		return MsgRefUpdate, true
	case 0x2D:
		return MsgRefUpdateAck, true
	case 0x2E:
		return MsgSnapshotFetch, true
	case 0x2F:
		return MsgSnapshotPage, true
	case 0x30:
		return MsgProjectFetch, true
	case 0x31:
		return MsgProjectPage, true
	case 0x32:
		return MsgProjectWrite, true
	case 0x33:
		return MsgProjectWriteResult, true
	case 0x34:
		return MsgTreeNodes, true
	case 0x35:
		return MsgTreeNodesResult, true
	case 0x36:
		return MsgObjectFetch, true
	case 0x37:
		return MsgObjectFetchResult, true
	case 0x38:
		return MsgDocFetch, true
	case 0x39:
		return MsgDocFetchResult, true
	case 0x3A:
		return MsgGraftPush, true
	case 0x3B:
		return MsgGraftPushResult, true
	case 0x3C:
		return MsgMetaView, true
	case 0x3D:
		return MsgMetaViewResult, true
	default:
		return 0, false
	}
}

type PayloadEncoding int

const (
	EncodingKdbBinary PayloadEncoding = iota
	EncodingJSON
)

func (e PayloadEncoding) String() string {
	switch e {
	case EncodingJSON:
		return "JSON"
	default:
		return "KDB_BINARY"
	}
}

func PayloadEncodingFromName(name string) PayloadEncoding {
	if name == "JSON" {
		return EncodingJSON
	}
	return EncodingKdbBinary
}

type ClientMode int

const (
	ClientStreamReadOnly ClientMode = iota
	ClientStreamWriteBack
	ClientFullPeer
	ClientSQL
)

func (m ClientMode) String() string {
	switch m {
	case ClientStreamWriteBack:
		return "STREAM_WRITE_BACK"
	case ClientFullPeer:
		return "FULL_PEER"
	case ClientSQL:
		return "SQL_CLIENT"
	default:
		return "STREAM_READ_ONLY"
	}
}

func ClientModeFromName(name string) ClientMode {
	switch name {
	case "STREAM_WRITE_BACK":
		return ClientStreamWriteBack
	case "FULL_PEER":
		return ClientFullPeer
	case "SQL_CLIENT":
		return ClientSQL
	default:
		return ClientStreamReadOnly
	}
}

type CapabilitySet struct {
	SupportsZstd              bool
	SupportsIndexHints        bool
	SupportsDirectDeltaIngest bool
	MaxFrameBytes             int
}

func DefaultCapabilities() CapabilitySet {
	return CapabilitySet{SupportsZstd: true, SupportsIndexHints: true, MaxFrameBytes: DefaultMaxFrameBytes}
}

type HandshakePayload struct {
	NodeID             string
	Namespaces         []string
	LocalHeads         map[string]string
	Capabilities       CapabilitySet
	PreferredEncodings []PayloadEncoding
	ClientMode         ClientMode
	ProtocolVersion    int
	// Credentials, all optional. TCP has no side channel for connection-level auth context
	// (unlike WebSocket's HTTP headers, which the Kotlin server's ConnectionContext reads) -
	// this is the only place a SQL_CLIENT handshake can carry them for the Go-native server
	// (component 38 §4, sub-phase C). User+Password is the common case; Token is an
	// alternative "user:secret" combined form, matching auth.Credentials.
	User     *string
	Password *string
	Token    *string
	// Filter, on a stream handshake, subscribes to only the documents matching this KDB-SQL
	// condition. Nil is every document. Omitted from the frame when nil, so older peers see the
	// handshake they always did.
	Filter *string
}

type HandshakeAckPayload struct {
	Accepted           bool
	NegotiatedEncoding PayloadEncoding
	ProtocolVersion    int
	RemoteHeads        map[string]string
	RejectionReason    *string
}

type IndexHint struct {
	IndexID    codec.UUID
	FieldName  string
	IndexType  string
	Action     string
	DocID      codec.UUID
	Key        *string
	CommitHash codec.Hash
}

type DeltaCommitPayload struct {
	Namespace        string
	CommitHash       codec.Hash
	ParentHash       codec.Hash
	TimestampMicros  int64
	Operations       []document.Op
	IndexHints       []IndexHint
	SchemaDeltaBytes []byte
}

type CompactionIntent struct {
	NamespaceID    string
	Boundary       codec.Hash
	IssuedAtMillis int64
}

type Message interface {
	Header() Header
}

type HandshakeMessage struct {
	H       Header
	Request HandshakePayload
}

func (m HandshakeMessage) Header() Header { return m.H }

type HandshakeAckMessage struct {
	H        Header
	Response HandshakeAckPayload
}

func (m HandshakeAckMessage) Header() Header { return m.H }

type DeltaCommitMessage struct {
	H       Header
	Payload DeltaCommitPayload
}

func (m DeltaCommitMessage) Header() Header { return m.H }

type CommitFetchMessage struct {
	H          Header
	Namespace  string
	SinceHash  *codec.Hash
	MaxCommits int
	// Haves are further commits the fetcher already holds (Go-only, additive). The host sends
	// nothing reachable from SinceHash or from any Have it recognizes - which is what keeps a
	// fetcher whose head the host has never seen (it diverged) from being sent all of history.
	Haves []codec.Hash
}

func (m CommitFetchMessage) Header() Header { return m.H }

type CommitPushMessage struct {
	H         Header
	Namespace string
	Commits   []document.Commit
	// Stubs name archived commits the sender cannot send whole but whose children it is sending
	// (Go-only, additive). The receiver records them so those children's parents resolve.
	Stubs []document.CommitStub
	// More marks a page of a multi-page push (Go-only, additive): the receiver stores these
	// commits but decides nothing until the page without More arrives. Deciding on a partial
	// push would merge half of a divergent branch, then merge again for every later page.
	More bool
}

// PeerErrorMessage is the peer-sync reply to a frame the host could not serve: an unknown
// message type, an authorization failure, a namespace mismatch, or an ingest error. Go-only.
// Before it existed every such case either went unanswered, leaving the caller to wait out its
// correlation timeout, or dropped the connection with no reason given.
type PeerErrorMessage struct {
	H         Header
	Namespace string
	Code      ErrorCode
	Message   string
}

func (m PeerErrorMessage) Header() Header { return m.H }

func (m CommitPushMessage) Header() Header { return m.H }

// CommitPushAckMessage acknowledges a CommitPush that did not conflict. AppliedCommits counts
// only the commits this push actually added (a re-push of history the peer already has is a
// legitimate zero), and HeadHex is the peer's branch head once the push was resolved - which is
// not necessarily the pushed commit, since a divergent-but-non-conflicting push lands on a
// freshly created two-parent merge commit instead.
type CommitPushAckMessage struct {
	H              Header
	Namespace      string
	AppliedCommits int
	HeadHex        string
}

func (m CommitPushAckMessage) Header() Header { return m.H }

type DagDiffMessage struct {
	H          Header
	Namespace  string
	LocalHead  codec.Hash
	RemoteHead codec.Hash
}

func (m DagDiffMessage) Header() Header { return m.H }

type TransactionReplayMessage struct {
	H                Header
	Namespace        string
	BaseVersion      codec.Hash
	TransactionBytes []byte
}

func (m TransactionReplayMessage) Header() Header { return m.H }

type ConflictReportMessage struct {
	H           Header
	Namespace   string
	ReportBytes []byte
	// ErrorCode and RetryAfterMs are additive to ReportBytes, the same way they are additive to
	// Error on SqlResultMessage (kdb-spec-layer13 Component 51 §8.1). ErrorCodeConflict names
	// this message's own condition - it was defined with a doc comment promising exactly that
	// and had no producer until these fields existed, because there was nowhere on a conflict
	// response to put it.
	//
	// RetryAfterMs is what makes an optimistic-concurrency client behave under contention. The
	// report says the transaction lost a race; it never said when to try again, so every client
	// retried immediately, and N clients retrying immediately against one document is a herd
	// that re-collides every round. The server sets this from its own live queue depth and
	// jitters it per response, which is the one place the spread can actually be chosen
	// independently for each loser (see server.conflictRetryAfterMs).
	ErrorCode    *ErrorCode
	RetryAfterMs *int
}

func (m ConflictReportMessage) Header() Header { return m.H }

type CompactionNoticeMessage struct {
	H      Header
	Intent CompactionIntent
}

func (m CompactionNoticeMessage) Header() Header { return m.H }

type IceArchiveNoticeMessage struct {
	H               Header
	Namespace       string
	OriginalHash    codec.Hash
	ArchiveLocation string
	BundleHash      codec.Hash
}

func (m IceArchiveNoticeMessage) Header() Header { return m.H }

type SnapshotRequestMessage struct {
	H          Header
	Namespace  string
	AnchorHash *codec.Hash
}

func (m SnapshotRequestMessage) Header() Header { return m.H }

type SnapshotResponseMessage struct {
	H             Header
	Namespace     string
	AnchorHash    codec.Hash
	SnapshotBytes []byte
	Compressed    bool
}

func (m SnapshotResponseMessage) Header() Header { return m.H }

type PositionAckMessage struct {
	H          Header
	Namespace  string
	CommitHash codec.Hash
}

func (m PositionAckMessage) Header() Header { return m.H }

type SchemaPushMessage struct {
	H           Header
	Namespace   string
	SchemaBytes []byte
	Revision    int64
}

func (m SchemaPushMessage) Header() Header { return m.H }

type SessionBeginMessage struct {
	H               Header
	Namespace       string
	SessionID       *string
	ReadConsistency string
	BaseVersionHex  *string
}

func (m SessionBeginMessage) Header() Header { return m.H }

type SessionBeginAckMessage struct {
	H               Header
	Namespace       string
	SessionID       string
	HeadHex         string
	ReadConsistency string
	// Error is set (with SessionID empty) when the session begin was rejected - an
	// authentication or authorization failure the client should surface rather than a bare
	// "rejected". nil on success and on frames from peers that predate the field.
	Error *string
}

func (m SessionBeginAckMessage) Header() Header { return m.H }

type SqlExecMessage struct {
	H              Header
	Namespace      string
	SessionID      string
	SQL            string
	ParametersJSON *string
}

func (m SqlExecMessage) Header() Header { return m.H }

type SqlResultMessage struct {
	H                 Header
	Namespace         string
	SessionID         string
	Columns           []string
	Rows              [][]string
	RowsAffected      int
	ResolvedCommitHex string
	ReadOnly          bool
	Error             *string
	GeneratedIDs      []string
	// ErrorCode and RetryAfterMs are additive to Error (kdb-spec-layer13 Component 51 §8.1): nil
	// on any message an old server or a success response produces. Set only alongside a non-nil
	// Error, to let a client decide whether/when to retry without parsing Error's prose.
	ErrorCode    *ErrorCode
	RetryAfterMs *int
}

func (m SqlResultMessage) Header() Header { return m.H }

type TxCommitMessage struct {
	H                Header
	Namespace        string
	SessionID        string
	TransactionBytes []byte
}

func (m TxCommitMessage) Header() Header { return m.H }

type TxRollbackMessage struct {
	H         Header
	Namespace string
	SessionID string
}

func (m TxRollbackMessage) Header() Header { return m.H }
