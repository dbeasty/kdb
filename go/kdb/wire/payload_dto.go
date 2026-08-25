package wire

type payloadEnvelope struct {
	Kind               string               `json:"kind"`
	Handshake          *handshakeDto        `json:"handshake,omitempty"`
	HandshakeAck       *handshakeAckDto     `json:"handshakeAck,omitempty"`
	DeltaCommit        *deltaCommitDto      `json:"deltaCommit,omitempty"`
	CommitFetch        *commitFetchDto      `json:"commitFetch,omitempty"`
	CommitPush         *commitPushDto       `json:"commitPush,omitempty"`
	DagDiff            *dagDiffDto          `json:"dagDiff,omitempty"`
	TransactionReplay  *transactionReplayDto `json:"transactionReplay,omitempty"`
	ConflictReport     *conflictReportDto   `json:"conflictReport,omitempty"`
	CompactionNotice   *compactionNoticeDto `json:"compactionNotice,omitempty"`
	IceArchiveNotice   *iceArchiveNoticeDto `json:"iceArchiveNotice,omitempty"`
	SnapshotRequest    *snapshotRequestDto  `json:"snapshotRequest,omitempty"`
	SnapshotResponse   *snapshotResponseDto `json:"snapshotResponse,omitempty"`
	PositionAck        *positionAckDto      `json:"positionAck,omitempty"`
	SchemaPush         *schemaPushDto       `json:"schemaPush,omitempty"`
	SessionBegin       *sessionBeginDto     `json:"sessionBegin,omitempty"`
	SessionBeginAck    *sessionBeginAckDto  `json:"sessionBeginAck,omitempty"`
	SqlExec            *sqlExecDto          `json:"sqlExec,omitempty"`
	SqlResult          *sqlResultDto        `json:"sqlResult,omitempty"`
	TxCommit           *txCommitDto         `json:"txCommit,omitempty"`
	TxRollback         *txRollbackDto       `json:"txRollback,omitempty"`

	// Component 40 additions - see document_ops.go.
	DocumentGet       *documentGetDto       `json:"documentGet,omitempty"`
	DocumentGetResult *documentGetResultDto `json:"documentGetResult,omitempty"`
	Upsert            *upsertDto            `json:"upsert,omitempty"`
	UpsertResult      *upsertResultDto      `json:"upsertResult,omitempty"`
}

type handshakeDto struct {
	NodeID                    string            `json:"nodeId"`
	Namespaces                []string          `json:"namespaces"`
	LocalHeads                map[string]string `json:"localHeads"`
	SupportsZstd              bool              `json:"supportsZstd"`
	SupportsIndexHints        bool              `json:"supportsIndexHints"`
	SupportsDirectDeltaIngest bool              `json:"supportsDirectDeltaIngest"`
	MaxFrameBytes             int               `json:"maxFrameBytes"`
	PreferredEncodings        []string          `json:"preferredEncodings"`
	ClientMode                string            `json:"clientMode"`
	ProtocolVersion           int               `json:"protocolVersion"`
	User                      *string           `json:"user,omitempty"`
	Password                  *string           `json:"password,omitempty"`
	Token                     *string           `json:"token,omitempty"`
}

type handshakeAckDto struct {
	Accepted           bool              `json:"accepted"`
	NegotiatedEncoding string            `json:"negotiatedEncoding"`
	ProtocolVersion    int               `json:"protocolVersion"`
	RemoteHeads        map[string]string `json:"remoteHeads"`
	RejectionReason    *string           `json:"rejectionReason,omitempty"`
}

type deltaCommitDto struct {
	Namespace        string         `json:"namespace"`
	CommitHashHex    string         `json:"commitHashHex"`
	ParentHashHex    string         `json:"parentHashHex"`
	TimestampMicros  int64          `json:"timestampMicros"`
	Operations       []opDto        `json:"operations"`
	IndexHints       []indexHintDto `json:"indexHints"`
	SchemaDeltaBytes []byte         `json:"schemaDeltaBytes,omitempty"`
}

type commitFetchDto struct {
	Namespace    string  `json:"namespace"`
	SinceHashHex *string `json:"sinceHashHex"`
	MaxCommits   int     `json:"maxCommits"`
}

type commitPushDto struct {
	Namespace      string `json:"namespace"`
	CommitsPayload []byte `json:"commitsPayload"`
}

type dagDiffDto struct {
	Namespace     string `json:"namespace"`
	LocalHeadHex  string `json:"localHeadHex"`
	RemoteHeadHex string `json:"remoteHeadHex"`
}

type transactionReplayDto struct {
	Namespace        string `json:"namespace"`
	BaseVersionHex   string `json:"baseVersionHex"`
	TransactionBytes []byte `json:"transactionBytes"`
}

type conflictReportDto struct {
	Namespace   string `json:"namespace"`
	ReportBytes []byte `json:"reportBytes"`
}

type compactionNoticeDto struct {
	NamespaceID    string `json:"namespaceId"`
	BoundaryHex    string `json:"boundaryHex"`
	IssuedAtMillis int64  `json:"issuedAtMillis"`
}

type iceArchiveNoticeDto struct {
	Namespace       string `json:"namespace"`
	OriginalHashHex string `json:"originalHashHex"`
	ArchiveLocation string `json:"archiveLocation"`
	BundleHashHex   string `json:"bundleHashHex"`
}

type snapshotRequestDto struct {
	Namespace     string  `json:"namespace"`
	AnchorHashHex *string `json:"anchorHashHex"`
}

type snapshotResponseDto struct {
	Namespace     string `json:"namespace"`
	AnchorHashHex string `json:"anchorHashHex"`
	SnapshotBytes []byte `json:"snapshotBytes"`
	Compressed    bool   `json:"compressed"`
}

type positionAckDto struct {
	Namespace     string `json:"namespace"`
	CommitHashHex string `json:"commitHashHex"`
}

type schemaPushDto struct {
	Namespace   string `json:"namespace"`
	SchemaBytes []byte `json:"schemaBytes"`
	Revision    int64  `json:"revision"`
}

type sessionBeginDto struct {
	Namespace       string  `json:"namespace"`
	SessionID       *string `json:"sessionId"`
	ReadConsistency string  `json:"readConsistency"`
	BaseVersionHex  *string `json:"baseVersionHex"`
}

type sessionBeginAckDto struct {
	Namespace       string `json:"namespace"`
	SessionID       string `json:"sessionId"`
	HeadHex         string `json:"headHex"`
	ReadConsistency string `json:"readConsistency"`
}

type sqlExecDto struct {
	Namespace      string  `json:"namespace"`
	SessionID      string  `json:"sessionId"`
	SQL            string  `json:"sql"`
	ParametersJSON *string `json:"parametersJson"`
}

type sqlResultDto struct {
	Namespace         string     `json:"namespace"`
	SessionID         string     `json:"sessionId"`
	Columns           []string   `json:"columns"`
	Rows              [][]string `json:"rows"`
	RowsAffected      int        `json:"rowsAffected"`
	ResolvedCommitHex string     `json:"resolvedCommitHex"`
	ReadOnly          bool       `json:"readOnly"`
	Error             *string    `json:"error"`
	GeneratedIDs      []string   `json:"generatedIds"`
}

type txCommitDto struct {
	Namespace        string `json:"namespace"`
	SessionID        string `json:"sessionId"`
	TransactionBytes []byte `json:"transactionBytes"`
}

type txRollbackDto struct {
	Namespace string `json:"namespace"`
	SessionID string `json:"sessionId"`
}

type opDto struct {
	Kind              string  `json:"kind"`
	DocID             *string `json:"docId,omitempty"`
	Patch             *string `json:"patch,omitempty"`
	Path              *string `json:"path,omitempty"`
	BlobHashHex       *string `json:"blobHashHex,omitempty"`
	MigrationID       *string `json:"migrationId,omitempty"`
	MigrationPayload  *string `json:"migrationPayload,omitempty"`
}

type indexHintDto struct {
	IndexID       string  `json:"indexId"`
	FieldName     string  `json:"fieldName"`
	IndexType     string  `json:"indexType"`
	Action        string  `json:"action"`
	DocID         string  `json:"docId"`
	Key           *string `json:"key"`
	CommitHashHex string  `json:"commitHashHex"`
}
