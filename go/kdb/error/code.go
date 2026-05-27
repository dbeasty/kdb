package error

// Code is a stable machine-readable error code. Numeric values must not change once published.
type Code int

const (
	KdbDecodeError Code = 1001
	KdbEncodeError Code = 1002
	KdbSchemaError Code = 1005

	JSONPathError Code = 2001

	SchemaViolation        Code = 3001
	SchemaMigrationFailed  Code = 3002

	VersionNotFound    Code = 3101
	IceStorage         Code = 3102
	CompactionBoundary Code = 3103

	Conflict       Code = 4001
	DocumentLocked Code = 4002

	StorageTierError     Code = 4101
	DataDirectoryLocked  Code = 4102
	NamespaceNotFound    Code = 4201

	IndexCorruption Code = 5001

	UnsupportedProtocolVersion Code = 6001
	EncodingNegotiationFailure Code = 6002

	ArchiveRestore Code = 7001

	TransportError       Code = 6101
	ComputeUnavailable   Code = 6201
	ComputeDispatchError Code = 6202

	AuthenticationFailed Code = 6301
	AuthorizationFailed  Code = 6302
)

func (c Code) Numeric() int { return int(c) }
