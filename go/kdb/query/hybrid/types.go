package hybrid

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
)

// ReadConsistency controls read isolation for hybrid queries.
type ReadConsistency int

const (
	ReadConsistencySnapshot ReadConsistency = iota
	ReadConsistencyReadCommitted
	ReadConsistencyReadYourWrites
)

// VersionClause pins a query to a version.
type VersionClause interface {
	versionClause()
}

// AtTag reads at a named tag.
type AtTag struct{ Tag string }

func (AtTag) versionClause() {}

// AtCommit reads at a commit hash hex.
type AtCommit struct{ Hex string }

func (AtCommit) versionClause() {}

// AtTime reads at an ISO-8601 timestamp.
type AtTime struct{ ISO8601 string }

func (AtTime) versionClause() {}

// Request is a hybrid SQL execution request.
type Request struct {
	NamespaceID     string
	Schema          schema.KdbSchema
	Version         VersionClause
	Parameters      []sql.Parameter
	MaxRows         int
	ReadConsistency ReadConsistency
	ReadPin         *codec.Hash
	SessionCheckout *CheckoutHandle
	WriteSessionID  string
	DeferCommit     bool
	TransactionBase *codec.Hash
}

// Result wraps SQL results with resolved commit metadata.
type Result struct {
	QueryResult    sql.QueryResult
	ResolvedCommit codec.Hash
	ReadOnly       bool
	AppliedSchema  *schema.KdbSchema
}

// CheckoutHandle pins a session checkout commit.
type CheckoutHandle struct {
	NamespaceID string
	CommitHash  codec.Hash
	ReadOnly    bool
}

// PreparedQuery is a prepared hybrid statement.
type PreparedQuery interface {
	ParameterCount() int
	Execute(bindings []sql.Parameter, request Request) (Result, error)
}

// ParsedStatement is SQL with an optional version clause stripped.
type ParsedStatement struct {
	SQL     string
	Version VersionClause
}
