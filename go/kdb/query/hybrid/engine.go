package hybrid

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/policy"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Engine executes version-aware hybrid SQL.
type Engine interface {
	Execute(sqlStr string, request Request) (Result, error)
	Prepare(sqlStr string, request Request) (PreparedQuery, error)
	Checkout(namespaceID string, ref dag.CommitRef) (CheckoutHandle, error)
	ResetCheckout(namespaceID string) error
}

// Config wires a hybrid query engine.
type Config struct {
	SQL             sql.Engine
	DAG             dag.CommitDAG
	PolicyRegistry  policy.Registry
	Storage         storage.Adapter
	Parser          SQLParser
	VersionResolver VersionResolver
	CheckoutStore   *CheckoutStore
}

// NewEngine returns a hybrid query engine.
func NewEngine(cfg Config) Engine {
	if cfg.Parser == nil {
		cfg.Parser = NewDefaultSQLParser(nil)
	}
	if cfg.VersionResolver == nil {
		cfg.VersionResolver = NewDefaultVersionResolver()
	}
	if cfg.CheckoutStore == nil {
		cfg.CheckoutStore = NewCheckoutStore()
	}
	return &defaultEngine{cfg: cfg}
}

type defaultEngine struct {
	cfg Config
}

func (e *defaultEngine) Execute(sqlStr string, request Request) (Result, error) {
	parsed, err := e.cfg.Parser.ParseWithVersion(sqlStr)
	if err != nil {
		return Result{}, err
	}
	if parsed.Version == nil {
		parsed.Version = request.Version
	}
	checkout, _ := e.cfg.CheckoutStore.Get(request.NamespaceID)
	commit, err := e.cfg.VersionResolver.Resolve(e.cfg.DAG, parsed.Version, checkout)
	if err != nil {
		return Result{}, e.explainResolution(request.NamespaceID, describeClause(parsed.Version), err)
	}
	ctx := sql.QueryContext{
		NamespaceID: request.NamespaceID,
		AtCommit:    &commit,
		Schema:      request.Schema,
		Parameters:  request.Parameters,
		MaxRows:     request.MaxRows,
	}
	if ctx.MaxRows <= 0 {
		ctx.MaxRows = 10_000
	}
	qr, err := e.cfg.SQL.Execute(parsed.SQL, ctx)
	if err != nil {
		return Result{}, err
	}
	// ReadOnly says whether this result came from a pinned past state, not
	// whether the statement happened to be a SELECT. It used to be
	// hardcoded true, which told every caller that an ordinary read at
	// head was a checkout.
	return Result{
		QueryResult:    qr,
		ResolvedCommit: commit,
		ReadOnly:       parsed.Version != nil || checkout != nil,
	}, nil
}

// explainResolution turns a failure to resolve a version into the error
// that names its actual cause.
//
// Under history=NONE the overwhelmingly likely reason a commit cannot be
// found is that the retention window reclaimed it, and that has a remedy
// (a longer window) which "no such commit" does not suggest. Under
// history=FULL the original error is already the whole truth.
func (e *defaultEngine) explainResolution(namespaceID, spec string, err error) error {
	mode, window := historyModeOf(e.cfg.Storage, e.cfg.PolicyRegistry, namespaceID)
	if mode != policy.HistoryModeNone {
		return err
	}
	if r := window.Resolve(); r.Duration <= 0 && r.Commits == 0 {
		return &HistoryDisabledError{NamespaceID: namespaceID}
	}
	return &HistoryNotRetainedError{
		NamespaceID: namespaceID, Spec: spec, Window: window.Resolve(), Err: err,
	}
}

// describeClause renders a version clause the way the operator wrote it,
// for error messages.
func describeClause(clause VersionClause) string {
	switch c := clause.(type) {
	case AtCommit:
		return "commit " + c.Hex
	case AtTag:
		return "tag " + c.Tag
	case AtTime:
		return "the state at " + c.ISO8601
	default:
		return "that version"
	}
}

func (e *defaultEngine) Prepare(sqlStr string, request Request) (PreparedQuery, error) {
	parsed, err := e.cfg.Parser.ParseWithVersion(sqlStr)
	if err != nil {
		return nil, err
	}
	if _, err := e.cfg.Parser.Parse(parsed.SQL); err != nil {
		return nil, err
	}
	return &preparedQuery{engine: e, sql: parsed.SQL, baseRequest: request}, nil
}

// Checkout pins this namespace's subsequent reads to a past commit.
//
// Every reference kind resolves through the DAG, including the ones this
// used to answer head for: a checkout of a branch or a hash that does not
// exist is an error, because silently checking out head instead means the
// caller believes they are looking at the past while looking at the
// present.
func (e *defaultEngine) Checkout(namespaceID string, ref dag.CommitRef) (CheckoutHandle, error) {
	nav, ok := e.cfg.DAG.(dag.HistoryNavigator)
	if !ok {
		return CheckoutHandle{}, fmt.Errorf(
			"kdb: namespace %q cannot check out a past commit: its commit graph does not navigate",
			namespaceID)
	}
	h, err := nav.ResolveRef(ref)
	if err != nil {
		return CheckoutHandle{}, e.explainResolution(namespaceID, describeRef(ref), err)
	}
	ch := CheckoutHandle{NamespaceID: namespaceID, CommitHash: h, ReadOnly: true}
	e.cfg.CheckoutStore.Set(namespaceID, &ch)
	return ch, nil
}

// describeRef renders a commit reference for an error message.
func describeRef(ref dag.CommitRef) string {
	switch r := ref.(type) {
	case dag.RefByHash:
		return "commit " + r.Hex
	case dag.RefByBranch:
		return "branch " + r.Name
	case dag.RefByTag:
		return "tag " + r.Name
	case dag.RefByTime:
		return "that point in time"
	default:
		return "that revision"
	}
}

func (e *defaultEngine) ResetCheckout(namespaceID string) error {
	e.cfg.CheckoutStore.Clear(namespaceID)
	return nil
}

type preparedQuery struct {
	engine      *defaultEngine
	sql         string
	baseRequest Request
}

func (p *preparedQuery) ParameterCount() int { return 0 }

func (p *preparedQuery) Execute(bindings []sql.Parameter, request Request) (Result, error) {
	request.Parameters = bindings
	return p.engine.Execute(p.sql, request)
}

// CheckoutStore holds per-namespace checkouts.
type CheckoutStore struct {
	m map[string]*CheckoutHandle
}

// NewCheckoutStore returns an empty checkout store.
func NewCheckoutStore() *CheckoutStore {
	return &CheckoutStore{m: make(map[string]*CheckoutHandle)}
}

func (s *CheckoutStore) Get(namespaceID string) (*CheckoutHandle, bool) {
	h, ok := s.m[namespaceID]
	return h, ok
}

func (s *CheckoutStore) Set(namespaceID string, h *CheckoutHandle) {
	s.m[namespaceID] = h
}

func (s *CheckoutStore) Clear(namespaceID string) { delete(s.m, namespaceID) }
