package hybrid

import (
	"github.com/limidus/kdb/go/kdb/codec"
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
	checkout, _ := e.cfg.CheckoutStore.Get(request.NamespaceID)
	commit, err := e.cfg.VersionResolver.Resolve(e.cfg.DAG, parsed.Version, checkout)
	if err != nil {
		return Result{}, err
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
	_, _ = e.cfg.PolicyRegistry.Get(request.NamespaceID)
	return Result{QueryResult: qr, ResolvedCommit: commit, ReadOnly: true}, nil
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

func (e *defaultEngine) Checkout(namespaceID string, ref dag.CommitRef) (CheckoutHandle, error) {
	var h codec.Hash
	var err error
	switch r := ref.(type) {
	case dag.RefByHash:
		h, err = codec.HashFromHex(r.Hex)
	case dag.RefByBranch:
		br, ok := e.cfg.DAG.GetBranch(r.Name)
		if !ok {
			h, err = e.cfg.DAG.Head()
			break
		}
		h = br.HeadHash
	default:
		h, err = e.cfg.DAG.Head()
	}
	if err != nil {
		return CheckoutHandle{}, err
	}
	ch := CheckoutHandle{NamespaceID: namespaceID, CommitHash: h, ReadOnly: true}
	e.cfg.CheckoutStore.Set(namespaceID, &ch)
	return ch, nil
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
