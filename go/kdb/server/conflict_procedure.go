package server

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	kdbjson "github.com/limidus/kdb/go/kdb/json"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/script"
	"github.com/limidus/kdb/go/kdb/sql"
)

// The resolver authority as a stored procedure.
//
// A chain's authority rule hands what it cannot settle to something outside the merge - a person,
// or an application service reached by webhook. This runs a stored procedure in that position
// instead: on the notifying node, after the merge, over the queued conflict.
//
// It is the other half of RuleProcedure and the opposite trade. Nothing here has to be
// deterministic, because the result is an ordinary commit that replicates like any write rather
// than content inside a merge commit's hash - so this procedure may read the database, and two
// nodes never race to produce the same answer because only the notifying node runs it. What it
// gives up is immediacy: the conflict is queued first, and the document holds the chain's
// fallback (nothing, or a provisional last-write) until the procedure settles it.
//
// It still may not write. The write is this package's, made as a named principal, so a settlement
// is an auditable commit by someone rather than an unattributable change.

// maxProcedureAttempts bounds how often one entry is retried. A procedure that fails the same way
// every pass would otherwise be a hot loop, and a conflict nothing can settle should reach a
// person rather than be tried forever.
const maxProcedureAttempts = 5

// ConflictProcedure settles queued authority conflicts by running the procedure the namespace's
// authority rule names.
type ConflictProcedure struct {
	// Interval is how often entries are retried; 0 means 10 seconds. A new entry is tried at
	// once, not on the next interval.
	Interval time.Duration
	// Principal is who the settlement is committed as. It needs "resolve" on the namespace, the
	// same as any operator settling a conflict by hand: a procedure acting for the authority is
	// still someone acting, and granting it rights is the operator's decision, not this code's.
	Principal auth.Principal
	// Limits bound each call; the zero value is script.DefaultLimits.
	Limits script.Limits

	set  *NamespaceSet
	node string
	kick chan struct{}
	stop chan struct{}
	done chan struct{}
	once sync.Once

	passMu   sync.Mutex
	hooked   map[*peersync.ConflictQueue]bool
	attempts map[string]int
	// failures records why each entry was last left open, for the control plane and for tests.
	failMu   sync.Mutex
	failures map[string]string
}

// StartConflictProcedure starts settling set's authority conflicts from node.
func StartConflictProcedure(set *NamespaceSet, node string, p *ConflictProcedure) *ConflictProcedure {
	p.set, p.node = set, node
	if p.Interval <= 0 {
		p.Interval = 10 * time.Second
	}
	if p.Principal.ID == "" {
		p.Principal = systemPrincipal()
	}
	p.kick = make(chan struct{}, 1)
	p.stop = make(chan struct{})
	p.done = make(chan struct{})
	p.hooked = map[*peersync.ConflictQueue]bool{}
	p.attempts = map[string]int{}
	p.failures = map[string]string{}
	go p.loop()
	p.Kick()
	return p
}

// Kick asks for a pass now.
func (p *ConflictProcedure) Kick() {
	select {
	case p.kick <- struct{}{}:
	default:
	}
}

// Close stops settling, waiting for a pass in flight.
func (p *ConflictProcedure) Close() {
	p.once.Do(func() { close(p.stop) })
	<-p.done
}

func (p *ConflictProcedure) loop() {
	defer close(p.done)
	t := time.NewTicker(p.Interval)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-p.kick:
		case <-t.C:
		}
		p.Pass()
	}
}

// LastFailure is why entry id was last left open, or "" if it was not.
func (p *ConflictProcedure) LastFailure(id string) string {
	p.failMu.Lock()
	defer p.failMu.Unlock()
	return p.failures[id]
}

// Pass runs the authority procedure over every queued conflict waiting on it, and returns how
// many it settled.
func (p *ConflictProcedure) Pass() int {
	p.passMu.Lock()
	defer p.passMu.Unlock()
	runtimes := p.set.Runtimes()
	names := make([]string, 0, len(runtimes))
	for ns := range runtimes {
		names = append(names, ns)
	}
	sort.Strings(names)
	settled := 0
	for _, ns := range names {
		rt := runtimes[ns]
		if rt.Conflicts == nil {
			continue
		}
		if !p.hooked[rt.Conflicts] {
			p.hooked[rt.Conflicts] = true
			rt.Conflicts.OnRecord(func(e peersync.ConflictEntry) {
				if e.Authority {
					p.Kick()
				}
			})
		}
		rule := rt.ResolutionChainOf().Authority()
		if rule == nil || rule.Procedure == "" || !rule.Notifies(p.node) {
			continue
		}
		for _, e := range rt.Conflicts.List() {
			if !e.Authority || len(e.Items) == 0 {
				continue
			}
			select {
			case <-p.stop:
				return settled
			default:
			}
			if p.attempts[e.ID] >= maxProcedureAttempts {
				continue
			}
			p.attempts[e.ID]++
			if err := p.settle(rt, rule.Procedure, e); err != nil {
				p.note(e.ID, err.Error())
				continue
			}
			p.note(e.ID, "")
			delete(p.attempts, e.ID)
			settled++
		}
	}
	return settled
}

func (p *ConflictProcedure) note(id, why string) {
	p.failMu.Lock()
	defer p.failMu.Unlock()
	if why == "" {
		delete(p.failures, id)
		return
	}
	p.failures[id] = why
}

// settle runs the procedure over every document of one entry and, if it decided them all,
// commits the settlement.
//
// All or nothing on purpose: an entry is settled by one commit, and a procedure that deferred on
// one document of a multi-document conflict has not said what should happen to that document.
// Settling the rest would leave the remainder in a conflict whose other half has already moved.
func (p *ConflictProcedure) settle(rt *KdbServerRuntime, name string, e peersync.ConflictEntry) error {
	src, ok := rt.ProcedureSource(name)
	if !ok {
		return fmt.Errorf("this node holds no procedure %q in %s", name, rt.Runtime.DefaultNamespace)
	}
	bases := map[string]*string{}
	origins := map[string][2]script.Origin{}
	for _, d := range e.Details {
		bases[d.DocumentID] = d.Base
		origins[d.DocumentID] = [2]script.Origin{procedureOrigin(d.LocalOrigin), procedureOrigin(d.IncomingOrigin)}
	}
	host := &runtimeHost{rt: rt}
	choices := map[codec.UUID]peersync.Choice{}
	for _, it := range e.Items {
		id, err := codec.ParseUUID(it.DocumentID)
		if err != nil {
			return err
		}
		o := origins[it.DocumentID]
		dec, err := procRuntime.ResolveConflict(context.Background(), src, script.ModeRead, host, script.ConflictInput{
			DocID:        it.DocumentID,
			Base:         bases[it.DocumentID],
			Local:        it.LocalDoc,
			Remote:       it.IncomingDoc,
			LocalOrigin:  o[0],
			RemoteOrigin: o[1],
		})
		if err != nil {
			return err
		}
		c, decided := authorityChoice(dec)
		if !decided {
			return fmt.Errorf("procedure %q left document %s to a person", name, it.DocumentID)
		}
		choices[id] = c
	}
	_, err := rt.ResolveConflict(e.ID, choices, p.Principal)
	return err
}

// authorityChoice translates a decision into the settlement an operator would have made by hand -
// the same Choice, through the same path, so a procedure can do nothing here that a person could
// not do themselves.
func authorityChoice(d script.Decision) (peersync.Choice, bool) {
	side := [2]string{"local", "remote"}
	switch d.Kind {
	case script.DecideTake:
		return peersync.Choice{Take: side[d.Side]}, true
	case script.DecideDoc:
		body := d.Doc
		return peersync.Choice{Body: &body}, true
	case script.DecideDelete:
		return peersync.Choice{Delete: true}, true
	case script.DecideFork:
		return peersync.Choice{Fork: &peersync.ForkChoice{Keep: side[d.Side]}}, true
	default:
		return peersync.Choice{}, false
	}
}

// runtimeHost is the database a ModeRead procedure reads: this namespace at its current head.
type runtimeHost struct{ rt *KdbServerRuntime }

func (h *runtimeHost) Get(docID string) (string, error) {
	id, err := codec.ParseUUID(docID)
	if err != nil {
		return "", err
	}
	body, _, found, err := h.rt.GetDocument(h.rt.Runtime.DefaultNamespace, id)
	if err != nil || !found {
		return "", err
	}
	return body, nil
}

func (h *runtimeHost) Query(statement string, params []any) (string, error) {
	head, err := h.rt.Runtime.DAG.Head()
	if err != nil {
		return "", err
	}
	ps := make([]sql.Parameter, 0, len(params))
	for _, v := range params {
		p, err := sqlParameter(v)
		if err != nil {
			return "", err
		}
		ps = append(ps, p)
	}
	res, err := h.rt.SQLEngine().Execute(statement, sql.QueryContext{
		NamespaceID: h.rt.Runtime.DefaultNamespace,
		Schema:      h.rt.Schema(),
		AtCommit:    &head,
		Parameters:  ps,
		MaxRows:     10_000,
	})
	if err != nil {
		return "", err
	}
	rows := kdbjson.ArrayValue{}
	for _, r := range res.Rows {
		row := kdbjson.ObjectValue{Fields: map[string]kdbjson.Value{}}
		for i, cell := range r.Values {
			if i >= len(res.Columns) {
				break
			}
			v, err := sql.CellToJSONValue(cell)
			if err != nil {
				return "", err
			}
			name := res.Columns[i].Name
			if _, dup := row.Fields[name]; !dup {
				row.Keys = append(row.Keys, name)
			}
			row.Fields[name] = v
		}
		rows.Elements = append(rows.Elements, row)
	}
	return kdbjson.ToJSONString(rows), nil
}

func sqlParameter(v any) (sql.Parameter, error) {
	switch t := v.(type) {
	case nil:
		return sql.ParamNull{}, nil
	case string:
		return sql.ParamString{Value: t}, nil
	case bool:
		return sql.ParamBool{Value: t}, nil
	case int64:
		return sql.ParamInt{Value: t}, nil
	case float64:
		// JavaScript has one number type, so an integral value arrives as a float. Passing it on
		// as a double would make "where qty = 3" miss a document holding 3.
		if t == float64(int64(t)) {
			return sql.ParamInt{Value: int64(t)}, nil
		}
		return sql.ParamDouble{Value: t}, nil
	default:
		return nil, fmt.Errorf("a query parameter must be a string, number, boolean or null, not %T", v)
	}
}
