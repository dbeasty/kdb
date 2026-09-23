package script

import (
	"context"
	"fmt"

	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// A stored procedure as a local transaction's conflict resolver.
//
// This is the third place a procedure can settle a conflict, and the smallest. A local commit
// conflicts when the document it writes has changed since the version the transaction was based
// on - one process, one document, no second node to agree with - so the engine asks its resolver
// for the body to store, and that is all it can accept: there is no merge to attach a forked
// document to, and no way to say "delete it instead".
//
// Because nothing here is replicated as a merge, determinism is not required, and an embedded
// caller with no server at all can use it. The sandbox mode is the caller's to choose;
// ModePure is still the sensible default, since a resolver that reads the database while the
// transaction it is resolving is mid-flight sees a head that is about to move.

// ConflictResolver runs Source over each conflicting document of a local transaction, for
// transaction.ConflictPolicyCustom.
type ConflictResolver struct {
	Runtime *Runtime
	Source  string
	// Mode and Host are passed to each call; Host is needed only in ModeRead.
	Mode Mode
	Host Host
	// Context bounds every call; nil means context.Background.
	Context context.Context
}

var _ transaction.ConflictResolver = ConflictResolver{}

// Resolve returns the document the transaction should store, or nil to leave the conflict
// reported - which is what a procedure's defer means here, and what anything it asks for that a
// local commit cannot express means too.
func (r ConflictResolver) Resolve(c transaction.DocumentConflict) (*document.Document, error) {
	ctx := r.Context
	if ctx == nil {
		ctx = context.Background()
	}
	rt := r.Runtime
	if rt == nil {
		rt = New(Limits{})
	}
	body := func(d *document.Document) *string {
		if d == nil {
			return nil
		}
		j := d.JSON
		return &j
	}
	dec, err := rt.ResolveConflict(ctx, r.Source, r.Mode, r.Host, ConflictInput{
		DocID:  c.DocID.String(),
		Base:   body(c.BaseDoc),
		Local:  body(c.ExistingDoc),
		Remote: body(c.IncomingDoc),
		LocalOrigin: Origin{
			NodeID: c.ExistingOrigin.NodeID.String(), Commit: c.ExistingOrigin.Commit.Hex(),
			TimestampMicros: c.ExistingOrigin.TimestampMicros,
		},
		RemoteOrigin: Origin{
			NodeID: c.IncomingOrigin.NodeID.String(), Commit: c.IncomingOrigin.Commit.Hex(),
			TimestampMicros: c.IncomingOrigin.TimestampMicros,
		},
	})
	if err != nil {
		return nil, err
	}
	switch dec.Kind {
	case DecideDefer:
		return nil, nil
	case DecideTake:
		side := [2]*document.Document{c.ExistingDoc, c.IncomingDoc}[dec.Side]
		if side == nil {
			return nil, fmt.Errorf("%w: the chosen side of %s is a delete, which a local commit's resolver cannot return", ErrOutput, c.DocID)
		}
		return &document.Document{ID: c.DocID, JSON: side.JSON}, nil
	case DecideDoc:
		return &document.Document{ID: c.DocID, JSON: dec.Doc}, nil
	default:
		// Fork and delete belong to a merge, which has a commit to put the extra document or the
		// deletion in. Saying so is better than quietly storing one side.
		return nil, fmt.Errorf("%w: only take, doc and defer mean anything when resolving a local commit", ErrOutput)
	}
}
