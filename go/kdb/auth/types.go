package auth

import "context"

// Principal is an authenticated identity.
type Principal struct {
	ID     string
	Roles  map[string]struct{}
	Claims map[string]string
}

// Credentials carries login material.
type Credentials struct {
	User     *string
	Password *string
	Token    *string
}

// ConnectionContext is transport-provided auth context.
type ConnectionContext struct {
	User     *string
	Password *string
	Token    *string
	Headers  map[string]string
}

// EmptyContext is the default empty connection context.
var EmptyContext = ConnectionContext{}

func (c ConnectionContext) ToCredentials() Credentials {
	return Credentials{User: c.User, Password: c.Password, Token: c.Token}
}

// Action is an authorization request.
type Action interface {
	isAction()
}

type SessionBeginAction struct{ Namespace string }

func (SessionBeginAction) isAction() {}

type SqlExecAction struct {
	Namespace string
	ReadOnly  bool
}

func (SqlExecAction) isAction() {}

type TxCommitAction struct{ Namespace string }

func (TxCommitAction) isAction() {}

// PeerSyncAction is peer sync of a namespace in both directions. An engine that allows it grants
// both; one that wants a peer to sync in one direction only denies it and answers PeerPullAction
// and PeerPushAction instead (see AuthorizePeer).
type PeerSyncAction struct{ Namespace string }

func (PeerSyncAction) isAction() {}

// PeerPullAction is a peer reading a namespace's history from this node: fetching commits,
// snapshots, tree nodes and bodies. Maps to the "sync_pull" permission kind ("sync" also grants
// it).
type PeerPullAction struct{ Namespace string }

func (PeerPullAction) isAction() {}

// PeerPushAction is a peer writing a namespace's history to this node: pushing commits and
// proposing its refs. Maps to the "sync_push" permission kind ("sync" also grants it).
type PeerPushAction struct{ Namespace string }

func (PeerPushAction) isAction() {}

// AuthorizePeer decides one direction of peer sync of ns: allowed when the engine allows
// PeerSyncAction (both directions - every engine written before directions existed), else when it
// allows the directional action. The error is the directional action's.
func AuthorizePeer(ctx context.Context, a Authorizer, p Principal, ns string, push bool) error {
	if a.Authorize(ctx, p, PeerSyncAction{Namespace: ns}) == nil {
		return nil
	}
	if push {
		return a.Authorize(ctx, p, PeerPushAction{Namespace: ns})
	}
	return a.Authorize(ctx, p, PeerPullAction{Namespace: ns})
}

// PeerDirections reports which directions p may sync ns in.
func PeerDirections(ctx context.Context, a Authorizer, p Principal, ns string) (pull, push bool) {
	if a.Authorize(ctx, p, PeerSyncAction{Namespace: ns}) == nil {
		return true, true
	}
	return a.Authorize(ctx, p, PeerPullAction{Namespace: ns}) == nil, a.Authorize(ctx, p, PeerPushAction{Namespace: ns}) == nil
}

// ConflictResolveAction is settling or dismissing a replication conflict in a namespace whose
// resolution chain hands conflicts to a resolver authority. It maps to the "resolve" permission
// kind, which only the authority's principal should hold; the resolution itself is also an
// ordinary write, so the principal needs "write" as well.
type ConflictResolveAction struct{ Namespace string }

func (ConflictResolveAction) isAction() {}

// StreamSubscribeAction is subscribing to a namespace's commit stream (Mode 1/2): every commit's
// full operations are sent, so it is a read of the whole namespace.
type StreamSubscribeAction struct{ Namespace string }

func (StreamSubscribeAction) isAction() {}

// DocumentWriteAction is a per-document write/delete check, resolved at document > collection >
// database grant specificity. Raised by the Transaction Engine for each op in a transaction, not
// just at the wire layer.
type DocumentWriteAction struct {
	Namespace string
	DocID     string
}

func (DocumentWriteAction) isAction() {}

type DocumentDeleteAction struct {
	Namespace string
	DocID     string
}

func (DocumentDeleteAction) isAction() {}

type DocumentReadAction struct {
	Namespace string
	DocID     string
}

func (DocumentReadAction) isAction() {}

// AdminAction is the RBAC admin surface: CREATE/DROP ROLE, GRANT/REVOKE, CREATE/DROP USER.
// Gated behind the "admin" permission kind, separate from "write".
type AdminAction struct {
	// Scope defaults to the reserved system namespace when empty; see engine wiring.
	Scope string
}

func (AdminAction) isAction() {}
