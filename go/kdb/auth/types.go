package auth

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

type PeerSyncAction struct{ Namespace string }

func (PeerSyncAction) isAction() {}

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
