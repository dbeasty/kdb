package auth

import "context"

// Authenticator validates credentials and returns a principal.
type Authenticator interface {
	Authenticate(ctx context.Context, credentials Credentials) (Principal, error)
}

// Authorizer checks whether a principal may perform an action.
type Authorizer interface {
	Authorize(ctx context.Context, principal Principal, action Action) error
}

// Engine combines authentication and authorization.
type Engine interface {
	Authenticator() Authenticator
	Authorizer() Authorizer
}

// AllowAll is a permissive auth engine for development and tests.
var AllowAll Engine = allowAllEngine{}

type allowAllEngine struct{}

func (allowAllEngine) Authenticator() Authenticator { return allowAllAuthenticator{} }
func (allowAllEngine) Authorizer() Authorizer       { return allowAllAuthorizer{} }

type allowAllAuthenticator struct{}

func (allowAllAuthenticator) Authenticate(_ context.Context, _ Credentials) (Principal, error) {
	return Principal{ID: "anonymous"}, nil
}

type allowAllAuthorizer struct{}

func (allowAllAuthorizer) Authorize(_ context.Context, _ Principal, _ Action) error { return nil }
