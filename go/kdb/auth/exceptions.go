package auth

import kdberr "github.com/limidus/kdb/go/kdb/error"

// AuthenticationError indicates invalid credentials.
type AuthenticationError struct{ *kdberr.TransportErr }

func NewAuthenticationError(msg string, cause error) *AuthenticationError {
	return &AuthenticationError{TransportErr: kdberr.NewTransportErr(msg, cause)}
}

// AuthorizationError indicates insufficient permissions.
type AuthorizationError struct{ *kdberr.TransportErr }

func NewAuthorizationError(msg string, cause error) *AuthorizationError {
	return &AuthorizationError{TransportErr: kdberr.NewTransportErr(msg, cause)}
}
