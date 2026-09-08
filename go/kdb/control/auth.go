package control

import (
	"encoding/base64"
	"net/http"
	"strings"

	"github.com/limidus/kdb/go/kdb/auth"
)

// AdminScope is the resource an admin-scoped control request is authorized against, so a
// deployment grants "admin:control" (or "admin:*") to whoever may read settings and operational
// state. Namespace-scoped requests use the namespace itself and need only an ordinary read grant.
const AdminScope = "control"

// principalKey is the request-context key carrying the authenticated principal.
type principalKey struct{}

// nsRead wraps a handler that reads one namespace: authenticate, then authorize the request as a
// read of the namespace in the path.
//
// Reusing auth.SqlExecAction{ReadOnly: true} rather than inventing a control-specific action is
// deliberate. registryAuthorizer maps every action it does not recognise to kind "unknown" on an
// empty resource, which no grant can match - so a new action type would deny every RBAC
// deployment. Reading a namespace over HTTP is the same authorization question as reading it over
// the wire, and it should have the same answer.
func (s *Server) nsRead(h func(http.ResponseWriter, *http.Request, auth.Principal, string)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := s.authenticate(w, r)
		if !ok {
			return
		}
		ns := r.PathValue("ns")
		if ns == "" {
			// The namespace-less routes (the namespace list) still authorize against the one
			// namespace this runtime serves - there is nothing else it could disclose.
			ns = s.opts.Namespace
		}
		if !s.authorize(w, r, principal, auth.SqlExecAction{Namespace: ns, ReadOnly: true}) {
			return
		}
		h(w, r, principal, ns)
	})
}

// adminRead wraps a process-scoped handler: settings, health detail, operational state.
func (s *Server) adminRead(h func(http.ResponseWriter, *http.Request, auth.Principal)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := s.authenticate(w, r)
		if !ok {
			return
		}
		if !s.authorize(w, r, principal, auth.AdminAction{Scope: AdminScope}) {
			return
		}
		h(w, r, principal)
	})
}

func (s *Server) authenticate(w http.ResponseWriter, r *http.Request) (auth.Principal, bool) {
	engine := s.opts.Runtime.AuthEngine
	if engine == nil {
		// A runtime with no engine configured is auth.AllowAll's situation by another name. Fail
		// closed rather than guessing: a control plane that authenticates nobody is exactly the
		// thing the plan's §10 says must not be reachable.
		writeError(w, http.StatusInternalServerError, "no_auth_engine",
			"this runtime has no auth engine configured; the control plane refuses to serve "+
				"unauthenticated requests")
		return auth.Principal{}, false
	}
	creds, err := credentialsFrom(r)
	if err != nil {
		w.Header().Set("WWW-Authenticate", `Basic realm="kdb", Bearer`)
		writeError(w, http.StatusUnauthorized, "unauthenticated", err.Error())
		return auth.Principal{}, false
	}
	principal, err := engine.Authenticator().Authenticate(r.Context(), creds)
	if err != nil {
		w.Header().Set("WWW-Authenticate", `Basic realm="kdb", Bearer`)
		writeError(w, http.StatusUnauthorized, "unauthenticated", err.Error())
		return auth.Principal{}, false
	}
	return principal, true
}

func (s *Server) authorize(w http.ResponseWriter, r *http.Request, principal auth.Principal, action auth.Action) bool {
	if err := s.opts.Runtime.AuthEngine.Authorizer().Authorize(r.Context(), principal, action); err != nil {
		writeError(w, http.StatusForbidden, "forbidden", err.Error())
		return false
	}
	return true
}

// credentialsFrom reads an Authorization header into auth.Credentials.
//
// Both schemes are accepted because both are already how KDB is spoken to: the wire handshake
// takes a token that the static engine parses as "user:pass", and a browser hitting this API
// directly will send Basic. They converge on the same Credentials either way.
func credentialsFrom(r *http.Request) (auth.Credentials, error) {
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if header == "" {
		return auth.Credentials{}, errMissingCredentials
	}
	scheme, rest, found := strings.Cut(header, " ")
	if !found {
		return auth.Credentials{}, errMalformedAuthorization
	}
	rest = strings.TrimSpace(rest)
	switch strings.ToLower(scheme) {
	case "bearer":
		if rest == "" {
			return auth.Credentials{}, errMalformedAuthorization
		}
		// The static and registry engines both read a bearer token as "user:pass" today (see
		// kdb-rbac-plan.md). Splitting here as well means a token that is already in that shape
		// arrives as real user/password fields, and one that is not still arrives as a token for
		// an engine that understands opaque tokens.
		if user, pass, ok := strings.Cut(rest, ":"); ok {
			return auth.Credentials{User: &user, Password: &pass, Token: &rest}, nil
		}
		return auth.Credentials{Token: &rest}, nil
	case "basic":
		raw, err := base64.StdEncoding.DecodeString(rest)
		if err != nil {
			return auth.Credentials{}, errMalformedAuthorization
		}
		user, pass, ok := strings.Cut(string(raw), ":")
		if !ok {
			return auth.Credentials{}, errMalformedAuthorization
		}
		token := user + ":" + pass
		return auth.Credentials{User: &user, Password: &pass, Token: &token}, nil
	default:
		return auth.Credentials{}, errUnsupportedScheme
	}
}

// nsWrite wraps a handler that changes one namespace: authenticate, check this deployment allows
// writes at all, then authorize as a write of that namespace.
//
// The order matters. A deployment-level refusal is not an authorization failure, and telling a
// properly-authorized operator "forbidden" when the real answer is "this server was started
// read-only" sends them to look at grants that are perfectly fine.
func (s *Server) nsWrite(h func(http.ResponseWriter, *http.Request, auth.Principal, string)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := s.authenticate(w, r)
		if !ok {
			return
		}
		if !s.opts.AllowWrites {
			writeError(w, http.StatusForbidden, "read_only",
				"this control plane is read-only; start the service with --control-write to enable "+
					"mutating endpoints")
			return
		}
		ns := r.PathValue("ns")
		if ns == "" {
			ns = s.opts.Namespace
		}
		if !s.authorize(w, r, principal, auth.SqlExecAction{Namespace: ns, ReadOnly: false}) {
			return
		}
		h(w, r, principal, ns)
	})
}
