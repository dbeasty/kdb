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

// nsRead wraps a handler that reads one namespace: authenticate, then authorize the request as a
// read of the namespace in the path.
//
// Reusing auth.SqlExecAction{ReadOnly: true} rather than inventing a control-specific action is
// deliberate. registryAuthorizer maps every action it does not recognise to kind "unknown" on an
// empty resource, which no grant can match - so a new action type would deny every RBAC
// deployment. Reading a namespace over HTTP is the same authorization question as reading it over
// the wire, and it should have the same answer.
func (s *Server) nsRead(h nsHandler) http.Handler {
	return s.nsScoped(h, true)
}

// nsScoped is the shared body of nsRead and nsWrite: authenticate, resolve the namespace in the
// path to the runtime serving it, authorize against that namespace, and hand the handler both.
//
// Resolving before authorizing is deliberate. A namespace this server does not serve is a 404, and
// answering 403 for it would leak which namespaces exist to someone with no grant on any of them.
func (s *Server) nsScoped(h nsHandler, readOnly bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := s.authenticate(w, r)
		if !ok {
			return
		}
		if !readOnly && isAttached(r.PathValue("ns")) {
			// The runtime would refuse this anyway - a read-only open creates no WAL and no delta
			// writer - but "this is a staged copy, not the database" is the answer the operator
			// needs, not ErrReadOnly from somewhere deeper.
			writeError(w, http.StatusForbidden, "staged_copy",
				"this is a staged restore attached for inspection, not a live namespace. It is open "+
					"read-only and cannot be written to; promoting it needs the service stopped.")
			return
		}
		if !readOnly && !s.opts.AllowWrites {
			writeError(w, http.StatusForbidden, "read_only",
				"this control plane is read-only; start the service with --control-write to enable "+
					"mutating endpoints")
			return
		}
		ns := r.PathValue("ns")
		if ns == "" {
			// The namespace-less routes still authorize against something; the default is the one
			// a single-namespace deployment serves.
			ns = s.defaultNamespace()
		}
		rt, found := s.namespaces().Runtime(ns)
		if !found {
			writeError(w, http.StatusNotFound, "unknown_namespace",
				"this control plane does not serve a namespace called "+ns)
			return
		}
		if !s.authorizeWith(w, r, rt, principal, auth.SqlExecAction{Namespace: ns, ReadOnly: readOnly}) {
			return
		}
		h(w, r, principal, ns, rt)
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

// nsHandler is a handler for a request about one namespace, given the runtime serving it.
type nsHandler func(http.ResponseWriter, *http.Request, auth.Principal, string, *serverRuntime)

func (s *Server) authenticate(w http.ResponseWriter, r *http.Request) (auth.Principal, bool) {
	engine := s.authEngine()
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
	if err := s.authEngine().Authorizer().Authorize(r.Context(), principal, action); err != nil {
		writeError(w, http.StatusForbidden, "forbidden", err.Error())
		return false
	}
	return true
}

// authorizeWith authorizes against the runtime that will serve the request, so a per-namespace auth
// engine is honoured rather than the first one that happened to be registered.
func (s *Server) authorizeWith(w http.ResponseWriter, r *http.Request, rt *serverRuntime, principal auth.Principal, action auth.Action) bool {
	engine := rt.AuthEngine
	if engine == nil {
		engine = s.authEngine()
	}
	if err := engine.Authorizer().Authorize(r.Context(), principal, action); err != nil {
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

// nsWrite wraps a handler that changes one namespace.
//
// The order inside nsScoped matters for this one: a deployment-level refusal is checked before
// authorization, because telling a properly-authorized operator "forbidden" when the real answer is
// "this server was started read-only" sends them to look at grants that are perfectly fine.
func (s *Server) nsWrite(h nsHandler) http.Handler {
	return s.nsScoped(h, false)
}
