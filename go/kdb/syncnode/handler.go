package syncnode

import (
	"encoding/base64"
	"net/http"
	"strings"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/ws"
)

// Handler serves peer sync over WebSocket from the application's own HTTP server: mount it on a
// route (zolik: /kdb/sync) and peers dial ws://host/route or wss://host/route. The request's
// Authorization header - "Bearer <token>" or "Basic <user:password>" - becomes the connection's
// credentials, so the node's auth engine authenticates the peer from it when the peer's hello
// carries none of its own. Every request header is also passed along, for an engine that reads
// others. An application that authenticates the request itself (its middleware) can still leave
// the per-namespace decisions to the engine: it sees the same token.
//
// Connections served by the handler are closed by Close.
func (n *Node) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.mu.Lock()
		closed := n.closed
		n.mu.Unlock()
		if closed {
			http.Error(w, "sync node is closed", http.StatusServiceUnavailable)
			return
		}
		cc := ConnectionContextFrom(r)
		conn, err := ws.Upgrade(w, r, core.DefaultConnectOptions())
		if err != nil {
			return // Upgrade has answered the request
		}
		if !n.track(conn) {
			_ = conn.Close()
			return
		}
		defer n.untrack(conn)
		server.ServePeerSyncConnection(conn, n.primary, n.primary.Runtime.DefaultNamespace, cc)
	})
}

// ConnectionContextFrom is the auth context an HTTP request carries: its Authorization header as
// credentials, and its headers by canonical name.
func ConnectionContextFrom(r *http.Request) auth.ConnectionContext {
	cc := auth.ConnectionContext{Headers: map[string]string{}}
	for name, values := range r.Header {
		if len(values) > 0 {
			cc.Headers[name] = values[0]
		}
	}
	authz := r.Header.Get("Authorization")
	scheme, value, _ := strings.Cut(authz, " ")
	value = strings.TrimSpace(value)
	switch {
	case strings.EqualFold(scheme, "Bearer") && value != "":
		cc.Token = &value
	case strings.EqualFold(scheme, "Basic") && value != "":
		if raw, err := base64.StdEncoding.DecodeString(value); err == nil {
			if user, pass, ok := strings.Cut(string(raw), ":"); ok {
				cc.User, cc.Password = &user, &pass
			}
		}
	}
	return cc
}

// track records a connection served by Handler, refusing it once the node is closed.
func (n *Node) track(conn stream.ConnectionHandle) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return false
	}
	if n.conns == nil {
		n.conns = map[stream.ConnectionHandle]struct{}{}
	}
	n.conns[conn] = struct{}{}
	return true
}

func (n *Node) untrack(conn stream.ConnectionHandle) {
	n.mu.Lock()
	delete(n.conns, conn)
	n.mu.Unlock()
}
