// Package replication keeps a node's namespaces in sync with its configured peers: one loop per
// peer, triggered by local commits, a periodic tick, and on demand, each running a peer-sync v2
// session (see go/kdb/peersync) and recording how far it got.
//
// The replicator holds nothing correctness depends on. Its state - per peer, the refs it last
// saw and the tips of an interrupted transfer - saves work, and losing it costs one full
// negotiation. Everything that decides what the data is lives in peersync.Ingest.
package replication

import (
	"fmt"
	"github.com/limidus/kdb/go/kdb/auth"
	"os"
	"strings"
	"time"

	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/sql"
)

// PeerConfig is one configured peer.
type PeerConfig struct {
	// Name identifies the peer in state, metrics and the control plane. Not its node id: a peer
	// is configured before anything is known about it.
	Name string
	// Addr is the peer's peer-sync listener: tcp://host:4242 or tcps://host:4242, or an HTTP
	// endpoint serving syncnode.Node.Handler - ws://host/kdb/sync or wss://host/kdb/sync.
	Addr string
	// Namespaces are the namespaces or patterns to sync with this peer ("!" excludes).
	Namespaces []string
	// Mode is which directions run: pull, push or both.
	Mode peersync.SyncMode
	// Interval is the anti-entropy tick: a full sync at least this often, whatever triggered or
	// did not trigger one in between. The correctness floor under commit-triggered syncs.
	Interval time.Duration
	// User/Password authenticate to the peer.
	User, Password *string
	// Token authenticates to the peer as a bearer token (token-env=VAR). Over ws:// and wss:// it
	// also rides the upgrade request as "Authorization: Bearer <token>", so an application
	// mounting the peer handler behind its own HTTP auth sees it there.
	Token *string
	// Credentials, when set, supplies the connection's credentials afresh for every connection -
	// a token that expires and is refreshed, say. It takes precedence over User, Password and
	// Token. Programmatic only.
	Credentials func() (auth.ConnectionContext, error) `json:"-"`
	// CreateLocal lets a pull create a namespace this node does not hold yet.
	CreateLocal bool
	// PreferSnapshot bootstraps an empty local namespace from a snapshot of the peer's main
	// instead of fetching its whole history (bootstrap=snapshot).
	PreferSnapshot bool
	// Filter makes this peer a filtered projection subscription: this node keeps only the
	// documents of Namespaces[0] that match it (and that it may read), in a read-only namespace
	// of its own - see peersync.SyncProjection. Always the last field of the spec, since a
	// filter may contain commas.
	Filter string
	// WriteBack lets clients write to a filtered peer's projection: writes commit locally and go
	// to the source on each sync (writeback=true; filtered peers only).
	WriteBack bool
	// ReadThrough lets a filtered peer's projection answer a read for a document it does not hold
	// by fetching it from the source, proved against the source commit the projection is at
	// (readthrough=true; filtered peers only).
	ReadThrough bool
	// Deepen fetches the history below any shallow root a synced namespace has - one bootstrapped
	// by snapshot - from this peer after each sync, until the peer has no more (deepen=true). Such
	// a namespace can then sync with nodes that have history of their own.
	Deepen bool
}

// DefaultInterval is the anti-entropy tick when a peer names none.
const DefaultInterval = 30 * time.Second

// ParsePeer reads one peer from its flag form:
//
//	name=cloud,addr=tcps://cloud:4242,namespaces=site/*|shared/*,mode=both,interval=30s,user=u,password-env=VAR,create=true,bootstrap=snapshot
//	name=hq,addr=tcps://hq:4242,namespaces=orders,writeback=true,filter=region = 'EU'
//
// Namespaces are separated by '|' because ',' separates fields. mode is pull, push or both
// (default both). The password is taken from an environment variable, never from the flag, so it
// does not appear in process listings. Unknown keys are an error, like every other setting.
func ParsePeer(spec string) (PeerConfig, error) {
	p := PeerConfig{Mode: peersync.SyncBoth, Interval: DefaultInterval}
	if i := strings.Index(spec, "filter="); i >= 0 && (i == 0 || spec[i-1] == ',') {
		p.Filter = strings.TrimSpace(spec[i+len("filter="):])
		spec = strings.TrimSuffix(spec[:i], ",")
		if p.Filter == "" {
			return PeerConfig{}, fmt.Errorf("peer %q: filter is empty", spec)
		}
	}
	for _, field := range strings.Split(spec, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			return PeerConfig{}, fmt.Errorf("peer %q: field %q is not key=value", spec, field)
		}
		switch key {
		case "name":
			p.Name = value
		case "addr":
			p.Addr = value
		case "namespaces":
			for _, ns := range strings.Split(value, "|") {
				if ns = strings.TrimSpace(ns); ns != "" {
					p.Namespaces = append(p.Namespaces, ns)
				}
			}
		case "mode":
			switch value {
			case "pull":
				p.Mode = peersync.SyncPull
			case "push":
				p.Mode = peersync.SyncPush
			case "both", "bidirectional":
				p.Mode = peersync.SyncBoth
			default:
				return PeerConfig{}, fmt.Errorf("peer %q: mode %q is not pull, push or both", spec, value)
			}
		case "interval":
			d, err := time.ParseDuration(value)
			if err != nil || d <= 0 {
				return PeerConfig{}, fmt.Errorf("peer %q: interval %q is not a positive duration", spec, value)
			}
			p.Interval = d
		case "user":
			v := value
			p.User = &v
		case "password-env":
			v, ok := os.LookupEnv(value)
			if !ok {
				return PeerConfig{}, fmt.Errorf("peer %q: password-env names %s, which is not set", spec, value)
			}
			p.Password = &v
		case "token-env":
			v, ok := os.LookupEnv(value)
			if !ok {
				return PeerConfig{}, fmt.Errorf("peer %q: token-env names %s, which is not set", spec, value)
			}
			p.Token = &v
		case "create":
			p.CreateLocal = value == "true"
		case "writeback":
			p.WriteBack = value == "true"
		case "readthrough":
			p.ReadThrough = value == "true"
		case "deepen":
			p.Deepen = value == "true"
		case "bootstrap":
			switch value {
			case "snapshot":
				p.PreferSnapshot = true
			case "history":
				p.PreferSnapshot = false
			default:
				return PeerConfig{}, fmt.Errorf("peer %q: bootstrap %q is not snapshot or history", spec, value)
			}
		default:
			return PeerConfig{}, fmt.Errorf("peer %q: unknown key %q", spec, key)
		}
	}
	if p.Name == "" || p.Addr == "" {
		return PeerConfig{}, fmt.Errorf("peer %q: name and addr are required", spec)
	}
	if p.Filter != "" {
		if len(p.Namespaces) != 1 || !literalNamespace(p.Namespaces[0]) {
			return PeerConfig{}, fmt.Errorf("peer %q: a filtered peer projects exactly one namespace, named literally", spec)
		}
		if _, err := sql.ParseFilter(p.Filter); err != nil {
			return PeerConfig{}, fmt.Errorf("peer %q: filter: %w", spec, err)
		}
		p.Mode = peersync.SyncPull
	} else if p.WriteBack {
		return PeerConfig{}, fmt.Errorf("peer %q: writeback applies only to a filtered peer", spec)
	} else if p.ReadThrough {
		return PeerConfig{}, fmt.Errorf("peer %q: readthrough applies only to a filtered peer", spec)
	}
	if p.Filter != "" && p.Deepen {
		return PeerConfig{}, fmt.Errorf("peer %q: deepen does not apply to a filtered peer, which keeps no source history", spec)
	}
	if len(p.Namespaces) == 0 {
		p.Namespaces = []string{"**"}
	}
	if strings.ContainsAny(p.Name, "/\\") {
		return PeerConfig{}, fmt.Errorf("peer %q: name must not contain path separators", spec)
	}
	return p, nil
}

// ParsePeers reads ';'-separated peers - the KDB_PEERS environment form.
func ParsePeers(specs string) ([]PeerConfig, error) {
	var out []PeerConfig
	seen := map[string]bool{}
	for _, spec := range strings.Split(specs, ";") {
		if strings.TrimSpace(spec) == "" {
			continue
		}
		p, err := ParsePeer(spec)
		if err != nil {
			return nil, err
		}
		if seen[p.Name] {
			return nil, fmt.Errorf("peer name %q is configured twice", p.Name)
		}
		seen[p.Name] = true
		out = append(out, p)
	}
	return out, nil
}

func literalNamespace(ns string) bool {
	return ns != "" && !strings.ContainsAny(ns, "*!")
}
