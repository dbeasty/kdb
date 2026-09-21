package cli

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// NodeStatusCmd prints this data root's node identity.
type NodeStatusCmd struct{}

// SyncCmd syncs one namespace with a peer's peer-sync listener, once.
type SyncCmd struct {
	Namespace   string
	Peer        string
	Mode        peersync.SyncMode
	User        string
	PasswordEnv string
}

// ConflictsCmd lists a namespace's open replication conflicts.
type ConflictsCmd struct{ Namespace string }

// ResolveCmd settles a queued divergence by taking one side for every conflicting document.
type ResolveCmd struct {
	Namespace string
	ID        string
	Take      string
}

func (NodeStatusCmd) command() {}
func (SyncCmd) command()       {}
func (ConflictsCmd) command()  {}
func (ResolveCmd) command()    {}

func parseReplicationCommand(rest []string) (Command, bool, error) {
	switch rest[0] {
	case "node":
		if len(rest) != 2 || rest[1] != "status" {
			return nil, true, fmt.Errorf("usage: kdb node status")
		}
		return NodeStatusCmd{}, true, nil
	case "sync":
		if len(rest) < 3 {
			return nil, true, fmt.Errorf("usage: kdb sync <namespace> <peer-addr> [--pull|--push] [--user U --password-env VAR]")
		}
		c := SyncCmd{Namespace: rest[1], Peer: rest[2], Mode: peersync.SyncBoth}
		for i := 3; i < len(rest); i++ {
			switch rest[i] {
			case "--pull":
				c.Mode = peersync.SyncPull
			case "--push":
				c.Mode = peersync.SyncPush
			case "--user", "--password-env":
				flag := rest[i]
				i++
				if i >= len(rest) {
					return nil, true, fmt.Errorf("%s requires a value", flag)
				}
				if flag == "--user" {
					c.User = rest[i]
				} else {
					c.PasswordEnv = rest[i]
				}
			default:
				return nil, true, fmt.Errorf("unknown option for sync: %s", rest[i])
			}
		}
		return c, true, nil
	case "conflicts":
		if len(rest) != 2 {
			return nil, true, fmt.Errorf("usage: kdb conflicts <namespace>")
		}
		return ConflictsCmd{Namespace: rest[1]}, true, nil
	case "resolve":
		if len(rest) != 5 || rest[3] != "--take" || (rest[4] != "local" && rest[4] != "remote") {
			return nil, true, fmt.Errorf("usage: kdb resolve <namespace> <conflict-id> --take local|remote")
		}
		return ResolveCmd{Namespace: rest[1], ID: rest[2], Take: rest[4]}, true, nil
	}
	return nil, false, nil
}

func cmdNodeStatus(cfg Config) int {
	id, err := embed.LoadOrCreateNodeID(cfg.DataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	fmt.Printf("node %s\ndata-dir %s\n", id, cfg.DataDir)
	return 0
}

// serverFor wraps rt the way kdb-service does, so a CLI sync ingests through the same write
// serialization, log and conflict queue a running service would use.
func serverFor(cfg Config, rt *embed.EmbeddedKdbRuntime) (*server.KdbServerRuntime, error) {
	id, err := embed.LoadOrCreateNodeID(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	srv := server.NewKdbServerRuntime(rt)
	srv.NodeID = id
	return srv, nil
}

func cmdSync(cfg Config, rt *embed.EmbeddedKdbRuntime, c SyncCmd) int {
	srv, err := serverFor(cfg, rt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	var cc auth.ConnectionContext
	if c.User != "" {
		user := c.User
		cc.User = &user
		if c.PasswordEnv != "" {
			pw := os.Getenv(c.PasswordEnv)
			cc.Password = &pw
		}
	}
	var tls *core.TransportTlsSettings
	if strings.HasPrefix(c.Peer, "tcps://") {
		tls = &core.TransportTlsSettings{Enabled: true}
	}
	opts := core.DefaultConnectOptions()
	opts.TLS = tls
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(opts), peersync.V2ClientConfig{
		NodeID: srv.NodeID.String(), PeerURI: c.Peer, ConnectionContext: cc, TLS: tls,
		Namespaces: []string{c.Namespace}, Mode: c.Mode, Local: srv.PeerNamespaces(), Timeout: time.Minute,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	code := 0
	for _, ns := range res.Namespaces {
		fmt.Printf("%s: pulled %d, pushed %d\n", ns.Namespace, ns.Pulled, ns.Pushed)
		for ref, out := range ns.Local {
			fmt.Printf("  local  %-24s %s\n", ref, out)
		}
		for ref, out := range ns.Remote {
			fmt.Printf("  remote %-24s %s\n", ref, out)
		}
		if len(ns.Conflicts) > 0 {
			fmt.Printf("  %d conflict(s) - see: kdb conflicts %s\n", len(ns.Conflicts), ns.Namespace)
			code = 3
		}
		if ns.Err != nil {
			fmt.Fprintf(os.Stderr, "Error: %s: %v\n", ns.Namespace, ns.Err)
			code = 1
		}
	}
	if len(res.Namespaces) == 0 {
		fmt.Fprintf(os.Stderr, "Error: the peer did not grant %s\n", c.Namespace)
		return 1
	}
	return code
}

func cmdConflicts(cfg Config, c ConflictsCmd) int {
	q, err := peersync.NewConflictQueue(peersync.ConflictQueueDir(cfg.DataDir, c.Namespace))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	for _, e := range q.List() {
		fmt.Printf("%s\t%s\t%s\tpeer=%s\tseen=%d\n", e.ID, e.Kind, e.Ref, e.Peer, e.Seen)
		for _, item := range e.Items {
			fmt.Printf("\t%s\t%s\n", item.DocumentID, item.OperationType)
		}
		if e.Detail != "" {
			fmt.Printf("\t%s\n", e.Detail)
		}
	}
	return 0
}

func cmdResolve(cfg Config, rt *embed.EmbeddedKdbRuntime, c ResolveCmd) int {
	srv, err := serverFor(cfg, rt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	e, ok := srv.Conflicts.Get(c.ID)
	if !ok {
		fmt.Fprintf(os.Stderr, "Error: no conflict %s in %s\n", c.ID, c.Namespace)
		return 1
	}
	choices := map[codec.UUID]peersync.Choice{}
	for _, item := range e.Items {
		id, err := codec.ParseUUID(item.DocumentID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return 1
		}
		choices[id] = peersync.Choice{Take: c.Take}
	}
	commit, err := srv.ResolveConflict(c.ID, choices, auth.Principal{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	fmt.Printf("resolved %s: %s\n", c.ID, commit.Hash.Hex())
	return 0
}
