package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
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

// ResolveCmd settles a queued conflict by taking one side for every conflicting document.
type ResolveCmd struct {
	Namespace string
	ID        string
	Take      string
}

// ResolveAllCmd settles every queued conflict a filter matches by taking one side.
type ResolveAllCmd struct {
	Namespace string
	Take      string
	Filter    server.ConflictFilter
	DryRun    bool
}

// ResolutionCmd shows a namespace's conflict resolution chain.
type ResolutionCmd struct{ Namespace string }

// ScrubCmd verifies every live document and repairs damaged ones from a peer, when one is given.
type ScrubCmd struct {
	Namespace   string
	Peer        string
	User        string
	PasswordEnv string
}

// DeepenCmd fetches the history below a snapshot's shallow roots from a peer.
type DeepenCmd struct {
	Namespace   string
	Peer        string
	User        string
	PasswordEnv string
}

// PeerDiffCmd lists the documents this node holds differently from a peer, by Merkle comparison.
type PeerDiffCmd struct {
	Namespace   string
	Peer        string
	User        string
	PasswordEnv string
}

func (NodeStatusCmd) command() {}
func (SyncCmd) command()       {}
func (ConflictsCmd) command()  {}
func (ResolveCmd) command()    {}
func (ResolveAllCmd) command() {}
func (ResolutionCmd) command() {}
func (ScrubCmd) command()      {}
func (PeerDiffCmd) command()   {}
func (DeepenCmd) command()     {}

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
		if len(rest) >= 3 && rest[2] == "--all" {
			return parseResolveAll(rest)
		}
		if len(rest) != 5 || rest[3] != "--take" || (rest[4] != "local" && rest[4] != "remote") {
			return nil, true, fmt.Errorf("usage: kdb resolve <namespace> <conflict-id> --take local|remote\n" +
				"       kdb resolve <namespace> --all --take local|remote [--kind K] [--peer NODE] [--origin NODE] [--dry-run]")
		}
		return ResolveCmd{Namespace: rest[1], ID: rest[2], Take: rest[4]}, true, nil
	case "scrub":
		if len(rest) < 2 {
			return nil, true, fmt.Errorf("usage: kdb scrub <namespace> [--peer ADDR] [--user U --password-env VAR]")
		}
		c := ScrubCmd{Namespace: rest[1]}
		if err := peerFlags(rest[2:], &c.Peer, &c.User, &c.PasswordEnv, true); err != nil {
			return nil, true, err
		}
		return c, true, nil
	case "deepen":
		if len(rest) < 3 {
			return nil, true, fmt.Errorf("usage: kdb deepen <namespace> <peer-addr> [--user U --password-env VAR]")
		}
		c := DeepenCmd{Namespace: rest[1], Peer: rest[2]}
		if err := peerFlags(rest[3:], nil, &c.User, &c.PasswordEnv, false); err != nil {
			return nil, true, err
		}
		return c, true, nil
	case "peer-diff":
		if len(rest) < 3 {
			return nil, true, fmt.Errorf("usage: kdb peer-diff <namespace> <peer-addr> [--user U --password-env VAR]")
		}
		c := PeerDiffCmd{Namespace: rest[1], Peer: rest[2]}
		if err := peerFlags(rest[3:], nil, &c.User, &c.PasswordEnv, false); err != nil {
			return nil, true, err
		}
		return c, true, nil
	case "resolution":
		if len(rest) != 2 {
			return nil, true, fmt.Errorf("usage: kdb resolution <namespace>")
		}
		return ResolutionCmd{Namespace: rest[1]}, true, nil
	}
	return nil, false, nil
}

func parseResolveAll(rest []string) (Command, bool, error) {
	c := ResolveAllCmd{Namespace: rest[1]}
	for i := 3; i < len(rest); i++ {
		switch rest[i] {
		case "--dry-run":
			c.DryRun = true
		case "--take", "--kind", "--peer", "--origin":
			flag := rest[i]
			i++
			if i >= len(rest) {
				return nil, true, fmt.Errorf("%s requires a value", flag)
			}
			switch flag {
			case "--take":
				c.Take = rest[i]
			case "--kind":
				c.Filter.Kind = peersync.ConflictKind(rest[i])
			case "--peer":
				c.Filter.Peer = rest[i]
			case "--origin":
				c.Filter.Origin = rest[i]
			}
		default:
			return nil, true, fmt.Errorf("unknown option for resolve --all: %s", rest[i])
		}
	}
	if c.Take != "local" && c.Take != "remote" {
		return nil, true, fmt.Errorf("resolve --all needs --take local|remote")
	}
	return c, true, nil
}

// chainFor reads ns's resolution chain from the data root's metadata namespace, if it has one,
// so a CLI sync or resolution decides conflicts as the service would. It opens and closes the
// metadata namespace before the command opens its own - one namespace holds the directory lock.
func chainFor(cfg Config, ns string) (*peersync.ResolutionChain, error) {
	if _, err := os.Stat(filepath.Join(cfg.DataDir, "ns", filepath.FromSlash(server.MetaNamespace))); err != nil {
		return nil, nil // no metadata namespace: no chain
	}
	meta, err := embed.OpenFileRuntime(cfg.DataDir, embed.CatalogFromNamespace(server.MetaNamespace), server.MetaNamespace, schema.None())
	if err != nil {
		return nil, err
	}
	defer meta.Close()
	return server.ReadResolutionChain(meta, ns)
}

func cmdResolution(cfg Config, c ResolutionCmd) int {
	chain, err := chainFor(cfg, c.Namespace)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	if chain == nil {
		fmt.Printf("%s: no resolution chain (conflicts follow the peer conflict policy)\n", c.Namespace)
		return 0
	}
	fmt.Printf("%s: chain %s\n", c.Namespace, chain.Hash())
	for i, r := range chain.Rules {
		line := fmt.Sprintf("  %d. %s", i+1, r.Kind)
		if len(r.Nodes) > 0 {
			line += " nodes=" + strings.Join(r.Nodes, ",")
		}
		if r.Kind == peersync.RuleAuthority {
			pending := r.Pending
			if pending == "" {
				pending = peersync.PendingHold
			}
			line += " pending=" + pending
			if r.Node != "" {
				line += " node=" + r.Node
			}
			if r.Timeout != "" {
				line += " timeout=" + r.Timeout
			}
		}
		fmt.Println(line)
	}
	return 0
}

func cmdResolveAll(cfg Config, rt *embed.EmbeddedKdbRuntime, chain *peersync.ResolutionChain, c ResolveAllCmd) int {
	srv, err := serverFor(cfg, rt, chain)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	res, err := srv.ResolveAll(c.Filter, c.Take, c.DryRun, auth.Principal{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	code := 0
	for _, r := range res {
		switch {
		case r.Error != "":
			fmt.Printf("%s\t%s\tfailed: %s\n", r.ID, r.Kind, r.Error)
			code = 1
		case c.DryRun:
			fmt.Printf("%s\t%s\twould take %s for %s\n", r.ID, r.Kind, c.Take, strings.Join(r.Documents, ","))
		default:
			fmt.Printf("%s\t%s\tresolved: %s\n", r.ID, r.Kind, r.CommitHex)
		}
	}
	if len(res) == 0 {
		fmt.Println("no matching conflicts")
	}
	return code
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
func serverFor(cfg Config, rt *embed.EmbeddedKdbRuntime, chain *peersync.ResolutionChain) (*server.KdbServerRuntime, error) {
	id, err := embed.LoadOrCreateNodeID(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	srv := server.NewKdbServerRuntime(rt)
	srv.NodeID = id
	srv.SetResolutionChain(chain)
	return srv, nil
}

func cmdSync(cfg Config, rt *embed.EmbeddedKdbRuntime, chain *peersync.ResolutionChain, c SyncCmd) int {
	srv, err := serverFor(cfg, rt, chain)
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
		fmt.Printf("%s\t%s\t%s\tpeer=%s\tseen=%d", e.ID, e.Kind, e.Ref, e.Peer, e.Seen)
		if e.Authority {
			fmt.Printf("\tauthority delivered=%t", e.Delivered)
			if e.AuthorityNode != "" {
				fmt.Printf(" node=%s", e.AuthorityNode)
			}
		}
		fmt.Println()
		origins := map[string]peersync.ConflictDetail{}
		for _, d := range e.Details {
			origins[d.DocumentID] = d
		}
		for _, item := range e.Items {
			fmt.Printf("\t%s\t%s", item.DocumentID, item.OperationType)
			if d, ok := origins[item.DocumentID]; ok {
				fmt.Printf("\tlocal-by=%s incoming-by=%s", d.LocalOrigin.NodeID, d.IncomingOrigin.NodeID)
			}
			fmt.Println()
		}
		if e.Detail != "" {
			fmt.Printf("\t%s\n", e.Detail)
		}
	}
	return 0
}

func cmdResolve(cfg Config, rt *embed.EmbeddedKdbRuntime, chain *peersync.ResolutionChain, c ResolveCmd) int {
	srv, err := serverFor(cfg, rt, chain)
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

func peerFlags(args []string, peer, user, passwordEnv *string, allowPeer bool) error {
	for i := 0; i < len(args); i++ {
		flag := args[i]
		switch flag {
		case "--peer", "--user", "--password-env":
			if flag == "--peer" && !allowPeer {
				return fmt.Errorf("unknown option: %s", flag)
			}
			i++
			if i >= len(args) {
				return fmt.Errorf("%s requires a value", flag)
			}
			switch flag {
			case "--peer":
				*peer = args[i]
			case "--user":
				*user = args[i]
			default:
				*passwordEnv = args[i]
			}
		default:
			return fmt.Errorf("unknown option: %s", flag)
		}
	}
	return nil
}

func repairSession(srv *server.KdbServerRuntime, ns, peer, user, passwordEnv string) (*peersync.RepairSession, error) {
	var cc auth.ConnectionContext
	if user != "" {
		u := user
		cc.User = &u
		if passwordEnv != "" {
			pw := os.Getenv(passwordEnv)
			cc.Password = &pw
		}
	}
	var tls *core.TransportTlsSettings
	if strings.HasPrefix(peer, "tcps://") {
		tls = &core.TransportTlsSettings{Enabled: true}
	}
	opts := core.DefaultConnectOptions()
	opts.TLS = tls
	return peersync.OpenRepairSession(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(opts), peersync.V2ClientConfig{
		NodeID: srv.NodeID.String(), PeerURI: peer, ConnectionContext: cc, TLS: tls,
		Namespaces: []string{ns}, Timeout: time.Minute,
	})
}

func cmdScrub(cfg Config, rt *embed.EmbeddedKdbRuntime, c ScrubCmd) int {
	srv, err := serverFor(cfg, rt, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	var fetch server.BodyFetcher
	if c.Peer != "" {
		fetch = func(ns string, wanted map[codec.UUID]codec.Hash, treeHex string) (map[codec.UUID]string, error) {
			s, err := repairSession(srv, ns, c.Peer, c.User, c.PasswordEnv)
			if err != nil {
				return nil, err
			}
			defer s.Close()
			return s.Fetch(ns, wanted, treeHex)
		}
	}
	rep, err := srv.Scrub(fetch)
	fmt.Printf("%s: checked %d, damaged %d, repaired %d\n", c.Namespace, rep.Checked, len(rep.Damaged), len(rep.Repaired))
	if rep.RepairCommit != "" {
		fmt.Printf("  repair commit %s\n", rep.RepairCommit)
	}
	for _, id := range rep.Unrepaired {
		fmt.Printf("  unrepaired %s\n", id)
	}
	if rep.FetchError != "" {
		fmt.Printf("  fetch: %s\n", rep.FetchError)
	}
	switch {
	case server.IsScrubUnrepaired(err):
		return 3
	case err != nil:
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	return 0
}

func cmdPeerDiff(cfg Config, rt *embed.EmbeddedKdbRuntime, c PeerDiffCmd) int {
	srv, err := serverFor(cfg, rt, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	local, err := srv.HeadTree()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	s, err := repairSession(srv, c.Namespace, c.Peer, c.User, c.PasswordEnv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	defer s.Close()
	peerTree, diff, err := s.Diff(c.Namespace, local, "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	fmt.Printf("%s: local tree %s, peer tree %s, %d difference(s)\n", c.Namespace, local.TreeHash.Hex(), peerTree, len(diff))
	for _, d := range diff {
		side := "both"
		switch {
		case d.Local == (codec.Hash{}):
			side = "peer only"
		case d.Remote == (codec.Hash{}):
			side = "local only"
		}
		fmt.Printf("  %s\t%s\n", d.DocID, side)
	}
	if len(diff) > 0 {
		return 3
	}
	return 0
}

func cmdDeepen(cfg Config, rt *embed.EmbeddedKdbRuntime, c DeepenCmd) int {
	srv, err := serverFor(cfg, rt, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	results, err := srv.Deepen(func() (*peersync.RepairSession, error) {
		return repairSession(srv, c.Namespace, c.Peer, c.User, c.PasswordEnv)
	})
	for _, r := range results {
		state := "still shallow"
		if r.Unshallowed {
			state = "history complete"
		}
		fmt.Printf("root %s: fetched %d commit(s), %s\n", r.Root.Hex(), r.Fetched, state)
	}
	left := srv.ShallowRoots()
	fmt.Printf("%s: %d shallow root(s) left\n", c.Namespace, len(left))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	if len(left) > 0 {
		return 3
	}
	return 0
}
