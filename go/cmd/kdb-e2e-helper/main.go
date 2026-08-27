// kdb-e2e-helper is a thin network client for the Python e2e harness
// (kdb-integration/e2e): put/get/upsert/exec/query over the SQL wire, and a full-peer "relay"
// that carries commits between servers over the peer-sync wire. It exists because the kdb CLI
// operates on local data directories only - the harness needs something that speaks the actual
// network protocols the way a real client would. Test tooling, not product surface: no
// stability promises.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	if err := run(os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `kdb-e2e-helper - network test client for the e2e harness

  put     --addr URI --namespace NS --doc-id ID --json JSON [--token U:P] [tls flags]
  get     --addr URI --namespace NS --doc-id ID
  upsert  --addr URI --namespace NS --doc-id ID --json JSON
  exec    --addr URI --namespace NS --sql SQL
  query   --addr URI --namespace NS --sql SQL
  relay   --namespace NS --servers URI,URI[,URI...] [--rounds N] [--token U:P] [tls flags]
  load    --addr URI --namespace NS --rounds N
  tx-drop --addr URI --namespace NS --json JSON

TLS flags: --tls-ca FILE [--tls-cert FILE --tls-key FILE]`)
}

type commonFlags struct {
	addr, namespace, token string
	tlsCA, tlsCert, tlsKey string
	timeout                time.Duration
}

func (c *commonFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&c.addr, "addr", "", "server URI (tcp://host:port, tcps://..., ws://...)")
	fs.StringVar(&c.namespace, "namespace", "", "namespace")
	fs.StringVar(&c.token, "token", "", "credentials as user:password")
	fs.StringVar(&c.tlsCA, "tls-ca", "", "PEM CA to verify the server")
	fs.StringVar(&c.tlsCert, "tls-cert", "", "PEM client certificate (mTLS)")
	fs.StringVar(&c.tlsKey, "tls-key", "", "PEM client key (mTLS)")
	fs.DurationVar(&c.timeout, "timeout", 30*time.Second, "operation timeout")
}

func (c *commonFlags) tls() *core.TransportTlsSettings {
	if c.tlsCA == "" && c.tlsCert == "" {
		return nil
	}
	return &core.TransportTlsSettings{
		Enabled:  true,
		CAFile:   c.tlsCA,
		CertFile: c.tlsCert,
		KeyFile:  c.tlsKey,
	}
}

func (c *commonFlags) dial(ctx context.Context) (*client.Client, error) {
	var namespaces []string
	if c.namespace != "" {
		namespaces = []string{c.namespace}
	}
	return client.ConnectWithOptions(ctx, c.addr, c.token, client.ConnectOptions{
		TLS:        c.tls(),
		Namespaces: namespaces,
	})
}

func run(cmd string, args []string) error {
	fs := flag.NewFlagSet("kdb-e2e-helper "+cmd, flag.ExitOnError)
	var cf commonFlags
	cf.register(fs)
	var docID, jsonBody, sqlText, servers string
	var rounds int
	fs.StringVar(&docID, "doc-id", "", "document id (32 hex chars)")
	fs.StringVar(&jsonBody, "json", "", "document JSON body")
	fs.StringVar(&sqlText, "sql", "", "SQL text")
	fs.StringVar(&servers, "servers", "", "comma-separated server URIs for relay")
	fs.IntVar(&rounds, "rounds", 1, "relay rounds over the server list")
	_ = fs.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), cf.timeout)
	defer cancel()

	switch cmd {
	case "put", "upsert", "get", "exec", "query":
		if cf.addr == "" || cf.namespace == "" {
			return fmt.Errorf("--addr and --namespace are required")
		}
		cl, err := cf.dial(ctx)
		if err != nil {
			return err
		}
		defer cl.Close()
		switch cmd {
		case "put":
			hash, err := cl.PutJSON(ctx, cf.namespace, docID, []byte(jsonBody))
			if err != nil {
				return err
			}
			fmt.Println(hash)
		case "upsert":
			hash, err := cl.Upsert(ctx, cf.namespace, docID, []byte(jsonBody))
			if err != nil {
				return err
			}
			fmt.Println(hash)
		case "get":
			body, commit, err := cl.GetJSON(ctx, cf.namespace, docID)
			if err != nil {
				return err
			}
			fmt.Printf("%s\n%s\n", commit, body)
		case "exec":
			if err := cl.Exec(ctx, cf.namespace, sqlText, nil); err != nil {
				return err
			}
			fmt.Println("ok")
		case "query":
			columns, rows, err := cl.QueryRaw(ctx, cf.namespace, sqlText, nil)
			if err != nil {
				return err
			}
			enc := json.NewEncoder(os.Stdout)
			return enc.Encode(map[string]any{"columns": columns, "rows": rows})
		}
		return nil
	case "relay":
		return relay(cf, servers, rounds)
	case "load":
		return load(ctx, cf, rounds)
	case "tx-drop":
		return txDrop(cf, docID, jsonBody)
	default:
		usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// load upserts --rounds documents sequentially over one connection - sustained-write pressure
// for the compaction / large-history e2e scenarios. Prints the final commit hash.
func load(ctx context.Context, cf commonFlags, count int) error {
	if cf.addr == "" || cf.namespace == "" {
		return fmt.Errorf("--addr and --namespace are required")
	}
	cl, err := cf.dial(ctx)
	if err != nil {
		return err
	}
	defer cl.Close()
	var last string
	for i := 0; i < count; i++ {
		docID := fmt.Sprintf("%032x", i%128+1)
		body := fmt.Sprintf(`{"i":%d,"padding":"%s"}`, i, strings.Repeat("x", 512))
		last, err = cl.Upsert(ctx, cf.namespace, docID, []byte(body))
		if err != nil {
			return fmt.Errorf("upsert %d: %w", i, err)
		}
	}
	fmt.Printf("loaded=%d last=%s\n", count, last)
	return nil
}

// txDrop opens a session, buffers one INSERT on it via SqlExec (which the Go server does not
// persist until TxCommit), and then drops the TCP connection without committing - the
// disconnect-mid-transaction scenario. The write must never become visible, and the server must
// stay healthy for later clients.
func txDrop(cf commonFlags, docID, jsonBody string) error {
	if cf.addr == "" || cf.namespace == "" {
		return fmt.Errorf("--addr and --namespace are required")
	}
	uri := cf.addr
	if !strings.Contains(uri, "://") {
		uri = "tcp://" + uri
	}
	opts := core.DefaultConnectOptions()
	opts.TLS = cf.tls()
	transport := tcp.NewTransport(opts)
	conn, err := transport.Connect(uri)
	if err != nil {
		return err
	}
	codec := wire.NewCodec(wire.EncodingJSON)
	correlation := 0
	request := func(msg wire.Message) (wire.Message, error) {
		frame, err := codec.Encode(msg)
		if err != nil {
			return nil, err
		}
		if err := conn.Send(frame); err != nil {
			return nil, err
		}
		select {
		case reply := <-conn.Incoming():
			return codec.Decode(reply)
		case <-time.After(cf.timeout):
			return nil, fmt.Errorf("timeout waiting for reply")
		}
	}
	nextHeader := func(mt wire.MessageType) wire.Header {
		correlation++
		return wire.Header{MessageType: mt, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: correlation}
	}

	hsReq := wire.HandshakePayload{NodeID: "kdb-e2e-txdrop", ClientMode: wire.ClientSQL, Namespaces: []string{cf.namespace}}
	if cf.token != "" {
		user, pass, ok := strings.Cut(cf.token, ":")
		if ok {
			hsReq.User, hsReq.Password = &user, &pass
		}
	}
	hsReply, err := request(wire.HandshakeMessage{H: nextHeader(wire.MsgHandshake), Request: hsReq})
	if err != nil {
		return fmt.Errorf("handshake: %w", err)
	}
	if ack, ok := hsReply.(wire.HandshakeAckMessage); !ok || !ack.Response.Accepted {
		return fmt.Errorf("handshake rejected: %+v", hsReply)
	}
	sbReply, err := request(wire.SessionBeginMessage{
		H: nextHeader(wire.MsgSessionBegin), Namespace: cf.namespace, ReadConsistency: "READ_COMMITTED",
	})
	if err != nil {
		return fmt.Errorf("session begin: %w", err)
	}
	sb, ok := sbReply.(wire.SessionBeginAckMessage)
	if !ok || sb.SessionID == "" {
		return fmt.Errorf("session begin rejected: %+v", sbReply)
	}
	insert := fmt.Sprintf(`INSERT INTO t (_doc) VALUES ('%s')`, jsonBody)
	sqlReply, err := request(wire.SqlExecMessage{
		H: nextHeader(wire.MsgSqlExec), Namespace: cf.namespace, SessionID: sb.SessionID, SQL: insert,
	})
	if err != nil {
		return fmt.Errorf("sql exec: %w", err)
	}
	if r, ok := sqlReply.(wire.SqlResultMessage); ok && r.Error != nil {
		return fmt.Errorf("insert buffered with error: %s", *r.Error)
	}
	// The whole point: no TxCommit, no orderly goodbye - just a dead socket.
	_ = conn.Close()
	fmt.Println("dropped-mid-transaction")
	return nil
}

// relay acts as one full peer that connects to each listed server in order, running a
// bidirectional sync with each - after enough rounds, every server converges on the union of
// all servers' histories (transitive propagation through this peer's local in-memory replica).
func relay(cf commonFlags, servers string, rounds int) error {
	if cf.namespace == "" || servers == "" {
		return fmt.Errorf("--namespace and --servers are required")
	}
	uris := strings.Split(servers, ",")

	rt, err := embed.OpenMemoryRuntime(embed.CatalogFromNamespace(cf.namespace), cf.namespace, schema.None())
	if err != nil {
		return err
	}
	defer rt.Close()
	localDag, ok := rt.DAG.(*dag.InMemoryCommitDag)
	if !ok {
		return fmt.Errorf("memory runtime DAG is %T, want *dag.InMemoryCommitDag", rt.DAG)
	}

	connCtx := auth.ConnectionContext{}
	if cf.token != "" {
		user, pass, ok := strings.Cut(cf.token, ":")
		if !ok {
			return fmt.Errorf("--token must be user:password")
		}
		connCtx = auth.ConnectionContext{User: &user, Password: &pass}
	}

	for round := 1; round <= rounds; round++ {
		for _, uri := range uris {
			uri = strings.TrimSpace(uri)
			w := wire.NewCodec(wire.EncodingJSON)
			var transport stream.Transport = tcp.NewTransport(core.DefaultConnectOptions())
			pc := peersync.NewClient(w, transport, localDag, rt.Storage)
			session, err := pc.Connect(peersync.ClientConfig{
				NamespaceID:       cf.namespace,
				NodeID:            "kdb-e2e-relay",
				PeerURI:           uri,
				ConnectionContext: connCtx,
				TLS:               cf.tls(),
				MaterializeCommit: func(commit document.Commit) error {
					return embed.MaterializeCommit(rt.Storage, localDag, cf.namespace, commit)
				},
			})
			if err != nil {
				return fmt.Errorf("round %d: connect %s: %w", round, uri, err)
			}
			result, err := session.SyncBidirectional()
			if err != nil {
				pc.Disconnect()
				return fmt.Errorf("round %d: sync %s: %w", round, uri, err)
			}
			pc.Disconnect()
			fmt.Printf("round=%d server=%s applied=%d pushed=%d\n", round, uri, result.AppliedCommits, result.PushedCommits)
		}
	}
	return nil
}
