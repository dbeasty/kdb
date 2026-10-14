// Command kdb-replicahost is the KDB node @kdb/replica's live interop tests sync against
// (docs/kdb-client-replica-plan.md §11). It is a real sync node - syncnode.Node, serving peer sync
// v2 over WebSocket exactly as a cloud mounting Node.Handler does - with bearer-token auth, and a
// line protocol on stdin so a test can write to it and read it back.
//
// Prints one line to stdout when ready:
//
//	ready ws://127.0.0.1:PORT/kdb/sync
//
// then reads one JSON command per stdin line and answers each with one JSON line on stdout:
//
//	{"op":"put","ns":"app/u/1","id":"<uuid>","json":"{...}"}       -> {"head":"<hex>"}
//	{"op":"commit","ns":"app/u/1","ops":[{"id":..,"json":..}|{"id":..,"delete":true}]} -> {"head":..}
//	{"op":"seed","ns":"app/u/1","commits":2000,"docs":10}          -> {"head":..}
//	{"op":"head","ns":"app/u/1"}                                   -> {"head":..,"tree":..}
//	{"op":"docs","ns":"app/u/1"}                                   -> {"docs":{"<id>":"<json>"}}
//	{"op":"denyRead","ns":..,"id":..} / {"op":"allowRead",..}       -> {}  (per-document read rights)
//	{"op":"setHome","ns":..} / {"op":"clearHome","ns":..}           -> {}  (another node is home)
//	{"op":"attach","ns":..,"id":..,"name":..,"mime":..,"text":..}  -> {"head","attachmentId","contentHash"}
//	{"op":"detach","ns":..,"id":..,"name":..}                       -> {"head"}
//
// Tokens: "rw-token" may pull and push every namespace; "ro-token" may only pull; anything else is
// refused. Namespaces under "secret/" are refused to both. The process exits when stdin closes, so a
// killed test runner never leaves it behind.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/syncnode"
)

// tokenEngine authenticates the test tokens and authorizes by them; unreadable lists the documents
// ("ns/id") no token may read, for per-document read rights.
type tokenEngine struct {
	mu         *sync.Mutex
	unreadable map[string]bool
}

func (e tokenEngine) Authenticator() auth.Authenticator { return e }
func (e tokenEngine) Authorizer() auth.Authorizer       { return e }

func (tokenEngine) Authenticate(_ context.Context, c auth.Credentials) (auth.Principal, error) {
	if c.Token != nil {
		switch *c.Token {
		case "rw-token":
			return auth.Principal{ID: "rw"}, nil
		case "ro-token":
			return auth.Principal{ID: "ro"}, nil
		}
	}
	return auth.Principal{}, errors.New("unauthenticated: bad or missing token")
}

func namespaceOf(a auth.Action) string {
	switch x := a.(type) {
	case auth.PeerSyncAction:
		return x.Namespace
	case auth.PeerPullAction:
		return x.Namespace
	case auth.PeerPushAction:
		return x.Namespace
	case auth.StreamSubscribeAction:
		return x.Namespace
	case auth.DocumentReadAction:
		return x.Namespace
	case auth.DocumentWriteAction:
		return x.Namespace
	case auth.DocumentDeleteAction:
		return x.Namespace
	case auth.TxCommitAction:
		return x.Namespace
	}
	return ""
}

func (e tokenEngine) Authorize(_ context.Context, p auth.Principal, a auth.Action) error {
	if strings.HasPrefix(namespaceOf(a), "secret/") {
		return errors.New("forbidden: secret namespace")
	}
	if r, ok := a.(auth.DocumentReadAction); ok && p.ID != "host" {
		e.mu.Lock()
		denied := e.unreadable[r.Namespace+"/"+r.DocID]
		e.mu.Unlock()
		if denied {
			return errors.New("forbidden: document not readable")
		}
	}
	if p.ID == "ro" {
		switch a.(type) {
		case auth.PeerSyncAction, auth.PeerPushAction, auth.DocumentWriteAction, auth.DocumentDeleteAction, auth.TxCommitAction:
			return errors.New("forbidden: read-only token")
		}
	}
	return nil
}

type opDto struct {
	ID     string `json:"id"`
	JSON   string `json:"json"`
	Delete bool   `json:"delete"`
}

type command struct {
	Op      string  `json:"op"`
	NS      string  `json:"ns"`
	ID      string  `json:"id"`
	JSON    string  `json:"json"`
	Ops     []opDto `json:"ops"`
	Commits int     `json:"commits"`
	Docs    int     `json:"docs"`
	Name    string  `json:"name"`
	Mime    string  `json:"mime"`
	Text    string  `json:"text"`
}

type host struct {
	set    *server.NamespaceSet
	engine tokenEngine
}

func (h *host) runtime(ns string) (*server.KdbServerRuntime, error) {
	return h.set.Resolve(ns, true)
}

func (h *host) commit(ns string, ops []document.Op) (codec.Hash, error) {
	rt, err := h.runtime(ns)
	if err != nil {
		return codec.Hash{}, err
	}
	head, err := rt.Runtime.DAG.Head()
	if err != nil {
		return codec.Hash{}, err
	}
	tx := document.Transaction{ID: mustRandomUUID(), BaseVersion: head, Operations: ops, Timestamp: codec.TimestampNow()}
	c, err := rt.Commit(ns, tx, "", auth.Principal{ID: "host"})
	return c.Hash, err
}

func mustRandomUUID() codec.UUID {
	u, err := codec.RandomUUID()
	if err != nil {
		panic(err)
	}
	return u
}

func (h *host) handle(c command) (any, error) {
	switch c.Op {
	case "put":
		id, err := codec.ParseUUID(c.ID)
		if err != nil {
			return nil, err
		}
		head, err := h.commit(c.NS, []document.Op{document.WriteOp{DocID: id, Patch: c.JSON}})
		return map[string]string{"head": head.Hex()}, err
	case "commit":
		var ops []document.Op
		for _, o := range c.Ops {
			id, err := codec.ParseUUID(o.ID)
			if err != nil {
				return nil, err
			}
			if o.Delete {
				ops = append(ops, document.DeleteOp{DocID: id})
			} else {
				ops = append(ops, document.WriteOp{DocID: id, Patch: o.JSON})
			}
		}
		head, err := h.commit(c.NS, ops)
		return map[string]string{"head": head.Hex()}, err
	case "seed":
		var head codec.Hash
		for i := 0; i < c.Commits; i++ {
			id := codec.DerivedUUID(fmt.Sprintf("seed:%s:%d", c.NS, i%max(c.Docs, 1)))
			var err error
			head, err = h.commit(c.NS, []document.Op{document.WriteOp{DocID: id, Patch: fmt.Sprintf(`{"n":%d}`, i)}})
			if err != nil {
				return nil, err
			}
		}
		return map[string]string{"head": head.Hex()}, nil
	case "head":
		rt, err := h.runtime(c.NS)
		if err != nil {
			return nil, err
		}
		hash, commit, ok, err := rt.Runtime.DAG.HeadCommit()
		if err != nil || !ok {
			return map[string]string{"head": "", "tree": ""}, err
		}
		return map[string]string{"head": hash.Hex(), "tree": commit.DocumentTreeHash.Hex()}, nil
	case "docs":
		rt, err := h.runtime(c.NS)
		if err != nil {
			return nil, err
		}
		tree, err := rt.HeadTree()
		if err != nil {
			return nil, err
		}
		docs := map[string]string{}
		var ferr error
		tree.Walk(func(id codec.UUID, _ codec.Hash) bool {
			body, _, found, err := rt.GetDocument(c.NS, id)
			if err != nil {
				ferr = err
				return false
			}
			if found {
				docs[id.String()] = body
			}
			return true
		})
		return map[string]any{"docs": docs}, ferr
	case "attach", "detach":
		rt, err := h.runtime(c.NS)
		if err != nil {
			return nil, err
		}
		id, err := codec.ParseUUID(c.ID)
		if err != nil {
			return nil, err
		}
		var head codec.Hash
		var info embed.AttachmentInfo
		if c.Op == "attach" {
			head, info, err = rt.Runtime.PutAttachment(c.NS, id, strings.NewReader(c.Text), embed.PutAttachmentOptions{Name: c.Name, Mime: c.Mime})
		} else {
			head, err = rt.Runtime.DeleteAttachment(c.NS, id, embed.AttachmentSelector{Name: c.Name})
		}
		if err != nil {
			return nil, err
		}
		return map[string]string{"head": head.Hex(), "attachmentId": info.ID.String(), "contentHash": info.ContentHash.Hex()}, nil
	case "denyRead", "allowRead":
		h.engine.mu.Lock()
		h.engine.unreadable[c.NS+"/"+c.ID] = c.Op == "denyRead"
		h.engine.mu.Unlock()
		return map[string]string{}, nil
	case "setHome", "clearHome":
		rt, err := h.runtime(c.NS)
		if err != nil {
			return nil, err
		}
		if c.Op == "clearHome" {
			rt.SetHome(nil)
		} else {
			// Another node is home since this head: this one refuses writes to the namespace with
			// NOT_HOME, and replicated commits written after the handover by anyone but the home.
			head, err := rt.Runtime.DAG.Head()
			if err != nil {
				return nil, err
			}
			rt.SetHome(&server.Home{Node: "elsewhere", Addr: "ws://elsewhere/kdb/sync", Fence: 1, Since: head.Hex()})
		}
		return map[string]string{}, nil
	}
	return nil, fmt.Errorf("unknown op %q", c.Op)
}

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "HTTP listen address")
	dir := flag.String("dir", "", "data directory (empty: in memory)")
	serve := flag.Bool("serve", false, "serve until killed instead of reading commands from stdin (browser tests)")
	var seeds []string
	flag.Func("seed", "ns:N - write N documents ({\"region\":\"EU\"|\"US\"}) to ns before serving; repeatable", func(v string) error {
		seeds = append(seeds, v)
		return nil
	})
	flag.Parse()

	var fileHost *embed.Host
	var rt *embed.EmbeddedKdbRuntime
	var err error
	const primaryNS = "replicahost/primary"
	if *dir != "" {
		if fileHost, err = embed.OpenFileHost(*dir, embed.FileRuntimeOptionsFromEnv()); err == nil {
			rt, err = fileHost.Namespace(embed.CatalogFromNamespace(primaryNS), primaryNS, schema.None())
		}
	} else {
		rt, err = embed.OpenMemoryRuntime(embed.CatalogFromNamespace(primaryNS), primaryNS, schema.None())
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "kdb-replicahost: open:", err)
		os.Exit(1)
	}
	primary := server.NewKdbServerRuntime(rt)
	engine := tokenEngine{mu: &sync.Mutex{}, unreadable: map[string]bool{}}
	primary.AuthEngine = engine
	primary.PeerCreateOnPush = true
	var set *server.NamespaceSet
	if fileHost != nil {
		set = server.NewNamespaceSet(fileHost.Transactions())
	} else {
		set = server.NewNamespaceSet(nil)
	}
	if err := set.Add(primary); err != nil {
		fmt.Fprintln(os.Stderr, "kdb-replicahost:", err)
		os.Exit(1)
	}
	node, err := syncnode.Open(fileHost, set, primary, syncnode.Config{DataDir: *dir})
	if err != nil {
		fmt.Fprintln(os.Stderr, "kdb-replicahost: sync node:", err)
		os.Exit(1)
	}
	set.SetOpener(node.Opener(nil))
	if err := node.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "kdb-replicahost: start:", err)
		os.Exit(1)
	}
	mux := http.NewServeMux()
	mux.Handle("/kdb/sync", node.Handler())
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "kdb-replicahost: listen:", err)
		os.Exit(1)
	}
	go func() { _ = http.Serve(ln, mux) }()

	h := &host{set: set, engine: engine}
	for _, sd := range seeds {
		ns, n, ok := strings.Cut(sd, ":")
		count, err := strconv.Atoi(n)
		if !ok || err != nil {
			fmt.Fprintln(os.Stderr, "kdb-replicahost: -seed wants ns:N, got", sd)
			os.Exit(2)
		}
		for i := 0; i < count; i++ {
			id := codec.DerivedUUID(fmt.Sprintf("seed:%s:%d", ns, i))
			region := []string{"EU", "US"}[i%2]
			if _, err := h.commit(ns, []document.Op{document.WriteOp{DocID: id, Patch: fmt.Sprintf(`{"region":%q,"i":%d}`, region, i)}}); err != nil {
				fmt.Fprintln(os.Stderr, "kdb-replicahost: seed:", err)
				os.Exit(1)
			}
		}
	}
	fmt.Printf("ready ws://%s/kdb/sync\n", ln.Addr().String())
	if *serve {
		select {}
	}
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 64<<20)
	out := json.NewEncoder(os.Stdout)
	for in.Scan() {
		var c command
		var res any
		if err = json.Unmarshal(in.Bytes(), &c); err == nil {
			res, err = h.handle(c)
		}
		if err != nil {
			_ = out.Encode(map[string]string{"error": err.Error()})
			continue
		}
		_ = out.Encode(res)
	}
	node.Close()
	if fileHost != nil {
		_ = fileHost.Close()
	}
}
