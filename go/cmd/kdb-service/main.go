package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/storage/mem"
)

// KdbServiceMain is a skeleton service entrypoint mirroring dev.kdb.service.KdbServiceMain.
func main() {
	fs := flag.NewFlagSet("kdb-service", flag.ExitOnError)
	var (
		dataDir   string
		memory    bool
		namespace string
	)
	var sqlAddr string
	var rbac bool
	fs.StringVar(&dataDir, "data-dir", "", "filesystem data root")
	fs.BoolVar(&memory, "memory", false, "use in-memory runtime")
	fs.StringVar(&namespace, "namespace", "demo/users", "default namespace")
	fs.StringVar(&sqlAddr, "sql-addr", "tcp://127.0.0.1:9090?bind=true", "SQL wire listen address (empty to disable)")
	fs.BoolVar(&rbac, "rbac", false, "enable RBAC (in-memory user/role registry - create users via the Go API; no admin SQL surface yet)")
	_ = fs.Parse(os.Args[1:])

	if dataDir == "" && !memory {
		memory = true
	}
	if dataDir != "" && memory {
		fmt.Fprintln(os.Stderr, "use either --data-dir or --memory")
		os.Exit(2)
	}

	var (
		rt  *embed.EmbeddedKdbRuntime
		err error
	)
	if memory {
		catalog := embed.CatalogFromNamespace(namespace)
		rt, err = embed.OpenMemoryRuntime(catalog, namespace, schema.None())
	} else {
		catalog := embed.CatalogFromNamespace(namespace)
		rt, err = embed.OpenFileRuntime(dataDir, catalog, namespace, schema.None())
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	srv := server.NewKdbServerRuntime(rt)

	rbacStatus := "disabled"
	if rbac {
		// In-memory registry only for now: file-backed durability (component 38 spec §7 test
		// 9) is proven directly against RegistryAuthStore in go/kdb/auth's own tests, but
		// wiring OpenFileRuntime's delta-log persistence into this CLI flag specifically is
		// left as follow-on plumbing rather than rushed in here.
		usersDag, err := dag.NewInMemoryCommitDag(auth.UsersNamespace)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: rbac users dag: %v\n", err)
			os.Exit(1)
		}
		rolesDag, err := dag.NewInMemoryCommitDag(auth.RolesNamespace)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: rbac roles dag: %v\n", err)
			os.Exit(1)
		}
		store, err := auth.NewRegistryAuthStore(usersDag, rolesDag, mem.NewInMemoryStorageAdapter())
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: rbac registry: %v\n", err)
			os.Exit(1)
		}
		srv.AuthEngine = auth.NewRegistryAuthEngine(store)
		rbacStatus = "enabled (in-memory registry, no users yet - use the Go API to create one)"
	}

	peerStatus := "disabled"
	streamStatus := "disabled"
	sqlStatus := "disabled"
	var sqlListener *server.Listener
	if sqlAddr != "" {
		sqlListener, err = server.ListenSqlWire(sqlAddr, srv)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: sql wire listen: %v\n", err)
			os.Exit(1)
		}
		defer sqlListener.Close()
		sqlStatus = fmt.Sprintf("enabled (%s)", sqlListener.Addr())
	}
	fmt.Printf("KDB service peer=%s stream=%s sql=%s rbac=%s namespace=%s\n", peerStatus, streamStatus, sqlStatus, rbacStatus, namespace)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	srv.Release()
}
