package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

// KdbServiceMain is a skeleton service entrypoint mirroring dev.kdb.service.KdbServiceMain.
func main() {
	fs := flag.NewFlagSet("kdb-service", flag.ExitOnError)
	var (
		dataDir   string
		memory    bool
		namespace string
	)
	fs.StringVar(&dataDir, "data-dir", "", "filesystem data root")
	fs.BoolVar(&memory, "memory", false, "use in-memory runtime")
	fs.StringVar(&namespace, "namespace", "demo/users", "default namespace")
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
	_ = srv

	peerStatus := "disabled"
	streamStatus := "disabled"
	sqlStatus := "disabled (wire listeners not ported)"
	fmt.Printf("KDB service peer=%s stream=%s sql=%s namespace=%s\n", peerStatus, streamStatus, sqlStatus, namespace)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	srv.Release()
}
