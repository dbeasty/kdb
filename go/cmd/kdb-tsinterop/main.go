// Command kdb-tsinterop starts an in-memory KDB server with a WebSocket listener and prints its
// URI, for the TypeScript client's live interop test to drive.
//
// This exists because Component 63 §9.2 - "wire-compatible, proven against a running server" -
// cannot be checked from either side alone. The Go tests prove the Go client and the Go server
// agree; the TS golden-fixture tests prove the TS codec reproduces Go's bytes. Neither shows
// that a TypeScript process can actually talk to a Go server over a real socket, which is the
// claim the package is actually making.
//
// Prints one line to stdout when ready:
//
//	ready ws://127.0.0.1:PORT/kdb
//
// then serves until stdin closes or it is signalled.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

func main() {
	addr := flag.String("addr", "ws://127.0.0.1:0/kdb", "WebSocket listen URI")
	namespace := flag.String("namespace", "app/users", "namespace to serve")
	flag.Parse()

	runtime, err := embed.OpenMemoryRuntime(embed.CatalogFromNamespace(*namespace), *namespace, schema.None())
	if err != nil {
		fmt.Fprintln(os.Stderr, "kdb-tsinterop: open runtime:", err)
		os.Exit(1)
	}
	srv := server.NewKdbServerRuntime(runtime)

	listener, err := server.ListenSqlWireWS(*addr, srv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "kdb-tsinterop: listen:", err)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Printf("ready ws://%s/kdb\n", listener.Addr().String())
	_ = os.Stdout.Sync()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Exiting when stdin closes is what keeps a crashed or killed test runner from leaving this
	// process behind holding a port.
	stdinClosed := make(chan struct{})
	go func() {
		defer close(stdinClosed)
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			// Ignore input; the point is only to notice EOF.
		}
	}()

	select {
	case <-signals:
	case <-stdinClosed:
	}
}
