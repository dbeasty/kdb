//go:build js && wasm

// Command embed is the in-process KDB engine for @kdb/embed (Component 63 §11, Phase 3).
//
// It is the same engine the Go server runs - the SQL planner, transaction engines, unique-key
// registry, document locks - compiled to WebAssembly and fronted by server.FrameHost, the very
// seam the TCP, WebSocket and gRPC listeners plug into. The only thing that differs from a real
// server is what carries the frames: here, a pair of JavaScript callbacks instead of a socket.
// That is deliberate. Everything that makes a KDB connection a KDB connection (handshake,
// sessions, RBAC, commit semantics, error classification) is inherited rather than reimplemented,
// so an embedded database cannot drift from a networked one.
//
// Exposed on globalThis.KdbEmbed:
//
//	open(namespaces: string[], onFrame: (frame: Uint8Array) => void, options?: { dataRoot?: string })
//	  -> Promise<{ send(frame), close() } | { error }>
//
// open returns a Promise because opening a file-backed host performs filesystem calls, and those
// complete through the JavaScript event loop: a js.FuncOf handler that blocked waiting for one
// would deadlock the Go runtime, so the work happens on a goroutine and the handler returns at once.
//
// With options.dataRoot the namespaces are opened under an embed.Host on that directory - the
// same file-backed engine the service runs: a delta log replayed on open, checkpoints, a
// cross-namespace transaction log. The directory lives in the JavaScript `fs` object the Go
// runtime was started with (see packages/kdb-embed/src/fs.ts), which is what decides whether it
// survives a reload. Without dataRoot everything is in memory.
//
// Each namespace is its own in-memory runtime, routed by a server.NamespaceSet exactly as the
// production service routes them. A namespace not in the list is refused rather than silently
// aliased onto another - which is what a bare single runtime does with a name it does not own.
package main

import (
	"errors"
	"sync"
	"syscall/js"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

func main() {
	js.Global().Set("KdbEmbed", map[string]any{
		"open": js.FuncOf(open),
	})
	// Keep the Go runtime alive; every entry point is a js.FuncOf callback.
	select {}
}

func open(_ js.Value, args []js.Value) any {
	// Copy what is needed out of args now: they are only valid for the duration of this call.
	captured := append([]js.Value(nil), args...)
	executor := js.FuncOf(func(_ js.Value, p []js.Value) any {
		resolve := p[0]
		go func() { resolve.Invoke(openEngine(captured)) }()
		return nil
	})
	defer executor.Release()
	return js.Global().Get("Promise").New(executor)
}

func openEngine(args []js.Value) any {
	if len(args) < 2 || args[0].Type() != js.TypeObject || args[1].Type() != js.TypeFunction {
		return errVal("open(namespaces, onFrame) requires an array of namespaces and a frame callback")
	}
	count := args[0].Get("length").Int()
	if count == 0 {
		return errVal("open: at least one namespace is required")
	}
	var host *embed.Host
	if len(args) >= 3 && args[2].Type() == js.TypeObject {
		if root := args[2].Get("dataRoot"); root.Type() == js.TypeString && root.String() != "" {
			// Zero options, not FileRuntimeOptionsFromEnv: a browser has no environment to read,
			// and an S3 replica configured by a stray variable in Node would be a surprise here.
			h, err := embed.OpenFileHost(root.String(), embed.FileRuntimeOptions{})
			if err != nil {
				return errVal(err.Error())
			}
			host = h
		}
	}
	var runtimes []*server.KdbServerRuntime
	teardown := func() {
		for _, srv := range runtimes {
			srv.Release()
		}
		if host != nil {
			_ = host.Close()
		}
	}
	var primary *server.KdbServerRuntime
	var set *server.NamespaceSet
	for i := 0; i < count; i++ {
		namespace := args[0].Index(i).String()
		var rt *embed.EmbeddedKdbRuntime
		var err error
		if host != nil {
			rt, err = host.Namespace(embed.CatalogFromNamespace(namespace), namespace, schema.None())
		} else {
			rt, err = embed.OpenMemoryRuntime(embed.CatalogFromNamespace(namespace), namespace, schema.None())
		}
		if err != nil {
			teardown()
			return errVal(err.Error())
		}
		srv := server.NewKdbServerRuntime(rt)
		runtimes = append(runtimes, srv)
		if primary == nil {
			primary = srv
			set = server.NewNamespaceSet(rt.TxnCoordinator())
		}
		if err := set.Add(srv); err != nil {
			teardown()
			return errVal(err.Error())
		}
	}
	primary.Namespaces = set
	frames := server.NewFrameHost(primary)

	c := newConn(args[1])
	served := make(chan struct{})
	go func() {
		defer close(served)
		frames.Serve(c)
	}()

	var releaseOnce sync.Once
	var sendFn, closeFn js.Func
	sendFn = js.FuncOf(func(_ js.Value, a []js.Value) any {
		if len(a) < 1 {
			return errVal("send(frame) requires a frame")
		}
		frame := make([]byte, a[0].Get("length").Int())
		js.CopyBytesToGo(frame, a[0])
		if err := c.push(frame); err != nil {
			return errVal(err.Error())
		}
		return nil
	})
	closeFn = js.FuncOf(func(_ js.Value, _ []js.Value) any {
		c.closeIncoming()
		// Returns a Promise that settles once the engine is fully shut down. Serve returns when
		// in-flight frames drain, releasing sessions and document locks; only then are the
		// runtimes flushed and the directory lock released, and the caller must not treat a
		// persistent database as closed (or reopen it) before that. The work runs off the event
		// loop: blocking here would deadlock any callback it is waiting on.
		executor := js.FuncOf(func(_ js.Value, a []js.Value) any {
			resolve := a[0]
			go func() {
				<-served
				teardown()
				releaseOnce.Do(func() {
					sendFn.Release()
					closeFn.Release()
				})
				resolve.Invoke()
			}()
			return nil
		})
		defer executor.Release()
		return js.Global().Get("Promise").New(executor)
	})
	return map[string]any{"send": sendFn, "close": closeFn}
}

func errVal(msg string) map[string]any { return map[string]any{"error": msg} }

// conn adapts a pair of JS callbacks to server.FrameConn.
type conn struct {
	onFrame js.Value

	mu     sync.Mutex
	queue  [][]byte
	wake   chan struct{}
	closed bool
	in     chan []byte
}

func newConn(onFrame js.Value) *conn {
	c := &conn{onFrame: onFrame, wake: make(chan struct{}, 1), in: make(chan []byte)}
	go c.pump()
	return c
}

// push is called from a JS callback, which must never block the event loop, so it only appends
// to an unbounded in-order queue; pump moves frames onto the (blocking) Incoming channel.
func (c *conn) push(frame []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("kdb embed: closed")
	}
	c.queue = append(c.queue, frame)
	select {
	case c.wake <- struct{}{}:
	default:
	}
	return nil
}

func (c *conn) closeIncoming() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *conn) pump() {
	defer close(c.in)
	for {
		c.mu.Lock()
		var next []byte
		if len(c.queue) > 0 {
			next = c.queue[0]
			c.queue = c.queue[1:]
		}
		closed := c.closed
		c.mu.Unlock()
		switch {
		case next != nil:
			c.in <- next
		case closed:
			return
		default:
			<-c.wake
		}
	}
}

func (c *conn) Incoming() <-chan []byte { return c.in }

func (c *conn) Send(frame []byte) error {
	arr := js.Global().Get("Uint8Array").New(len(frame))
	js.CopyBytesToJS(arr, frame)
	c.onFrame.Invoke(arr)
	return nil
}
