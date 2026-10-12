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
//	open(namespace: string, onFrame: (frame: Uint8Array) => void) -> { send(frame), close() } | { error }
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
	if len(args) < 2 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeFunction {
		return errVal("open(namespace, onFrame) requires a namespace string and a frame callback")
	}
	namespace := args[0].String()
	rt, err := embed.OpenMemoryRuntime(embed.CatalogFromNamespace(namespace), namespace, schema.None())
	if err != nil {
		return errVal(err.Error())
	}
	srv := server.NewKdbServerRuntime(rt)
	host := server.NewFrameHost(srv)

	c := newConn(args[1])
	served := make(chan struct{})
	go func() {
		defer close(served)
		host.Serve(c)
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
		// Serve returns once in-flight frames drain, releasing sessions and document locks; wait
		// for that off the event loop (blocking here would deadlock any callback it is running).
		go func() {
			<-served
			releaseOnce.Do(func() {
				sendFn.Release()
				closeFn.Release()
			})
		}()
		return nil
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
