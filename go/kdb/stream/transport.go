package stream

import (
	"strings"
	"sync"
)

// Transport connects to a wire endpoint.
type Transport interface {
	Connect(uri string) (ConnectionHandle, error)
}

// ConnectionHandle is a bidirectional framed wire connection.
type ConnectionHandle interface {
	Send(frame []byte) error
	Incoming() <-chan []byte
	Close() error
	TryPoll() []byte
}

// InMemoryTransport routes frames through an in-process hub (memory:// URIs).
type InMemoryTransport struct{}

func NewInMemoryTransport() *InMemoryTransport { return &InMemoryTransport{} }

func (t *InMemoryTransport) Connect(uri string) (ConnectionHandle, error) {
	name := strings.TrimPrefix(uri, "memory://")
	return globalHub.connect(name)
}

var globalHub = &inMemoryHub{hubs: make(map[string]*Hub)}

type inMemoryHub struct {
	mu   sync.Mutex
	hubs map[string]*Hub
}

// Hub is an in-memory wire transport rendezvous point.
type Hub struct {
	name          string
	mu            sync.Mutex
	clients       []*clientLink
	ServerHandler func(frame []byte)
}

func (h *inMemoryHub) hub(name string) *Hub {
	h.mu.Lock()
	defer h.mu.Unlock()
	if hub, ok := h.hubs[name]; ok {
		return hub
	}
	hub := &Hub{name: name}
	h.hubs[name] = hub
	return hub
}

func (h *inMemoryHub) connect(name string) (*clientLink, error) {
	return h.hub(name).createConnection()
}

// HubFor returns (creating if needed) the named in-memory hub.
func HubFor(name string) *Hub {
	return globalHub.hub(name)
}

func (hub *Hub) createConnection() (*clientLink, error) {
	link := &clientLink{
		hub:      hub,
		incoming: make(chan []byte, 64),
		done:     make(chan struct{}),
	}
	hub.mu.Lock()
	hub.clients = append(hub.clients, link)
	hub.mu.Unlock()
	return link, nil
}

// ServerSend delivers a frame to every connected client, in order, and never drops one: when a
// client's buffer is full it waits until that client reads (a slow reader is caught up when it
// drains) or closes. A closed client is skipped. This is backpressure on the sender, which is how
// a push transport with nothing to catch up from keeps the stream contract ("no dropped frames").
// The network StreamHub catches each subscriber up from the DAG instead, so its Publish never
// waits.
func (hub *Hub) ServerSend(frame []byte) {
	hub.mu.Lock()
	clients := append([]*clientLink(nil), hub.clients...)
	hub.mu.Unlock()
	for _, c := range clients {
		c.deliver(frame)
	}
}

// clientLink is one memory:// connection. Close closes incoming only after every deliver that got
// past the closed check has returned, so a send never hits a closed channel; done (closed by
// Close) releases a deliver waiting for room. mu guards closed and the in-flight count and is
// never held while a deliver waits, so Close cannot deadlock against one.
type clientLink struct {
	hub      *Hub
	incoming chan []byte
	done     chan struct{}
	inflight sync.WaitGroup
	closed   bool
	mu       sync.Mutex
}

func (c *clientLink) Send(frame []byte) error {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return NewNotConnectedError()
	}
	if c.hub.ServerHandler != nil {
		c.hub.ServerHandler(frame)
	}
	return nil
}

func (c *clientLink) Incoming() <-chan []byte { return c.incoming }

func (c *clientLink) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.done)
	c.mu.Unlock()
	// No deliver can start now (closed was set under mu). Wait out the ones already sending,
	// which done has just released, before closing the channel they send on.
	c.inflight.Wait()
	close(c.incoming)
	return nil
}

func (c *clientLink) TryPoll() []byte {
	select {
	case frame := <-c.incoming:
		return frame
	default:
		return nil
	}
}

// deliver hands frame to the client, waiting for room while its buffer is full, until the client
// closes. A live client never loses a frame.
func (c *clientLink) deliver(frame []byte) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.inflight.Add(1)
	c.mu.Unlock()
	defer c.inflight.Done()
	select {
	case c.incoming <- frame:
	case <-c.done:
	}
}
