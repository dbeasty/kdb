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
	}
	hub.mu.Lock()
	hub.clients = append(hub.clients, link)
	hub.mu.Unlock()
	return link, nil
}

// ServerSend delivers a frame to all connected clients.
func (hub *Hub) ServerSend(frame []byte) {
	hub.mu.Lock()
	clients := append([]*clientLink(nil), hub.clients...)
	hub.mu.Unlock()
	for _, c := range clients {
		c.deliver(frame)
	}
}

type clientLink struct {
	hub      *Hub
	incoming chan []byte
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
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
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

func (c *clientLink) deliver(frame []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	select {
	case c.incoming <- frame:
	default:
	}
}
