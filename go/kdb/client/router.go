package client

import (
	"context"
	"errors"
	"regexp"
	"sync"
)

// ErrNotHome is the sentinel every NotHomeError unwraps to.
var ErrNotHome = errors.New("kdb: not the namespace's home")

// NotHomeError is a write refused because the namespace is single-home and this server is not
// its home. Home is the address to write to instead, when the server named one.
type NotHomeError struct {
	Message string
	Home    string
}

func (e *NotHomeError) Error() string { return "kdb: " + e.Message }
func (e *NotHomeError) Unwrap() error { return ErrNotHome }

var homeAddrPattern = regexp.MustCompile(`home=([^\s)]+)`)

func homeAddr(msg string) string {
	if m := homeAddrPattern.FindStringSubmatch(msg); m != nil {
		return m[1]
	}
	return ""
}

// Router is a Client that follows single-home redirects: a write refused with NotHomeError is
// retried once at the home the refusal names, over a connection the router opens with the same
// credentials and options, and the namespace's writes go straight there afterwards. Reads stay on
// the first connection - every node serves reads of a single-home namespace.
//
// Safe for concurrent use.
type Router struct {
	token string
	opts  ConnectOptions

	primary *Client
	mu      sync.Mutex
	byAddr  map[string]*Client
	homes   map[string]string // namespace -> home address
}

// NewRouter connects to addr and returns a router over it.
func NewRouter(ctx context.Context, addr, token string, opts ConnectOptions) (*Router, error) {
	c, err := ConnectWithOptions(ctx, addr, token, opts)
	if err != nil {
		return nil, err
	}
	return &Router{token: token, opts: opts, primary: c, byAddr: map[string]*Client{}, homes: map[string]string{}}, nil
}

// Primary is the router's first connection.
func (r *Router) Primary() *Client { return r.primary }

// Close closes every connection the router opened.
func (r *Router) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	err := r.primary.Close()
	for _, c := range r.byAddr {
		_ = c.Close()
	}
	r.byAddr = map[string]*Client{}
	return err
}

// writer is the client ns's writes go to.
func (r *Router) writer(ns string) *Client {
	r.mu.Lock()
	defer r.mu.Unlock()
	if addr, ok := r.homes[ns]; ok {
		if c, ok := r.byAddr[addr]; ok {
			return c
		}
	}
	return r.primary
}

// redirect records that ns's home is at err's address and returns a client for it; false when
// err is not a redirect or names nowhere to go.
func (r *Router) redirect(ctx context.Context, ns string, err error) (*Client, bool) {
	var nh *NotHomeError
	if !errors.As(err, &nh) || nh.Home == "" {
		return nil, false
	}
	r.mu.Lock()
	c, ok := r.byAddr[nh.Home]
	r.mu.Unlock()
	if !ok {
		var derr error
		if c, derr = ConnectWithOptions(ctx, nh.Home, r.token, r.opts); derr != nil {
			return nil, false
		}
		r.mu.Lock()
		if existing, raced := r.byAddr[nh.Home]; raced {
			_ = c.Close()
			c = existing
		} else {
			r.byAddr[nh.Home] = c
		}
		r.mu.Unlock()
	}
	r.mu.Lock()
	r.homes[ns] = nh.Home
	r.mu.Unlock()
	return c, true
}

func routed[T any](r *Router, ctx context.Context, ns string, call func(*Client) (T, error)) (T, error) {
	out, err := call(r.writer(ns))
	if err == nil {
		return out, nil
	}
	if c, ok := r.redirect(ctx, ns, err); ok {
		return call(c)
	}
	return out, err
}

// PutJSON is Client.PutJSON, following a redirect to the namespace's home.
func (r *Router) PutJSON(ctx context.Context, ns, docID string, body []byte) (string, error) {
	return routed(r, ctx, ns, func(c *Client) (string, error) { return c.PutJSON(ctx, ns, docID, body) })
}

// Upsert is Client.Upsert, following a redirect to the namespace's home.
func (r *Router) Upsert(ctx context.Context, ns, docID string, body []byte) (string, error) {
	return routed(r, ctx, ns, func(c *Client) (string, error) { return c.Upsert(ctx, ns, docID, body) })
}

// Commit is Client.Commit, following a redirect to the namespace's home.
func (r *Router) Commit(ctx context.Context, tx Transaction) (string, error) {
	return routed(r, ctx, tx.Namespace, func(c *Client) (string, error) { return c.Commit(ctx, tx) })
}

// GetJSON reads from the router's first connection.
func (r *Router) GetJSON(ctx context.Context, ns, docID string) ([]byte, string, error) {
	return r.primary.GetJSON(ctx, ns, docID)
}
