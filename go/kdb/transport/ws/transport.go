package ws

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
)

// Transport connects and listens over WebSocket binary frames.
type Transport interface {
	stream.Transport
	ConnectWithOptions(uri string, options core.TransportConnectOptions) (stream.ConnectionHandle, error)
	Listen(ctx context.Context, uri string, options core.TransportConnectOptions, handler func(stream.ConnectionHandle)) error
}

type defaultTransport struct {
	options core.TransportConnectOptions
}

// NewTransport returns a WebSocket wire transport stub (network impl pending).
func NewTransport(options core.TransportConnectOptions) Transport {
	if options.MaxFrameBytes == 0 {
		options = core.DefaultConnectOptions()
	}
	return &defaultTransport{options: options}
}

func (t *defaultTransport) Connect(uri string) (stream.ConnectionHandle, error) {
	return t.ConnectWithOptions(uri, t.options)
}

func (t *defaultTransport) ConnectWithOptions(uri string, options core.TransportConnectOptions) (stream.ConnectionHandle, error) {
	parsed, err := ParseURI(uri)
	if err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("websocket connect not implemented for %s:%d (use in-memory or tcp transport)", parsed.Host, parsed.Port)
}

func (t *defaultTransport) Listen(ctx context.Context, uri string, options core.TransportConnectOptions, handler func(stream.ConnectionHandle)) error {
	parsed, err := ParseURI(uri)
	if err != nil {
		return err
	}
	_ = handler
	srv := &http.Server{
		Addr: netJoin(parsed.Host, parsed.Port),
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "websocket upgrade not implemented", http.StatusNotImplemented)
		}),
	}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	return srv.ListenAndServe()
}

// ParseURI parses ws:// or wss:// transport URIs.
func ParseURI(raw string) (TransportURI, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return TransportURI{}, err
	}
	if u.Scheme != "ws" && u.Scheme != "wss" {
		return TransportURI{}, fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		host = "127.0.0.1"
	}
	port := 8080
	if u.Scheme == "wss" {
		port = 443
	}
	if p := u.Port(); p != "" {
		port, err = strconv.Atoi(p)
		if err != nil {
			return TransportURI{}, err
		}
	}
	path := u.Path
	if path == "" {
		path = "/kdb"
	}
	return TransportURI{
		Host:   host,
		Port:   port,
		Path:   path,
		Secure: u.Scheme == "wss",
	}, nil
}

// TransportURI is a parsed WebSocket transport URI.
type TransportURI struct {
	Host   string
	Port   int
	Path   string
	Secure bool
}

func netJoin(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}
