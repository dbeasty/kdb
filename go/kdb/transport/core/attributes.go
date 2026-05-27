package core

// WireConnectionAttributes carries transport metadata for wire connections.
type WireConnectionAttributes struct {
	RemoteAddress string
	LocalAddress  string
	Protocol      string
	TLS           bool
}
