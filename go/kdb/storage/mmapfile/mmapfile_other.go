//go:build !unix

package mmapfile

// Supported reports whether Open can map files on this platform.
const Supported = false

// Open is unsupported here; callers keep their heap path.
func Open(string) (*Mapping, error) { return nil, ErrUnsupported }
