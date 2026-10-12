//go:build !unix

package mmapfile

// Open is unsupported here; callers keep their heap path.
func Open(string) (*Mapping, error) { return nil, ErrUnsupported }
