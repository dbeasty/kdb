//go:build !(linux || darwin)

package mmapfile

// dropResident has nothing to call here; the pages stay until the mapping is closed.
func dropResident([]byte) error { return nil }
