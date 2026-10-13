//go:build !(linux || darwin)

package mmapfile

// residentOf has no residency query here, so the whole mapping is charged.
func residentOf(data []byte, _ *[]byte) int64 { return int64(len(data)) }
