//go:build !linux

package storage

// CgroupMemoryLimitBytes has no implementation outside Linux (macOS and Windows have no cgroups);
// see the Linux build's version. Callers fall back to TotalSystemMemoryBytes.
func CgroupMemoryLimitBytes() (uint64, bool) { return 0, false }
