//go:build !linux

package server

// cgroupMemoryCurrentBytes has no implementation outside Linux (macOS/Windows have no cgroups) -
// MemoryGuard falls back to the runtime/metrics-based estimate, matching
// storage.totalSystemMemoryBytes' equivalent per-platform fallback pattern.
func cgroupMemoryCurrentBytes() (uint64, bool) {
	return 0, false
}

// cgroupMemoryLimitBytes has no implementation outside Linux - see the Linux build's version.
func cgroupMemoryLimitBytes() (uint64, bool) {
	return 0, false
}
