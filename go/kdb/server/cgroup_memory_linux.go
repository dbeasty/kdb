//go:build linux

package server

import (
	"os"
	"strconv"
	"strings"
)

// cgroupMemoryCurrentBytes reads the exact byte count a Linux cgroup memory limit is enforced
// against - kdb-spec-layer13 Component 48 §5.1's preferred measurement source, over the
// runtime/metrics-based estimate currentMemoryUsageBytes falls back to. Tries cgroup v2's single
// memory.current file first, then cgroup v1's memory.usage_in_bytes (same numeric contract for
// this purpose). Returns ok=false if neither is readable - not every Linux host runs inside a
// cgroup with a memory controller, and macOS/Windows never do.
//
// This exists because runtime/metrics' /memory/classes/total:bytes measures virtual address
// space the Go runtime has *mapped* (including large arena reservations it may never actually
// touch), which can run well ahead of the cgroup-enforced resident/charged figure - found
// empirically running kdb-spec-layer13's Docker e2e harness (docs/benchmarks/
// resource-governance-sim): docker stats showed a stable ~15MB resident throughout a sustained
// write burst that runtime/metrics' number alone would have reported very differently against.
// The cgroup number is what the container's --memory limit is actually checked against, so it is
// what a MemoryGuard sized off that limit should check against too.
func cgroupMemoryCurrentBytes() (uint64, bool) {
	if v, ok := readUintFile("/sys/fs/cgroup/memory.current"); ok { // cgroup v2
		return v, true
	}
	if v, ok := readUintFile("/sys/fs/cgroup/memory/memory.usage_in_bytes"); ok { // cgroup v1
		return v, true
	}
	return 0, false
}

func readUintFile(path string) (uint64, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}
