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

// cgroupMemoryLimitBytes reads the memory ceiling this process is actually subject to - the
// number a container's --memory is enforced as, and so the right default for the server's memory
// budget (kdb-spec-layer13 §13: "Default: cgroup limit if detectable"). Returns ok=false when
// there is no cgroup memory controller, or when there is one but it imposes no limit: cgroup v2
// spells that "max", and cgroup v1 spells it as a sentinel so large it is indistinguishable from
// "unlimited" in practice. Treating either as a real budget would size the guard against a number
// that has nothing to do with the memory available.
func cgroupMemoryLimitBytes() (uint64, bool) {
	if b, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil { // cgroup v2
		text := strings.TrimSpace(string(b))
		if text == "max" {
			return 0, false
		}
		if v, err := strconv.ParseUint(text, 10, 64); err == nil && v > 0 {
			return v, true
		}
	}
	if v, ok := readUintFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); ok { // cgroup v1
		// v1's "no limit" is PAGE_COUNTER_MAX scaled by page size - astronomically larger than
		// any real machine. Anything at or above an exbibyte is that sentinel, not a budget.
		if v > 0 && v < (1<<60) {
			return v, true
		}
	}
	return 0, false
}
