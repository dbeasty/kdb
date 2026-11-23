//go:build linux

package storage

import (
	"os"
	"strconv"
	"strings"
)

// CgroupMemoryLimitBytes reads the memory ceiling this process is actually subject to - the
// number a container's --memory is enforced as. kdb-service sizes its memory budget and its cache
// pool from it (kdb-spec-layer13 §13: "Default: cgroup limit if detectable"), and an embedded
// runtime sizes its default cache from it (embed.DefaultMemoryBudgetBytes). Returns ok=false when
// there is no cgroup memory controller, or when there is one but it imposes no limit: cgroup v2
// spells that "max", and cgroup v1 spells it as a sentinel so large it is indistinguishable from
// "unlimited" in practice. Treating either as a real budget would size against a number that has
// nothing to do with the memory available.
func CgroupMemoryLimitBytes() (uint64, bool) { return cgroupMemoryLimitBytesAt(cgroupMountRoot) }

// cgroupMountRoot is where the cgroup filesystem is mounted; tests drive
// cgroupMemoryLimitBytesAt with a fixture tree instead.
const cgroupMountRoot = "/sys/fs/cgroup"

func cgroupMemoryLimitBytesAt(root string) (uint64, bool) {
	if b, err := os.ReadFile(root + "/memory.max"); err == nil { // cgroup v2
		text := strings.TrimSpace(string(b))
		if text == "max" {
			return 0, false
		}
		if v, err := strconv.ParseUint(text, 10, 64); err == nil && v > 0 {
			return v, true
		}
	}
	if b, err := os.ReadFile(root + "/memory/memory.limit_in_bytes"); err == nil { // cgroup v1
		// v1's "no limit" is PAGE_COUNTER_MAX scaled by page size - astronomically larger than
		// any real machine. Anything at or above an exbibyte is that sentinel, not a budget.
		if v, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64); err == nil && v > 0 && v < (1<<60) {
			return v, true
		}
	}
	return 0, false
}
