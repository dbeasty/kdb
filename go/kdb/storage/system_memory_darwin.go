//go:build darwin

package storage

import (
	"os/exec"
	"strconv"
	"strings"
)

// totalSystemMemoryBytes shells out to `sysctl -n hw.memsize` for total
// physical RAM. This is total, not currently-available, memory - a
// reasonable and stable basis for "percent of machine memory" without
// pulling in a platform-memory-stats dependency (Go's stdlib syscall
// package doesn't expose a uint64 sysctl reader on Darwin); true
// available memory fluctuates continuously and isn't needed for a
// one-time budget sizing decision at startup. Not on any hot path -
// called once per engine construction, not per write.
func totalSystemMemoryBytes() (int64, error) {
	out, err := exec.Command("sysctl", "-n", "hw.memsize").Output()
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0, err
	}
	return v, nil
}
