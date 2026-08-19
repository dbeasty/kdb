//go:build linux

package storage

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// totalSystemMemoryBytes parses MemTotal out of /proc/meminfo. Uses
// MemTotal (not MemAvailable) to match the "total machine memory" basis
// documented on HotTierMemoryConfig.PercentOfAvailable and to keep
// behavior consistent with the Darwin implementation, which can only
// report total physical memory.
func totalSystemMemoryBytes() (int64, error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0, fmt.Errorf("unexpected MemTotal line format: %q", line)
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, err
		}
		return kb * 1024, nil
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return 0, fmt.Errorf("MemTotal not found in /proc/meminfo")
}
