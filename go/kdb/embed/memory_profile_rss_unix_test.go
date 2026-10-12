//go:build unix

package embed

import (
	"runtime"
	"syscall"
)

// peakRSS is the process's high-water resident set. getrusage reports it in bytes on macOS and
// kilobytes elsewhere.
func peakRSS() int64 {
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) != nil {
		return -1
	}
	if runtime.GOOS == "darwin" {
		return int64(ru.Maxrss)
	}
	return int64(ru.Maxrss) * 1024
}
