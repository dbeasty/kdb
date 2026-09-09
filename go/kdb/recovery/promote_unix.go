//go:build unix

package recovery

import (
	"errors"
	"os"
	"syscall"
)

// deviceID reports which filesystem a file is on, so a promotion that would need a cross-device
// move is refused before an operator is asked to restart anything.
func deviceID(info os.FileInfo) (uint64, error) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New("cannot determine which filesystem this path is on")
	}
	return uint64(st.Dev), nil
}
