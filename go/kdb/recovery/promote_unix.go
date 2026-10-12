//go:build unix

package recovery

import (
	"errors"
	"os"
	"syscall"
)

// deviceID reports which filesystem path is on, so a promotion that would need a cross-device
// move is refused before an operator is asked to restart anything.
func deviceID(path string) (uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New("cannot determine which filesystem this path is on")
	}
	return uint64(st.Dev), nil
}
