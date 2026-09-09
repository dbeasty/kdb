//go:build unix

package io

import (
	"fmt"
	"syscall"
)

// availableBytes is the free space a non-privileged writer can actually use: statfs reports both
// the total free blocks and the smaller figure left after the filesystem's reserve, and it is the
// second that answers "will this write succeed".
func availableBytes(root string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(root, &st); err != nil {
		return 0, fmt.Errorf("could not read free space at %s: %w", root, err)
	}
	return int64(st.Bavail) * int64(st.Bsize), nil
}
