//go:build windows

package io

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// availableBytes is the free space available to this caller: GetDiskFreeSpaceEx's first figure
// already accounts for per-user quotas, the Windows counterpart of statfs's Bavail.
func availableBytes(root string) (int64, error) {
	p, err := windows.UTF16PtrFromString(root)
	if err != nil {
		return 0, err
	}
	var avail, total, free uint64
	if err := windows.GetDiskFreeSpaceEx(p, &avail, &total, &free); err != nil {
		return 0, fmt.Errorf("could not read free space at %s: %w", root, err)
	}
	return int64(avail), nil
}
