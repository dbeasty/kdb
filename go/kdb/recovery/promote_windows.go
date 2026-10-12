//go:build windows

package recovery

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// deviceID identifies the volume path is on by its serial number, Windows' counterpart of
// st_dev: a rename between two paths succeeds only if they share one.
func deviceID(path string) (uint64, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	root := make([]uint16, windows.MAX_PATH+1)
	if err := windows.GetVolumePathName(p, &root[0], uint32(len(root))); err != nil {
		return 0, fmt.Errorf("cannot determine which volume %s is on: %w", path, err)
	}
	var serial uint32
	if err := windows.GetVolumeInformation(&root[0], nil, 0, &serial, nil, nil, nil, 0); err != nil {
		return 0, fmt.Errorf("cannot determine which volume %s is on: %w", path, err)
	}
	return uint64(serial), nil
}
