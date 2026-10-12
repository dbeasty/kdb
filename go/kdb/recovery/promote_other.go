//go:build !unix && !windows

package recovery

import "errors"

// deviceID is unavailable off unix. SameFilesystem's callers treat the error as "cannot tell",
// which for promotion means the check is reported as unavailable rather than silently passed.
func deviceID(string) (uint64, error) {
	return 0, errors.New("filesystem identity is not available on this platform")
}
