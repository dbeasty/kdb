//go:build !unix

package recovery

import (
	"errors"
	"os"
)

// deviceID is unavailable off unix. SameFilesystem's callers treat the error as "cannot tell",
// which for promotion means the check is reported as unavailable rather than silently passed.
func deviceID(os.FileInfo) (uint64, error) {
	return 0, errors.New("filesystem identity is not available on this platform")
}
