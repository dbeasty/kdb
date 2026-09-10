//go:build !linux && !darwin

package io

import "os"

// preallocateFile is the portable fallback: no reservation primitive, just a
// zero-fill, which still produces a file whose size stops changing on every
// append. Slower to create than fallocate; identical afterwards.
func preallocateFile(f *os.File, bytes int64) error {
	return zeroFill(f, bytes)
}
