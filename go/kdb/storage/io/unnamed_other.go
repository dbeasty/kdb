//go:build !linux

package io

import (
	"errors"
	"os"
)

// unnamedFilesSupported is false off Linux: there is no O_TMPFILE, so delta segments are always
// created by name, at the moment they are first written (see spareSegment).
const unnamedFilesSupported = false

var errUnnamedUnsupported = errors.New("unnamed files are not supported on this platform")

func createUnnamedFile(dir string) (*os.File, error) { return nil, errUnnamedUnsupported }

func linkUnnamedFile(f *os.File, path string) error { return errUnnamedUnsupported }

// preallocateFileTrickle is only used for spares, which exist only on Linux.
func preallocateFileTrickle(f *os.File, bytes, trickle int64) error { return preallocateFile(f, bytes) }
