//go:build !darwin && !linux

package dag

import "os"

func syncGraphFile(f *os.File) error { return f.Sync() }

const graphTrickleBytes = 0
