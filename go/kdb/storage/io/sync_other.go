//go:build !darwin && !linux

package io

import "os"

func syncFile(f *os.File, _ SyncMode) error { return f.Sync() }
