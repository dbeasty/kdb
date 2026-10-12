//go:build !windows

package fsx

import "os"

// OpenFile is os.OpenFile.
func OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}

// Rename is os.Rename.
func Rename(oldpath, newpath string) error { return os.Rename(oldpath, newpath) }

// Remove is os.Remove.
func Remove(name string) error { return os.Remove(name) }

// RemoveAll is os.RemoveAll.
func RemoveAll(path string) error { return os.RemoveAll(path) }

// SyncDir makes a create, rename or remove inside dir durable.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
