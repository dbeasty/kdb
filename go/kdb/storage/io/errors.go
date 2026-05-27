package io

import kdberr "github.com/limidus/kdb/go/kdb/error"

// PlatformIOError is raised for segment I/O failures.
type PlatformIOError struct {
	Message     string
	SegmentName string
	Cause       error
}

func (e *PlatformIOError) Error() string {
	if e.SegmentName != "" {
		return e.Message + " (" + e.SegmentName + ")"
	}
	return e.Message
}

func (e *PlatformIOError) Unwrap() error { return e.Cause }

func (e *PlatformIOError) Code() kdberr.Code { return kdberr.StorageTierError }
