//go:build linux || darwin

package mmapfile

import (
	"os"
	"syscall"
	"unsafe"
)

// mincore is called through the raw syscall: neither the standard library nor golang.org/x/sys
// wraps it on linux or darwin. On darwin syscall.Syscall goes through libc's syscall(2).
func mincore(b, vec []byte) error {
	_, _, errno := syscall.Syscall(syscall.SYS_MINCORE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)),
		uintptr(unsafe.Pointer(&vec[0])))
	if errno != 0 {
		return errno
	}
	return nil
}

// mincoreChunkPages bounds the mincore output buffer: one byte per page, so 64K pages is 64 KiB
// of buffer however large the mapping.
const mincoreChunkPages = 64 << 10

var pageSize = os.Getpagesize()

// residentOf counts the resident bytes of one live, page-aligned mapping with mincore. vec is a
// reusable buffer. Any error counts the rest of the mapping as resident: over-charging is the
// safe direction for a memory budget.
func residentOf(data []byte, vec *[]byte) int64 {
	if cap(*vec) < mincoreChunkPages {
		*vec = make([]byte, mincoreChunkPages)
	}
	chunk := mincoreChunkPages * pageSize
	var n int64
	for off := 0; off < len(data); off += chunk {
		end := min(off+chunk, len(data))
		pages := (end - off + pageSize - 1) / pageSize
		v := (*vec)[:pages]
		if err := mincore(data[off:end], v); err != nil {
			return n + int64(len(data)-off)
		}
		for i, b := range v {
			if b&1 == 0 {
				continue
			}
			// The last page of a file may be partial; count only the bytes the mapping spans.
			n += int64(min(pageSize, end-off-i*pageSize))
		}
	}
	return n
}
