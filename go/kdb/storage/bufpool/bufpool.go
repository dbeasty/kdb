// Package bufpool keeps size-classed pools of byte buffers for the storage read path - the
// analogue of Cassandra's BufferPool, for the buffers a read fills and is finished with before it
// returns: a compressed frame or block read off disk, and a decompressed commit payload that is
// decoded into fresh structures and then dropped.
//
// The rule for using it is the whole point: Put a buffer back only once nothing can still
// reference it. Never Put a buffer whose bytes (or any sub-slice of them) are returned to a
// caller, stored in a cache, or aliased by a decoded value. Every caller in this repository
// documents why its buffer cannot escape.
package bufpool

import "sync"

// classes are the pooled capacities. A request is served from the smallest class that fits; one
// larger than the largest class is allocated exactly and never pooled, so a rare huge read does
// not pin a huge buffer in the pool.
var classes = [...]int{4 << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20, 4 << 20}

var pools [len(classes)]sync.Pool

// MaxPooled is the largest buffer the pool will hold.
const MaxPooled = 4 << 20

func classFor(n int) int {
	for i, c := range classes {
		if n <= c {
			return i
		}
	}
	return -1
}

// Get returns a buffer of length n with unspecified contents. The pointer is what Put takes back;
// passing the buffer around by pointer is what lets Put avoid allocating.
func Get(n int) *[]byte {
	i := classFor(n)
	if i < 0 {
		b := make([]byte, n)
		return &b
	}
	if v := pools[i].Get(); v != nil {
		bp := v.(*[]byte)
		*bp = (*bp)[:n]
		return bp
	}
	b := make([]byte, n, classes[i])
	return &b
}

// Put returns a buffer obtained from Get. Buffers too large to pool, or whose capacity is not a
// class size (one the caller grew by appending), are dropped for the GC.
func Put(bp *[]byte) {
	if bp == nil {
		return
	}
	c := cap(*bp)
	i := classFor(c)
	if i < 0 || classes[i] != c {
		return
	}
	*bp = (*bp)[:0]
	pools[i].Put(bp)
}
