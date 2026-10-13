package bufpool

import "testing"

func TestGetReturnsRequestedLength(t *testing.T) {
	for _, n := range []int{0, 1, 4 << 10, 4<<10 + 1, 1 << 20, MaxPooled, MaxPooled + 1} {
		bp := Get(n)
		if len(*bp) != n {
			t.Fatalf("Get(%d) has length %d", n, len(*bp))
		}
		if n <= MaxPooled && cap(*bp) != classes[classFor(n)] {
			t.Fatalf("Get(%d) has capacity %d, want class %d", n, cap(*bp), classes[classFor(n)])
		}
		Put(bp)
	}
}

func TestPutDropsForeignCapacities(t *testing.T) {
	b := make([]byte, 10, 5000) // not a class size
	Put(&b)
	big := make([]byte, MaxPooled+1)
	Put(&big)
	Put(nil)
}

func TestPooledGetDoesNotAllocate(t *testing.T) {
	Put(Get(1000))
	if n := testing.AllocsPerRun(100, func() { Put(Get(1000)) }); n != 0 {
		t.Fatalf("%v allocations per Get/Put", n)
	}
}
