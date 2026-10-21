package server

import "testing"

func TestCachePoolBytesExplicit(t *testing.T) {
	if got, source := CachePoolBytes(512); got != 512<<20 || source != "explicit" {
		t.Fatalf("CachePoolBytes(512) = %d, %q", got, source)
	}
}

// Auto-sizing takes an eighth of the machine, clamped: a 4 GiB container gets 512 MiB, a tiny one
// still gets the old 64 MiB, and a large host stops at 4 GiB.
func TestClampCachePool(t *testing.T) {
	for _, tc := range []struct{ machine, want int64 }{
		{4 << 30, 512 << 20},
		{128 << 20, MinCachePoolBytes},
		{256 << 30, MaxCachePoolBytes},
	} {
		if got := clampCachePool(int64(float64(tc.machine) * CachePoolFractionOfMemory)); got != tc.want {
			t.Errorf("machine %d MiB: pool %d MiB, want %d MiB", tc.machine>>20, got>>20, tc.want>>20)
		}
	}
	if got, _ := CachePoolBytes(0); got < MinCachePoolBytes || got > MaxCachePoolBytes {
		t.Fatalf("auto-sized pool %d outside [%d, %d]", got, MinCachePoolBytes, MaxCachePoolBytes)
	}
}
