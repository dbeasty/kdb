//go:build !darwin && !linux

package storage

import "fmt"

// totalSystemMemoryBytes has no implementation for this platform;
// ResolveHotTierBytes falls back to DefaultHotTierBytes when this
// errors, so PercentOfAvailable is simply unavailable here rather than
// producing a wrong number.
func totalSystemMemoryBytes() (int64, error) {
	return 0, fmt.Errorf("total system memory detection not implemented on this platform")
}
