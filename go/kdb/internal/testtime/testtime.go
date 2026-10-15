// Package testtime scales the bounds tests wait for asynchronous progress by, so a slow runner
// - the race detector is 5-10x slower, a shared CI box slower still - does not fail a test that
// was making progress (qtask production plan U10: two releases failed on a 5 s wait that took
// 12 s under -race).
//
// Use it for waits that fail the test when exceeded. A bound that asserts something does NOT
// happen must stay unscaled.
package testtime

import (
	"os"
	"strconv"
	"time"
)

// Scale returns d multiplied by the factor in force: KDB_TEST_TIME_SCALE when set to a positive
// number, otherwise 10 under the race detector and 1 without it.
func Scale(d time.Duration) time.Duration {
	return time.Duration(float64(d) * Factor())
}

// Factor is the multiplier Scale applies.
func Factor() float64 {
	if v, err := strconv.ParseFloat(os.Getenv("KDB_TEST_TIME_SCALE"), 64); err == nil && v > 0 {
		return v
	}
	if raceEnabled {
		return 10
	}
	return 1
}
