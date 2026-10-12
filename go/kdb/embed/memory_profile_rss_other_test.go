//go:build !unix

package embed

func peakRSS() int64 { return -1 }
