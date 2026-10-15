package sstable

import (
	"errors"
	"testing"
)

// A table whose last four bytes (the footer's index-length trailer) decode to a length longer
// than the file is damaged. Opening it and compacting it must say so, promptly, rather than size
// an allocation or a read from that length.
func TestFooterLongerThanFileIsCorrupt(t *testing.T) {
	good := testShim(t)
	h := bigTable(t, good, "app/docs", 50, 64, 1, nil)
	data := readSegment(t, good, h.SegmentName)
	data[len(data)-4], data[len(data)-3], data[len(data)-2], data[len(data)-1] = 0x1B, 0x2C, 0x3D, 0x4E // ~456 million

	shim := testShim(t)
	if _, err := shim.AppendToSegment(h.SegmentName, data); err != nil {
		t.Fatal(err)
	}
	if _, err := NewDefaultReader(shim, h).Index(); !errors.Is(err, ErrCorruptTable) {
		t.Fatalf("Index error = %v, want ErrCorruptTable", err)
	}
	if _, err := Compact(shim, "app/docs", 1, []Handle{h}, false, nil); !errors.Is(err, ErrCorruptTable) {
		t.Fatalf("Compact error = %v, want ErrCorruptTable", err)
	}
}
