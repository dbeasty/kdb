package embed

import "testing"

func TestWindowsUnsafeSegment(t *testing.T) {
	for seg, unsafe := range map[string]bool{
		"orders": false, "con1": false, "console": false, "a.b": false, "COM10": false,
		"con": true, "CON": true, "nul.txt": true, "Aux.json": true, "lpt9": true, "com1": true,
		"trailing.": true, "..x.": true,
	} {
		if got := windowsUnsafeSegment(seg) != ""; got != unsafe {
			t.Errorf("windowsUnsafeSegment(%q) unsafe = %v, want %v", seg, got, unsafe)
		}
	}
}
