package compute

import (
	"context"
	"testing"
)

func TestCPUDot(t *testing.T) {
	a := NewCPUAdapter()
	v, err := a.Dot(context.Background(), []float32{1, 2, 3}, []float32{4, 5, 6})
	if err != nil || v != 32 {
		t.Fatalf("dot=%v err=%v", v, err)
	}
}
