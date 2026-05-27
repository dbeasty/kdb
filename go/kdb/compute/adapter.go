package compute

import "context"

// Adapter runs bulk numeric work (vector ops, filters).
type Adapter interface {
	Name() string
	Dot(ctx context.Context, a, b []float32) (float32, error)
}

// CPUAdapter is the portable CPU backend.
type CPUAdapter struct{}

func NewCPUAdapter() *CPUAdapter { return &CPUAdapter{} }

func (c *CPUAdapter) Name() string { return "cpu" }

func (c *CPUAdapter) Dot(_ context.Context, a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errDimMismatch
	}
	var s float32
	for i := range a {
		s += a[i] * b[i]
	}
	return s, nil
}
