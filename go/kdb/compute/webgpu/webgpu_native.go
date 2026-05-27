//go:build !js

package webgpu

import (
	"context"

	"github.com/limidus/kdb/go/kdb/compute"
)

// Adapter on native targets uses CPU fallback (CUDA/Vulkan deferred).
type Adapter struct {
	cpu compute.Adapter
}

func NewAdapter() *Adapter {
	return &Adapter{cpu: compute.NewCPUAdapter()}
}

func (a *Adapter) Name() string { return "cpu-fallback" }

func (a *Adapter) Dot(ctx context.Context, x, y []float32) (float32, error) {
	return a.cpu.Dot(ctx, x, y)
}
