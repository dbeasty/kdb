//go:build js && wasm

package webgpu

import (
	"context"
	"syscall/js"

	"github.com/limidus/kdb/go/kdb/compute"
)

// Adapter delegates to WebGPU via syscall/js when available, else CPU fallback.
type Adapter struct {
	fallback compute.Adapter
}

func NewAdapter() *Adapter {
	return &Adapter{fallback: compute.NewCPUAdapter()}
}

func (a *Adapter) Name() string { return "webgpu-wasm" }

func (a *Adapter) Dot(ctx context.Context, x, y []float32) (float32, error) {
	gpu := js.Global().Get("navigator").Get("gpu")
	if gpu.IsUndefined() || gpu.IsNull() {
		return a.fallback.Dot(ctx, x, y)
	}
	// Full WebGPU pipeline deferred; CPU fallback preserves API parity.
	return a.fallback.Dot(ctx, x, y)
}
