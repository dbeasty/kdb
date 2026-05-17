package dev.kdb.compute.webgpu

import dev.kdb.compute.ComputeAdapter

public actual fun createWebGpuComputeAdapter(): ComputeAdapter? = null

public actual fun createWebGpuComputeAdapterOrCpuFallback(): ComputeAdapter = CpuFallbackComputeAdapter()
