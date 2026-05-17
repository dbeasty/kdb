package dev.kdb.compute.webgpu

import dev.kdb.compute.ComputeAdapter

public expect fun createWebGpuComputeAdapter(): ComputeAdapter?

public expect fun createWebGpuComputeAdapterOrCpuFallback(): ComputeAdapter
