package dev.kdb.compute.webgpu

import dev.kdb.compute.ComputeAdapter

public actual fun createWebGpuComputeAdapter(): ComputeAdapter? =
    if (isWebGpuAvailable()) {
        CpuFallbackComputeAdapter()
    } else {
        null
    }

public actual fun createWebGpuComputeAdapterOrCpuFallback(): ComputeAdapter =
    createWebGpuComputeAdapter() ?: CpuFallbackComputeAdapter()

public fun isWebGpuAvailable(): Boolean =
    js(
        """
        (function() {
          try {
            return typeof navigator !== 'undefined' && navigator.gpu != null;
          } catch (e) { return false; }
        })()
        """,
    ) as Boolean
