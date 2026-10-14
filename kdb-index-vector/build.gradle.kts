plugins {
    alias(libs.plugins.kotlin.multiplatform)
}

kotlin {
    jvm()
    js(IR) {
        // VectorStoreTest.hnswRecallAtTenIsAtLeastNinetyFivePercent builds a 2,000-vector HNSW graph
        // and runs exact and approximate searches (~0.8s locally). On CI's js shard,
        // `jsTest --parallel` runs every module's karma Chrome and mocha node tests at once on a
        // 4-vCPU runner, which pushed it past mocha's default 2s timeout - a bare "Error", not a
        // recall miss. Raised as in kdb-compression and kdb-storage-chunking.
        val jsTestTimeout = "60s"
        browser { testTask { useMocha { timeout = jsTestTimeout } } }
        nodejs { testTask { useMocha { timeout = jsTestTimeout } } }
    }
    linuxX64()
    macosArm64()

    sourceSets {
        val commonMain by getting
        val nativeMain by creating {
            dependsOn(commonMain)
        }
        val linuxX64Main by getting { dependsOn(nativeMain) }
        val macosArm64Main by getting { dependsOn(nativeMain) }

        commonMain.dependencies {
            implementation(project(":kdb-codec"))
            implementation(project(":kdb-index"))
            implementation(project(":kdb-dag"))
            implementation(project(":kdb-storage"))
            implementation(project(":kdb-error"))
            implementation(project(":kdb-json"))
            implementation(project(":kdb-document"))
            implementation(libs.kotlinx.coroutines.core)
        }
        commonTest.dependencies {
            implementation(libs.kotlin.test)
            implementation(project(":kdb-dag"))
            implementation(project(":kdb-storage"))
            implementation(project(":kdb-json"))
            implementation(project(":kdb-document"))
            // The store's API is suspending; fixture parity tests need an in-memory storage adapter.
            implementation(libs.kotlinx.coroutines.test)
        }
    }
}
