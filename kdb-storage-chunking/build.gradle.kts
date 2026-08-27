plugins {
    alias(libs.plugins.kotlin.multiplatform)
}

kotlin {
    jvm()

    js(IR) {
        // ingestRevisions_dedupNearDuplicates_thenGcAfterHistoryPrune does real content-defined
        // chunking (rolling hash) over ~12MB of random data across three blobs plus a GC sweep -
        // legitimately CPU-heavy work, appropriate for what it's testing (dedup/GC correctness
        // over realistic-sized input), but Kotlin/JS's default Mocha test timeout (2000ms) can be
        // too tight for it on a slower/shared CI runner even though it finishes well within time
        // on a fast local machine - confirmed failing consistently in CI (both js,node and
        // js,browser targets) while never reproducing locally. Raised generously rather than
        // shrinking the test's input size, which would weaken what it actually verifies.
        val jsTestTimeout = "60s"
        browser {
            testTask {
                useMocha { timeout = jsTestTimeout }
            }
        }
        nodejs {
            testTask {
                useMocha { timeout = jsTestTimeout }
            }
        }
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
            implementation(project(":kdb-compaction"))
            implementation(project(":kdb-document"))
            implementation(project(":kdb-storage"))
            implementation(libs.kotlinx.coroutines.core)
        }

        commonTest.dependencies {
            implementation(libs.kotlin.test)
            implementation(libs.kotlinx.coroutines.test)
        }

        val jvmTest by getting {
            dependencies {
                implementation(project(":kdb-dag"))
                implementation(project(":kdb-document"))
                implementation(project(":kdb-file"))
            }
        }
    }
}
