plugins {
    alias(libs.plugins.kotlin.multiplatform)
    alias(libs.plugins.kotlin.plugin.serialization)
}

kotlin {
    jvm()

    js(IR) {
        // AttachmentProofTest.merkleTreeMatchesRfc6962 checks every inclusion proof of every tree up to
        // 70 leaves against the recursive RFC 6962 reference (~0.4s locally). On CI's js shard,
        // `jsTest --parallel` runs every module's karma Chrome and mocha node tests at once on a
        // 4-vCPU runner, which pushed it past mocha's default 2s timeout - a bare "Error", not a
        // failed assertion. Raised as in kdb-compression and kdb-storage-chunking.
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
        val linuxX64Main by getting {
            dependsOn(nativeMain)
        }
        val macosArm64Main by getting {
            dependsOn(nativeMain)
        }

        commonMain.dependencies {
            implementation(project(":kdb-codec"))
            implementation(project(":kdb-error"))
            implementation(libs.kotlinx.serialization.json)
        }

        commonTest.dependencies {
            implementation(libs.kotlin.test)
        }
    }
}
