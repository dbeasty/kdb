plugins {
    alias(libs.plugins.kotlin.multiplatform)
}

kotlin {
    jvm()
    js(IR) {
        browser()
        nodejs()
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
            implementation(project(":kdb-auth"))
            implementation(project(":kdb-codec"))
            implementation(project(":kdb-dag"))
            implementation(project(":kdb-document"))
            // api, not implementation: component 39's public surface exposes both of these
            // directly (PeerSyncResult.conflict: ConflictReport?, PeerHostConfig/
            // PeerClientConfig.conflictPolicy: ConflictPolicy) - a consumer calling
            // pullMissing()/reading PeerHostConfig needs these types resolvable without a
            // separate direct dependency on kdb-error/kdb-transaction.
            api(project(":kdb-error"))
            api(project(":kdb-transaction"))
            implementation(project(":kdb-storage"))
            implementation(project(":kdb-stream"))
            implementation(project(":kdb-transport-core"))
            implementation(project(":kdb-wire"))
            implementation(libs.kotlinx.coroutines.core)
        }
        commonTest.dependencies {
            implementation(libs.kotlin.test)
            implementation(libs.kotlinx.coroutines.test)
        }
        val jvmTest by getting {
            dependencies {
                implementation(project(":kdb-auth-static"))
            }
        }
    }
}
