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
            implementation(project(":kdb-error"))
            implementation(project(":kdb-storage"))
            implementation(project(":kdb-stream"))
            implementation(project(":kdb-transport-core"))
            implementation(project(":kdb-transaction"))
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
