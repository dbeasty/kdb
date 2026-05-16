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
            implementation(project(":kdb-codec"))
            implementation(project(":kdb-dag"))
            implementation(project(":kdb-document"))
            implementation(project(":kdb-error"))
            implementation(project(":kdb-namespace-policy"))
            implementation(project(":kdb-storage"))
            implementation(libs.kotlinx.coroutines.core)
        }
        commonTest.dependencies {
            implementation(libs.kotlin.test)
            implementation(project(":kdb-dag"))
            implementation(project(":kdb-schema"))
            implementation(project(":kdb-storage"))
            implementation(project(":kdb-transaction"))
            implementation(libs.kotlinx.coroutines.test)
        }
    }
}
