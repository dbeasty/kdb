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
            implementation(project(":kdb-error"))
            implementation(project(":kdb-index"))
            implementation(project(":kdb-index-vector"))
            implementation(project(":kdb-storage"))
        }
    }
}
