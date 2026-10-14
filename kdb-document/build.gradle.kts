plugins {
    alias(libs.plugins.kotlin.multiplatform)
    alias(libs.plugins.kotlin.plugin.serialization)
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
