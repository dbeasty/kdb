plugins {
    alias(libs.plugins.kotlin.multiplatform)
}

kotlin {
    jvm()
    js(IR) {
        browser()
        nodejs()
    }

    sourceSets {
        val commonMain by getting
        val jvmMain by getting
        val jsMain by getting

        commonMain.dependencies {
            implementation(project(":kdb-error"))
            implementation(project(":kdb-stream"))
            implementation(project(":kdb-transport-core"))
            implementation(project(":kdb-wire"))
            implementation(libs.kotlinx.coroutines.core)
        }
        jvmMain.dependencies {}
        jsMain.dependencies {}
        commonTest.dependencies {
            implementation(libs.kotlin.test)
            implementation(libs.kotlinx.coroutines.test)
        }
        jvmTest.dependencies {
            implementation(libs.kotlin.test)
            implementation(libs.kotlinx.coroutines.test)
        }
    }
}
