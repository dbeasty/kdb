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
        val jsMain by getting

        commonMain.dependencies {
            implementation(project(":kdb-codec"))
            implementation(project(":kdb-compute"))
            implementation(project(":kdb-compression"))
            implementation(project(":kdb-error"))
            implementation(project(":kdb-index"))
            implementation(project(":kdb-index-vector"))
            implementation(project(":kdb-storage"))
            implementation(libs.kotlinx.coroutines.core)
        }
        jsMain.dependencies {}
        jsTest.dependencies {
            implementation(libs.kotlin.test)
            implementation(libs.kotlinx.coroutines.test)
        }
    }
}
