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
        commonMain.dependencies {
            implementation(project(":kdb-codec"))
            implementation(project(":kdb-error"))
            implementation(project(":kdb-index"))
            implementation(project(":kdb-index-vector"))
            implementation(project(":kdb-storage"))
        }
    }
}
