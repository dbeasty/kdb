plugins {
    alias(libs.plugins.kotlin.multiplatform)
    alias(libs.plugins.kotlin.plugin.serialization)
}

kotlin {
    jvm()
    js(IR) {
        browser()
        nodejs()
        binaries.executable()
    }

    sourceSets {
        val commonMain by getting {
            dependencies {
                implementation(project(":kdb-codec"))
                implementation(project(":kdb-error"))
                implementation(project(":kdb-dag"))
                implementation(project(":kdb-document"))
                implementation(project(":kdb-hybrid-query"))
                implementation(project(":kdb-index"))
                implementation(project(":kdb-index-composite"))
                implementation(project(":kdb-json"))
                implementation(project(":kdb-namespace-policy"))
                implementation(project(":kdb-schema"))
                implementation(project(":kdb-transaction"))
                implementation(project(":kdb-sql"))
                implementation(project(":kdb-storage"))
                implementation(project(":kdb-peer-sync"))
                implementation(libs.kotlinx.coroutines.core)
                implementation(libs.kotlinx.serialization.json)
            }
        }
        val commonTest by getting {
            dependencies {
                implementation(libs.kotlin.test)
                implementation(libs.kotlinx.coroutines.test)
            }
        }
        val jvmMain by getting {
            dependencies {
                implementation(project(":kdb-auth"))
                implementation(project(":kdb-stream"))
                implementation(project(":kdb-transport-core"))
                implementation(project(":kdb-transport-ws"))
            }
        }
        val jsMain by getting {
            dependencies {
                implementation(project(":kdb-auth"))
                implementation(project(":kdb-transport-core"))
                implementation(project(":kdb-stream"))
                implementation(project(":kdb-transaction"))
                implementation(project(":kdb-transport-ws"))
                implementation(project(":kdb-wire"))
            }
        }
        val jsTest by getting {
            dependencies {
                implementation(libs.kotlin.test)
                implementation(libs.kotlinx.coroutines.test)
            }
        }
    }
}
