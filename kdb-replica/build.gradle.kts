plugins {
    alias(libs.plugins.kotlin.multiplatform)
    alias(libs.plugins.kotlin.plugin.serialization)
}

// The Kotlin client replica (docs/kdb-client-replica-plan.md §3.3): namespaces held on a device
// and kept in sync with a KDB node over peer sync v2. A port of packages/kdb-replica (TS).
//
// JVM only for now. Everything but the tests is in commonMain, so adding js()/native targets is a
// matter of the dependencies' targets (kdb-transport-ws has jvm and js). Android is a JVM consumer
// of this module; it is not built here (no Android target in the Gradle build yet, plan §3.3).
kotlin {
    jvm()

    sourceSets {
        commonMain.dependencies {
            implementation(project(":kdb-codec"))
            implementation(project(":kdb-compression"))
            implementation(project(":kdb-document"))
            implementation(project(":kdb-stream"))
            implementation(project(":kdb-transport-core"))
            implementation(project(":kdb-transport-ws"))
            implementation(project(":kdb-wire"))
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlinx.serialization.json)
        }
        // On disk, full-mode namespaces live in a Kotlin file runtime (FileReplicaNamespace).
        jvmMain.dependencies {
            implementation(project(":kdb-dag"))
            implementation(project(":kdb-embed"))
            implementation(project(":kdb-jdbc"))
            implementation(project(":kdb-schema"))
            implementation(project(":kdb-storage"))
        }
        commonTest.dependencies {
            implementation(libs.kotlin.test)
            implementation(libs.kotlinx.coroutines.test)
        }
    }
}

// The live interop tests build and spawn go/cmd/kdb-replicahost; they find the repo by walking up
// from the working directory, and skip when there is no Go toolchain.
tasks.withType<Test>().configureEach {
    workingDir = projectDir
    maxHeapSize = "1g"
}
