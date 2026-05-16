plugins { alias(libs.plugins.kotlin.multiplatform) }

kotlin {
    jvm(); js(IR) { browser(); nodejs() }; linuxX64(); macosArm64()
    sourceSets {
        val nativeMain by creating { dependsOn(commonMain.get()) }
        linuxX64Main.get().dependsOn(nativeMain); macosArm64Main.get().dependsOn(nativeMain)
        commonMain.dependencies {
            implementation(project(":kdb-storage"))
            implementation(project(":kdb-storage-io"))
            implementation(project(":kdb-storage-wal"))
            implementation(project(":kdb-storage-memtable"))
            implementation(project(":kdb-storage-sstable"))
            implementation(project(":kdb-storage-delta"))
            implementation(project(":kdb-document"))
            implementation(project(":kdb-codec"))
            implementation(project(":kdb-error"))
            implementation(libs.kotlinx.coroutines.core)
        }
        commonTest.dependencies {
            implementation(libs.kotlin.test)
            implementation(libs.kotlinx.coroutines.test)
            implementation(project(":kdb-storage-io"))
            implementation(project(":kdb-storage"))
        }
    }
}
