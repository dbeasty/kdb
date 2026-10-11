plugins {
    alias(libs.plugins.kotlin.multiplatform)
}

apply(from = rootProject.file("gradle/physical-golden-fixtures.gradle.kts"))

kotlin {
    jvm(); js(IR) { browser(); nodejs() }; linuxX64(); macosArm64()
    sourceSets {
        val nativeMain by creating { dependsOn(commonMain.get()) }
        linuxX64Main.get().dependsOn(nativeMain); macosArm64Main.get().dependsOn(nativeMain)
        val nativeTest by creating { dependsOn(commonTest.get()) }
        linuxX64Test.get().dependsOn(nativeTest); macosArm64Test.get().dependsOn(nativeTest)
        commonMain.dependencies {
            implementation(project(":kdb-storage-io"))
            implementation(project(":kdb-storage"))
            implementation(project(":kdb-compression"))
            implementation(project(":kdb-codec"))
            implementation(project(":kdb-error"))
            implementation(project(":kdb-document"))
            implementation(libs.kotlinx.coroutines.core)
        }
        commonTest.dependencies { implementation(libs.kotlin.test); implementation(libs.kotlinx.coroutines.test) }
        // Go's and the JVM's zstd-compressed segments, readable from JS and Native tests (roadmap R1).
        commonTest { kotlin.srcDir(tasks.named("generatePhysicalGoldenFixtures")) }
    }
}
