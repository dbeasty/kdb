plugins {
    alias(libs.plugins.kotlin.multiplatform)
}

apply(from = rootProject.file("gradle/physical-golden-fixtures.gradle.kts"))

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
        val nativeMain by creating { dependsOn(commonMain) }
        linuxX64Main.get().dependsOn(nativeMain)
        macosArm64Main.get().dependsOn(nativeMain)
        val nativeTest by creating { dependsOn(commonTest.get()) }
        linuxX64Test.get().dependsOn(nativeTest)
        macosArm64Test.get().dependsOn(nativeTest)
        commonTest.dependencies { implementation(libs.kotlin.test) }
        // Go's and the JVM's zstd frames, readable from JS and Native tests (roadmap R1).
        commonTest { kotlin.srcDir(tasks.named("generatePhysicalGoldenFixtures")) }
    }
}

kotlin {
    compilerOptions { freeCompilerArgs.add("-Xexpect-actual-classes") }
    sourceSets {
        jvmMain.dependencies { implementation(libs.zstd.jni) }
        jvmTest.dependencies { implementation(libs.zstd.jni) }
    }
}
