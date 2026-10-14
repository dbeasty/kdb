plugins {
    alias(libs.plugins.kotlin.multiplatform)
}

apply(from = rootProject.file("gradle/physical-golden-fixtures.gradle.kts"))

kotlin {
    jvm()
    js(IR) {
        // ZstdCorruptionTest decodes ~1,400 damaged frames per test (~0.4s on a fast machine). On
        // CI's js shard, `jsTest --parallel` runs every module's karma Chrome and mocha node tests at
        // once on a 4-vCPU runner, which pushed damagedGoldenFramesFailTyped past mocha's default
        // 2s timeout on both js,node and js,browser - a bare "Error", not a wrong exception type.
        // Raised as in kdb-storage-chunking rather than shrinking the mutation count.
        val jsTestTimeout = "60s"
        browser { testTask { useMocha { timeout = jsTestTimeout } } }
        nodejs { testTask { useMocha { timeout = jsTestTimeout } } }
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
