plugins {
    alias(libs.plugins.kotlin.multiplatform)
}

kotlin {
    jvm()
    js(IR) { browser(); nodejs() }
    linuxX64()
    macosArm64()
    sourceSets {
        val commonMain by getting
        val nativeMain by creating { dependsOn(commonMain) }
        linuxX64Main.get().dependsOn(nativeMain)
        macosArm64Main.get().dependsOn(nativeMain)
        commonTest.dependencies { implementation(libs.kotlin.test) }
    }
}

kotlin {
    compilerOptions { freeCompilerArgs.add("-Xexpect-actual-classes") }
    sourceSets {
        jvmMain.dependencies { implementation(libs.zstd.jni) }
        jvmTest.dependencies { implementation(libs.zstd.jni) }
    }
}
