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
        commonMain.dependencies {
            implementation(project(":kdb-storage"))
            implementation(project(":kdb-codec"))
            implementation(project(":kdb-error"))
            implementation(libs.kotlinx.coroutines.core)
        }
        nativeMain.dependencies {
            implementation(libs.kotlinx.io.core)
        }
        commonTest.dependencies {
            implementation(libs.kotlin.test)
            implementation(libs.kotlinx.coroutines.test)
            implementation(project(":kdb-storage"))
        }
        jvmTest.dependencies { implementation(project(":kdb-storage")) }
    }
}

kotlin {
    compilerOptions { freeCompilerArgs.add("-Xexpect-actual-classes") }
}

