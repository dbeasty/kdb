plugins {
    alias(libs.plugins.kotlin.multiplatform)
    alias(libs.plugins.kotlin.plugin.serialization)
}

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
        val commonTest by getting
        val jvmMain by getting
        val jvmTest by getting
        val nativeMain by creating {
            dependsOn(commonMain)
        }
        val linuxX64Main by getting { dependsOn(nativeMain) }
        val macosArm64Main by getting { dependsOn(nativeMain) }

        commonMain.dependencies {
            implementation(project(":kdb-codec"))
            implementation(project(":kdb-document"))
            implementation(project(":kdb-storage"))
            implementation(project(":kdb-storage-delta"))
            implementation(project(":kdb-compression"))
            implementation(project(":kdb-wire"))
            implementation(project(":kdb-compaction"))
            implementation(project(":kdb-index"))
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.kotlinx.coroutines.core)
        }
        commonTest.dependencies {
            implementation(libs.kotlin.test)
            implementation(libs.kotlinx.coroutines.test)
            implementation(project(":kdb-storage-io"))
        }
        jvmMain.dependencies {
            implementation(project(":kdb-storage-io"))
        }
        jvmTest.dependencies {
            implementation(libs.kotlin.test)
        }
    }
}

tasks.register<JavaExec>("inspectCli") {
    group = "application"
    description = "Run kdb inspect CLI"
    dependsOn("jvmJar")
    mainClass.set("dev.kdb.inspect.cli.InspectMainKt")
    classpath(
        kotlin.jvm().compilations.getByName("main").runtimeDependencyFiles,
        kotlin.jvm().compilations.getByName("main").output.allOutputs,
    )
    standardInput = System.`in`
}
