plugins {
    alias(libs.plugins.kotlin.jvm)
}

// This project's name ("kdb-compute-jvm") collides with the jvm-target jar Gradle's Kotlin
// Multiplatform plugin auto-names for the *separate* :kdb-compute module ("<project>-<target>" =
// "kdb-compute-jvm" too) - both would otherwise write build/libs/kdb-compute-jvm-<version>.jar,
// silently overwriting one with the other in anything that collects jars by filename (e.g. a
// release; see scripts/collect-kotlin-jars.sh). Renaming this side rather than :kdb-compute's
// keeps the KMP per-target naming convention that every other multiplatform module follows.
tasks.withType<Jar>().configureEach {
    archiveBaseName.set("kdb-compute-jvm-adapter")
}

dependencies {
    implementation(project(":kdb-codec"))
    implementation(project(":kdb-compute"))
    implementation(project(":kdb-compression"))
    implementation(project(":kdb-error"))
    implementation(project(":kdb-index"))
    implementation(project(":kdb-index-vector"))
    implementation(project(":kdb-storage"))
    implementation(libs.kotlinx.coroutines.core)
    testImplementation(libs.kotlin.test)
    testImplementation(libs.kotlinx.coroutines.test)
}
