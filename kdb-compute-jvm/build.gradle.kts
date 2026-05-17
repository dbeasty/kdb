plugins {
    alias(libs.plugins.kotlin.jvm)
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
