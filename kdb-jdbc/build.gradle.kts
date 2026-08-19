plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    implementation(project(":kdb-error"))
    implementation(project(":kdb-embed"))
    implementation(project(":kdb-codec"))
    implementation(project(":kdb-dag"))
    implementation(project(":kdb-document"))
    implementation(project(":kdb-hybrid-query"))
    implementation(project(":kdb-index"))
    implementation(project(":kdb-index-composite"))
    implementation(project(":kdb-namespace-policy"))
    implementation(project(":kdb-schema"))
    implementation(project(":kdb-sql"))
    implementation(project(":kdb-storage"))
    implementation(project(":kdb-storage-delta"))
    implementation(project(":kdb-storage-engine"))
    implementation(project(":kdb-storage-io"))
    implementation(project(":kdb-transaction"))
    implementation(project(":kdb-stream"))
    implementation(project(":kdb-transport-core"))
    implementation(project(":kdb-transport-ws"))
    implementation(project(":kdb-wire"))
    implementation(libs.kotlinx.coroutines.core)
    testImplementation(libs.kotlin.test)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.hikari)
    testImplementation(project(":kdb-auth"))
    testImplementation(project(":kdb-server"))
    testImplementation(project(":kdb-transport-core"))
    testImplementation(project(":kdb-transport-ws"))
}

tasks.test {
    maxParallelForks = 1
}
