plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.plugin.serialization)
}

dependencies {
    implementation(project(":kdb-codec"))
    implementation(project(":kdb-dag"))
    implementation(project(":kdb-document"))
    implementation(project(":kdb-embed"))
    implementation(project(":kdb-error"))
    implementation(project(":kdb-hybrid-query"))
    implementation(project(":kdb-namespace-policy"))
    implementation(project(":kdb-schema"))
    implementation(project(":kdb-sql"))
    implementation(project(":kdb-storage"))
    implementation(project(":kdb-transaction"))
    implementation(project(":kdb-wire"))
    implementation(project(":kdb-stream"))
    implementation(project(":kdb-transport-ws"))
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.serialization.json)
    testImplementation(libs.kotlin.test)
    testImplementation(libs.kotlinx.coroutines.test)
}
