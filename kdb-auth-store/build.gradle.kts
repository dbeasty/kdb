plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.plugin.serialization)
}

dependencies {
    implementation(project(":kdb-auth"))
    implementation(project(":kdb-codec"))
    implementation(project(":kdb-document"))
    implementation(project(":kdb-dag"))
    implementation(project(":kdb-schema"))
    implementation(project(":kdb-storage"))
    implementation(project(":kdb-transaction"))
    implementation(project(":kdb-error"))
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.kotlinx.coroutines.core)
    testImplementation(libs.kotlin.test)
    testImplementation(libs.kotlinx.coroutines.test)
}
