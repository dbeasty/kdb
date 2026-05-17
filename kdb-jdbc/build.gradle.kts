plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
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
    implementation(project(":kdb-transaction"))
    implementation(libs.kotlinx.coroutines.core)
    testImplementation(libs.kotlin.test)
    testImplementation(libs.kotlinx.coroutines.test)
}
