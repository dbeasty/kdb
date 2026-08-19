plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.plugin.serialization)
}

// JVM-only by design (Component 32 spec, §1.2): stored procedures run in the
// backend process only; browser/embedded targets only ever call them over the
// wire, so this module needs no expect/actual scripting engine.
dependencies {
    implementation(project(":kdb-error"))
    implementation(project(":kdb-auth"))
    implementation(project(":kdb-codec"))
    implementation(project(":kdb-document"))
    implementation(project(":kdb-schema"))
    implementation(project(":kdb-dag"))
    implementation(project(":kdb-storage"))
    implementation(project(":kdb-transaction"))
    implementation(project(":kdb-sql"))
    implementation(project(":kdb-hybrid-query"))
    implementation(project(":kdb-index"))
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.graalvm.polyglot)
    implementation(libs.graalvm.js)

    testImplementation(project(":kdb-index-composite"))
    testImplementation(project(":kdb-namespace-policy"))
    testImplementation(libs.kotlin.test)
    testImplementation(libs.kotlinx.coroutines.test)
}
