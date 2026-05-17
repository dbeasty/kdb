plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    testImplementation(project(":kdb-codec"))
    testImplementation(project(":kdb-dag"))
    testImplementation(project(":kdb-document"))
    testImplementation(project(":kdb-schema"))
    testImplementation(project(":kdb-storage"))
    testImplementation(project(":kdb-transaction"))
    testImplementation(project(":kdb-hybrid-query"))
    testImplementation(project(":kdb-sql"))
    testImplementation(project(":kdb-jdbc"))
    testImplementation(project(":kdb-peer-sync"))
    testImplementation(project(":kdb-auth-static"))
    testImplementation(project(":kdb-stream"))
    testImplementation(project(":kdb-transport-core"))
    testImplementation(project(":kdb-transport-tcp"))
    testImplementation(project(":kdb-transport-ws"))
    testImplementation(project(":kdb-auth"))
    testImplementation(project(":kdb-auth-static"))
    testImplementation(project(":kdb-embed"))
    testImplementation(project(":kdb-server"))
    testImplementation(project(":kdb-wire"))
    testImplementation(libs.kotlin.test)
    testImplementation(libs.kotlinx.coroutines.test)
}

tasks.test {
    maxParallelForks = 1
}
