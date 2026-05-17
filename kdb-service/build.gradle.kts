plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    implementation(project(":kdb-document"))
    implementation(project(":kdb-embed"))
    implementation(project(":kdb-server"))
    implementation(project(":kdb-jdbc"))
    implementation(project(":kdb-peer-sync"))
    implementation(project(":kdb-dag"))
    implementation(project(":kdb-schema"))
    implementation(project(":kdb-storage"))
    implementation(project(":kdb-transaction"))
    implementation(project(":kdb-transport-ws"))
    implementation(project(":kdb-wire"))
    implementation(libs.kotlinx.coroutines.core)
}

tasks.register<JavaExec>("runService") {
    group = "application"
    description = "Run KDB network service (WebSocket peer sync)"
    dependsOn("jar")
    mainClass.set("dev.kdb.service.KdbServiceMainKt")
    classpath(sourceSets.main.get().runtimeClasspath)
}
