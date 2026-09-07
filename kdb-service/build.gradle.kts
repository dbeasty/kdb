plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    implementation(project(":kdb-codec"))
    implementation(project(":kdb-auth"))
    implementation(project(":kdb-auth-static"))
    implementation(project(":kdb-auth-store"))
    implementation(project(":kdb-config"))
    implementation(project(":kdb-document"))
    implementation(project(":kdb-embed"))
    implementation(project(":kdb-server"))
    implementation(project(":kdb-jdbc"))
    implementation(project(":kdb-namespace-policy"))
    implementation(project(":kdb-peer-sync"))
    implementation(project(":kdb-stream"))
    implementation(project(":kdb-dag"))
    implementation(project(":kdb-schema"))
    implementation(project(":kdb-storage"))
    implementation(project(":kdb-transaction"))
    implementation(project(":kdb-transport-core"))
    implementation(project(":kdb-transport-ws"))
    implementation(project(":kdb-wire"))
    implementation(libs.kotlinx.coroutines.core)
    testImplementation(libs.kotlin.test)
}

tasks.register<JavaExec>("runService") {
    group = "application"
    description = "Run KDB network service (WebSocket peer sync)"
    dependsOn("jar")
    mainClass.set("dev.kdb.service.KdbServiceMainKt")
    classpath(sourceSets.main.get().runtimeClasspath)
}
