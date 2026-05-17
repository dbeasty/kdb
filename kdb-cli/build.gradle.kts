plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.plugin.serialization)
}

dependencies {
    implementation(project(":kdb-auth"))
    implementation(project(":kdb-config"))
    implementation(project(":kdb-codec"))
    implementation(project(":kdb-dag"))
    implementation(project(":kdb-document"))
    implementation(project(":kdb-file"))
    implementation(project(":kdb-error"))
    implementation(project(":kdb-embed"))
    implementation(project(":kdb-jdbc"))
    implementation(project(":kdb-peer-sync"))
    implementation(project(":kdb-transport-core"))
    implementation(project(":kdb-transport-tcp"))
    implementation(project(":kdb-hybrid-query"))
    implementation(project(":kdb-schema"))
    implementation(project(":kdb-sql"))
    implementation(project(":kdb-storage"))
    implementation(project(":kdb-transaction"))
    implementation(project(":kdb-stream"))
    implementation(project(":kdb-wire"))
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.kotlinx.coroutines.core)
    testImplementation(libs.kotlin.test)
    testImplementation(libs.kotlinx.coroutines.test)
}

tasks.register("writeCliClasspath") {
    dependsOn("jar")
    val out = layout.buildDirectory.file("cli-classpath.txt")
    outputs.file(out)
    doLast {
        out.get().asFile.writeText(sourceSets.main.get().runtimeClasspath.asPath)
    }
}

tasks.register<JavaExec>("runCli") {
    group = "application"
    description = "Run kdb CLI"
    dependsOn("jar")
    mainClass.set("dev.kdb.cli.KdbCliKt")
    classpath(sourceSets.main.get().runtimeClasspath)
    standardInput = System.`in`
}
