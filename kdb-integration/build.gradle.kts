plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    testImplementation(project(":kdb-codec"))
    testImplementation(project(":kdb-index"))
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
    testImplementation(project(":kdb-auth-store"))
    testImplementation(project(":kdb-embed"))
    testImplementation(project(":kdb-server"))
    testImplementation(project(":kdb-wire"))
    testImplementation(libs.kotlin.test)
    testImplementation(libs.kotlinx.coroutines.test)
}

tasks.test {
    maxParallelForks = 1
}

val e2eDir = layout.projectDirectory.dir("e2e").asFile
val e2eVenvPython = File(e2eDir, ".venv/bin/python")

tasks.register<Exec>("bootstrapE2ePythonVenv") {
    group = "verification"
    description = "Create Python venv for E2E tests"
    workingDir = e2eDir
    commandLine("python3", "-m", "venv", ".venv")
    onlyIf { !e2eVenvPython.exists() }
}

tasks.register<Exec>("installE2ePythonDeps") {
    group = "verification"
    description = "Install Python dependencies for E2E tests"
    dependsOn("bootstrapE2ePythonVenv")
    workingDir = e2eDir
    commandLine(".venv/bin/pip", "install", "-q", "-r", "requirements.txt")
}

tasks.register<Exec>("e2ePython") {
    group = "verification"
    description = "Run Python subprocess E2E tests against kdb CLI"
    dependsOn("installE2ePythonDeps", ":kdb-cli:writeCliClasspath", ":kdb-cli:jar")
    workingDir = e2eDir
    commandLine(".venv/bin/python", "-m", "pytest", "-q")
    environment(
        "KDB_CLI_CLASSPATH_FILE",
        project(":kdb-cli").layout.buildDirectory.file("cli-classpath.txt").get().asFile.absolutePath,
    )
    isIgnoreExitValue = false
}
