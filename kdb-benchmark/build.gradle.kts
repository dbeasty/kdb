plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.jmh)
}

dependencies {
    jmh(libs.jmh.core)
    annotationProcessor(libs.jmh.generator)
    implementation(project(":kdb-cli"))
    implementation(project(":kdb-embed"))
    implementation(project(":kdb-jdbc"))
    implementation(project(":kdb-codec"))
    implementation(project(":kdb-dag"))
    implementation(project(":kdb-document"))
    implementation(project(":kdb-index"))
    implementation(project(":kdb-index-composite"))
    implementation(project(":kdb-schema"))
    implementation(project(":kdb-hybrid-query"))
    implementation(project(":kdb-storage"))
    implementation(project(":kdb-transaction"))
    implementation(libs.kotlinx.coroutines.core)
}

jmh {
    warmupIterations.set(3)
    iterations.set(5)
    fork.set(2)
    benchmarkMode.set(listOf("avgt"))
    timeUnit.set("ms")
    resultFormat.set("TEXT")
    includes.set(listOf("dev.kdb.benchmark.*"))
}
