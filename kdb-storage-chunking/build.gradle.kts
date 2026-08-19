plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    implementation(project(":kdb-codec"))
    implementation(project(":kdb-document"))
    testImplementation(libs.kotlin.test)
}
