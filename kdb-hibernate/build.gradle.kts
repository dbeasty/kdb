plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    implementation(libs.hibernate.core)
    testImplementation(project(":kdb-jdbc"))
    testImplementation(libs.kotlin.test)
}
