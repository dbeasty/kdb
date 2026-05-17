plugins {
    alias(libs.plugins.kotlin.multiplatform)
}

import org.gradle.api.file.DuplicatesStrategy

kotlin {
    js(IR) {
        browser {
            commonWebpackConfig {
                cssSupport {
                    enabled.set(true)
                }
            }
        }
        binaries.executable()
    }

    sourceSets {
        val jsMain by getting {
            dependencies {
                implementation(project(":kdb-embed"))
            }
            resources.srcDir("src/jsMain/resources")
        }
    }
}

tasks.withType<org.gradle.api.tasks.Copy>().configureEach {
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
}
