rootProject.name = "kdb"

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

include(":kdb-error")
include(":kdb-codec")
