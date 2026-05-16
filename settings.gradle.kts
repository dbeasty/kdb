rootProject.name = "kdb"

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

include(":kdb-error")
include(":kdb-codec")
include(":kdb-document")
include(":kdb-json")
include(":kdb-schema")
include(":kdb-dag")
