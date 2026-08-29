plugins {
    alias(libs.plugins.kotlin.multiplatform) apply false
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.plugin.serialization) apply false
    alias(libs.plugins.jmh) apply false
}

// Build identity for the Kotlin artifacts, mirroring what go/kdb/version injects into the Go
// binaries at link time: the version comes from the repo's VERSION file - the single source both
// sides read - and the git commit is stamped into every jar manifest, so a built artifact can be
// traced back to the exact source it came from:
//
//   unzip -p kdb-cli/build/libs/kdb-cli-0.1.0.jar META-INF/MANIFEST.MF
//
// Deliberately no build timestamp here: it would change on every build and invalidate every jar
// task's up-to-date check for no traceability the commit doesn't already give.
val kdbVersion: String = rootProject.file("VERSION").readText().trim()

fun gitOutput(vararg command: String): String {
    val output = providers.exec {
        commandLine(*command)
        isIgnoreExitValue = true
    }
    // A source tarball or a checkout without git still has to build; it just reports "unknown".
    return if (output.result.get().exitValue == 0) output.standardOutput.asText.get().trim() else ""
}

val kdbCommit: String = gitOutput("git", "rev-parse", "HEAD").ifEmpty { "unknown" }
val kdbDirty: Boolean = gitOutput("git", "status", "--porcelain").isNotEmpty()

allprojects {
    group = "dev.kdb"
    version = kdbVersion

    tasks.withType<Jar>().configureEach {
        manifest {
            attributes(
                "Implementation-Title" to project.name,
                "Implementation-Version" to kdbVersion,
                "Implementation-Vendor" to "dev.kdb",
                // Full SHA, not the short form - short SHAs stop being unique as a repo grows.
                "Implementation-Commit" to kdbCommit,
                "Implementation-Commit-Dirty" to kdbDirty.toString(),
            )
        }
    }
}
