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

    // Mocha's default 2s per-test timeout is too tight for CI's js shard: `jsTest --parallel` runs
    // every module's karma Chrome and node tests at once on a 4-vCPU runner, and whichever CPU-heavy
    // test is unlucky that run (ZstdCorruptionTest, AttachmentProofTest, VectorStoreTest...) fails
    // with a bare "Error" (node: "Error at node:internal/timers"). Set here, on whatever framework the
    // target chose: calling useMocha on a browser task would replace karma and stop testing in Chrome.
    tasks.withType<org.jetbrains.kotlin.gradle.targets.js.testing.KotlinJsTest>().configureEach {
        onTestFrameworkSet {
            when (this) {
                is org.jetbrains.kotlin.gradle.targets.js.testing.mocha.KotlinMocha -> timeout = "60s"
                is org.jetbrains.kotlin.gradle.targets.js.testing.karma.KotlinKarma ->
                    useConfigDirectory(rootProject.file("gradle/karma.config.d"))
                else -> {}
            }
        }
    }

    // The commit attributes below make every jar differ from one commit to the next. Without this,
    // every test task (and every compile that has a jar on its classpath) would see a changed input
    // and miss the build cache on every commit, even in a module nobody touched - which is what
    // lets CI reuse main's results.
    normalization {
        runtimeClasspath {
            metaInf {
                ignoreAttribute("Implementation-Commit")
                ignoreAttribute("Implementation-Commit-Dirty")
            }
        }
    }

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
