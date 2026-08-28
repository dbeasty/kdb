package dev.kdb.inspect.cli

import dev.kdb.integrity.Action
import dev.kdb.integrity.Classification
import dev.kdb.integrity.Finding
import dev.kdb.integrity.Level
import dev.kdb.integrity.Options
import dev.kdb.integrity.Report
import dev.kdb.integrity.repair
import dev.kdb.integrity.verify
import dev.kdb.recovery.Source
import dev.kdb.recovery.hybridRestore
import dev.kdb.storage.CompressionCodec
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.io.FileBackedPlatformIoShimFactory
import dev.kdb.storage.io.PlatformIoConfig
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlin.system.exitProcess

/**
 * Opens a plain, unlocked, OS-backed segment shim rooted at dataDir - no
 * S3 replication, no directory lock. These maintenance commands are meant
 * to be run against a data directory whose owning process (kdb-service,
 * an embedded runtime) is stopped; unlike EmbeddedKdbRuntime's file
 * runtime they do not enforce that themselves, matching the existing
 * kdb-inspect dump-* commands' precedent of operating directly on bytes
 * on disk.
 */
private fun openDirShim(dataDir: String): PlatformIoShim =
    FileBackedPlatformIoShimFactory.open(PlatformIoConfig(rootDirectory = dataDir, fsyncOnFlush = true))

private fun parseCompression(s: String?): CompressionCodec =
    when (s) {
        null, "", "zstd" -> CompressionCodec.ZSTD
        "none" -> CompressionCodec.NONE
        else -> error("unknown --codec '$s' (want zstd or none)")
    }

private fun parseLevel(s: String?): Level =
    when (s) {
        null, "", "L2" -> Level.L2
        "L1" -> Level.L1
        else -> error("unknown --level '$s' (want L1 or L2)")
    }

private val json = Json { prettyPrint = true; encodeDefaults = true }

@Serializable
private data class FindingDto(
    val level: String,
    val segment: Long,
    val offset: Int,
    val classification: String,
    val detail: String,
    val commitHash: String,
)

@Serializable
private data class SegmentSummaryDto(val sequence: Long, val sizeBytes: Long, val frameCount: Int)

@Serializable
private data class ReportDto(val namespaceId: String, val segments: List<SegmentSummaryDto>, val findings: List<FindingDto>)

private fun Report.toDto(): ReportDto =
    ReportDto(
        namespaceId = namespaceId,
        segments = segments.map { SegmentSummaryDto(it.sequence, it.sizeBytes, it.frameCount) },
        findings = findings.map { FindingDto(it.level.name, it.segment, it.offset, it.classification.name, it.detail, it.commitHash) },
    )

internal fun verifyCmd(args: List<String>) =
    runBlocking {
        val dataDir = argValue(args, "--data-dir") ?: error("usage: inspect verify --data-dir DIR --namespace NS [--level L1|L2] [--json]")
        val namespace = argValue(args, "--namespace") ?: error("--namespace required")
        val level = parseLevel(argValue(args, "--level"))
        val asJson = args.contains("--json")

        val shim = openDirShim(dataDir)
        val report = verify(shim, namespace, Options(level))

        if (asJson) {
            println(json.encodeToString(report.toDto()))
        } else {
            printReport(report)
        }
        if (!report.isClean) exitProcess(1)
    }

private fun printReport(report: Report) {
    println("namespace ${report.namespaceId}: ${report.segments.size} segment(s) scanned")
    for (s in report.segments) {
        println("  segment ${s.sequence}: ${s.sizeBytes} byte(s), ${s.frameCount} frame(s)")
    }
    if (report.isClean) {
        println("clean: no findings")
        return
    }
    println("${report.findings.size} finding(s):")
    for (f in report.findings) {
        println("  [${f.level}] ${f.classification} segment=${f.segment} offset=${f.offset}: ${f.detail}")
    }
}

internal fun repairSegmentsCmd(args: List<String>) =
    runBlocking {
        val dataDir = argValue(args, "--data-dir") ?: error("usage: inspect repair-segments --data-dir DIR --namespace NS [--dry-run]")
        val namespace = argValue(args, "--namespace") ?: error("--namespace required")
        val dryRun = args.contains("--dry-run")

        val shim = openDirShim(dataDir)
        val opts = Options(Level.L1)
        val report = verify(shim, namespace, opts)
        if (report.isClean) {
            println("clean: no repair needed")
            return@runBlocking
        }
        if (dryRun) {
            println("--dry-run: would attempt to repair:")
            printReport(report)
            return@runBlocking
        }

        val result = repair(shim, report, opts)
        for (step in result.steps) {
            when (step.action) {
                Action.REFUSED -> {
                    println("REFUSED segment ${step.finding.segment}: ${step.detail}")
                    println("  missing commit(s): ${step.missingHashes}")
                    println("  run 'inspect restore' to rebuild from a backup or peer instead")
                }
                else -> println("${step.action} segment ${step.finding.segment}: ${step.detail}")
            }
        }
        if (result.anyRefused) exitProcess(1)
    }

internal fun restoreCmd(args: List<String>) =
    runBlocking {
        val namespace = argValue(args, "--namespace")
        val outDir = argValue(args, "--out")
        val sourceArgs = argValues(args, "--source")
        if (namespace == null || outDir == null || sourceArgs.isEmpty()) {
            error(
                "usage: inspect restore --namespace NS --out DIR --source LABEL=PATH [--source LABEL=PATH ...] [--codec zstd|none]\n" +
                    "  Sources are read in the order given; a damaged local data directory and a\n" +
                    "  backup directory are both just paths - list both to hybrid-restore.",
            )
        }
        val compression = parseCompression(argValue(args, "--codec"))

        val sources =
            sourceArgs.map { sa ->
                val idx = sa.indexOf('=')
                require(idx > 0 && idx < sa.length - 1) { "--source must be LABEL=PATH, got '$sa'" }
                val label = sa.substring(0, idx)
                val path = sa.substring(idx + 1)
                Source(label, openDirShim(path))
            }

        val outShim = openDirShim(outDir)
        val result = hybridRestore(sources, namespace, compression, outShim)

        println("restored namespace $namespace to $outDir")
        println("  sources contributing data: ${result.sourcesUsed}")
        println("  commits applied: ${result.appliedCount}")
        if (result.missingHashes.isNotEmpty()) {
            println("  WARNING: ${result.missingHashes.size} commit(s) could not be resolved from any source and were not applied:")
            for (h in result.missingHashes) println("    $h")
            println("  add another --source (a peer, an older backup) that has them, or accept this as a partial restore")
        }
    }

private fun argValues(args: List<String>, name: String): List<String> {
    val out = mutableListOf<String>()
    var i = 0
    while (i < args.size) {
        if (args[i] == name && i + 1 < args.size) {
            out += args[i + 1]
            i++
        }
        i++
    }
    return out
}
