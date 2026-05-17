package dev.kdb.cli

import dev.kdb.codec.KdbUuid
import dev.kdb.jdbc.file.openFileRuntime
import java.nio.file.Files
import kotlin.io.path.createTempDirectory
import kotlin.io.path.writeBytes
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking

class FileCliTest {
  @Test
  fun filePutGet_survivesReopen() {
    val dir = createTempDirectory("kdb-file-cli").toString()
    val fileId = "00000000-0000-0000-0000-0000000000aa"
    val src = createTempDirectory("kdb-file-src").resolve("payload.bin")
    val payload = byteArrayOf(9, 8, 7, 6, 5)
    src.writeBytes(payload)
    val out = createTempDirectory("kdb-file-out").resolve("out.bin")

    assertEquals(0, KdbCli.run(arrayOf("--data-dir", dir, "init", "app/files")))
    assertEquals(
        0,
        KdbCli.run(
            arrayOf(
                "--data-dir",
                dir,
                "file",
                "put",
                "app/files",
                "--id",
                fileId,
                src.toString(),
            ),
        ),
    )
    assertEquals(
        0,
        KdbCli.run(
            arrayOf(
                "--data-dir",
                dir,
                "file",
                "get",
                "app/files",
                "--id",
                fileId,
                "-o",
                out.toString(),
            ),
        ),
    )
    assertTrue(payload.contentEquals(Files.readAllBytes(out)))

    runBlocking {
      val rt = openFileRuntime(dir, "app", "app/files")
      val head = rt.dag.head()
      val doc = rt.storage.getDocument("app/files", KdbUuid.fromString(fileId), head)
      assertTrue(doc!!.json.contains("\"kdbKind\":\"kdb.file\""))
    }
  }
}
