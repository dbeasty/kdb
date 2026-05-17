package dev.kdb.transport.ws

import java.nio.file.Files
import java.nio.file.Path

internal data class TlsTestMaterial(
    val serverKeyStore: Path,
    val clientKeyStore: Path,
    val trustStore: Path,
    val password: String = "changeit",
)

internal object TlsTestCertificates {
    private var cached: TlsTestMaterial? = null

    fun material(): TlsTestMaterial =
        cached ?: synchronized(this) {
            cached ?: generate().also { cached = it }
        }

    private fun generate(): TlsTestMaterial {
        val dir = Files.createTempDirectory("kdb-tls-test-")
        val server = dir.resolve("server.p12")
        val client = dir.resolve("client.p12")
        val trust = dir.resolve("trust.p12")
        val password = "changeit"
        runKeytool(
            "-genkeypair",
            "-alias",
            "server",
            "-keyalg",
            "RSA",
            "-keysize",
            "2048",
            "-storetype",
            "PKCS12",
            "-keystore",
            server.toString(),
            "-storepass",
            password,
            "-keypass",
            password,
            "-validity",
            "365",
            "-dname",
            "CN=localhost",
        )
        runKeytool(
            "-genkeypair",
            "-alias",
            "client",
            "-keyalg",
            "RSA",
            "-keysize",
            "2048",
            "-storetype",
            "PKCS12",
            "-keystore",
            client.toString(),
            "-storepass",
            password,
            "-keypass",
            password,
            "-validity",
            "365",
            "-dname",
            "CN=kdb-client",
        )
        runKeytool(
            "-exportcert",
            "-alias",
            "server",
            "-keystore",
            server.toString(),
            "-storepass",
            password,
            "-file",
            dir.resolve("server.cer").toString(),
        )
        runKeytool(
            "-exportcert",
            "-alias",
            "client",
            "-keystore",
            client.toString(),
            "-storepass",
            password,
            "-file",
            dir.resolve("client.cer").toString(),
        )
        runKeytool(
            "-importcert",
            "-noprompt",
            "-alias",
            "server",
            "-file",
            dir.resolve("server.cer").toString(),
            "-keystore",
            trust.toString(),
            "-storepass",
            password,
            "-storetype",
            "PKCS12",
        )
        runKeytool(
            "-importcert",
            "-noprompt",
            "-alias",
            "client",
            "-file",
            dir.resolve("client.cer").toString(),
            "-keystore",
            trust.toString(),
            "-storepass",
            password,
            "-storetype",
            "PKCS12",
        )
        return TlsTestMaterial(
            serverKeyStore = server,
            clientKeyStore = client,
            trustStore = trust,
            password = password,
        )
    }

    private fun runKeytool(vararg args: String) {
        val keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString()
        val process =
            ProcessBuilder(listOf(keytool) + args)
                .redirectErrorStream(true)
                .start()
        val output = process.inputStream.bufferedReader().readText()
        check(process.waitFor() == 0) { "keytool failed: $output" }
    }
}
