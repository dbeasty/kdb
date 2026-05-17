package dev.kdb.transport.ws

import dev.kdb.error.TransportException
import dev.kdb.transport.core.TransportTlsSettings
import java.io.FileInputStream
import java.net.InetSocketAddress
import java.security.KeyStore
import java.security.SecureRandom
import java.security.cert.X509Certificate
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLParameters
import javax.net.ssl.SSLServerSocket
import javax.net.ssl.SSLSocket
import javax.net.ssl.TrustManager
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509TrustManager

internal object JvmTransportTls {
    fun resolveTlsSettings(
        secure: Boolean,
        tls: TransportTlsSettings?,
    ): TransportTlsSettings? {
        if (!secure) return null
        if (tls == null || !tls.enabled) {
            throw TransportException(
                "secure WebSocket URI (kdb-wss/wss) requires TLS settings " +
                    "(TransportConnectOptions.tls or product config tls block)",
            )
        }
        return tls
    }

    fun createClientSocket(
        host: String,
        port: Int,
        settings: TransportTlsSettings,
        connectTimeoutMs: Long,
    ): SSLSocket {
        if (settings.keyStorePath == null && settings.trustStorePath == null && !settings.trustAll) {
            throw TransportException(
                "TLS client requires trustStorePath or trustAll=true (tests only)",
            )
        }
        val context = sslContext(settings, clientMode = true)
        val socket =
            (context.socketFactory.createSocket() as SSLSocket).apply {
                connect(InetSocketAddress(host, port), connectTimeoutMs.toInt().coerceAtLeast(1))
                tcpNoDelay = true
                val params: SSLParameters = sslParameters
                params.endpointIdentificationAlgorithm =
                    if (settings.trustAll) null else "HTTPS"
                sslParameters = params
            }
        socket.startHandshake()
        return socket
    }

    fun createServerSocket(
        host: String,
        portHint: Int,
        settings: TransportTlsSettings,
    ): SSLServerSocket {
        val keyStorePath =
            settings.keyStorePath
                ?: throw TransportException("TLS server requires keyStorePath")
        val context = sslContext(settings, clientMode = false)
        val serverSocket =
            (context.serverSocketFactory.createServerSocket() as SSLServerSocket).apply {
                reuseAddress = true
                bind(InetSocketAddress(host, portHint), 128)
                if (settings.requireClientAuth) {
                    needClientAuth = true
                }
            }
        return serverSocket
    }

    private fun sslContext(
        settings: TransportTlsSettings,
        clientMode: Boolean,
    ): SSLContext {
        val keyManagers =
            settings.keyStorePath?.let { path ->
                val ks = loadKeyStore(path, settings.keyStorePassword, settings.keyStoreType)
                val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
                kmf.init(ks, settings.keyStorePassword?.toCharArray() ?: CharArray(0))
                kmf.keyManagers
            }
        val trustManagers = trustManagers(settings)
        val context = SSLContext.getInstance("TLS")
        context.init(keyManagers, trustManagers, SecureRandom())
        return context
    }

    private fun trustManagers(settings: TransportTlsSettings): Array<TrustManager>? {
        if (settings.trustAll) {
            return arrayOf(TrustAllX509TrustManager)
        }
        val path = settings.trustStorePath ?: return null
        val ks = loadKeyStore(path, settings.trustStorePassword, settings.trustStoreType)
        val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
        tmf.init(ks)
        return tmf.trustManagers
    }

    private fun loadKeyStore(
        path: String,
        password: String?,
        type: String,
    ): KeyStore {
        val ks = KeyStore.getInstance(type)
        FileInputStream(path).use { input ->
            ks.load(input, password?.toCharArray())
        }
        return ks
    }

    private object TrustAllX509TrustManager : X509TrustManager {
        override fun checkClientTrusted(
            chain: Array<out X509Certificate>?,
            authType: String?,
        ) {}

        override fun checkServerTrusted(
            chain: Array<out X509Certificate>?,
            authType: String?,
        ) {}

        override fun getAcceptedIssuers(): Array<X509Certificate> = emptyArray()
    }
}
