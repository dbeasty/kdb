package dev.kdb.peersync

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.ATTACHMENT_CHUNK_BYTES
import dev.kdb.document.ATTACHMENT_HASH_SCHEME_CHUNKED_1
import dev.kdb.document.AttachmentHasher
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.document.chunkLeafHash
import dev.kdb.storage.mem.InMemoryStorageAdapter
import dev.kdb.stream.InMemoryWireTransport
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.WireBlobFetchEntry
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.runBlocking
import java.security.MessageDigest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Peer sync v2 with attachment blobs (go/kdb/peersync/attachments.go): Go's golden frames, a
 * Kotlin host answering BLOB_FETCH byte for byte as Go's does, and Kotlin-to-Kotlin syncs that move
 * multi-chunk blobs both ways, refuse a chunk failing its proof, and resume after an interruption.
 */
class SyncV2AttachmentsTest {
    private val wire = defaultWireCodec()

    // --- Go's golden frames (go/kdb/peersync/kotlin_golden_test.go) -------------------------

    private val syncGolden =
        listOf(
            "ca0000002600010011270000017b226b696e64223a2273796e6348656c6c6f222c2273796e6348656c6c6f223a7b226e6f64654964223a226e6f64652d61222c2270726f746f636f6c223a322c226361706162696c6974696573223a5b226272616e63686573222c2274616773222c227a7374642d7061636b222c226174746163686d656e7473225d2c226e616d65737061636573223a5b226170702f64617461222c22216170702f78225d2c2275736572223a2270656572222c22746f6b656e223a2274306b227d7d",
            "680100002700010011270000017b226b696e64223a2273796e6348656c6c6f41636b222c2273796e6348656c6c6f41636b223a7b226163636570746564223a747275652c226e6f64654964223a226e6f64652d62222c2270726f746f636f6c223a322c226361706162696c6974696573223a5b226272616e63686573222c226174746163686d656e7473225d2c2272656673223a5b7b226e616d657370616365223a226170702f64617461222c226272616e63686573223a7b226d61696e223a2233636535643662636438363833333935333830363036633861656365323965616136643935663665363334626534326462626434353935363632316563353634227d2c2274616773223a7b227631223a2230313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031227d2c22616363657373223a2270756c6c227d5d7d7d",
            "750000002700010011270000017b226b696e64223a2273796e6348656c6c6f41636b222c2273796e6348656c6c6f41636b223a7b226163636570746564223a66616c73652c226e6f64654964223a226e6f64652d62222c2270726f746f636f6c223a322c22726561736f6e223a226e6f7065227d7d",
            "4d0000002800010012270000017b226b696e64223a227265667352657175657374222c227265667352657175657374223a7b226e616d65737061636573223a5b226170702f64617461225d7d7d",
            "aa0000002900010012270000017b226b696e64223a2272656673526573756c74222c2272656673526573756c74223a7b2272656673223a5b7b226e616d657370616365223a226170702f64617461222c226272616e63686573223a7b226d61696e223a2230313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031227d7d5d7d7d",
            "f90000002a00010013270000017b226b696e64223a22666574636852657175657374222c22666574636852657175657374223a7b226e616d657370616365223a226170702f64617461222c2277616e7473223a5b2233636535643662636438363833333935333830363036633861656365323965616136643935663665363334626534326462626434353935363632316563353634225d2c226861766573223a5b2230313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031225d2c226d61784279746573223a343139343330347d7d",
            "840200002b00010013270000017b226b696e64223a227061636b50616765222c227061636b50616765223a7b226e616d657370616365223a226170702f64617461222c22636f6d6d6974735061796c6f6164223a2241514141414938424141434e4177454b415145424151454241514542415145424151454241514542415145424151454241514542415145424151454241676b49595842774c32526864474544447741414141414141414142414141414141414141414945425544697a2b36315141594142513841414141414141414141774141414141414141414542676f444145674244322b5747662b4c68744152744330417a302f4a5a50384343544e37496d6c6b496a6f694e6d59354e6a45355a6d59744f4749344e69316b4d4445784c5749304d6d51744d44426a5a6a526d597a6b324e475a6d49697769626949364d583045626745506235595a2f347547304247304c51445054386c6b2f774950433238384869704e546f2b61477a7864667038614b774d4a4358426f623352764c6e42755a77514541514141414155504277634842776348427763484277634842776348427763484277634842776348427763484277634842776347425141414d4141414141414142776b4a615731685a3255766347356e4253514244322b5747662b4c68744152744330417a302f4a5a503843447741414141414141414149414141414141414141416b48447749434167494341674943416749434167494341674943416749434167494341674943416749434167494343516b475a3239735a4756754367344243516b4a43516b4a43516b4a43516b4a43516b4a43516b4a43516b4a43516b4a43516b4a43516b4a43516b3d222c22646f6e65223a747275657d7d",
            "3b0300002c00010014270000017b226b696e64223a22726566557064617465222c22726566557064617465223a7b226e616d657370616365223a226170702f64617461222c226b696e64223a226272616e6368222c22726566223a226d61696e222c226f6c64486578223a2230313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031222c226e6577486578223a2233636535643662636438363833333935333830363036633861656365323965616136643935663665363334626534326462626434353935363632316563353634222c22636f6d6d6974735061796c6f6164223a2241514141414938424141434e4177454b415145424151454241514542415145424151454241514542415145424151454241514542415145424151454241676b49595842774c32526864474544447741414141414141414142414141414141414141414945425544697a2b36315141594142513841414141414141414141774141414141414141414542676f444145674244322b5747662b4c68744152744330417a302f4a5a50384343544e37496d6c6b496a6f694e6d59354e6a45355a6d59744f4749344e69316b4d4445784c5749304d6d51744d44426a5a6a526d597a6b324e475a6d49697769626949364d583045626745506235595a2f347547304247304c51445054386c6b2f774950433238384869704e546f2b61477a7864667038614b774d4a4358426f623352764c6e42755a77514541514141414155504277634842776348427763484277634842776348427763484277634842776348427763484277634842776347425141414d4141414141414142776b4a615731685a3255766347356e4253514244322b5747662b4c68744152744330417a302f4a5a503843447741414141414141414149414141414141414141416b48447749434167494341674943416749434167494341674943416749434167494341674943416749434167494343516b475a3239735a4756754367344243516b4a43516b4a43516b4a43516b4a43516b4a43516b4a43516b4a43516b4a43516b4a43516b4a43516b3d222c226d6f7265223a747275657d7d",
            "450100002d00010014270000017b226b696e64223a2272656655706461746541636b222c2272656655706461746541636b223a7b226e616d657370616365223a226170702f64617461222c226b696e64223a226272616e6368222c22726566223a226d61696e222c226f7574636f6d65223a22636f6e666c696374222c2268656164486578223a2230313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031222c2273746f726564223a312c22636f6e666c6963744279746573223a2265794a30636d46756332466a64476c76626b6c6b496a6f6965434973496d4a686332564959584e6f496a6f6959534973496e5268636d646c64456868633267694f694a6949697769593239755a6d78705933527a496a70625858303d227d7d",
            "620000002500010015270000017b226b696e64223a22706565724572726f72222c22706565724572726f72223a7b226e616d657370616365223a22222c22636f6465223a22554e535550504f52544544222c226d657373616765223a226e6f227d7d",
        )

    /** Each Go BLOB_FETCH_RESULT frame's length and SHA-256. */
    private val blobGolden =
        listOf(
            2621728 to "cdcdca2b753a88122500887dba0820046c9314c7d98e4310c1f9bbfd12d38dcf",
            1048869 to "59ed88a2d317077f63a780101e055b5cb1fe9c4ce8a296e36acc342e0cda900d",
        )

    private fun hex(s: String): ByteArray = ByteArray(s.length / 2) { s.substring(it * 2, it * 2 + 2).toInt(16).toByte() }

    private fun toHex(b: ByteArray): String = b.joinToString("") { (it.toInt() and 0xff).toString(16).padStart(2, '0') }

    private fun sha256(b: ByteArray): String = toHex(MessageDigest.getInstance("SHA-256").digest(b))

    private fun repeatHash(b: Int) = KdbHash(ByteArray(32) { b.toByte() })

    private val goldenDoc = KdbUuid.fromString("6f9619ff-8b86-d011-b42d-00cf4fc964ff")
    private val goldenAtt = KdbUuid.fromString("0b6f3c1e-2a4d-4e8f-9a1b-3c5d7e9f1a2b")

    @Test
    fun goSyncFramesDecodeAndEncodeToTheSameBytes() {
        for ((i, g) in syncGolden.withIndex()) {
            val msg = SyncV2Codec.decode(hex(g))
            assertEquals(g, toHex(SyncV2Codec.encode(msg)), "frame $i ($msg)")
        }
        val hello = SyncV2Codec.decode(hex(syncGolden[0])) as SyncV2Message.Hello
        assertEquals(listOf("app/data", "!app/x"), hello.namespaces)
        assertEquals("t0k", hello.token)
        val ack = SyncV2Codec.decode(hex(syncGolden[1])) as SyncV2Message.HelloAck
        assertEquals(SyncV2.ACCESS_PULL, ack.refs.single().access)
        // The commit Go built - a write, an attachment put and delete, an attachment root - decodes to
        // the hash Go named it by, and re-encodes to the same payload (the frame matched above).
        val page = SyncV2Codec.decode(hex(syncGolden[6])) as SyncV2Message.PackPage
        val c = page.commits.single()
        assertEquals(ack.refs.single().branches.getValue("main"), c.hash.toHex())
        assertEquals(repeatHash(9), c.attachmentTreeHash)
        val put = c.operations.filterIsInstance<KdbOp.AttachmentPut>().single()
        assertEquals("photo.png", put.name)
        assertEquals(3L shl 20, put.size)
        assertEquals(1, c.operations.filterIsInstance<KdbOp.AttachmentDelete>().size)
        val ref = SyncV2Codec.decode(hex(syncGolden[8])) as SyncV2Message.RefUpdateAck
        assertTrue(ref.conflictBytes!!.decodeToString().startsWith("{\"transactionId\""))
        val err = SyncV2Codec.decode(hex(syncGolden[9])) as SyncV2Message.PeerError
        assertEquals(PeerErrorCodes.UNSUPPORTED, err.code)
    }

    @Test
    fun zstdPackPagesRoundTrip() {
        val page = SyncV2Codec.decode(hex(syncGolden[6])) as SyncV2Message.PackPage
        val back = SyncV2Codec.decode(SyncV2Codec.encode(page.copy(zstd = true))) as SyncV2Message.PackPage
        assertTrue(back.zstd)
        assertEquals(page.commits.single().hash, back.commits.single().hash)
    }

    /** Go's goldenBlob: two whole chunks and a short third. */
    private fun goldenBlob(): ByteArray = ByteArray(5 shl 19) { (it * 7 + (it shr 12)).toByte() }

    @Test
    fun kotlinHostAnswersBlobFetchAsGoDoes() =
        runBlocking {
            val ns = "app/data"
            val node = Node(ns)
            val c = node.attach(goldenDoc, goldenAtt, "blob.bin", goldenBlob())
            val session = SyncV2HostSession(wire, node.namespace, "host", dev.kdb.auth.AllowAllAuth, dev.kdb.auth.ConnectionContext.EMPTY)
            session.handleFrame(SyncV2Codec.encode(SyncV2Message.Hello(1, "peer", 2, SyncV2.CAPABILITIES, listOf(ns))))
            val hash = node.blobs.only()
            val entry = WireBlobFetchEntry(goldenDoc, goldenAtt, c.hash.toHex(), hash)
            val requests =
                listOf(
                    WireMessage.BlobFetch(header(20), ns, listOf(entry), 0),
                    WireMessage.BlobFetch(
                        header(21), ns,
                        listOf(entry.copy(offset = ATTACHMENT_CHUNK_BYTES.toLong(), maxBytes = 1), entry.copy(offset = 5)), 0,
                    ),
                )
            for ((i, req) in requests.withIndex()) {
                val frame = session.handleFrame(wire.encode(req))
                assertEquals(blobGolden[i], frame.size to sha256(frame), "BLOB_FETCH_RESULT $i")
            }
            session.close()
        }

    private fun header(cid: Int) = WireHeader(WireMessageType.BLOB_FETCH, KDB_WIRE_PROTOCOL_VERSION, cid, 0)

    // --- Kotlin to Kotlin -------------------------------------------------------------------

    @Test
    fun pullFetchesAMultiChunkBlobBeforeStoringItsCommit() =
        runBlocking {
            val ns = "app/pull-blob"
            val remote = Node(ns)
            val data = goldenBlob()
            remote.attach(KdbUuid.random(), KdbUuid.random(), "a.bin", data)
            val local = Node(ns)
            remote.host("pull-blob").use {
                val session = local.connect("pull-blob")
                session.pullMissing()
                assertEquals(remote.dag.head(), local.dag.head())
                assertContentEquals(data, local.blobs.bytes(remote.blobs.only()))
            }
        }

    @Test
    fun pushUploadsTheBlobBeforeTheRefUpdate() =
        runBlocking {
            val ns = "app/push-blob"
            val remote = Node(ns)
            val local = Node(ns)
            val data = goldenBlob()
            local.attach(KdbUuid.random(), KdbUuid.random(), "a.bin", data)
            remote.host("push-blob").use {
                val result = local.connect("push-blob").syncBidirectional()
                assertEquals(1 + 1, result.pushedCommits) // the document's commit and the attachment's
                assertEquals(local.dag.head(), remote.dag.head())
                assertContentEquals(data, remote.blobs.bytes(local.blobs.only()))
                // A second push of the same blob in a new commit costs one Expect round trip, no bytes.
                val puts = remote.blobs.puts
                local.attach(KdbUuid.random(), KdbUuid.random(), "b.bin", data)
                local.connect("push-blob").syncBidirectional()
                assertEquals(local.dag.head(), remote.dag.head())
                assertEquals(puts, remote.blobs.puts)
            }
        }

    @Test
    fun aChunkFailingItsProofStoresNeitherTheBlobNorTheCommit() =
        runBlocking {
            val ns = "app/corrupt"
            val remote = Node(ns)
            val before = remote.dag.head()
            remote.attach(KdbUuid.random(), KdbUuid.random(), "a.bin", goldenBlob())
            val hash = remote.blobs.only()
            remote.blobs.lieAt = ATTACHMENT_CHUNK_BYTES.toLong() + 5 // the host serves chunk 1 flipped
            val local = Node(ns)
            remote.host("corrupt").use {
                val e = assertFailsWith<BlobIntegrityException> { local.connect("corrupt").pullMissing() }
                assertTrue("chunk 1" in e.message!!, e.message)
                assertFalse(local.blobs.has(hash))
                assertFalse(local.dag.hasCommit(remote.dag.head()))
                assertEquals(before, local.dag.head())
            }
        }

    @Test
    fun anInterruptedPullResumesWithoutRefetchingWhatItStored() =
        runBlocking {
            val ns = "app/resume"
            val remote = Node(ns)
            val first = ByteArray(3 shl 20) { (it * 13).toByte() }
            val second = ByteArray(2 shl 20) { (it * 17).toByte() }
            remote.attach(KdbUuid.random(), KdbUuid.random(), "a.bin", first)
            val h1 = remote.blobs.only()
            remote.attach(KdbUuid.random(), KdbUuid.random(), "b.bin", second)
            val h2 = remote.blobs.hashes().single { it != h1 }
            remote.blobs.failOpen = h2 // the transfer dies on the second blob
            val local = Node(ns)
            remote.host("resume").use {
                assertFailsWith<RemotePeerException> { local.connect("resume", pageBytes = 1).pullMissing() }
                assertTrue(local.blobs.has(h1))
                assertFalse(local.blobs.has(h2))
                remote.blobs.failOpen = null
                val opens = remote.blobs.opens.toMap()
                local.connect("resume", pageBytes = 1).pullMissing()
                assertEquals(remote.dag.head(), local.dag.head())
                assertContentEquals(second, local.blobs.bytes(h2))
                assertEquals(opens[h1], remote.blobs.opens[h1], "the stored blob was fetched again")
            }
        }

    @Test
    fun aPeerWithoutAttachmentsIsRefusedCommitsCarryingThem() =
        runBlocking {
            val ns = "app/no-attach"
            val remote = Node(ns)
            remote.attach(KdbUuid.random(), KdbUuid.random(), "a.bin", byteArrayOf(1, 2, 3))
            val local = Node(ns)
            remote.host("no-attach").use {
                val e = assertFailsWith<RemotePeerException> { local.connect("no-attach", blobs = false).pullMissing() }
                assertEquals(PeerErrorCodes.UNSUPPORTED, e.errorCode)
            }
        }

    @Test
    fun aV1OnlyRequestStillWorks() =
        runBlocking {
            val ns = "app/v1"
            val remote = Node(ns)
            remote.attach(KdbUuid.random(), KdbUuid.random(), "a.bin", byteArrayOf(1))
            val local = Node(ns)
            remote.host("v1").use {
                val client = peerSyncClient(wire, InMemoryWireTransport(), local.dag, local.storage)
                val s = client.connect(PeerClientConfig(ns, "local", "memory://v1", protocolV2 = false))
                s.pullMissing()
                assertEquals(remote.dag.head(), local.dag.head())
                client.disconnect()
            }
        }

    @Test
    fun namespacePatterns() {
        assertEquals(listOf("app/data"), selectNamespaces(listOf("app/*"), listOf("app/data", "other")))
        assertEquals(listOf("a/b/c"), selectNamespaces(listOf("**", "!other"), listOf("a/b/c", "other")))
        assertTrue(selectNamespaces(listOf("app/*", "!app/data"), listOf("app/data")).isEmpty())
    }

    // --- fixtures ---------------------------------------------------------------------------

    private inner class Node(
        val ns: String,
    ) {
        val dag: CommitDag = inMemoryCommitDag(ns)
        val storage = InMemoryStorageAdapter()
        val blobs = MemBlobStore()
        val namespace = PeerSyncNamespace(ns, dag, storage, blobs)

        /** Writes a document and, in a second commit, attaches [data] to it. */
        suspend fun attach(
            doc: KdbUuid,
            att: KdbUuid,
            name: String,
            data: ByteArray,
        ): KdbCommit {
            val stored = blobs.put(source(data))
            val d = KdbDocument(doc, """{"name":"$name"}""")
            storage.putDocument(ns, d)
            var parent = dag.head()
            val tree = storage.commitTree(ns, dag.getCommitOrThrow(parent).documentTreeHash)
            parent = dag.appendCommit(tx(parent, KdbOp.Write(doc, d.json)), parent, tree, null).hash
            val put = KdbOp.AttachmentPut(doc, att, name, ATTACHMENT_HASH_SCHEME_CHUNKED_1, stored.hash, stored.size, "application/octet-stream")
            return dag.appendCommit(tx(parent, put), parent, tree, null)
        }

        private fun tx(
            parent: KdbHash,
            op: KdbOp,
        ) = KdbTransaction(KdbUuid.random(), parent, listOf(op), KdbTimestamp.now(), KdbUuid.random())

        suspend fun host(hub: String): AutoCloseable {
            val host = peerSyncHost(wire, dag, storage)
            host.start(PeerHostConfig(ns, "host-$hub", hub, blobs = blobs))
            return AutoCloseable { runBlocking { host.stop() } }
        }

        suspend fun connect(
            hub: String,
            blobs: Boolean = true,
            pageBytes: Int = 0,
        ): PeerSession {
            val client = peerSyncClient(wire, InMemoryWireTransport(), dag, storage)
            val s = client.connect(PeerClientConfig(ns, "local", "memory://$hub", blobs = if (blobs) this.blobs else null))
            if (pageBytes > 0 && s is V2PeerSession) return s.withPageBytes(pageBytes)
            return s
        }
    }

    private fun source(data: ByteArray): suspend (ByteArray, Int, Int) -> Int {
        var pos = 0
        return { buf, off, len ->
            if (pos >= data.size) {
                -1
            } else {
                val n = minOf(len, data.size - pos)
                data.copyInto(buf, off, pos, pos + n)
                pos += n
                n
            }
        }
    }

    /** A blob store in memory. [lieAt] makes reads return that byte flipped while the stored leaves
     * stay true - a host serving damage it did not detect; [failOpen] makes opening a blob fail. */
    private class MemBlobStore : PeerBlobStore {
        private val blobs = LinkedHashMap<KdbHash, ByteArray>()
        var lieAt: Long = -1
        var failOpen: KdbHash? = null
        val opens = HashMap<KdbHash, Int>()
        var puts = 0

        fun only(): KdbHash = blobs.keys.single()

        fun hashes(): Set<KdbHash> = blobs.keys

        fun bytes(h: KdbHash): ByteArray = blobs.getValue(h)

        override suspend fun has(hash: KdbHash): Boolean = hash in blobs

        override suspend fun open(hash: KdbHash): PeerBlobReader? {
            if (hash == failOpen) throw PeerSyncException("injected failure opening ${hash.toHex()}")
            val data = blobs[hash] ?: return null
            opens[hash] = (opens[hash] ?: 0) + 1
            val lie = lieAt
            return object : PeerBlobReader {
                override val size: Long = data.size.toLong()

                override suspend fun chunkLeaf(index: Long): KdbHash {
                    val off = (index * ATTACHMENT_CHUNK_BYTES).toInt()
                    return chunkLeafHash(data, off, minOf(ATTACHMENT_CHUNK_BYTES, data.size - off))
                }

                override suspend fun readAt(
                    position: Long,
                    buffer: ByteArray,
                    offset: Int,
                    length: Int,
                ): Int {
                    if (position >= data.size) return -1
                    val n = minOf(length.toLong(), data.size - position).toInt()
                    data.copyInto(buffer, offset, position.toInt(), position.toInt() + n)
                    if (lie in position until position + n) buffer[offset + (lie - position).toInt()] = (buffer[offset + (lie - position).toInt()].toInt() xor 1).toByte()
                    return n
                }

                override suspend fun close() {}
            }
        }

        override suspend fun put(source: suspend (ByteArray, Int, Int) -> Int): PeerStoredBlob {
            val out = java.io.ByteArrayOutputStream()
            val buf = ByteArray(64 shl 10)
            while (true) {
                val n = source(buf, 0, buf.size)
                if (n < 0) break
                out.write(buf, 0, n)
            }
            val data = out.toByteArray()
            val hasher = AttachmentHasher()
            hasher.update(data)
            val hash = hasher.finish()
            puts++
            blobs[hash] = data
            return PeerStoredBlob(hash, data.size.toLong())
        }

        override suspend fun hold(hash: KdbHash): Boolean = hash in blobs
    }
}
