package dev.kdb.embed.js

import kotlinx.coroutines.await
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertTrue

class KdbBrowserTest {
    @Test
    fun putAndQuery() =
        runTest {
            val db = KdbBrowser.open("demo/users").await()
            db.put("""{"userId":"js1","name":"Test"}""").await()
            val json = db.query("SELECT _doc FROM users").await()
            assertTrue(json.contains("js1"))
            db.close().await()
        }
}
