package dev.kdb.server

import dev.kdb.auth.connectionContextFor as authConnectionContextFor
import dev.kdb.stream.WireConnection

public fun connectionContextFor(conn: WireConnection) = authConnectionContextFor(conn)
