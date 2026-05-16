package dev.kdb.inspect

import dev.kdb.wire.WireCodec
import dev.kdb.wire.WireMessage
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject

public class WireFrameInspector(
    private val codec: WireCodec,
) {
    private val prettyJson =
        Json {
            ignoreUnknownKeys = true
            prettyPrint = true
        }

    public fun dumpFrame(
        frame: ByteArray,
        pretty: Boolean = true,
    ): String {
        val header = codec.decodeHeader(frame)
        val message = codec.decode(frame)
        val line = InspectJson.wireMessageToJsonLine(message, "capture")
        return if (pretty) {
            prettyJson.encodeToString(
                buildJsonObject {
                    put(
                        "header",
                        buildJsonObject {
                            put("messageType", JsonPrimitive(header.messageType.name))
                            put("protocolVersion", JsonPrimitive(header.protocolVersion))
                            put("correlationId", JsonPrimitive(header.correlationId))
                            put("payloadLength", JsonPrimitive(header.payloadLength))
                        },
                    )
                    put("body", prettyJson.parseToJsonElement(line))
                },
            )
        } else {
            line
        }
    }

    public fun decodeMessage(frame: ByteArray): WireMessage = codec.decode(frame)
}
