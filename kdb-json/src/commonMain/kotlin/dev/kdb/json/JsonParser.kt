package dev.kdb.json

import dev.kdb.error.JsonPathException

internal class JsonParser(private val s: String) {
    private var i = 0

    fun parseValue(): JsonValue {
        skipWs()
        val v =
            when (peek()) {
                '{' -> parseObject()
                '[' -> parseArray()
                '"' -> JsonValue.JString(parseStringLit())
                't' -> {
                    expect("true")
                    JsonValue.JBool(true)
                }
                'f' -> {
                    expect("false")
                    JsonValue.JBool(false)
                }
                'n' -> {
                    expect("null")
                    JsonValue.JNull
                }
                '-', in '0'..'9' -> parseNumber()
                else -> throw JsonPathException("invalid JSON at $i", "\$")
            }
        skipWs()
        if (i < s.length) throw JsonPathException("trailing data", "\$")
        return v
    }

    fun parseValueFragment(): JsonValue {
        skipWs()
        return when (peek()) {
            '{' -> parseObject()
            '[' -> parseArray()
            '"' -> JsonValue.JString(parseStringLit())
            't' -> {
                expect("true")
                JsonValue.JBool(true)
            }
            'f' -> {
                expect("false")
                JsonValue.JBool(false)
            }
            'n' -> {
                expect("null")
                JsonValue.JNull
            }
            '-', in '0'..'9' -> parseNumber()
            else -> throw JsonPathException("invalid JSON at $i", "\$")
        }
    }

    private fun parseObject(): JsonValue.JObject {
        expect('{')
        val m = LinkedHashMap<String, JsonValue>()
        skipWs()
        if (peek() == '}') {
            i++
            return JsonValue.JObject(m)
        }
        while (true) {
            skipWs()
            if (peek() != '"') throw JsonPathException("expected string key", "\$")
            val key = parseStringLit()
            skipWs()
            expect(':')
            skipWs()
            m[key] = parseValueFragment()
            skipWs()
            when (peek()) {
                ',' -> i++
                '}' -> {
                    i++
                    return JsonValue.JObject(m)
                }
                else -> throw JsonPathException("expected , or }", "\$")
            }
        }
    }

    private fun parseArray(): JsonValue.JArray {
        expect('[')
        skipWs()
        val list = mutableListOf<JsonValue>()
        if (peek() == ']') {
            i++
            return JsonValue.JArray(list)
        }
        while (true) {
            skipWs()
            list.add(parseValueFragment())
            skipWs()
            when (peek()) {
                ',' -> i++
                ']' -> {
                    i++
                    return JsonValue.JArray(list)
                }
                else -> throw JsonPathException("expected , or ]", "\$")
            }
        }
    }

    private fun parseNumber(): JsonValue {
        val start = i
        if (peek() == '-') i++
        if (peek() == '0') {
            i++
        } else {
            requireDigit()
            while (peek() in '0'..'9') i++
        }
        var isFloat = false
        if (peek() == '.') {
            isFloat = true
            i++
            requireDigit()
            while (peek() in '0'..'9') i++
        }
        if (peek() == 'e' || peek() == 'E') {
            isFloat = true
            i++
            if (peek() == '+' || peek() == '-') i++
            requireDigit()
            while (peek() in '0'..'9') i++
        }
        val txt = s.substring(start, i)
        if (isFloat || txt.contains('e') || txt.contains('E') || txt.contains('.')) {
            val d =
                txt.toDoubleOrNull()
                    ?: throw JsonPathException("bad number", "\$")
            if (!d.isFinite()) throw JsonPathException("non-finite number", "\$")
            return JsonValue.JNumber(d)
        }
        val long =
            txt.toLongOrNull()
                ?: return JsonValue.JNumber(txt.toDouble())
        return JsonValue.JInt(long)
    }

    private fun parseStringLit(): String {
        expect('"')
        val sb = StringBuilder()
        while (i < s.length) {
            val c = s[i++]
            when (c) {
                '"' -> return sb.toString()
                '\\' -> {
                    if (i >= s.length) throw JsonPathException("bad escape", "\$")
                    when (val e = s[i++]) {
                        '"' -> sb.append('"')
                        '\\' -> sb.append('\\')
                        '/' -> sb.append('/')
                        'b' -> sb.append('\b')
                        'f' -> sb.append('\u000C')
                        'n' -> sb.append('\n')
                        'r' -> sb.append('\r')
                        't' -> sb.append('\t')
                        'u' -> sb.append(parseHex4())
                        else -> throw JsonPathException("bad escape", "\$")
                    }
                }
                else -> sb.append(c)
            }
        }
        throw JsonPathException("unclosed string", "\$")
    }

    private fun parseHex4(): Char {
        var v = 0
        repeat(4) {
            if (i >= s.length) throw JsonPathException("bad \\u", "\$")
            val ch = s[i++]
            val d =
                when (ch) {
                    in '0'..'9' -> ch.code - '0'.code
                    in 'a'..'f' -> 10 + ch.code - 'a'.code
                    in 'A'..'F' -> 10 + ch.code - 'A'.code
                    else -> throw JsonPathException("bad \\u", "\$")
                }
            v = v * 16 + d
        }
        return v.toChar()
    }

    private fun skipWs() {
        while (i < s.length && s[i].code in listOf(0x20, 0x0A, 0x0D, 0x09)) i++
    }

    private fun peek(): Char = if (i < s.length) s[i] else '\u0000'

    private fun expect(ch: Char) {
        if (i >= s.length || s[i] != ch) throw JsonPathException("expected $ch", "\$")
        i++
    }

    private fun expect(lit: String) {
        if (!s.startsWith(lit, i)) throw JsonPathException("expected $lit", "\$")
        i += lit.length
    }

    private fun requireDigit() {
        if (peek() !in '0'..'9') throw JsonPathException("digit needed", "\$")
    }
}
