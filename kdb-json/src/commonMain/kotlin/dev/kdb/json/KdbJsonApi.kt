package dev.kdb.json

import dev.kdb.error.JsonPathException

public fun kdbJsonGet(
    json: String,
    path: JsonPath,
): JsonValue? {
    if (path.hasWildcards()) {
        throw JsonPathException("wildcards not allowed in kdbJsonGet", path.expression)
    }
    val root = JsonParser(json).parseValue()
    return navigateGet(root, path, 1)
}

public fun kdbJsonGet(
    json: String,
    path: String,
): JsonValue? = kdbJsonGet(json, JsonPath.compile(path))

public fun kdbJsonGetAll(
    json: String,
    path: JsonPath,
): List<JsonValue> {
    val root = JsonParser(json).parseValue()
    if (!path.hasWildcards()) {
        return listOfNotNull(navigateGet(root, path, 1))
    }
    val out = mutableListOf<JsonValue>()
    collectAll(root, path.segments, 1, out, path.expression)
    return out
}

public fun kdbJsonGetAll(
    json: String,
    path: String,
): List<JsonValue> = kdbJsonGetAll(json, JsonPath.compile(path))

public fun kdbJsonSet(
    json: String,
    path: JsonPath,
    value: JsonValue,
): String {
    if (path.hasWildcards()) {
        throw JsonPathException("wildcards not allowed in kdbJsonSet", path.expression)
    }
    val root = JsonParser(json).parseValue()
    val obj = root as? JsonValue.JObject ?: throw JsonPathException("root must be JSON object", path.expression)
    if (path.segments.size < 2) {
        throw JsonPathException("cannot set root", path.expression)
    }
    val updated = jsonSet(obj, path.segments, 1, value, path.expression)
    return updated.toJsonString()
}

public fun kdbJsonSet(
    json: String,
    path: String,
    value: JsonValue,
): String = kdbJsonSet(json, JsonPath.compile(path), value)

public fun kdbJsonDelete(
    json: String,
    path: JsonPath,
): String {
    if (path.hasWildcards()) {
        throw JsonPathException("wildcards not allowed in kdbJsonDelete", path.expression)
    }
    val root = JsonParser(json).parseValue()
    if (path.segments.size < 2) {
        return json
    }
    val updated = jsonDelete(root, path.segments, 1, path.expression)
    return updated.toJsonString()
}

public fun kdbJsonDelete(
    json: String,
    path: String,
): String = kdbJsonDelete(json, JsonPath.compile(path))

public fun kdbJsonMerge(
    json: String,
    patchJson: String,
): String {
    val base = JsonParser(json).parseValue()
    val patch = JsonParser(patchJson).parseValue()
    val bo = base as? JsonValue.JObject ?: throw JsonPathException("left must be JSON object", "\$")
    val po = patch as? JsonValue.JObject ?: throw JsonPathException("patch must be JSON object", "\$")
    val out = LinkedHashMap<String, JsonValue>()
    bo.fields.forEach { (k, v) -> out[k] = v }
    po.fields.forEach { (k, v) -> out[k] = v }
    return JsonValue.JObject(out).toJsonString()
}

public fun kdbJsonContains(
    json: String,
    path: JsonPath,
    value: JsonValue,
): Boolean {
    if (path.hasWildcards()) {
        throw JsonPathException("wildcards not allowed in kdbJsonContains", path.expression)
    }
    val root = JsonParser(json).parseValue()
    val target = navigateGet(root, path, 1) ?: return false
    if (target === JsonValue.JNull) {
        return false
    }
    val arr = target as? JsonValue.JArray ?: throw JsonPathException("not array", path.expression)
    return arr.elements.any { jsonDeepEquals(it, value) }
}

public fun kdbJsonContains(
    json: String,
    path: String,
    value: JsonValue,
): Boolean = kdbJsonContains(json, JsonPath.compile(path), value)

public fun kdbJsonKeys(
    json: String,
    path: JsonPath,
): List<String>? {
    if (path.hasWildcards()) {
        throw JsonPathException("wildcards not allowed in kdbJsonKeys", path.expression)
    }
    val root = JsonParser(json).parseValue()
    val target =
        if (path.segments.size == 1) {
            root
        } else {
            navigateGet(root, path, 1)
        } ?: return null
    val o = target as? JsonValue.JObject ?: throw JsonPathException("not object", path.expression)
    return o.fields.keys.toList()
}

public fun kdbJsonKeys(
    json: String,
    path: String,
): List<String>? = kdbJsonKeys(json, JsonPath.compile(path))

public fun kdbJsonType(
    json: String,
    path: JsonPath,
): String? {
    if (path.hasWildcards()) {
        throw JsonPathException("wildcards not allowed in kdbJsonType", path.expression)
    }
    val root = JsonParser(json).parseValue()
    val target =
        if (path.segments.size == 1) {
            root
        } else {
            navigateGet(root, path, 1)
        } ?: return null
    return jsonTypeName(target)
}

public fun kdbJsonType(
    json: String,
    path: String,
): String? = kdbJsonType(json, JsonPath.compile(path))

public fun kdbJsonArrayLength(
    json: String,
    path: JsonPath,
): Int? {
    if (path.hasWildcards()) {
        throw JsonPathException("wildcards not allowed in kdbJsonArrayLength", path.expression)
    }
    val root = JsonParser(json).parseValue()
    val target =
        if (path.segments.size == 1) {
            root
        } else {
            navigateGet(root, path, 1)
        } ?: return null
    if (target === JsonValue.JNull) {
        return null
    }
    val a = target as? JsonValue.JArray ?: throw JsonPathException("not array", path.expression)
    return a.elements.size
}

public fun kdbJsonArrayLength(
    json: String,
    path: String,
): Int? = kdbJsonArrayLength(json, JsonPath.compile(path))

private fun jsonTypeName(v: JsonValue): String =
    when (v) {
        is JsonValue.JString -> "string"
        is JsonValue.JNumber, is JsonValue.JInt -> "number"
        is JsonValue.JBool -> "boolean"
        JsonValue.JNull -> "null"
        is JsonValue.JObject -> "object"
        is JsonValue.JArray -> "array"
    }

private fun navigateGet(
    root: JsonValue,
    path: JsonPath,
    startIdx: Int,
): JsonValue? {
    var cur = root
    for (si in startIdx until path.segments.size) {
        when (val seg = path.segments[si]) {
            PathSeg.Root -> throw JsonPathException("internal root seg", path.expression)
            is PathSeg.Field -> {
                val o = cur as? JsonValue.JObject ?: throw JsonPathException("not object", path.expression)
                cur = o.fields[seg.name] ?: return null
            }
            is PathSeg.Idx -> {
                val a = cur as? JsonValue.JArray ?: throw JsonPathException("not array", path.expression)
                val ix = normalizeIndex(seg.index, a.elements.size, path.expression, forGet = true)
                if (ix < 0 || ix >= a.elements.size) {
                    return null
                }
                cur = a.elements[ix]
            }
            is PathSeg.WildcardElem, is PathSeg.WildcardField ->
                throw JsonPathException("wildcard in kdbJsonGet", path.expression)
        }
    }
    return cur
}

private fun normalizeIndex(
    index: Int,
    len: Int,
    expr: String,
    forGet: Boolean,
): Int {
    if (index == -1) {
        if (len == 0) {
            return -1
        }
        return len - 1
    }
    if (index < -1) {
        throw JsonPathException("bad array index", expr)
    }
    if (!forGet && index < 0) {
        throw JsonPathException("bad array index", expr)
    }
    return index
}

private fun collectAll(
    cur: JsonValue,
    segs: List<PathSeg>,
    idx: Int,
    out: MutableList<JsonValue>,
    expr: String,
) {
    if (idx == segs.size) {
        out.add(cur)
        return
    }
    when (val seg = segs[idx]) {
        PathSeg.Root -> collectAll(cur, segs, idx + 1, out, expr)
        is PathSeg.Field -> {
            val o = cur as? JsonValue.JObject ?: throw JsonPathException("not object", expr)
            val next = o.fields[seg.name] ?: return
            collectAll(next, segs, idx + 1, out, expr)
        }
        is PathSeg.Idx -> {
            val a = cur as? JsonValue.JArray ?: throw JsonPathException("not array", expr)
            val ix = normalizeIndex(seg.index, a.elements.size, expr, forGet = true)
            if (ix < 0 || ix >= a.elements.size) {
                return
            }
            collectAll(a.elements[ix], segs, idx + 1, out, expr)
        }
        is PathSeg.WildcardElem -> {
            val a = cur as? JsonValue.JArray ?: throw JsonPathException("not array", expr)
            for (e in a.elements) {
                collectAll(e, segs, idx + 1, out, expr)
            }
        }
        is PathSeg.WildcardField -> {
            val o = cur as? JsonValue.JObject ?: throw JsonPathException("not object", expr)
            for (e in o.fields.values) {
                collectAll(e, segs, idx + 1, out, expr)
            }
        }
    }
}

private fun jsonSet(
    cur: JsonValue,
    segs: List<PathSeg>,
    idx: Int,
    newVal: JsonValue,
    expr: String,
): JsonValue {
    val seg = segs[idx]
    val last = idx == segs.size - 1
    when (seg) {
        is PathSeg.Field -> {
            val obj = cur as? JsonValue.JObject ?: throw JsonPathException("not object", expr)
            val copy = LinkedHashMap(obj.fields)
            if (last) {
                copy[seg.name] = newVal
            } else {
                val old = copy[seg.name] ?: JsonValue.JObject(LinkedHashMap())
                copy[seg.name] = jsonSet(old, segs, idx + 1, newVal, expr)
            }
            return JsonValue.JObject(copy)
        }
        is PathSeg.Idx -> {
            val arr = cur as? JsonValue.JArray ?: throw JsonPathException("not array", expr)
            val list = arr.elements.toMutableList()
            val ix = normalizeIndex(seg.index, list.size, expr, forGet = false)
            while (list.size <= ix) {
                list.add(JsonValue.JNull)
            }
            if (last) {
                list[ix] = newVal
            } else {
                val old = list[ix]
                val base = if (old === JsonValue.JNull) JsonValue.JObject(LinkedHashMap()) else old
                list[ix] = jsonSet(base, segs, idx + 1, newVal, expr)
            }
            return JsonValue.JArray(list)
        }
        else -> throw JsonPathException("invalid segment for set", expr)
    }
}

private fun jsonDelete(
    cur: JsonValue,
    segs: List<PathSeg>,
    idx: Int,
    expr: String,
): JsonValue {
    val seg = segs[idx]
    val last = idx == segs.size - 1
    when (seg) {
        is PathSeg.Field -> {
            val obj = cur as? JsonValue.JObject ?: throw JsonPathException("not object", expr)
            if (last) {
                if (!obj.fields.containsKey(seg.name)) {
                    return cur
                }
                val copy = LinkedHashMap(obj.fields)
                copy.remove(seg.name)
                return JsonValue.JObject(copy)
            }
            val child = obj.fields[seg.name] ?: return cur
            val newChild = jsonDelete(child, segs, idx + 1, expr)
            val copy = LinkedHashMap(obj.fields)
            copy[seg.name] = newChild
            return JsonValue.JObject(copy)
        }
        is PathSeg.Idx -> {
            val arr = cur as? JsonValue.JArray ?: throw JsonPathException("not array", expr)
            if (last) {
                val ix = normalizeIndex(seg.index, arr.elements.size, expr, forGet = true)
                if (ix < 0 || ix >= arr.elements.size) {
                    return cur
                }
                val list = arr.elements.toMutableList()
                list.removeAt(ix)
                return JsonValue.JArray(list)
            }
            val ix = normalizeIndex(seg.index, arr.elements.size, expr, forGet = true)
            if (ix < 0 || ix >= arr.elements.size) {
                return cur
            }
            val list = arr.elements.toMutableList()
            list[ix] = jsonDelete(list[ix], segs, idx + 1, expr)
            return JsonValue.JArray(list)
        }
        else -> throw JsonPathException("invalid segment for delete", expr)
    }
}

private fun jsonDeepEquals(
    a: JsonValue,
    b: JsonValue,
): Boolean {
    if (a::class != b::class) {
        return numericEquals(a, b)
    }
    return when (a) {
        is JsonValue.JString -> a.value == (b as JsonValue.JString).value
        is JsonValue.JBool -> a.value == (b as JsonValue.JBool).value
        JsonValue.JNull -> b === JsonValue.JNull
        is JsonValue.JInt ->
            when (b) {
                is JsonValue.JInt -> a.value == b.value
                is JsonValue.JNumber -> a.value.toDouble() == b.value
                else -> false
            }
        is JsonValue.JNumber ->
            when (b) {
                is JsonValue.JNumber -> a.value == b.value
                is JsonValue.JInt -> a.value == b.value.toDouble()
                else -> false
            }
        is JsonValue.JArray ->
            a.elements.size == (b as JsonValue.JArray).elements.size &&
                a.elements.zip(b.elements).all { (x, y) -> jsonDeepEquals(x, y) }
        is JsonValue.JObject -> {
            val ob = b as JsonValue.JObject
            if (a.fields.size != ob.fields.size) {
                return false
            }
            a.fields.all { (k, v) -> ob.fields[k]?.let { jsonDeepEquals(v, it) } == true }
        }
    }
}

private fun numericEquals(
    a: JsonValue,
    b: JsonValue,
): Boolean =
    when {
        a is JsonValue.JInt && b is JsonValue.JNumber -> a.value.toDouble() == b.value
        a is JsonValue.JNumber && b is JsonValue.JInt -> a.value == b.value.toDouble()
        else -> false
    }
