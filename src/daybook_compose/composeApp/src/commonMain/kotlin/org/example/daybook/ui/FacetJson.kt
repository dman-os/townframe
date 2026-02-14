package org.example.daybook.ui

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.encodeToString

private val facetJsonCodec = Json {
    ignoreUnknownKeys = true
    isLenient = true
}

fun quoteJsonString(value: String): String = Json.encodeToString(value)

fun dequoteJson(json: String): String {
    val parsed = runCatching { facetJsonCodec.parseToJsonElement(json) }.getOrNull() ?: return json
    val parsedPrimitive = parsed as? JsonPrimitive ?: return json
    if (!parsedPrimitive.isString) {
        return json
    }
    return parsedPrimitive.content
}

fun noteFacetJson(content: String): String =
    buildJsonObject {
        put("mime", JsonPrimitive("text/plain"))
        put("content", JsonPrimitive(content))
    }.toString()

fun noteContentFromFacetJson(noteFacetJson: String?): String {
    if (noteFacetJson == null) {
        return ""
    }
    val parsed = runCatching { facetJsonCodec.parseToJsonElement(noteFacetJson) }.getOrNull() ?: return ""
    return parsed.jsonObject["content"]?.jsonPrimitive?.contentOrNull ?: ""
}
