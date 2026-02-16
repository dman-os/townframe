package org.example.daybook.ui

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.encodeToString
import org.example.daybook.uniffi.types.Blob
import org.example.daybook.uniffi.types.ImageMetadata
import org.example.daybook.uniffi.types.Note

private val facetJsonCodec = Json {
    ignoreUnknownKeys = true
    isLenient = true
}

// FIXME: suspicious. Anyone authoring facets should probably use the typed values
fun quoteJsonString(value: String): String = facetJsonCodec.encodeToString(value)

fun decodeJsonStringFacet(value: String): Result<String> =
    runCatching { facetJsonCodec.decodeFromString<String>(value) }

fun decodeNoteFacet(value: String): Result<Note> =
    runCatching { facetJsonCodec.decodeFromString<Note>(value) }

fun decodeBlobFacet(value: String): Result<Blob> =
    runCatching { facetJsonCodec.decodeFromString<Blob>(value) }

fun decodeImageMetadataFacet(value: String): Result<ImageMetadata> =
    runCatching { facetJsonCodec.decodeFromString<ImageMetadata>(value) }

// FIXME: no one should be using this except previewFacetValue
fun dequoteJson(json: String): String {
    val parsed = runCatching { facetJsonCodec.parseToJsonElement(json) }.getOrNull() ?: return json
    val parsedPrimitive = parsed as? JsonPrimitive ?: return json
    if (!parsedPrimitive.isString) {
        return json
    }
    return parsedPrimitive.content
}

fun noteFacetJson(content: String): String = facetJsonCodec.encodeToString(Note("text/plain", content))

// FIXME: remove this, they should use decodeNoteFacet directly
fun noteContentFromFacetJson(noteFacetJson: String?): String {
    if (noteFacetJson == null) {
        return ""
    }
    return decodeNoteFacet(noteFacetJson).getOrNull()?.content ?: ""
}
