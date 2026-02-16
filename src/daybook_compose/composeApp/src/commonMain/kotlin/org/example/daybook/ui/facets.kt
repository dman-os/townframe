package org.example.daybook.ui

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import org.example.daybook.uniffi.types.Blob
import org.example.daybook.uniffi.types.FacetKey
import org.example.daybook.uniffi.types.ImageMetadata
import org.example.daybook.uniffi.types.Note
import org.example.daybook.uniffi.types.WellKnownFacet

@PublishedApi
internal val facetJsonCodec = Json {
    ignoreUnknownKeys = true
    isLenient = true
}

fun encodeJsonString(value: String): String = facetJsonCodec.encodeToString(value)

fun decodeJsonString(value: String): Result<String> =
    runCatching { facetJsonCodec.decodeFromString<String>(value) }

fun decodeJsonStringOrRaw(value: String): String = decodeJsonString(value).getOrDefault(value)

fun encodeWellKnownFacet(facet: WellKnownFacet): String =
    when (facet) {
        is WellKnownFacet.Dmeta -> facetJsonCodec.encodeToString(facet.v1)
        is WellKnownFacet.RefGeneric -> facetJsonCodec.encodeToString(facet.v1)
        is WellKnownFacet.LabelGeneric -> facetJsonCodec.encodeToString(facet.v1)
        is WellKnownFacet.PseudoLabel -> facetJsonCodec.encodeToString(facet.v1)
        is WellKnownFacet.TitleGeneric -> facetJsonCodec.encodeToString(facet.v1)
        is WellKnownFacet.PathGeneric -> facetJsonCodec.encodeToString(facet.v1)
        is WellKnownFacet.Pending -> facetJsonCodec.encodeToString(facet.v1)
        is WellKnownFacet.Note -> facetJsonCodec.encodeToString(facet.v1)
        is WellKnownFacet.Blob -> facetJsonCodec.encodeToString(facet.v1)
        is WellKnownFacet.ImageMetadata -> facetJsonCodec.encodeToString(facet.v1)
        is WellKnownFacet.OcrResult -> facetJsonCodec.encodeToString(facet.v1)
        is WellKnownFacet.Embedding -> facetJsonCodec.encodeToString(facet.v1)
    }

@Suppress("UNCHECKED_CAST")
inline fun <reified T : WellKnownFacet> decodeWellKnownFacet(value: String): Result<T> =
    runCatching {
        val facetValue: WellKnownFacet =
            when (T::class) {
                WellKnownFacet.Dmeta::class ->
                    WellKnownFacet.Dmeta(facetJsonCodec.decodeFromString(value))
                WellKnownFacet.RefGeneric::class ->
                    WellKnownFacet.RefGeneric(facetJsonCodec.decodeFromString(value))
                WellKnownFacet.LabelGeneric::class ->
                    WellKnownFacet.LabelGeneric(facetJsonCodec.decodeFromString(value))
                WellKnownFacet.PseudoLabel::class ->
                    WellKnownFacet.PseudoLabel(facetJsonCodec.decodeFromString(value))
                WellKnownFacet.TitleGeneric::class ->
                    WellKnownFacet.TitleGeneric(facetJsonCodec.decodeFromString(value))
                WellKnownFacet.PathGeneric::class ->
                    WellKnownFacet.PathGeneric(facetJsonCodec.decodeFromString(value))
                WellKnownFacet.Pending::class ->
                    WellKnownFacet.Pending(facetJsonCodec.decodeFromString(value))
                WellKnownFacet.Note::class ->
                    WellKnownFacet.Note(facetJsonCodec.decodeFromString(value))
                WellKnownFacet.Blob::class ->
                    WellKnownFacet.Blob(facetJsonCodec.decodeFromString(value))
                WellKnownFacet.ImageMetadata::class ->
                    WellKnownFacet.ImageMetadata(facetJsonCodec.decodeFromString(value))
                WellKnownFacet.OcrResult::class ->
                    WellKnownFacet.OcrResult(facetJsonCodec.decodeFromString(value))
                WellKnownFacet.Embedding::class ->
                    WellKnownFacet.Embedding(facetJsonCodec.decodeFromString(value))
                else -> error("Unsupported WellKnownFacet type: ${T::class.qualifiedName}")
            }
        facetValue as T
    }

fun putWellKnownFacet(
    facets: MutableMap<FacetKey, String>,
    key: FacetKey,
    facet: WellKnownFacet,
) {
    facets[key] = encodeWellKnownFacet(facet)
}

fun buildNoteFacet(content: String, mime: String = "text/plain"): WellKnownFacet.Note =
    WellKnownFacet.Note(Note(mime = mime, content = content))

fun buildBlobFacetFromDigest(
    digest: String,
    lengthOctets: ULong,
    mime: String,
): WellKnownFacet.Blob =
    WellKnownFacet.Blob(
        Blob(
            mime = mime,
            lengthOctets = lengthOctets,
            digest = digest,
            inline = null,
            urls = listOf("db+blob:///$digest"),
        )
    )

fun buildImageMetadataFacet(
    mime: String,
    widthPx: ULong,
    heightPx: ULong,
    facetRef: String = "db+facet:///self/org.example.daybook.blob/main",
): WellKnownFacet.ImageMetadata =
    WellKnownFacet.ImageMetadata(
        ImageMetadata(
            facetRef = facetRef,
            refHeads = emptyList(),
            mime = mime,
            widthPx = widthPx,
            heightPx = heightPx,
        )
    )

fun previewFacetValue(json: String): String {
    val parsed = runCatching { facetJsonCodec.parseToJsonElement(json) }.getOrNull() ?: return json
    val primitive = parsed as? JsonPrimitive ?: return json.take(120)
    return if (primitive.isString) primitive.content.take(120) else json.take(120)
}
