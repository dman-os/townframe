package org.example.daybook.ui

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonPrimitive
import org.example.daybook.uniffi.types.Blob
import org.example.daybook.uniffi.types.Body
import org.example.daybook.uniffi.types.FacetKey
import org.example.daybook.uniffi.types.FacetTag
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
        is WellKnownFacet.Body -> facetJsonCodec.encodeToString(facet.v1)
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
                WellKnownFacet.Body::class ->
                    WellKnownFacet.Body(facetJsonCodec.decodeFromString(value))
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

fun buildBodyFacet(order: List<String>): WellKnownFacet.Body =
    WellKnownFacet.Body(Body(order = order))

fun buildSelfFacetRefUrl(key: FacetKey): String {
    val tagString =
        when (val tag = key.tag) {
            is FacetTag.WellKnown -> when (tag.v1) {
                org.example.daybook.uniffi.types.WellKnownFacetTag.DMETA -> "org.example.daybook.dmeta"
                org.example.daybook.uniffi.types.WellKnownFacetTag.REF_GENERIC -> "org.example.daybook.refgeneric"
                org.example.daybook.uniffi.types.WellKnownFacetTag.LABEL_GENERIC -> "org.example.daybook.labelgeneric"
                org.example.daybook.uniffi.types.WellKnownFacetTag.PSEUDO_LABEL -> "org.example.daybook.pseudolabel"
                org.example.daybook.uniffi.types.WellKnownFacetTag.TITLE_GENERIC -> "org.example.daybook.titlegeneric"
                org.example.daybook.uniffi.types.WellKnownFacetTag.PATH_GENERIC -> "org.example.daybook.pathgeneric"
                org.example.daybook.uniffi.types.WellKnownFacetTag.PENDING -> "org.example.daybook.pending"
                org.example.daybook.uniffi.types.WellKnownFacetTag.BODY -> "org.example.daybook.body"
                org.example.daybook.uniffi.types.WellKnownFacetTag.NOTE -> "org.example.daybook.note"
                org.example.daybook.uniffi.types.WellKnownFacetTag.BLOB -> "org.example.daybook.blob"
                org.example.daybook.uniffi.types.WellKnownFacetTag.IMAGE_METADATA -> "org.example.daybook.imagemetadata"
                org.example.daybook.uniffi.types.WellKnownFacetTag.OCR_RESULT -> "org.example.daybook.ocrresult"
                org.example.daybook.uniffi.types.WellKnownFacetTag.EMBEDDING -> "org.example.daybook.embedding"
            }
            is FacetTag.Any -> tag.v1
        }
    return "db+facet:///self/$tagString/${key.id}"
}

fun stripFacetRefFragment(url: String): String = url.substringBefore('#')

fun withFacetRefCommitHeads(url: String, heads: List<String>): String {
    val base = stripFacetRefFragment(url)
    if (heads.isEmpty()) {
        return "$base#"
    }
    return "$base#${heads.joinToString("|")}"
}

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

data class DmetaSidebarDetails(
    val createdAt: String?,
    val lastModifiedAt: String?,
)

fun parseDmetaSidebarDetails(raw: String): Result<DmetaSidebarDetails> =
    runCatching {
        val root =
            facetJsonCodec.parseToJsonElement(raw) as? JsonObject
                ?: error("dmeta must be a JSON object")
        val createdAt = root["createdAt"]?.jsonPrimitive?.contentOrNull
        val facets = root["facets"] as? JsonObject
        val lastModifiedAt =
            facets?.values
                ?.asSequence()
                ?.mapNotNull { it as? JsonObject }
                ?.flatMap { facetMeta ->
                    val updatedAt = facetMeta["updatedAt"] as? JsonArray
                    (updatedAt?.asSequence() ?: emptySequence())
                }
                ?.mapNotNull { it.jsonPrimitive.contentOrNull }
                ?.maxOrNull()
        DmetaSidebarDetails(createdAt = createdAt, lastModifiedAt = lastModifiedAt)
    }
