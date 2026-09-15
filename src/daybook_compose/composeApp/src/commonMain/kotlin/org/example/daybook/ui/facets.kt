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
import org.example.daybook.uniffi.types.WellKnownFacetTag

@PublishedApi
internal val facetJsonCodec = Json {
    ignoreUnknownKeys = true
    isLenient = true
}

fun encodeJsonString(value: String): String = facetJsonCodec.encodeToString(value)

fun decodeJsonString(value: String): Result<String> = runCatching { facetJsonCodec.decodeFromString<String>(value) }

fun decodeJsonStringOrRaw(value: String): String = decodeJsonString(value).getOrDefault(value)

fun encodeWellKnownFacet(facet: WellKnownFacet): String = when (facet) {
    is WellKnownFacet.Dmeta -> facetJsonCodec.encodeToString(facet.v1)
    is WellKnownFacet.RefGeneric -> facetJsonCodec.encodeToString(facet.v1)
    is WellKnownFacet.LabelGeneric -> facetJsonCodec.encodeToString(facet.v1)
    is WellKnownFacet.TitleGeneric -> facetJsonCodec.encodeToString(facet.v1)
    is WellKnownFacet.PathGeneric -> facetJsonCodec.encodeToString(facet.v1)
    is WellKnownFacet.Pending -> facetJsonCodec.encodeToString(facet.v1)
    is WellKnownFacet.Body -> facetJsonCodec.encodeToString(facet.v1)
    is WellKnownFacet.Note -> facetJsonCodec.encodeToString(facet.v1)
    is WellKnownFacet.Blob -> facetJsonCodec.encodeToString(facet.v1)
    is WellKnownFacet.BlobPin -> facetJsonCodec.encodeToString(facet.v1)
    is WellKnownFacet.ImageMetadata -> facetJsonCodec.encodeToString(facet.v1)
    is WellKnownFacet.OcrResult -> facetJsonCodec.encodeToString(facet.v1)
    is WellKnownFacet.Embedding -> facetJsonCodec.encodeToString(facet.v1)
}

@Suppress("UNCHECKED_CAST", "CyclomaticComplexMethod", "ComplexMethod")
inline fun <reified T : WellKnownFacet> decodeWellKnownFacet(value: String): Result<T> = runCatching {
    val facetValue: WellKnownFacet =
        when (T::class) {
            WellKnownFacet.Dmeta::class ->
                WellKnownFacet.Dmeta(facetJsonCodec.decodeFromString(value))

            WellKnownFacet.RefGeneric::class ->
                WellKnownFacet.RefGeneric(facetJsonCodec.decodeFromString(value))

            WellKnownFacet.LabelGeneric::class ->
                WellKnownFacet.LabelGeneric(facetJsonCodec.decodeFromString(value))

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

            WellKnownFacet.BlobPin::class ->
                WellKnownFacet.BlobPin(facetJsonCodec.decodeFromString(value))

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

fun putWellKnownFacet(facets: MutableMap<FacetKey, String>, key: FacetKey, facet: WellKnownFacet) {
    facets[key] = encodeWellKnownFacet(facet)
}

fun buildNoteFacet(content: String, mime: String = "text/plain"): WellKnownFacet.Note =
    WellKnownFacet.Note(Note(mime = mime, content = content))

fun buildBodyFacet(order: List<String>): WellKnownFacet.Body = WellKnownFacet.Body(Body(order = order))

// Canonical tag strings are a fixed data table rather than logic: one entry per WellKnownFacetTag.
private val wellKnownFacetTagCanonicalStrings: Map<WellKnownFacetTag, String> = mapOf(
    WellKnownFacetTag.DMETA to "org.example.daybook.dmeta",
    WellKnownFacetTag.REF_GENERIC to "org.example.daybook.refGeneric",
    WellKnownFacetTag.LABEL_GENERIC to "org.example.daybook.labelGeneric",
    WellKnownFacetTag.TITLE_GENERIC to "org.example.daybook.titleGeneric",
    WellKnownFacetTag.PATH_GENERIC to "org.example.daybook.pathGeneric",
    WellKnownFacetTag.PENDING to "org.example.daybook.pending",
    WellKnownFacetTag.BODY to "org.example.daybook.body",
    WellKnownFacetTag.NOTE to "org.example.daybook.note",
    WellKnownFacetTag.BLOB to "org.example.daybook.blob",
    WellKnownFacetTag.BLOB_PIN to "org.example.daybook.blobPin",
    WellKnownFacetTag.IMAGE_METADATA to "org.example.daybook.imageMetadata",
    WellKnownFacetTag.OCR_RESULT to "org.example.daybook.ocrResult",
    WellKnownFacetTag.EMBEDDING to "org.example.daybook.embedding",
    WellKnownFacetTag.PLUG_MANIFEST to "org.example.daybook.plugManifest",
    WellKnownFacetTag.PLUGS_CONFIG to "org.example.daybook.plugsConfig",
    WellKnownFacetTag.BRANCH to "org.example.daybook.branch",
    WellKnownFacetTag.BRANCHES to "org.example.daybook.branches",
)

fun wellKnownFacetTagCanonicalString(tag: WellKnownFacetTag): String = wellKnownFacetTagCanonicalStrings.getValue(tag)

fun buildSelfFacetRefUrl(key: FacetKey): String {
    val tagString =
        when (val tag = key.tag) {
            is FacetTag.WellKnown -> wellKnownFacetTagCanonicalString(tag.v1)
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

fun buildBlobFacetFromDigest(digest: String, lengthOctets: ULong, mime: String): WellKnownFacet.Blob =
    WellKnownFacet.Blob(
        Blob(
            mime = mime,
            lengthOctets = lengthOctets,
            digest = digest,
            inline = null,
            urls = listOf("db+blob:///$digest"),
        ),
    )

fun buildImageMetadataFacet(
    mime: String,
    widthPx: ULong,
    heightPx: ULong,
    facetRef: String = "db+facet:///self/org.example.daybook.blob/main",
): WellKnownFacet.ImageMetadata = WellKnownFacet.ImageMetadata(
    ImageMetadata(
        facetRef = facetRef,
        refHeads = emptyList(),
        mime = mime,
        widthPx = widthPx,
        heightPx = heightPx,
    ),
)

fun previewFacetValue(json: String): String {
    val parsed = runCatching { facetJsonCodec.parseToJsonElement(json) }.getOrNull() ?: return json
    val primitive = parsed as? JsonPrimitive ?: return json.take(120)
    return if (primitive.isString) primitive.content.take(120) else json.take(120)
}

data class DmetaSidebarDetails(val createdAt: String?, val lastModifiedAt: String?)

fun parseDmetaSidebarDetails(raw: String): Result<DmetaSidebarDetails> = runCatching {
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
