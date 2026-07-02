package org.example.daybook.ui.editor

import org.example.daybook.uniffi.types.FacetKey
import org.example.daybook.uniffi.types.FacetTag
import org.example.daybook.uniffi.types.WellKnownFacetTag

private val WELL_KNOWN_FACET_DISPLAY_HINT_KEYS =
    mapOf(
        WellKnownFacetTag.DMETA to "org.example.daybook.dmeta",
        WellKnownFacetTag.REF_GENERIC to "org.example.daybook.refgeneric",
        WellKnownFacetTag.LABEL_GENERIC to "org.example.daybook.labelgeneric",
        WellKnownFacetTag.TITLE_GENERIC to "org.example.daybook.titlegeneric",
        WellKnownFacetTag.PATH_GENERIC to "org.example.daybook.pathgeneric",
        WellKnownFacetTag.PENDING to "org.example.daybook.pending",
        WellKnownFacetTag.BODY to "org.example.daybook.body",
        WellKnownFacetTag.NOTE to "org.example.daybook.note",
        WellKnownFacetTag.BLOB to "org.example.daybook.blob",
        WellKnownFacetTag.IMAGE_METADATA to "org.example.daybook.imagemetadata",
        WellKnownFacetTag.OCR_RESULT to "org.example.daybook.ocrresult",
        WellKnownFacetTag.EMBEDDING to "org.example.daybook.embedding",
    )

fun titleFacetKey(): FacetKey = FacetKey(FacetTag.WellKnown(WellKnownFacetTag.TITLE_GENERIC), "main")

fun noteFacetKey(): FacetKey = FacetKey(FacetTag.WellKnown(WellKnownFacetTag.NOTE), "main")

fun bodyFacetKey(): FacetKey = FacetKey(FacetTag.WellKnown(WellKnownFacetTag.BODY), "main")

fun dmetaFacetKey(): FacetKey = FacetKey(FacetTag.WellKnown(WellKnownFacetTag.DMETA), "main")

fun blobFacetKey(): FacetKey = FacetKey(FacetTag.WellKnown(WellKnownFacetTag.BLOB), "main")

fun imageMetadataFacetKey(): FacetKey = FacetKey(FacetTag.WellKnown(WellKnownFacetTag.IMAGE_METADATA), "main")

fun facetKeyString(key: FacetKey): String {
    val tagString =
        when (val tag = key.tag) {
            is FacetTag.WellKnown -> tag.v1.name.lowercase()
            is FacetTag.Any -> tag.v1
        }
    return if (key.id == "main") tagString else "$tagString:${key.id}"
}

fun facetTagDisplayString(tag: FacetTag): String = when (tag) {
    is FacetTag.WellKnown -> WELL_KNOWN_FACET_DISPLAY_HINT_KEYS.getValue(tag.v1)
    is FacetTag.Any -> tag.v1
}

fun facetKeyRefPathString(key: FacetKey): String =
    org.example.daybook.ui.buildSelfFacetRefUrl(key).removePrefix("db+facet:///self/")

fun facetDisplayHintKey(key: FacetKey): String = when (val tag = key.tag) {
    is FacetTag.WellKnown -> WELL_KNOWN_FACET_DISPLAY_HINT_KEYS.getValue(tag.v1)
    is FacetTag.Any -> tag.v1
}
