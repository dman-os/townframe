package org.example.daybook.ui.editor

import org.example.daybook.uniffi.types.FacetKey
import org.example.daybook.uniffi.types.FacetTag
import org.example.daybook.uniffi.types.WellKnownFacetTag

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

fun facetKeyRefPathString(key: FacetKey): String =
    org.example.daybook.ui.buildSelfFacetRefUrl(key).removePrefix("db+facet:///self/")

fun facetDisplayHintKey(key: FacetKey): String = when (val tag = key.tag) {
    is FacetTag.WellKnown -> when (tag.v1) {
        WellKnownFacetTag.DMETA -> "org.example.daybook.dmeta"
        WellKnownFacetTag.REF_GENERIC -> "org.example.daybook.refgeneric"
        WellKnownFacetTag.LABEL_GENERIC -> "org.example.daybook.labelgeneric"
        WellKnownFacetTag.TITLE_GENERIC -> "org.example.daybook.titlegeneric"
        WellKnownFacetTag.PATH_GENERIC -> "org.example.daybook.pathgeneric"
        WellKnownFacetTag.PENDING -> "org.example.daybook.pending"
        WellKnownFacetTag.BODY -> "org.example.daybook.body"
        WellKnownFacetTag.NOTE -> "org.example.daybook.note"
        WellKnownFacetTag.BLOB -> "org.example.daybook.blob"
        WellKnownFacetTag.IMAGE_METADATA -> "org.example.daybook.imagemetadata"
        WellKnownFacetTag.OCR_RESULT -> "org.example.daybook.ocrresult"
        WellKnownFacetTag.EMBEDDING -> "org.example.daybook.embedding"
    }

    is FacetTag.Any -> tag.v1
}
