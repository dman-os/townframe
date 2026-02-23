package org.example.daybook.ui.editor

import org.example.daybook.uniffi.types.FacetKey
import org.example.daybook.uniffi.types.FacetTag
import org.example.daybook.uniffi.types.WellKnownFacetTag

fun titleFacetKey(): FacetKey =
    FacetKey(FacetTag.WellKnown(WellKnownFacetTag.TITLE_GENERIC), "main")

fun noteFacetKey(): FacetKey =
    FacetKey(FacetTag.WellKnown(WellKnownFacetTag.NOTE), "main")

fun bodyFacetKey(): FacetKey =
    FacetKey(FacetTag.WellKnown(WellKnownFacetTag.BODY), "main")

fun dmetaFacetKey(): FacetKey =
    FacetKey(FacetTag.WellKnown(WellKnownFacetTag.DMETA), "main")

fun blobFacetKey(): FacetKey =
    FacetKey(FacetTag.WellKnown(WellKnownFacetTag.BLOB), "main")

fun imageMetadataFacetKey(): FacetKey =
    FacetKey(FacetTag.WellKnown(WellKnownFacetTag.IMAGE_METADATA), "main")

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
