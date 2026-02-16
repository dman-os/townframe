package org.example.daybook.ui.editor

import org.example.daybook.uniffi.types.FacetKey
import org.example.daybook.uniffi.types.FacetTag
import org.example.daybook.uniffi.types.WellKnownFacetTag

fun titleFacetKey(): FacetKey =
    FacetKey(FacetTag.WellKnown(WellKnownFacetTag.TITLE_GENERIC), "main")

fun noteFacetKey(): FacetKey =
    FacetKey(FacetTag.WellKnown(WellKnownFacetTag.NOTE), "main")

fun blobFacetKey(): FacetKey =
    FacetKey(FacetTag.WellKnown(WellKnownFacetTag.BLOB), "main")

fun imageMetadataFacetKey(): FacetKey =
    FacetKey(FacetTag.WellKnown(WellKnownFacetTag.IMAGE_METADATA), "main")
