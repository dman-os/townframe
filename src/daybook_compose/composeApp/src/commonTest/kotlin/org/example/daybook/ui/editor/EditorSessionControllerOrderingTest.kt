@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.ui.editor

import kotlin.test.Test
import kotlin.test.assertEquals
import org.example.daybook.ui.buildSelfFacetRefUrl
import org.example.daybook.uniffi.types.FacetKey
import org.example.daybook.uniffi.types.FacetTag
import org.example.daybook.uniffi.types.WellKnownFacetTag

class EditorSessionControllerOrderingTest {
    @Test
    fun `makePrimaryOrderUrls moves selected to front and dedupes`() {
        val a = "db+facet:///self/org.example.daybook.note/main"
        val b = "db+facet:///self/org.example.daybook.blob/main"
        val c = "db+facet:///self/org.example.daybook.imagemetadata/main"

        val result = makePrimaryOrderUrls(b, listOf(a, b, c, b, a))

        assertEquals(listOf(b, a, c), result)
    }

    @Test
    fun `insertRefAfterOrderUrls inserts after anchor and dedupes`() {
        val a = "a"
        val b = "b"
        val c = "c"
        val newRef = "n"

        val result = insertRefAfterOrderUrls(listOf(a, b, b, c), b, newRef)

        assertEquals(listOf(a, b, newRef, c), result)
    }

    @Test
    fun `insertRefAfterOrderUrls appends when anchor missing`() {
        val result = insertRefAfterOrderUrls(listOf("a", "b"), "missing", "n")
        assertEquals(listOf("a", "b", "n"), result)
    }

    @Test
    fun `orderFacetKeysForDisplay appends unmentioned keys alphabetically`() {
        val noteMain = noteKey("main")
        val noteAlt = noteKey("zeta")
        val blobMain = blobKey("main")
        val imageMain = imageKey("main")

        val displayable = listOf(blobMain, imageMain, noteAlt, noteMain)
            .sortedBy { keySortLabel(it) }
        val bodyOrder = listOf(buildSelfFacetRefUrl(imageMain), buildSelfFacetRefUrl(noteMain))

        val result = orderFacetKeysForDisplay(displayable, bodyOrder)

        assertEquals(listOf(imageMain, noteMain, blobMain, noteAlt), result)
    }

    @Test
    fun `orderFacetKeysForDisplay ignores unknown refs and dedupes`() {
        val noteMain = noteKey("main")
        val blobMain = blobKey("main")
        val displayable = listOf(blobMain, noteMain).sortedBy { keySortLabel(it) }
        val bodyOrder =
            listOf(
                "db+facet:///self/org.example.daybook.unknown/main",
                buildSelfFacetRefUrl(noteMain),
                buildSelfFacetRefUrl(noteMain),
            )

        val result = orderFacetKeysForDisplay(displayable, bodyOrder)

        assertEquals(listOf(noteMain, blobMain), result)
    }

    @Test
    fun `reorderBodyOrderPreservingUnseen moves selected among seen and preserves unseen slots`() {
        val seenA = "db+facet:///self/org.example.daybook.note/a#ha"
        val seenB = "db+facet:///self/org.example.daybook.note/b#hb"
        val seenC = "db+facet:///self/org.example.daybook.imagemetadata/main#hc"
        val unseen = "db+facet:///self/org.example.daybook.embedding/main#hu"

        val result =
            reorderBodyOrderPreservingUnseen(
                baseUrls = listOf(seenA, unseen, seenB, seenC),
                seenUrls = listOf(seenA, seenB, seenC),
                selectedRef = seenC,
                direction = -1,
            )

        assertEquals(listOf(seenA, unseen, seenC, seenB), result)
    }

    @Test
    fun `reorderBodyOrderPreservingUnseen crystallizes missing seen refs before moving`() {
        val seenA = "db+facet:///self/org.example.daybook.note/a#ha"
        val seenB = "db+facet:///self/org.example.daybook.note/b#hb"
        val seenC = "db+facet:///self/org.example.daybook.imagemetadata/main#hc"
        val unseen = "db+facet:///self/org.example.daybook.ocrresult/main#hu"

        val result =
            reorderBodyOrderPreservingUnseen(
                baseUrls = listOf(seenA, unseen),
                seenUrls = listOf(seenA, seenB, seenC),
                selectedRef = seenB,
                direction = +1,
            )

        assertEquals(listOf(seenA, unseen, seenC, seenB), result)
    }
}

private fun noteKey(id: String): FacetKey =
    FacetKey(FacetTag.WellKnown(WellKnownFacetTag.NOTE), id)

private fun blobKey(id: String): FacetKey =
    FacetKey(FacetTag.WellKnown(WellKnownFacetTag.BLOB), id)

private fun imageKey(id: String): FacetKey =
    FacetKey(FacetTag.WellKnown(WellKnownFacetTag.IMAGE_METADATA), id)

private fun keySortLabel(key: FacetKey): String {
    val tagString =
        when (val tag = key.tag) {
            is FacetTag.WellKnown -> tag.v1.name.lowercase()
            is FacetTag.Any -> tag.v1
        }
    return if (key.id == "main") tagString else "$tagString:${key.id}"
}
