@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.ui.editor

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
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

    @Test
    fun `reorderBodyOrderPreservingUnseen returns null when direction is zero`() {
        val result =
            reorderBodyOrderPreservingUnseen(
                baseUrls = listOf("a"),
                seenUrls = listOf("a"),
                selectedRef = "a",
                direction = 0,
            )
        assertNull(result)
    }

    @Test
    fun `reorderBodyOrderPreservingUnseen returns null when seen urls are empty`() {
        val result =
            reorderBodyOrderPreservingUnseen(
                baseUrls = listOf("a", "b"),
                seenUrls = emptyList(),
                selectedRef = "a",
                direction = 1,
            )
        assertNull(result)
    }

    @Test
    fun `reorderBodyOrderPreservingUnseen crystallizes from empty base using seen order`() {
        val a = "db+facet:///self/org.example.daybook.note/a#ha"
        val b = "db+facet:///self/org.example.daybook.note/b#hb"

        val result =
            reorderBodyOrderPreservingUnseen(
                baseUrls = emptyList(),
                seenUrls = listOf(a, b),
                selectedRef = a,
                direction = 1,
            )

        assertEquals(listOf(b, a), result)
    }

    @Test
    fun `reorderBodyOrderPreservingUnseen returns null for single seen item boundary moves`() {
        val a = "db+facet:///self/org.example.daybook.note/a#ha"

        assertNull(
            reorderBodyOrderPreservingUnseen(
                baseUrls = listOf(a),
                seenUrls = listOf(a),
                selectedRef = a,
                direction = -1,
            )
        )
        assertNull(
            reorderBodyOrderPreservingUnseen(
                baseUrls = listOf(a),
                seenUrls = listOf(a),
                selectedRef = a,
                direction = 1,
            )
        )
    }

    @Test
    fun `reorderBodyOrderPreservingUnseen returns null when selected ref not found after canonicalization`() {
        val a = "db+facet:///self/org.example.daybook.note/a#ha"
        val b = "db+facet:///self/org.example.daybook.note/b#hb"

        val result =
            reorderBodyOrderPreservingUnseen(
                baseUrls = listOf(a, b),
                seenUrls = listOf(a, b),
                selectedRef = "db+facet:///self/org.example.daybook.note/missing#hx",
                direction = 1,
            )

        assertNull(result)
    }

    @Test
    fun `reorderBodyOrderPreservingUnseen returns null at start and end boundaries`() {
        val a = "db+facet:///self/org.example.daybook.note/a#ha"
        val b = "db+facet:///self/org.example.daybook.note/b#hb"
        val c = "db+facet:///self/org.example.daybook.note/c#hc"

        assertNull(
            reorderBodyOrderPreservingUnseen(
                baseUrls = listOf(a, b, c),
                seenUrls = listOf(a, b, c),
                selectedRef = a,
                direction = -1,
            )
        )
        assertNull(
            reorderBodyOrderPreservingUnseen(
                baseUrls = listOf(a, b, c),
                seenUrls = listOf(a, b, c),
                selectedRef = c,
                direction = 1,
            )
        )
    }

    @Test
    fun `reorderBodyOrderPreservingUnseen swaps at start and end boundaries when valid`() {
        val a = "db+facet:///self/org.example.daybook.note/a#ha"
        val b = "db+facet:///self/org.example.daybook.note/b#hb"
        val c = "db+facet:///self/org.example.daybook.note/c#hc"

        val moveStartDown =
            reorderBodyOrderPreservingUnseen(
                baseUrls = listOf(a, b, c),
                seenUrls = listOf(a, b, c),
                selectedRef = a,
                direction = 1,
            )
        val moveEndUp =
            reorderBodyOrderPreservingUnseen(
                baseUrls = listOf(a, b, c),
                seenUrls = listOf(a, b, c),
                selectedRef = c,
                direction = -1,
            )

        assertEquals(listOf(b, a, c), moveStartDown)
        assertEquals(listOf(a, c, b), moveEndUp)
    }

    @Test
    fun `reorderBodyOrderPreservingUnseen dedupes canonical urls and respects stripFacetRefFragment`() {
        val aBase = "db+facet:///self/org.example.daybook.note/a#old"
        val aSeen = "db+facet:///self/org.example.daybook.note/a#new"
        val bBase = "db+facet:///self/org.example.daybook.note/b#old"
        val bSeen = "db+facet:///self/org.example.daybook.note/b#new"
        val unseen = "db+facet:///self/org.example.daybook.embedding/main#hu"

        val result =
            reorderBodyOrderPreservingUnseen(
                baseUrls = listOf(aBase, aSeen, unseen, bBase, bSeen),
                seenUrls = listOf(aSeen, bSeen, aBase),
                selectedRef = bSeen,
                direction = -1,
            )

        assertEquals(listOf(bSeen, unseen, aSeen), result)
    }

    @Test
    fun `reorderBodyOrderPreservingUnseen appends missing seen canonicals in original seen order`() {
        val a = "db+facet:///self/org.example.daybook.note/a#ha"
        val b = "db+facet:///self/org.example.daybook.note/b#hb"
        val c = "db+facet:///self/org.example.daybook.note/c#hc"
        val x = "db+facet:///self/org.example.daybook.embedding/main#hx"

        val result =
            reorderBodyOrderPreservingUnseen(
                baseUrls = listOf(a, x),
                seenUrls = listOf(c, b, a),
                selectedRef = b,
                direction = -1,
            )

        assertEquals(listOf(a, x, b, c), result)
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
