package org.example.daybook.ui

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.example.daybook.uniffi.types.WellKnownFacet

class FacetParsingTest {
    @Test
    fun `decode body facet parses order urls`() {
        val raw =
            """
            {"order":["db+facet:///self/org.example.daybook.imagemetadata/main#","db+facet:///self/org.example.daybook.note/main#abc"]}
            """.trimIndent()

        val decoded = decodeWellKnownFacet<WellKnownFacet.Body>(raw)

        assertTrue(decoded.isSuccess, decoded.exceptionOrNull()?.stackTraceToString() ?: "decode failed")
        assertEquals(
            listOf(
                "db+facet:///self/org.example.daybook.imagemetadata/main#",
                "db+facet:///self/org.example.daybook.note/main#abc",
            ),
            decoded.getOrThrow().v1.order,
        )
    }

    @Test
    fun `parse dmeta sidebar details from generated metadata payload`() {
        val raw =
            """
            {
              "createdAt":"2026-02-23T03:36:25.187859494Z",
              "facetUuids":{
                "37f9dac6-bbec-4afa-9938-3f2de43124a8":"org.example.daybook.note/main",
                "88fed038-2410-482d-9a16-7e07025a2b2a":"org.example.daybook.imagemetadata/main",
                "c6e73c65-7df5-4347-beba-2025644cd85a":"org.example.daybook.titlegeneric/main",
                "e11f2202-1eff-4d65-a8d8-d97e306d584e":"org.example.daybook.body/main",
                "fca7891a-a2ae-49a8-86c9-d7f1d0692969":"org.example.daybook.blob/main"
              },
              "facets":{
                "org.example.daybook.blob/main":{"createdAt":"2026-02-23T03:36:25.187859494Z","updatedAt":["2026-02-23T03:36:25.187859494Z"],"uuid":["fca7891a-a2ae-49a8-86c9-d7f1d0692969"]},
                "org.example.daybook.body/main":{"createdAt":"2026-02-23T03:36:36Z","updatedAt":["2026-02-23T04:24:26Z"],"uuid":["e11f2202-1eff-4d65-a8d8-d97e306d584e"]},
                "org.example.daybook.imagemetadata/main":{"createdAt":"2026-02-23T03:36:25.187859494Z","updatedAt":["2026-02-23T03:36:25.187859494Z"],"uuid":["88fed038-2410-482d-9a16-7e07025a2b2a"]},
                "org.example.daybook.note/main":{"createdAt":"2026-02-23T03:36:36Z","updatedAt":["2026-02-23T04:24:37Z"],"uuid":["37f9dac6-bbec-4afa-9938-3f2de43124a8"]},
                "org.example.daybook.titlegeneric/main":{"createdAt":"2026-02-23T03:59:43Z","updatedAt":["2026-02-23T03:59:43Z"],"uuid":["c6e73c65-7df5-4347-beba-2025644cd85a"]}
              },
              "id":"2ErkecXNEtWXiSDtmbqzexRxroaD",
              "updatedAt":["2026-02-23T04:24:37Z"]
            }
            """.trimIndent()

        val parsed = parseDmetaSidebarDetails(raw)

        assertTrue(parsed.isSuccess, parsed.exceptionOrNull()?.stackTraceToString() ?: "parse failed")
        assertEquals("2026-02-23T03:36:25.187859494Z", parsed.getOrThrow().createdAt)
        assertEquals("2026-02-23T04:24:37Z", parsed.getOrThrow().lastModifiedAt)
    }
}
