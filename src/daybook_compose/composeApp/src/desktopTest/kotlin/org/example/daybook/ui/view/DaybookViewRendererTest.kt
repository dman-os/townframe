package org.example.daybook.ui.view

import androidx.compose.ui.test.ExperimentalTestApi
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.onAllNodesWithText
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.v2.runComposeUiTest
import kotlin.test.Test
import kotlin.test.assertEquals
import org.example.daybook.theme.DaybookTheme
import org.example.daybook.theme.ThemeConfig
import org.example.daybook.uniffi.types.AmountNodeV1
import org.example.daybook.uniffi.types.BadgeNodeV1
import org.example.daybook.uniffi.types.BadgeToneV1
import org.example.daybook.uniffi.types.ButtonNodeV1
import org.example.daybook.uniffi.types.CardNodeV1
import org.example.daybook.uniffi.types.EmitViewActionV1
import org.example.daybook.uniffi.types.MarkdownNodeV1
import org.example.daybook.uniffi.types.ViewActionV1
import org.example.daybook.uniffi.types.ViewEventBindingV1
import org.example.daybook.uniffi.types.ViewEventKindV1
import org.example.daybook.uniffi.types.ViewNodeKindV1
import org.example.daybook.uniffi.types.ViewNodeV1
import org.example.daybook.uniffi.types.ViewSpec
import org.example.daybook.uniffi.types.ViewSpecV1

@OptIn(ExperimentalTestApi::class)
class DaybookViewRendererTest {
    @Test
    fun staticSpec_rendersSemanticNodesAndDispatchesButtonEvent() = runComposeUiTest {
        val events = mutableListOf<DaybookViewEvent>()

        setContent {
            DaybookTheme(themeConfig = ThemeConfig.Light) {
                DaybookView(spec = staticSpec(), onEvent = events::add)
            }
        }

        onNodeWithTag(DaybookViewSemantics.Root).assertIsDisplayed()
        onNodeWithTag(DaybookViewSemantics.node("root")).assertIsDisplayed()
        onNodeWithTag(DaybookViewSemantics.kind("card")).assertIsDisplayed()
        onNodeWithText("Claim summary").assertIsDisplayed()
        waitForMarkdownText("Markdown smoke line")
        onNodeWithTag(DaybookViewSemantics.button("approve")).assertIsDisplayed().performClick()

        runOnIdle {
            assertEquals(listOf("claim.approve"), events.mapNotNull(DaybookViewEvent::emitName))
        }
    }
}

private fun staticSpec(): ViewSpec =
    ViewSpec.V1(
        ViewSpecV1(
            root =
            ViewNodeV1(
                id = "root",
                kind =
                ViewNodeKindV1.Card(
                    CardNodeV1(
                        title = "Claim summary",
                        children =
                        listOf(
                            ViewNodeV1(
                                id = "markdown",
                                kind = ViewNodeKindV1.Markdown(MarkdownNodeV1("Markdown smoke line with **emphasis**.")),
                                events = emptyList(),
                            ),
                            ViewNodeV1(
                                id = "status",
                                kind = ViewNodeKindV1.Badge(BadgeNodeV1(label = "Ready", tone = BadgeToneV1.SUCCESS)),
                                events = emptyList(),
                            ),
                            ViewNodeV1(
                                id = "total",
                                kind = ViewNodeKindV1.Amount(AmountNodeV1(decimal = "12.34", commodity = "USD")),
                                events = emptyList(),
                            ),
                            ViewNodeV1(
                                id = "approve",
                                kind = ViewNodeKindV1.Button(ButtonNodeV1("Approve")),
                                events =
                                listOf(
                                    ViewEventBindingV1(
                                        event = ViewEventKindV1.CLICK,
                                        action =
                                        ViewActionV1.Emit(
                                            EmitViewActionV1(name = "claim.approve", payload = "{}"),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                events = emptyList(),
            ),
        ),
    )

@OptIn(ExperimentalTestApi::class)
private fun androidx.compose.ui.test.ComposeUiTest.waitForMarkdownText(text: String) {
    waitUntil(timeoutMillis = 5_000) {
        onAllNodesWithText(text, substring = true).fetchSemanticsNodes().isNotEmpty()
    }
    onNodeWithText(text, substring = true).assertIsDisplayed()
}
