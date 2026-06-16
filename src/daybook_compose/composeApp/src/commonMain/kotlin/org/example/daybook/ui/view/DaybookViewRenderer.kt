@file:Suppress("FunctionNaming")

package org.example.daybook.ui.view

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import org.example.daybook.uniffi.types.BadgeToneV1
import org.example.daybook.uniffi.types.ViewActionV1
import org.example.daybook.uniffi.types.ViewEventKindV1
import org.example.daybook.uniffi.types.ViewNodeKindV1
import org.example.daybook.uniffi.types.ViewNodeV1
import org.example.daybook.uniffi.types.ViewSpec
import com.mikepenz.markdown.m3.Markdown as MarkdownContent

@Composable
fun DaybookView(spec: ViewSpec, modifier: Modifier = Modifier, onEvent: (DaybookViewEvent) -> Unit = {}) {
    Box(modifier = modifier.testTag(DaybookViewSemantics.ROOT)) {
        when (spec) {
            is ViewSpec.V1 -> DaybookViewNode(spec.v1.root, onEvent = onEvent)
        }
    }
}

data class DaybookViewEvent(val nodeId: String, val event: ViewEventKindV1, val action: ViewActionV1)

val DaybookViewEvent.emitName: String?
    get() = (action as? ViewActionV1.Emit)?.v1?.name

@Composable
private fun DaybookViewNode(node: ViewNodeV1, modifier: Modifier = Modifier, onEvent: (DaybookViewEvent) -> Unit) {
    val kindName = node.kind.kindName
    Column(
        modifier =
        modifier
            .testTag(DaybookViewSemantics.node(node.id))
            .semantics {
                contentDescription = "$kindName ${node.id}"
            },
    ) {
        Box(modifier = Modifier.testTag(DaybookViewSemantics.kind(kindName))) {
            DaybookViewNodeBody(node = node, kind = node.kind, onEvent = onEvent)
        }
    }
}

@Composable
private fun DaybookViewNodeBody(node: ViewNodeV1, kind: ViewNodeKindV1, onEvent: (DaybookViewEvent) -> Unit) {
    when (kind) {
        is ViewNodeKindV1.Card -> CardNode(kind = kind, onEvent = onEvent)
        is ViewNodeKindV1.Section -> SectionNode(kind = kind, onEvent = onEvent)
        is ViewNodeKindV1.Text -> Text(kind.v1.text, style = MaterialTheme.typography.bodyMedium)
        is ViewNodeKindV1.Markdown -> MarkdownContent(content = kind.v1.markdown, modifier = Modifier.fillMaxWidth())
        is ViewNodeKindV1.Badge -> BadgeNode(kind)
        is ViewNodeKindV1.Amount -> AmountNode(kind)
        is ViewNodeKindV1.List -> ListNode(kind = kind, onEvent = onEvent)
        is ViewNodeKindV1.ActionGroup -> ActionGroupNode(kind = kind, onEvent = onEvent)
        is ViewNodeKindV1.Button -> ButtonNode(node = node, kind = kind, onEvent = onEvent)
    }
}

@Composable
private fun CardNode(kind: ViewNodeKindV1.Card, onEvent: (DaybookViewEvent) -> Unit) {
    Card(modifier = Modifier.fillMaxWidth()) {
        Column(
            modifier = Modifier.padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(12.dp),
        ) {
            kind.v1.title?.let { title ->
                Text(title, style = MaterialTheme.typography.titleMedium, fontWeight = FontWeight.SemiBold)
            }
            kind.v1.children.forEach { child ->
                DaybookViewNode(node = child, onEvent = onEvent)
            }
        }
    }
}

@Composable
private fun SectionNode(kind: ViewNodeKindV1.Section, onEvent: (DaybookViewEvent) -> Unit) {
    Column(
        modifier = Modifier.fillMaxWidth(),
        verticalArrangement = Arrangement.spacedBy(8.dp),
    ) {
        kind.v1.title?.let { title ->
            Text(title, style = MaterialTheme.typography.titleSmall, fontWeight = FontWeight.Medium)
            HorizontalDivider()
        }
        kind.v1.children.forEach { child ->
            DaybookViewNode(node = child, onEvent = onEvent)
        }
    }
}

@Composable
private fun BadgeNode(kind: ViewNodeKindV1.Badge) {
    Surface(
        color = badgeContainerColor(kind.v1.tone),
        contentColor = badgeContentColor(kind.v1.tone),
        shape = RoundedCornerShape(percent = 50),
    ) {
        Text(
            text = kind.v1.label,
            modifier = Modifier.padding(horizontal = 10.dp, vertical = 4.dp),
            style = MaterialTheme.typography.labelMedium,
        )
    }
}

@Composable
private fun AmountNode(kind: ViewNodeKindV1.Amount) {
    Text(
        text = "${kind.v1.decimal} ${kind.v1.commodity}",
        style = MaterialTheme.typography.bodyMedium,
        fontFamily = FontFamily.Monospace,
    )
}

@Composable
private fun ListNode(kind: ViewNodeKindV1.List, onEvent: (DaybookViewEvent) -> Unit) {
    Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
        kind.v1.items.forEach { item ->
            Row(
                horizontalArrangement = Arrangement.spacedBy(8.dp),
                verticalAlignment = Alignment.Top,
            ) {
                Text("•", style = MaterialTheme.typography.bodyMedium)
                Box(modifier = Modifier.weight(1f)) {
                    DaybookViewNode(node = item, onEvent = onEvent)
                }
            }
        }
    }
}

@Composable
private fun ActionGroupNode(kind: ViewNodeKindV1.ActionGroup, onEvent: (DaybookViewEvent) -> Unit) {
    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
        kind.v1.actions.forEach { action ->
            DaybookViewNode(node = action, onEvent = onEvent)
        }
    }
}

@Composable
private fun ButtonNode(node: ViewNodeV1, kind: ViewNodeKindV1.Button, onEvent: (DaybookViewEvent) -> Unit) {
    val clickBinding = node.events.firstOrNull { it.event == ViewEventKindV1.CLICK }
    Button(
        onClick = {
            if (clickBinding != null) {
                onEvent(DaybookViewEvent(nodeId = node.id, event = clickBinding.event, action = clickBinding.action))
            }
        },
        enabled = clickBinding != null,
        modifier = Modifier.testTag(DaybookViewSemantics.button(node.id)),
    ) {
        Text(kind.v1.label)
    }
}

@Composable
private fun badgeContainerColor(tone: BadgeToneV1): Color = when (tone) {
    BadgeToneV1.NEUTRAL -> MaterialTheme.colorScheme.surfaceVariant
    BadgeToneV1.INFO -> MaterialTheme.colorScheme.primaryContainer
    BadgeToneV1.SUCCESS -> MaterialTheme.colorScheme.tertiaryContainer
    BadgeToneV1.WARNING -> MaterialTheme.colorScheme.secondaryContainer
    BadgeToneV1.DANGER -> MaterialTheme.colorScheme.errorContainer
}

@Composable
private fun badgeContentColor(tone: BadgeToneV1): Color = when (tone) {
    BadgeToneV1.NEUTRAL -> MaterialTheme.colorScheme.onSurfaceVariant
    BadgeToneV1.INFO -> MaterialTheme.colorScheme.onPrimaryContainer
    BadgeToneV1.SUCCESS -> MaterialTheme.colorScheme.onTertiaryContainer
    BadgeToneV1.WARNING -> MaterialTheme.colorScheme.onSecondaryContainer
    BadgeToneV1.DANGER -> MaterialTheme.colorScheme.onErrorContainer
}

private val ViewNodeKindV1.kindName: String
    get() = when (this) {
        is ViewNodeKindV1.Card -> "card"
        is ViewNodeKindV1.Section -> "section"
        is ViewNodeKindV1.Text -> "text"
        is ViewNodeKindV1.Markdown -> "markdown"
        is ViewNodeKindV1.Badge -> "badge"
        is ViewNodeKindV1.Amount -> "amount"
        is ViewNodeKindV1.List -> "list"
        is ViewNodeKindV1.Button -> "button"
        is ViewNodeKindV1.ActionGroup -> "actionGroup"
    }
