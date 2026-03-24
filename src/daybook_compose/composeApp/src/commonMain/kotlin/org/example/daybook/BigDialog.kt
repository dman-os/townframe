package org.example.daybook

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.core.MutableTransitionState
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.animation.slideInHorizontally
import androidx.compose.animation.slideOutHorizontally
import androidx.compose.foundation.Image
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.widthIn
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.ModalBottomSheet
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.ImageBitmap
import androidx.compose.ui.graphics.RectangleShape
import androidx.compose.ui.platform.LocalClipboardManager
import androidx.compose.ui.text.AnnotatedString
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import kotlinx.coroutines.delay

sealed interface BigDialogContent {
    data object None : BigDialogContent
    data object CloneShare : BigDialogContent
}

class BigDialogState {
    var content: BigDialogContent by mutableStateOf(BigDialogContent.None)

    fun showCloneShare() {
        content = BigDialogContent.CloneShare
    }

    fun dismiss() {
        content = BigDialogContent.None
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun BigDialogHost(
    state: BigDialogState,
    narrowScreen: Boolean,
    modifier: Modifier = Modifier,
) {
    val content = state.content
    var displayedContent by remember { mutableStateOf<BigDialogContent>(BigDialogContent.None) }
    LaunchedEffect(content) {
        if (content != BigDialogContent.None) {
            displayedContent = content
        }
    }
    LaunchedEffect(narrowScreen, content) {
        if (narrowScreen && content == BigDialogContent.None) {
            displayedContent = BigDialogContent.None
        }
    }
    if (displayedContent == BigDialogContent.None) return

    val body: @Composable () -> Unit = {
        when (displayedContent) {
            BigDialogContent.CloneShare -> {
                CloneShareDialogContent(onClose = { state.dismiss() })
            }

            BigDialogContent.None -> {}
        }
    }

    if (narrowScreen) {
        if (content == BigDialogContent.None) {
            return
        }
        ModalBottomSheet(
            onDismissRequest = { state.dismiss() },
            modifier = modifier,
        ) {
            body()
        }
    } else {
        val visibilityState = remember { MutableTransitionState(false) }
        LaunchedEffect(content) {
            visibilityState.targetState = content != BigDialogContent.None
        }
        LaunchedEffect(visibilityState.isIdle, visibilityState.currentState, content) {
            if (content == BigDialogContent.None &&
                visibilityState.isIdle &&
                !visibilityState.currentState
            ) {
                displayedContent = BigDialogContent.None
            }
        }
        Box(
            modifier = modifier.fillMaxSize()
        ) {
            Box(
                modifier =
                    Modifier
                        .fillMaxSize()
                        .background(MaterialTheme.colorScheme.scrim.copy(alpha = 0.32f))
                        .clickable(onClick = { state.dismiss() })
            )
            AnimatedVisibility(
                visibleState = visibilityState,
                enter = slideInHorizontally { it } + fadeIn(),
                exit = slideOutHorizontally { it } + fadeOut(),
                modifier = Modifier.align(Alignment.CenterEnd)
            ) {
                Surface(
                    shape = RectangleShape,
                    tonalElevation = 2.dp,
                    shadowElevation = 8.dp,
                    modifier =
                        Modifier
                            .fillMaxHeight()
                            .widthIn(min = 420.dp, max = 560.dp),
                ) {
                    body()
                }
            }
        }
    }
}

@Composable
private fun CloneShareDialogContent(
    onClose: () -> Unit,
) {
    val syncRepo = LocalContainer.current.syncRepo
    var ticketUrl by remember { mutableStateOf<String?>(null) }
    var qrPngBytes by remember { mutableStateOf<ByteArray?>(null) }
    var errorMessage by remember { mutableStateOf<String?>(null) }
    var reloadKey by remember { mutableIntStateOf(0) }
    var copied by remember { mutableStateOf(false) }
    val clipboardManager = LocalClipboardManager.current

    LaunchedEffect(reloadKey) {
        copied = false
        errorMessage = null
        ticketUrl = null
        qrPngBytes = null
        try {
            val url = syncRepo.getTicketUrl()
            val qr = syncRepo.getTicketQrPng(768u)
            ticketUrl = url
            qrPngBytes = qr
        } catch (error: Throwable) {
            val details = error.message ?: error.toString()
            errorMessage = "Failed to prepare clone ticket: $details"
        }
    }

    val qrBitmap: ImageBitmap? = remember(qrPngBytes) {
        qrPngBytes?.let { decodePngImageBitmap(it) }
    }

    Column(
        modifier =
            Modifier
                .fillMaxWidth()
                .verticalScroll(rememberScrollState())
                .padding(horizontal = 20.dp, vertical = 16.dp),
        verticalArrangement = Arrangement.spacedBy(14.dp)
    ) {
        Text(
            text = "Clone This Repo",
            style = MaterialTheme.typography.headlineSmall,
        )
        Text(
            text = "Share this URL or QR code to clone from this device.",
            style = MaterialTheme.typography.bodyMedium,
            color = MaterialTheme.colorScheme.onSurfaceVariant
        )

        HorizontalDivider()

        if (errorMessage != null) {
            Text(
                text = errorMessage ?: "",
                color = MaterialTheme.colorScheme.error,
                style = MaterialTheme.typography.bodyMedium,
            )
            Button(
                onClick = { reloadKey += 1 }
            ) {
                Text("Retry")
            }
        } else if (ticketUrl == null || qrBitmap == null) {
            Column(
                modifier = Modifier.fillMaxWidth().heightIn(min = 220.dp),
                horizontalAlignment = Alignment.CenterHorizontally,
                verticalArrangement = Arrangement.Center
            ) {
                CircularProgressIndicator()
                Text(
                    "Preparing clone details…",
                    modifier = Modifier.padding(top = 12.dp),
                    style = MaterialTheme.typography.bodyMedium
                )
            }
        } else {
            Surface(
                shape = RoundedCornerShape(16.dp),
                color = MaterialTheme.colorScheme.surfaceContainerLow,
                modifier = Modifier.fillMaxWidth()
            ) {
                Box(
                    modifier = Modifier.fillMaxWidth().padding(16.dp),
                    contentAlignment = Alignment.Center
                ) {
                    Image(
                        bitmap = qrBitmap,
                        contentDescription = "Clone URL QR code",
                        modifier = Modifier.size(260.dp)
                    )
                }
            }

            Surface(
                shape = RoundedCornerShape(12.dp),
                color = MaterialTheme.colorScheme.surfaceContainerLowest,
                modifier = Modifier.fillMaxWidth()
            ) {
                Text(
                    text = ticketUrl ?: "",
                    style = MaterialTheme.typography.bodySmall,
                    maxLines = 4,
                    overflow = TextOverflow.Ellipsis,
                    fontFamily = FontFamily.Monospace,
                    modifier = Modifier.fillMaxWidth().padding(12.dp)
                )
            }

            Button(
                onClick = {
                    clipboardManager.setText(AnnotatedString(ticketUrl ?: ""))
                    copied = true
                },
                modifier = Modifier.fillMaxWidth()
            ) {
                Text("Copy URL")
            }
            if (copied) {
                Text(
                    text = "Copied.",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.primary
                )
                LaunchedEffect(ticketUrl, copied) {
                    delay(1400)
                    copied = false
                }
            }
        }

        OutlinedButton(
            onClick = onClose,
            modifier = Modifier.fillMaxWidth()
        ) {
            Text("Close")
        }
    }
}
