package org.example.daybook.layouts

import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import org.example.daybook.DaybookContentType
import org.example.daybook.Routes
import org.example.daybook.navigation.DaybookNavigationState

@Composable
fun ExpandedLayout(
    modifier: Modifier = Modifier,
    navState: DaybookNavigationState,
    extraAction: (() -> Unit)? = null,
    contentType: DaybookContentType,
    onShowCloneShare: () -> Unit = {},
) {
    Routes(
        modifier = modifier,
        contentType = contentType,
        extraAction = extraAction,
        onShowCloneShare = onShowCloneShare,
        chrome = navState.currentChromeSpec(onBack = {
            if (navState.backStack.size > 1) {
                navState.pop()
            }
        }),
        navState = navState,
    )
}
