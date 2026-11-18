package dev.dioxus.main

import android.os.Bundle
import androidx.activity.OnBackPressedCallback

typealias BuildConfig = com.example.mobile.BuildConfig

class MainActivity : WryActivity() {
    /**
     * Wry's own back handling navigates the WebView's history and, with none, finishes the
     * activity. The daytxt shell keeps no WebView history, so that would close the app on every
     * back gesture; the shell's callback below replaces it.
     */
    override val handleBackNavigation: Boolean = false

    /**
     * Bound by name to `Java_dev_dioxus_main_MainActivity_requestSystemBack` in libmain.so:
     * whether the daytxt shell took the back gesture (it has somewhere to go back to).
     */
    private external fun requestSystemBack(): Boolean

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        // androidx's dispatcher receives back on every API level: the predictive back gesture
        // (targetSdk 36 no longer delivers KEYCODE_BACK on API 33+) and the back key before it.
        // The shell takes the gesture when it has somewhere to go back to; otherwise the task moves
        // to the background, which is what the system does for an app with no callback.
        onBackPressedDispatcher.addCallback(
            this,
            object : OnBackPressedCallback(true) {
                override fun handleOnBackPressed() {
                    if (!requestSystemBack()) {
                        moveTaskToBack(true)
                    }
                }
            },
        )
    }
}
