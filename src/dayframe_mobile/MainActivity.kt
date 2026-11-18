package dev.dioxus.main

import android.os.Build
import android.os.Bundle
import android.window.OnBackInvokedDispatcher

typealias BuildConfig = com.example.mobile.BuildConfig

class MainActivity : WryActivity() {
    /**
     * Bound by name to `Java_dev_dioxus_main_MainActivity_requestSystemBack` in libmain.so:
     * whether the Dayframe shell took the back gesture (it has somewhere to go back to).
     */
    private external fun requestSystemBack(): Boolean

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        // From targetSdk 36 predictive back is on, and the platform no longer delivers
        // KEYCODE_BACK to the app: it invokes the activity's back callback or, with none
        // registered, closes the app. Claim the gesture for the shell, and let the system have it
        // when the shell is on its home. Before API 33 the WebView navigates its own history on
        // back, which the shell does not use, so the app closes as before.
        if (Build.VERSION.SDK_INT >= 33) {
            onBackInvokedDispatcher.registerOnBackInvokedCallback(
                OnBackInvokedDispatcher.PRIORITY_DEFAULT,
            ) {
                if (!requestSystemBack()) {
                    moveTaskToBack(true)
                }
            }
        }
    }
}
