package dev.dioxus.main

import android.app.NativeActivity
import android.os.Build
import android.os.Bundle
import android.window.OnBackInvokedDispatcher

typealias BuildConfig = com.example.mobile.BuildConfig

class MainActivity : NativeActivity() {
    /**
     * Bound to Rust by `register_back_callback` in `android_main`: whether the Dayframe shell
     * took the back gesture (it has somewhere to go back to).
     */
    private external fun requestSystemBack(): Boolean

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        // From targetSdk 36 predictive back is on, and the platform no longer delivers
        // KEYCODE_BACK to the native input queue: it invokes the activity's back callback or,
        // with none registered, closes the app. Claim the gesture for the shell, and let the
        // system have it when the shell is on its home. Before API 33 the key event still
        // reaches winit as BrowserBack.
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
