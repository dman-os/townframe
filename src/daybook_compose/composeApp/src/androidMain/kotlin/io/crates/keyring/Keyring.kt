package io.crates.keyring;

import android.content.Context

// NOTE: read https://github.com/open-source-cooperative/android-native-keyring-store
// for more details
class Keyring {
    companion object {
        init {
            System.loadLibrary("daybook_ffi")
        }
        external fun initializeNdkContext(context: Context);
    }
}
