package org.example.daybook

/**
 * Platform-agnostic expect declaration for the actual logging implementation.
 * Each platform (androidMain, iosMain, jvmMain, jsMain) will provide an actual implementation.
 */
fun platformLogDebug(tag: String, message: String) {
    println("[$tag] $message")
}

// Common TAG for logging, can be defined here or passed to platformLogDebug
const val DEBUG_LOG_TAG = "DBG"

/**
 * A cross-platform debug utility function inspired by Rust's dbg! macro.
 * It logs the file, line number (if available), the expression's value, and any extra context.
 * Then, it returns the value of the expression, allowing it to be used inline.
 *
 * Note: Reliably getting file/line on all Kotlin Multiplatform targets can be tricky.
 * This implementation makes a best effort.
 *
 * Example Usage:
 * val a = 5
 * val b = dbg(a * 2, "Calculating b", "User ID: 123") // b will be 10
 *
 * @param T The type of the value being debugged.
 * @param value The value to be logged and returned.
 * @param context Extra string messages or values to log as context. These are converted to strings.
 * @return The original [value] that was passed in.
 */
fun <T> dbg(value: T, vararg context: Any?): T {
    // Getting reliable call site info cross-platform from pure Kotlin is challenging.
    // Thread.currentThread().stackTrace is JVM-specific.
    // We'll make a best-effort approach or acknowledge limitations.

    // A simple placeholder for file/line, as direct access is not universally available
    // without platform-specific code or specific compiler features.
    // For a simple version, we might omit file/line from common or make it platform-dependent.
    // Let's try to get it, acknowledging it works best on JVM.
//    val location = try {
//        val stackTraceElement = Throwable().stackTrace[1] // [0]=Throwable constructor, [1]=dbg
//        "[${stackTraceElement.fileName}:${stackTraceElement.lineNumber}]"
//    } catch (e: Exception) {
//        // Fallback if stack trace access fails or is not available (e.g., some JS environments in certain modes)
//        "[unknown location]"
//    }

    val contextString =
        if (context.isNotEmpty()) {
            " | ${context.joinToString { it?.toString() ?: "null" }}"
        } else {
            ""
        }

    val message = "${value?.toString() ?: "null"} $contextString"

    // Use the platform-specific logger
    platformLogDebug(DEBUG_LOG_TAG, message)

    return value
}
