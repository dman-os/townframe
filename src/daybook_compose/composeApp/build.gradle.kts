import org.jetbrains.compose.desktop.application.dsl.TargetFormat
import org.jetbrains.kotlin.gradle.ExperimentalKotlinGradlePluginApi
import org.jetbrains.kotlin.gradle.ExperimentalWasmDsl
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.targets.js.webpack.KotlinWebpackConfig
import java.io.File

plugins {
    alias(libs.plugins.kotlinMultiplatform)
    alias(libs.plugins.kotlinSerialization)
    alias(libs.plugins.androidApplication)
    alias(libs.plugins.composeMultiplatform)
    alias(libs.plugins.composeCompiler)
    alias(libs.plugins.composeHotReload)
    alias(libs.plugins.dev.detekt)
    // alias(libs.plugins.gobleyCargo)
    // alias(libs.plugins.gobleyUniffi)
    // kotlin("plugin.atomicfu") version libs.versions.kotlin
    // alias(libs.plugins.kotlinAndroid)
}

detekt {
    toolVersion = "2.0.0-alpha.1"
    config.setFrom(file("../config/detekt/detekt.yml"))
    buildUponDefaultConfig = true
}

// cargo {
//     builds {
//         jvm {
//             embedRustLibrary = (rustTarget == GobleyHost.current.rustTarget)
//         }
//     }
// }

// uniffi {
//     // Generate the bindings using library mode.
//     bindgenFromPath(rootProject.layout.projectDirectory.dir("../../daybook_ffi/"))
//     generateFromLibrary {}
//     // formatCode = true
// }

kotlin {
    androidTarget {
        @OptIn(ExperimentalKotlinGradlePluginApi::class)
        compilerOptions {
            jvmTarget.set(JvmTarget.JVM_11)
        }
    }

    /*listOf(
        iosX64(),
        iosArm64(),
        iosSimulatorArm64()
    ).forEach { iosTarget ->
        iosTarget.binaries.framework {
            baseName = "ComposeApp"
            isStatic = true
        }
    }*/

    jvm("desktop")

    @OptIn(ExperimentalWasmDsl::class)
    /* wasmJs {
        outputModuleName.set("composeApp")
        browser {
            val rootDirPath = project.rootDir.path
            val projectDirPath = project.projectDir.path
            commonWebpackConfig {
                outputFileName = "composeApp.js"
                devServer = (devServer ?: KotlinWebpackConfig.DevServer()).apply {
                    static = (static ?: mutableListOf()).apply {
                        // Serve sources to debug inside browser
                        add(rootDirPath)
                        add(projectDirPath)
                    }
                }
            }
        }
        binaries.executable()
    } */

    sourceSets {
        val desktopMain by getting

        androidMain.dependencies {
            implementation(compose.preview)
            implementation(libs.androidx.activity.compose)
            implementation(libs.androidx.lifecycle.viewmodel)
            implementation(libs.androidx.lifecycle.runtimeCompose)
            implementation("${libs.jna.get()}@aar")
            implementation(libs.camerax.core)
            implementation(libs.camerax.camera2)
            implementation(libs.camerax.lifecycle)
            implementation(libs.camerax.view)
            implementation(libs.camerax.extensions)
            implementation(libs.kotlinx.coroutinesGuava)
        }
        commonMain.dependencies {
            implementation(compose.runtime)
            implementation(compose.foundation)
            implementation(compose.material3)
            implementation(compose.materialIconsExtended)
            implementation(compose.ui)
            implementation(compose.components.resources)
            implementation(compose.components.uiToolingPreview)
            implementation(libs.androidx.lifecycle.viewmodel)
            implementation(libs.androidx.lifecycle.viewmodel.compose)
            implementation(libs.androidx.navigation.compose)
            implementation(libs.androidx.navigation.compose)
            implementation(libs.kotlinx.coroutinesCore)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.coil.compose)
            implementation(libs.coil.network.ktor)
        }
        commonTest.dependencies {
            implementation(libs.kotlin.test)

            @OptIn(org.jetbrains.compose.ExperimentalComposeLibrary::class)
            implementation(compose.uiTest)
        }
        desktopMain.dependencies {
            implementation(libs.skikoLinuxX64)
            implementation(compose.desktop.currentOs)
            implementation(libs.kotlinx.coroutinesSwing)
            implementation(libs.jna)
            // implementation(compose.foundation)
            // implementation(compose.ui)
        }
        val desktopTest by getting {
            dependencies {
                implementation(compose.desktop.uiTestJUnit4)
                implementation(compose.desktop.currentOs)
            }
        }
    }
}

android {
    namespace = "org.example.daybook"
    compileSdk =
        libs.versions.android.compileSdk
            .get()
            .toInt()

    defaultConfig {
        applicationId = "org.example.daybook"
        minSdk =
            libs.versions.android.minSdk
                .get()
                .toInt()
        targetSdk =
            libs.versions.android.targetSdk
                .get()
                .toInt()
        versionCode = 1
        versionName = "1.0"
    }
    packaging {
        resources {
            excludes += "/META-INF/{AL2.0,LGPL2.1}"
        }
        jniLibs {
            useLegacyPackaging = false
        }
    }
    buildTypes {
        getByName("release") {
            isMinifyEnabled = false
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }
    lint {
        disable += "UnsafeOptInUsageError"
    }
}

dependencies {
    implementation(libs.calf.permissions)
    implementation(libs.androidx.lifecycle.service)
    detektPlugins(libs.dev.detekt.rules.ktlint.wrapper)
    debugImplementation(compose.uiTooling)
}

compose.desktop {
    application {
        mainClass = "org.example.daybook.MainKt"

        nativeDistributions {
            targetFormats(TargetFormat.Dmg, TargetFormat.Msi, TargetFormat.Deb, TargetFormat.AppImage)
            packageName = "org.example.daybook"
            packageVersion = "1.0.0"
        }
        buildTypes {
            release {
                proguard {
                    isEnabled = false
                }
            }
        }
    }
}

// Build the Rust core for Android ABIs and copy .so into jniLibs
val rustAndroidTargets =
    mapOf(
        "arm64-v8a" to "aarch64-linux-android",
        "armeabi-v7a" to "armv7-linux-androideabi",
        "x86_64" to "x86_64-linux-android",
        "x86" to "i686-linux-android",
    )

// Detect which ABI we're actually building for
val targetAbi =
    when {
        // Check if we're building for a specific ABI (from Android Studio or command line)
        project.hasProperty("android.injected.build.abi") -> {
            val abi = project.findProperty("android.injected.build.abi") as String
            // Handle comma-separated ABIs by taking the first one
            abi.split(",").first().trim()
        }

        // Check if we're building for a specific device/emulator
        project.hasProperty("android.injected.device.abi") -> {
            val abi = project.findProperty("android.injected.device.abi") as String
            abi.split(",").first().trim()
        }

        // Check if we're building for a specific target (from gradle properties)
        project.hasProperty("target.abi") -> {
            project.findProperty("target.abi") as String
        }

        // Default to arm64-v8a for modern devices
        else -> {
            "arm64-v8a"
        }
    }

val targetRustTriple = rustAndroidTargets[targetAbi] ?: "aarch64-linux-android"
val androidApiLevel = "31"

data class AndroidRustToolchain(
    val targetTriple: String,
    val ccPath: String,
    val cxxPath: String,
    val arPath: String,
)

// Desktop Rust builds
tasks.register<Exec>("buildRustDesktopDebug") {
    group = "build"
    description = "Build Rust daybook_ffi (debug) for desktop with nokhwa"
    commandLine("cargo", "build", "-p", "daybook_ffi", "--features", "nokhwa")
}

tasks.register<Exec>("buildRustDesktopRelease") {
    group = "build"
    description = "Build Rust daybook_ffi (release) for desktop with nokhwa"
    commandLine("cargo", "build", "-p", "daybook_ffi", "--release", "--features", "nokhwa")
}

fun androidRustToolchainForAbi(targetAbi: String, ndkToolchainBinDir: String): AndroidRustToolchain? {
    val arPath = "$ndkToolchainBinDir/llvm-ar"
    return when (targetAbi) {
        "arm64-v8a" ->
            AndroidRustToolchain(
                targetTriple = "aarch64-linux-android",
                ccPath = "$ndkToolchainBinDir/aarch64-linux-android${androidApiLevel}-clang",
                cxxPath = "$ndkToolchainBinDir/aarch64-linux-android${androidApiLevel}-clang++",
                arPath = arPath
            )

        "armeabi-v7a" ->
            AndroidRustToolchain(
                targetTriple = "armv7-linux-androideabi",
                ccPath = "$ndkToolchainBinDir/armv7a-linux-androideabi${androidApiLevel}-clang",
                cxxPath = "$ndkToolchainBinDir/armv7a-linux-androideabi${androidApiLevel}-clang++",
                arPath = arPath
            )

        "x86_64" ->
            AndroidRustToolchain(
                targetTriple = "x86_64-linux-android",
                ccPath = "$ndkToolchainBinDir/x86_64-linux-android${androidApiLevel}-clang",
                cxxPath = "$ndkToolchainBinDir/x86_64-linux-android${androidApiLevel}-clang++",
                arPath = arPath
            )

        "x86" ->
            AndroidRustToolchain(
                targetTriple = "i686-linux-android",
                ccPath = "$ndkToolchainBinDir/i686-linux-android${androidApiLevel}-clang",
                cxxPath = "$ndkToolchainBinDir/i686-linux-android${androidApiLevel}-clang++",
                arPath = arPath
            )

        else -> null
    }
}

fun ndkLibCppSharedForAbi(targetAbi: String, androidNdkRoot: String): File? {
    val ndkSysrootLibDir = "$androidNdkRoot/toolchains/llvm/prebuilt/linux-x86_64/sysroot/usr/lib"
    val ndkAbiTriple =
        when (targetAbi) {
            "arm64-v8a" -> "aarch64-linux-android"
            "armeabi-v7a" -> "arm-linux-androideabi"
            "x86_64" -> "x86_64-linux-android"
            "x86" -> "i686-linux-android"
            else -> return null
        }
    return File("$ndkSysrootLibDir/$ndkAbiTriple/libc++_shared.so")
}

// Debug variant: build Rust in debug mode
tasks.register<Exec>("buildRustAndroidDebug") {
    group = "build"
    description = "Build Rust daybook_ffi (debug) for Android ABIs"

    commandLine("cargo", "build", "-p", "daybook_ffi", "--no-default-features", "--target", targetRustTriple)
    val ndkToolchainBinDir = System.getenv("ANDROID_NDK_TOOLCHAIN_BIN_DIR")
    if (!ndkToolchainBinDir.isNullOrBlank()) {
        val toolchain = androidRustToolchainForAbi(targetAbi, ndkToolchainBinDir)
        if (toolchain != null) {
            val targetEnvSuffix = toolchain.targetTriple.replace("-", "_")
            val cargoTargetSuffix = toolchain.targetTriple.uppercase().replace("-", "_")

            environment("CC_$targetEnvSuffix", toolchain.ccPath)
            environment("CXX_$targetEnvSuffix", toolchain.cxxPath)
            environment("AR_$targetEnvSuffix", toolchain.arPath)

            environment("CARGO_TARGET_${cargoTargetSuffix}_LINKER", toolchain.ccPath)
            environment("CARGO_TARGET_${cargoTargetSuffix}_AR", toolchain.arPath)
        }
    }
}

// Copy task for debug variant
tasks.register<Copy>("copyRustAndroidDebug") {
    group = "build"
    description = "Copy Rust daybook_ffi (debug) to jniLibs"

    dependsOn("buildRustAndroidDebug")

    val repoRoot = rootProject.rootDir.parentFile!!.parentFile!!
    val sourceSoFile = File(repoRoot, "target/$targetRustTriple/debug/libdaybook_ffi.so")
    val androidNdkRoot = System.getenv("ANDROID_NDK_ROOT")
    val libcxxSourceFile = if (!androidNdkRoot.isNullOrBlank()) ndkLibCppSharedForAbi(targetAbi, androidNdkRoot) else null
    val destDir = File(project.projectDir, "src/androidMain/jniLibs/$targetAbi")
    val destSoFile = File(destDir, "libdaybook_ffi.so")
    val destLibcxxFile = File(destDir, "libc++_shared.so")

    // Only copy if source is newer than destination
    onlyIf {
        val needsRustCopy = !destSoFile.exists() || sourceSoFile.lastModified() > destSoFile.lastModified()
        val needsLibcxxCopy =
            libcxxSourceFile?.let { source ->
                !destLibcxxFile.exists() || source.lastModified() > destLibcxxFile.lastModified()
            } ?: false
        needsRustCopy || needsLibcxxCopy
    }

    from(sourceSoFile)
    if (libcxxSourceFile != null && libcxxSourceFile.exists()) {
        from(libcxxSourceFile)
    }
    into(destDir)

    // Declare inputs and outputs for proper up-to-date checking
    inputs.file(sourceSoFile)
    if (libcxxSourceFile != null && libcxxSourceFile.exists()) {
        inputs.file(libcxxSourceFile)
    }
    outputs.file(destSoFile)
    outputs.file(destLibcxxFile)
}

// Release variant: build Rust in release mode
tasks.register<Exec>("buildRustAndroidRelease") {
    group = "build"
    description = "Build Rust daybook_ffi (release) for Android ABIs"

    commandLine("cargo", "build", "-p", "daybook_ffi", "--no-default-features", "--release", "--target", targetRustTriple)
    val ndkToolchainBinDir = System.getenv("ANDROID_NDK_TOOLCHAIN_BIN_DIR")
    if (!ndkToolchainBinDir.isNullOrBlank()) {
        val toolchain = androidRustToolchainForAbi(targetAbi, ndkToolchainBinDir)
        if (toolchain != null) {
            val targetEnvSuffix = toolchain.targetTriple.replace("-", "_")
            val cargoTargetSuffix = toolchain.targetTriple.uppercase().replace("-", "_")

            environment("CC_$targetEnvSuffix", toolchain.ccPath)
            environment("CXX_$targetEnvSuffix", toolchain.cxxPath)
            environment("AR_$targetEnvSuffix", toolchain.arPath)

            environment("CARGO_TARGET_${cargoTargetSuffix}_LINKER", toolchain.ccPath)
            environment("CARGO_TARGET_${cargoTargetSuffix}_AR", toolchain.arPath)
        }
    }
}

// Copy task for release variant
tasks.register<Copy>("copyRustAndroidRelease") {
    group = "build"
    description = "Copy Rust daybook_ffi (release) to jniLibs"

    dependsOn("buildRustAndroidRelease")

    val repoRoot = rootProject.rootDir.parentFile!!.parentFile!!
    val sourceSoFile = File(repoRoot, "target/$targetRustTriple/release/libdaybook_ffi.so")
    val androidNdkRoot = System.getenv("ANDROID_NDK_ROOT")
    val libcxxSourceFile = if (!androidNdkRoot.isNullOrBlank()) ndkLibCppSharedForAbi(targetAbi, androidNdkRoot) else null
    val destDir = File(project.projectDir, "src/androidMain/jniLibs/$targetAbi")
    val destSoFile = File(destDir, "libdaybook_ffi.so")
    val destLibcxxFile = File(destDir, "libc++_shared.so")

    // Only copy if source is newer than destination
    onlyIf {
        val needsRustCopy = !destSoFile.exists() || sourceSoFile.lastModified() > destSoFile.lastModified()
        val needsLibcxxCopy =
            libcxxSourceFile?.let { source ->
                !destLibcxxFile.exists() || source.lastModified() > destLibcxxFile.lastModified()
            } ?: false
        needsRustCopy || needsLibcxxCopy
    }

    from(sourceSoFile)
    if (libcxxSourceFile != null && libcxxSourceFile.exists()) {
        from(libcxxSourceFile)
    }
    into(destDir)

    // Declare inputs and outputs for proper up-to-date checking
    inputs.file(sourceSoFile)
    if (libcxxSourceFile != null && libcxxSourceFile.exists()) {
        inputs.file(libcxxSourceFile)
    }
    outputs.file(destSoFile)
    outputs.file(destLibcxxFile)
}

// Wire tasks to Android variants
tasks.matching { it.name == "preDebugBuild" }.configureEach {
    dependsOn("copyRustAndroidDebug")
}
// Only build release Rust when actually assembling/building release (not during check)
// The check task builds all variants but we only need debug Rust for testing
tasks.matching { it.name == "preReleaseBuild" }.configureEach {
    val taskNames = gradle.startParameter.taskNames
    val isCheckTask = taskNames.contains("check")
    // Skip release Rust build during check - we only need debug for testing
    if (!isCheckTask) {
        dependsOn("buildRustAndroidRelease")
    }
}

// Wire desktop tasks to Rust desktop builds with nokhwa enabled
tasks.matching {
    it.name in
        setOf(
            "compileKotlinDesktop",
            "desktopRun",
            "desktopRunHot",
        )
}.configureEach {
    dependsOn("buildRustDesktopDebug")
}

tasks.matching {
    it.name in
        setOf(
            "packageReleaseAppImage",
            "packageReleaseDeb",
            "packageReleaseDmg",
            "packageReleaseMsi",
        )
}.configureEach {
    dependsOn("buildRustDesktopRelease")
}
