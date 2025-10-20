import org.jetbrains.compose.desktop.application.dsl.TargetFormat
import org.jetbrains.kotlin.gradle.ExperimentalKotlinGradlePluginApi
import org.jetbrains.kotlin.gradle.ExperimentalWasmDsl
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.targets.js.webpack.KotlinWebpackConfig
import java.io.File

plugins {
    alias(libs.plugins.kotlinMultiplatform)
    alias(libs.plugins.androidApplication)
    alias(libs.plugins.composeMultiplatform)
    alias(libs.plugins.composeCompiler)
    alias(libs.plugins.composeHotReload)
//    alias(libs.plugins.kotlinAndroid)
}

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
        }
        commonMain.dependencies {
            implementation(compose.runtime)
            implementation(compose.foundation)
            implementation(compose.material3)
            implementation(compose.ui)
            implementation(compose.components.resources)
            implementation(compose.components.uiToolingPreview)
            implementation(libs.androidx.lifecycle.viewmodel)
            implementation(libs.androidx.lifecycle.viewmodel.compose)
            implementation(libs.androidx.navigation.compose)
            implementation(libs.kotlinx.coroutinesCore)
        }
        commonTest.dependencies {
            implementation(libs.kotlin.test)
        }
        desktopMain.dependencies {
            implementation(libs.skikoLinuxX64)
            implementation(compose.desktop.currentOs)
            implementation(libs.kotlinx.coroutinesSwing)
            implementation(libs.jna)
        }
    }
}

android {
    namespace = "org.example.daybook"
    compileSdk = libs.versions.android.compileSdk.get().toInt()

    defaultConfig {
        applicationId = "org.example.daybook"
        minSdk = libs.versions.android.minSdk.get().toInt()
        targetSdk = libs.versions.android.targetSdk.get().toInt()
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
}

dependencies {
    implementation(libs.calf.permissions)
    implementation(libs.androidx.lifecycle.service)
    debugImplementation(compose.uiTooling)
}

compose.desktop {
    application {
        mainClass = "org.example.daybook.MainKt"

        nativeDistributions {
            targetFormats(TargetFormat.Dmg, TargetFormat.Msi, TargetFormat.Deb)
            packageName = "org.example.daybook"
            packageVersion = "1.0.0"
        }
    }
}

// Build the Rust core for Android ABIs and copy .so into jniLibs
val rustAndroidTargets = mapOf(
    "arm64-v8a" to "aarch64-linux-android",
    "armeabi-v7a" to "armv7-linux-androideabi",
    "x86_64" to "x86_64-linux-android",
    "x86" to "i686-linux-android",
)

// Detect which ABI we're actually building for
val targetAbi = when {
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
    project.hasProperty("target.abi") -> project.findProperty("target.abi") as String
    // Default to arm64-v8a for modern devices
    else -> "arm64-v8a"
}

val targetRustTriple = rustAndroidTargets[targetAbi] ?: "aarch64-linux-android"

// Debug variant: build Rust in debug mode
tasks.register<Exec>("buildRustAndroidDebug") {
    group = "build"
    description = "Build Rust daybook_ffi (debug) for Android ABIs"
    
    commandLine("cargo", "build", "-p", "daybook_ffi", "--target", targetRustTriple)
    // Only pass essential environment variables for cargo
    // environment("PATH", System.getenv("PATH"))
    // environment("HOME", System.getenv("HOME"))
    // environment("CARGO_HOME", System.getenv("CARGO_HOME"))
    // environment("RUSTUP_HOME", System.getenv("RUSTUP_HOME"))
    // environment("RUSTUP_TOOLCHAIN", System.getenv("RUSTUP_TOOLCHAIN"))
}

// Copy task for debug variant
tasks.register<Copy>("copyRustAndroidDebug") {
    group = "build"
    description = "Copy Rust daybook_ffi (debug) to jniLibs"
    
    dependsOn("buildRustAndroidDebug")
    
    val repoRoot = rootProject.rootDir.parentFile!!.parentFile!!
    val sourceSoFile = File(repoRoot, "target/$targetRustTriple/debug/libdaybook_ffi.so")
    val destDir = File(project.projectDir, "src/androidMain/jniLibs/$targetAbi")
    val destSoFile = File(destDir, "libdaybook_ffi.so")
    
    // Only copy if source is newer than destination
    onlyIf {
        !destSoFile.exists() || sourceSoFile.lastModified() > destSoFile.lastModified()
    }
    
    from(sourceSoFile)
    into(destDir)
    
    // Declare inputs and outputs for proper up-to-date checking
    inputs.file(sourceSoFile)
    outputs.file(destSoFile)
}

// Release variant: build Rust in release mode
tasks.register<Exec>("buildRustAndroidRelease") {
    group = "build"
    description = "Build Rust daybook_ffi (release) for Android ABIs"
    
    commandLine("cargo", "build", "-p", "daybook_ffi", "--release", "--target", targetRustTriple)
}

// Copy task for release variant
tasks.register<Copy>("copyRustAndroidRelease") {
    group = "build"
    description = "Copy Rust daybook_ffi (release) to jniLibs"
    
    dependsOn("buildRustAndroidRelease")
    
    val repoRoot = rootProject.rootDir.parentFile!!.parentFile!!
    val sourceSoFile = File(repoRoot, "target/$targetRustTriple/release/libdaybook_ffi.so")
    val destDir = File(project.projectDir, "src/androidMain/jniLibs/$targetAbi")
    val destSoFile = File(destDir, "libdaybook_ffi.so")
    
    // Only copy if source is newer than destination
    onlyIf {
        !destSoFile.exists() || sourceSoFile.lastModified() > destSoFile.lastModified()
    }
    
    from(sourceSoFile)
    into(destDir)
    
    // Declare inputs and outputs for proper up-to-date checking
    inputs.file(sourceSoFile)
    outputs.file(destSoFile)
}

// Wire tasks to Android variants
tasks.matching { it.name == "preDebugBuild" }.configureEach {
    dependsOn("copyRustAndroidDebug")
}
// tasks.matching { it.name == "preReleaseBuild" }.configureEach {
//     dependsOn("buildRustAndroidRelease")
// }
