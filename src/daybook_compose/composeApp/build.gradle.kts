import org.jetbrains.compose.desktop.application.dsl.TargetFormat
import org.jetbrains.kotlin.gradle.ExperimentalKotlinGradlePluginApi
import org.jetbrains.kotlin.gradle.ExperimentalWasmDsl
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.targets.js.webpack.KotlinWebpackConfig
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Exec
import org.gradle.process.CommandLineArgumentProvider
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
            implementation(libs.filekit.dialogs.compose)
        }
        commonTest.dependencies {
            implementation(libs.kotlin.test)

            @OptIn(org.jetbrains.compose.ExperimentalComposeLibrary::class)
            implementation(compose.uiTest)
        }
        desktopMain.dependencies {
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

val repoRoot = rootProject.rootDir.parentFile!!.parentFile!!

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

fun rustDesktopLibraryNameForHost(hostOs: org.gradle.internal.os.OperatingSystem): String =
    when {
        hostOs.isWindows -> "daybook_ffi.dll"
        hostOs.isMacOsX -> "libdaybook_ffi.dylib"
        else -> "libdaybook_ffi.so"
    }

fun desktopComposeAppDir(isRelease: Boolean): File {
    val buildVariantDir = if (isRelease) "main-release" else "main"
    return File(project.buildDir, "compose/binaries/$buildVariantDir/app/org.example.daybook")
}

fun registerRustAndroidCopyTask(
    taskName: String,
    buildTaskName: String,
    sourceLibPath: String,
) =
    tasks.register<Copy>(taskName) {
        group = "build"
        description = "Copy Rust daybook_ffi and libc++_shared.so to Android jniLibs"

        dependsOn(buildTaskName)

        val sourceSoFile = File(repoRoot, sourceLibPath)
        val androidNdkRoot = System.getenv("ANDROID_NDK_ROOT")
        val libcxxSourceFile =
            if (!androidNdkRoot.isNullOrBlank()) ndkLibCppSharedForAbi(targetAbi, androidNdkRoot) else null
        val destDir = File(project.projectDir, "src/androidMain/jniLibs/$targetAbi")
        val destSoFile = File(destDir, "libdaybook_ffi.so")
        val destLibcxxFile = File(destDir, "libc++_shared.so")
        
        doFirst {
            if (!sourceSoFile.exists()) {
                throw GradleException("Missing Rust library: ${sourceSoFile.absolutePath}")
            }
            if (androidNdkRoot.isNullOrBlank()) {
                throw GradleException("ANDROID_NDK_ROOT is not set; cannot locate libc++_shared.so")
            }
            if (libcxxSourceFile == null || !libcxxSourceFile.exists()) {
                throw GradleException("Missing libc++_shared.so for ABI $targetAbi")
            }
        }

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

        inputs.file(sourceSoFile)
        if (libcxxSourceFile != null && libcxxSourceFile.exists()) {
            inputs.file(libcxxSourceFile)
        }
        outputs.file(destSoFile)
        if (libcxxSourceFile != null && libcxxSourceFile.exists()) {
            outputs.file(destLibcxxFile)
        }
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

registerRustAndroidCopyTask(
    taskName = "copyRustAndroidDebug",
    buildTaskName = "buildRustAndroidDebug",
    sourceLibPath = "target/$targetRustTriple/debug/libdaybook_ffi.so",
)

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

registerRustAndroidCopyTask(
    taskName = "copyRustAndroidRelease",
    buildTaskName = "buildRustAndroidRelease",
    sourceLibPath = "target/$targetRustTriple/release/libdaybook_ffi.so",
)

val hostOsForNativePackaging = org.gradle.internal.os.OperatingSystem.current()!!
val hostArchForNativePackaging = System.getProperty("os.arch")
val resourcesDirNameForNativePackaging = when {
    hostOsForNativePackaging.isLinux && hostArchForNativePackaging in setOf("amd64", "x86_64") -> "linux-x64"
    hostOsForNativePackaging.isLinux && hostArchForNativePackaging in setOf("aarch64", "arm64") -> "linux-arm64"
    else -> "unsupported"
}

tasks.register<Copy>("copyRustDesktopDebugToComposeApp") {
    group = "build"
    description = "Copy desktop debug Rust FFI library to Compose desktop app directory"
    dependsOn("buildRustDesktopDebug")

    val sourceLibFile = File(repoRoot, "target/debug/${rustDesktopLibraryNameForHost(hostOsForNativePackaging)}")
    val destLibDir = File(desktopComposeAppDir(false), "lib/app")
    val destLibFile = File(destLibDir, sourceLibFile.name)

    doFirst {
        if (!sourceLibFile.exists()) {
            throw GradleException("Missing desktop debug Rust library: ${sourceLibFile.absolutePath}")
        }
        destLibDir.mkdirs()
    }

    from(sourceLibFile)
    into(destLibDir)

    inputs.file(sourceLibFile)
    outputs.file(destLibFile)
}

tasks.register<Copy>("copyRustDesktopReleaseToComposeApp") {
    group = "build"
    description = "Copy desktop release Rust FFI library to Compose desktop app directory"
    dependsOn("buildRustDesktopRelease")

    val sourceLibFile = File(repoRoot, "target/release/${rustDesktopLibraryNameForHost(hostOsForNativePackaging)}")
    val destLibDir = File(desktopComposeAppDir(true), "lib/app")
    val destLibFile = File(destLibDir, sourceLibFile.name)

    doFirst {
        if (!sourceLibFile.exists()) {
            throw GradleException("Missing desktop release Rust library: ${sourceLibFile.absolutePath}")
        }
        destLibDir.mkdirs()
    }

    from(sourceLibFile)
    into(destLibDir)

    inputs.file(sourceLibFile)
    outputs.file(destLibFile)
}

// Compose desktop packaging regenerates the app directory during packaging tasks.
// Ensure our Rust .so copy runs after those generation steps so it is not overwritten.
tasks.named("copyRustDesktopDebugToComposeApp").configure {
    mustRunAfter(
        "prepareAppResources",
        "unpackDefaultComposeDesktopJvmApplicationResources",
        "createRuntimeImage",
    )
}

tasks.named("copyRustDesktopReleaseToComposeApp").configure {
    mustRunAfter(
        "prepareAppResources",
        "unpackDefaultComposeDesktopJvmApplicationResources",
        "createRuntimeImage",
    )
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
    it.name in setOf("desktopRun", "desktopRunHot")
}.configureEach {
    dependsOn("buildRustDesktopDebug")
    dependsOn("copyRustDesktopDebugToComposeApp")
}

tasks.matching {
    it.name in
        setOf(
            "createDistributable",
            "packageDeb",
            "packageDmg",
            "packageMsi",
            "packageDistributionForCurrentOS",
        )
}.configureEach {
    dependsOn("buildRustDesktopDebug")
    dependsOn("copyRustDesktopDebugToComposeApp")
}

tasks.matching { it.name == "packageAppImage" }.configureEach {
    // Our custom linuxdeploy script copies the Compose app dir after this task completes.
    // Re-copy to restore libdaybook_ffi.so if Compose rewrites lib/app during packaging.
    finalizedBy("copyRustDesktopDebugToComposeApp")
}

tasks.matching {
    it.name in
        setOf(
            "packageReleaseDeb",
            "packageReleaseDmg",
            "packageReleaseMsi",
            "packageReleaseDistributionForCurrentOS",
        )
}.configureEach {
    dependsOn("buildRustDesktopRelease")
    dependsOn("copyRustDesktopReleaseToComposeApp")
    doFirst {
        val rustDesktopReleaseLib = File(
            repoRoot,
            "target/release/${rustDesktopLibraryNameForHost(hostOsForNativePackaging)}"
        )
        if (!rustDesktopReleaseLib.exists()) {
            throw GradleException(
                "Missing Rust desktop library: ${rustDesktopReleaseLib.absolutePath}. " +
                    "Build it explicitly (for example: cargo build -p daybook_ffi --release --features nokhwa)."
            )
        }
    }
}

tasks.matching { it.name == "packageReleaseAppImage" }.configureEach {
    finalizedBy("copyRustDesktopReleaseToComposeApp")
}


tasks.register<Exec>("buildNativeImageDayb") {
    group = "distribution"
    description = "Build a Linux native image for Daybook from the uber jar"

    val outputDir = file("build/compose/native/$resourcesDirNameForNativePackaging")
    val outputFile = File(outputDir, "daybook")
    val reachabilityDir = file("reachability-metadata/linux")
    val rustLibFile = File(repoRoot, "target/release/libdaybook_ffi.so")
    val nativeImageCmd = System.getenv("NATIVE_IMAGE_BIN") ?: "native-image"

    fun findUberJar(resourcesDirName: String): File {
        val jarCandidates = fileTree("build/compose/jars") {
            include("org.example.daybook-$resourcesDirName-*.jar")
        }.files.sortedBy { it.name }
        return jarCandidates.lastOrNull()
            ?: throw GradleException("Missing uber jar for $resourcesDirName in build/compose/jars")
    }

    dependsOn("packageUberJarForCurrentOS")
    dependsOn("buildRustDesktopRelease")

    inputs.dir(file("build/compose/jars"))
    inputs.file(rustLibFile)
    inputs.dir(reachabilityDir)
    outputs.file(outputFile)
    notCompatibleWithConfigurationCache("experimental native-image packaging task")
    executable = nativeImageCmd
    argumentProviders.add(CommandLineArgumentProvider {
        val jarFile = findUberJar(resourcesDirNameForNativePackaging)
        listOf(
            "--no-fallback",
            "--enable-native-access=ALL-UNNAMED",
            "--add-modules=java.desktop,jdk.unsupported",
            "--add-opens=java.desktop/sun.awt.X11=ALL-UNNAMED",
            "-H:ConfigurationFileDirectories=${reachabilityDir.absolutePath}",
            "-H:+AddAllCharsets",
            "-J-Dcompose.application.configure.swing.globals=true",
            "-J-Djava.awt.headless=false",
            "-J-Dsun.java2d.dpiaware=true",
            "-J-Dfile.encoding=UTF-8",
            "-H:+ReportExceptionStackTraces",
            "-jar",
            jarFile.absolutePath,
            "-o",
            outputFile.absolutePath,
        )
    })

    doFirst {
        if (!hostOsForNativePackaging.isLinux || resourcesDirNameForNativePackaging == "unsupported") {
            throw GradleException("buildNativeImageDayb currently supports Linux only")
        }
        if (!rustLibFile.exists()) {
            throw GradleException("Missing Rust desktop library: ${rustLibFile.absolutePath}")
        }
        if (!reachabilityDir.exists()) {
            throw GradleException("Missing reachability metadata dir: ${reachabilityDir.absolutePath}")
        }

        findUberJar(resourcesDirNameForNativePackaging)

        outputDir.mkdirs()

        val existingLibraryPath = System.getenv("LIBRARY_PATH").orEmpty()
        val joinedLibraryPath = listOf(rustLibFile.parentFile.absolutePath, existingLibraryPath)
            .filter { it.isNotBlank() }
            .joinToString(":")
        environment("LIBRARY_PATH", joinedLibraryPath)
    }

    doLast {
        val libDir = File(outputDir, "lib")
        libDir.mkdirs()
        rustLibFile.copyTo(File(libDir, rustLibFile.name), overwrite = true)

        val jarFile = findUberJar(resourcesDirNameForNativePackaging)

        val jarEntriesToExtract = when (resourcesDirNameForNativePackaging) {
            "linux-x64" -> arrayOf(
                "libskiko-linux-x64.so",
                "libskiko-linux-x64.so.sha256",
                "natives/linux_x64/libsqliteJni.so",
                "com/sun/jna/linux-x86-64/libjnidispatch.so",
            )
            "linux-arm64" -> arrayOf(
                "libskiko-linux-arm64.so",
                "libskiko-linux-arm64.so.sha256",
                "natives/linux_arm64/libsqliteJni.so",
                "com/sun/jna/linux-aarch64/libjnidispatch.so",
            )
            else -> emptyArray()
        }

        if (jarEntriesToExtract.isNotEmpty()) {
            zipTree(jarFile).matching {
                include(*jarEntriesToExtract)
            }.forEach { extracted ->
                extracted.copyTo(File(outputDir, extracted.name), overwrite = true)
            }
        }

        val nativeImageBin = System.getenv("NATIVE_IMAGE_BIN")
        val graalHome =
            nativeImageBin
                ?.let { File(it).absoluteFile.parentFile?.parentFile }
                ?: System.getenv("GRAALVM_HOME")?.let { File(it) }
        val jawtSource = graalHome?.let { File(it, "lib/libjawt.so") }
        if (jawtSource != null && jawtSource.exists()) {
            jawtSource.copyTo(File(libDir, "libjawt.so"), overwrite = true)
        }
    }
}

tasks.register<Exec>("packageLinuxAppImageAndTarballDayb") {
    group = "distribution"
    description = "Package Linux tarball and AppImage from native image artifacts"
    dependsOn("buildNativeImageDayb")
    notCompatibleWithConfigurationCache("experimental Linux packaging task")
    commandLine(
        "bash",
        "../package-for-linux-dayb.sh",
    )
}

tasks.register<Exec>("packageLinuxAppImageWithLinuxdeployDayb") {
    group = "distribution"
    description = "Package Linux tarball and AppImage via linuxdeploy"
    dependsOn("buildNativeImageDayb")
    notCompatibleWithConfigurationCache("experimental Linux packaging task using linuxdeploy")
    commandLine(
        "bash",
        "../package-for-linux-dayb-linuxdeploy.sh",
    )
}

tasks.configureEach {
    when (name) {
        "buildNativeImageDayb" -> {
            mustRunAfter("packageUberJarForCurrentOS")
            dependsOn("packageUberJarForCurrentOS")
        }
        "packageLinuxAppImageAndTarballDayb" -> {
            dependsOn("buildNativeImageDayb")
        }
    }
}
