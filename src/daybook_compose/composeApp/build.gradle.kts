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
    
    listOf(
        iosX64(),
        iosArm64(),
        iosSimulatorArm64()
    ).forEach { iosTarget ->
        iosTarget.binaries.framework {
            baseName = "ComposeApp"
            isStatic = true
        }
    }
    
    jvm("desktop")
    
    @OptIn(ExperimentalWasmDsl::class)
    wasmJs {
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
    }
    
    sourceSets {
        val desktopMain by getting
        
        androidMain.dependencies {
            implementation(compose.preview)
            implementation(libs.androidx.activity.compose)
            implementation(libs.androidx.lifecycle.viewmodel)
            implementation(libs.androidx.lifecycle.runtimeCompose)
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
            implementation(libs.jna)
        }
        commonTest.dependencies {
            implementation(libs.kotlin.test)
        }
        desktopMain.dependencies {
            implementation(libs.skikoLinuxX64)
            implementation(compose.desktop.currentOs)
            implementation(libs.kotlinx.coroutinesSwing)
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

// Debug variant: build Rust in debug mode
tasks.register("buildRustAndroidDebug") {
    group = "build"
    description = "Build Rust daybook_core (debug) for Android ABIs and copy into jniLibs"

    doLast {
        val repoRoot = rootProject.rootDir.parentFile!!.parentFile!!

        rustAndroidTargets.values.toSet().forEach { target ->
            project.exec {
                workingDir = repoRoot
                commandLine("cargo", "build", "-p", "daybook_core", "--target", target)
                environment(System.getenv())
            }
        }

        rustAndroidTargets.forEach { (abi, target) ->
            val soFile = File(repoRoot, "target/$target/debug/libdaybook_core.so")
            if (!soFile.exists()) {
                throw GradleException("Expected native library not found: ${soFile.absolutePath}")
            }
            val destDir = File(project.projectDir, "src/androidMain/jniLibs/$abi")
            destDir.mkdirs()
            project.copy {
                from(soFile)
                into(destDir)
            }
        }
    }
}

// Release variant: build Rust in release mode
tasks.register("buildRustAndroidRelease") {
    group = "build"
    description = "Build Rust daybook_core (release) for Android ABIs and copy into jniLibs"

    doLast {
        val repoRoot = rootProject.rootDir.parentFile!!.parentFile!!

        rustAndroidTargets.values.toSet().forEach { target ->
            project.exec {
                workingDir = repoRoot
                commandLine("cargo", "build", "-p", "daybook_core", "--release", "--target", target)
                environment(System.getenv())
            }
        }

        rustAndroidTargets.forEach { (abi, target) ->
            val soFile = File(repoRoot, "target/$target/release/libdaybook_core.so")
            if (!soFile.exists()) {
                throw GradleException("Expected native library not found: ${soFile.absolutePath}")
            }
            val destDir = File(project.projectDir, "src/androidMain/jniLibs/$abi")
            destDir.mkdirs()
            project.copy {
                from(soFile)
                into(destDir)
            }
        }
    }
}

// Wire tasks to Android variants
// tasks.matching { it.name == "preDebugBuild" }.configureEach {
//     dependsOn("buildRustAndroidDebug")
// }
// tasks.matching { it.name == "preReleaseBuild" }.configureEach {
//     dependsOn("buildRustAndroidRelease")
// }

